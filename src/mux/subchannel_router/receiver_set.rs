// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::mux::demux::{
    ProtoSender, ResolvedMessageOrDisconnect, clear_deserialization_context,
    establish_deserialization_context,
};
use crate::mux::error::MuxError;
use crate::mux::protocol::SubChannelId;
use crate::mux::ipc_channel::{
    SyncOpaqueIpcReceiver, clear_ipc_receiver_deserialization_context,
    clear_ipc_sender_deserialization_context, set_ipc_receivers_for_recv, set_ipc_senders_for_recv,
};
use crate::mux::shared_memory::{clear_shmem_deserialization_context, set_shmems_for_recv};
use crate::mux::subchannel_router::select::{
    MultiReceiverSet, OpaqueSelectableSubReceiver, SelectableSubChannelReceiver,
    SelectableSubReceiver,
};
use ipc_channel::ipc::{IpcSharedMemory, OpaqueIpcSender};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use tracing::instrument;

/// Collection of [SubReceiver]s moved into a set; thus creating a common
/// (and exclusive) endpoint for receiving messages on any of the added
/// subchannels.
///
/// This type is not exposed on the crate API because of various restrictions.
/// For example, a `SubReceiverSet` cannot contain a `SubReceiver` which shares
/// an IPC channel with a `SubReceiver` not in a `SubReceiverSet`.
///
/// [SubReceiver]: struct.SubReceiver.html
#[derive(Debug)]
pub struct SubReceiverSet {
    next_id: u64,
    rxs: HashMap<u64, SelectableSubChannelReceiver>,
    ids: HashMap<SubChannelId, u64>,
    multi_receiver_set: Arc<Mutex<MultiReceiverSet>>,
    rx: Receiver<ResolvedMessageOrDisconnect>,
    tx: Sender<ResolvedMessageOrDisconnect>,
}

/// Result for readable events returned from [SubReceiverSet::select].
///
/// [SubReceiverSet::select]: struct.SubReceiverSet.html#method.select
#[derive(Debug)]
pub enum SubSelectionResult {
    /// A message received from the [`SubReceiver`] in the [`RawMessage`] form,
    /// identified by the `u64` value.
    MessageReceived(u64, RawMessage),
    /// The channel has been closed for the [SubReceiver] identified by the `u64` value.
    /// [SubReceiver]: struct.SubReceiver.html
    /// [RawMessage]: struct.RawMessage.html
    ChannelClosed(u64),
}

/// A message received on a subchannel prior to deserialisation.
#[derive(Debug)]
pub struct RawMessage {
    payload: Vec<u8>,
    senders: VecDeque<ProtoSender>,
    scid: SubChannelId,
    shmems: Vec<IpcSharedMemory>,
    ipc_senders: Vec<OpaqueIpcSender>,
    ipc_receivers: Vec<SyncOpaqueIpcReceiver>,
}

impl RawMessage {
    /// Deserialise the raw message into the inferred type.
    pub fn to<T>(self) -> Result<T, MuxError>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        establish_deserialization_context(self.senders, self.scid);
        set_shmems_for_recv(self.shmems);
        set_ipc_senders_for_recv(self.ipc_senders);
        set_ipc_receivers_for_recv(self.ipc_receivers);

        let result = postcard::from_bytes::<T>(self.payload.as_slice());

        clear_deserialization_context();
        clear_shmem_deserialization_context();
        clear_ipc_sender_deserialization_context();
        clear_ipc_receiver_deserialization_context();

        result.map_err(From::from)
    }
}

impl SubReceiverSet {
    /// Create a new empty [SubReceiverSet].
    ///
    /// SubReceivers may then be added to the set with the [add]
    /// method.
    ///
    /// [add]: #method.add
    /// [SubReceiverSet]: struct.SubReceiverSet.html
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn new() -> Result<SubReceiverSet, io::Error> {
        let (tx, rx) = mpsc::channel();
        #[allow(clippy::arc_with_non_send_sync)]
        Ok(SubReceiverSet {
            next_id: 0,
            rxs: HashMap::new(),
            ids: HashMap::new(),
            multi_receiver_set: Arc::new(Mutex::new(MultiReceiverSet::new()?)),
            rx,
            tx,
        })
    }

    /// Add and move the given [SubReceiver] into the set of subreceivers to be polled.
    ///
    /// Restrictions:
    /// * A [SubReceiver] sharing an IPC channel with another [SubReceiver]
    ///   in a SubReceiverSet cannot receive.
    /// * No two [SubReceiver]s sharing an IPC channel may belong to distinct
    ///   [SubReceiverSet]s with distinct IpcReceiverSets.
    ///
    /// Because of these restrictions, [SubReceiverSet] is not part of the crate API.
    ///
    /// [SubReceiver]: struct.SubReceiver.html
    /// [SubReceiverSet]: struct.SubReceiverSet.html
    #[instrument(level = "debug", skip(subreceiver), err(level = "debug"))]
    pub fn add<T>(&mut self, subreceiver: SelectableSubReceiver<T>) -> Result<u64, MuxError>
    where
        T: for<'x> Deserialize<'x> + Serialize,
    {
        self.add_opaque(subreceiver.to_opaque())
    }

    /// Add an [OpaqueSubReceiver] to the set of subreceivers to be polled.
    ///
    /// Note: this function is not part of the crate API and is only called by
    /// the router with a freshly constructed receiver whose sender has not been
    /// used. Consequently, no messages are present on the internal channel for
    /// the receiver and it is not necessary to return these as events from the
    /// SubReceiverSet.
    ///
    /// [OpaqueSubReceiver]: struct.OpaqueSubReceiver.html
    #[instrument(level = "debug", skip(receiver), ret, err(level = "debug"))]
    pub fn add_opaque(&mut self, receiver: OpaqueSelectableSubReceiver) -> Result<u64, MuxError> {
        // The subreceiver is associated with a MultiReceiverSet from construction.
        let incoming_multi_receiver_set = receiver.multi_receiver_set();

        // If this SubReceiverSet's MultiReceiverSet is the same as the MultiReceiverSet associated
        // with the subreceiver being added, do nothing.
        if Arc::ptr_eq(&self.multi_receiver_set, &incoming_multi_receiver_set) {
        } else
        // Otherwise, if this SubReceiverSet's MultiReceiverSet is empty, we can replace it with the
        // incoming MultiReceiverSet and register its pending IPC receiver (if any).
        if MultiReceiverSet::is_empty(&self.multi_receiver_set) {
            self.multi_receiver_set = incoming_multi_receiver_set;
            self.multi_receiver_set.lock().unwrap().register_pending()?;
        } else {
            // Check if the incoming MRS was already merged into ours (e.g. a second subreceiver
            // from the same heterogeneous channel being added to this SubReceiverSet).
            let already_merged = incoming_multi_receiver_set
                .lock()
                .unwrap()
                .is_merged_into(&self.multi_receiver_set);
            if !already_merged {
                // Merge: take the pending IPC receiver from the incoming MRS and register it in
                // ours. IpcReceiverSets cannot be merged, so we only support merging an incoming
                // MRS that has a pending (not yet registered) IPC receiver.
                let mut incoming_locked = incoming_multi_receiver_set.lock().unwrap();
                if let Some((ipc_receiver, multi_receiver)) = incoming_locked.take_pending() {
                    self.multi_receiver_set
                        .lock()
                        .unwrap()
                        .merge_receiver(ipc_receiver, &multi_receiver)?;
                    incoming_locked.set_merged_into(&self.multi_receiver_set);
                } else {
                    return Err(MuxError::InternalError(
                        "Cannot merge non-empty MultiReceiverSets".to_string(),
                    ));
                }
            }
        }

        // Modify MultiReceiver so that messages for the subchannel are sent to the set.
        let scid = receiver.sub_channel_id();
        receiver
            .demuxer()
            .lock()
            .unwrap()
            .switch_sub_channel_sender(scid, self.tx.clone())?;

        let id = self.next_id;
        self.next_id += 1;
        self.ids.insert(scid, id);
        self.rxs.insert(id, receiver.into_inner());
        Ok(id)
    }

    /// Wait for a message to be received or the channel to be closed for any of the
    /// receivers in the set. The method may return multiple events. An event may be
    /// either a message received or a channel closed event.
    #[instrument(level = "debug", ret, err(level = "debug"))]
    pub fn select(&mut self) -> Result<Vec<SubSelectionResult>, MuxError> {
        let mut results = Vec::new();
        loop {
            match self.rx.try_recv() {
                Ok(ResolvedMessageOrDisconnect::ResolvedMessage(resolved)) => {
                    let (scid, payload, senders, shmems, ipc_senders, ipc_receivers) =
                        resolved.into_parts();
                    let id = *self.ids.get(&scid).ok_or_else(|| {
                        MuxError::InternalError(format!("missing id for subchannel {scid}"))
                    })?;
                    results.push(SubSelectionResult::MessageReceived(
                        id,
                        RawMessage {
                            payload,
                            senders,
                            scid,
                            shmems,
                            ipc_senders,
                            ipc_receivers,
                        },
                    ));
                },
                Ok(ResolvedMessageOrDisconnect::Disconnect(scid)) => {
                    let id = self.ids.remove(&scid).ok_or_else(|| {
                        MuxError::InternalError(format!(
                            "missing id for disconnected subchannel {scid}"
                        ))
                    })?;
                    self.rxs.remove(&id);
                    results.push(SubSelectionResult::ChannelClosed(id));
                },
                Err(mpsc::TryRecvError::Empty) => {
                    if !results.is_empty() {
                        return Ok(results);
                    }
                    MultiReceiverSet::select(&self.multi_receiver_set)?;
                },
                Err(_) => {
                    return Err(MuxError::Disconnected);
                },
            }
        }
    }
}

impl Drop for SubReceiverSet {
    fn drop(&mut self) {
        // Close all SelectableMultiReceivers' response channels so that is_receiver_connected
        // detects the disconnection via IpcError on the response channel.
        self.multi_receiver_set.lock().unwrap().close();
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use crate::mux::{
        SubOneShotServer, SubReceiver, SubSender, subchannel_router::select::SelectableChannel,
    };

    use super::*;

    #[test]
    // A homogeneous SubReceiverSet is one whose SubReceivers all have the same underlying IpcChannel.
    fn receiver_set_homogeneous() {
        let channel = SelectableChannel::new().unwrap();
        let (tx1, rx1) = channel.sub_channel::<i32>();

        let mut rx_set = SubReceiverSet::new().unwrap();
        let rx1_id = rx_set.add(rx1).unwrap();

        let (tx2, rx2) = channel.sub_channel::<String>();
        let rx2_id = rx_set.add(rx2).unwrap();

        tx1.send(1).unwrap();
        tx2.send("test".to_string()).unwrap();

        let mut recvd1 = false;
        let mut recvd2 = false;
        while !recvd1 || !recvd2 {
            for event in rx_set.select().unwrap() {
                if let SubSelectionResult::MessageReceived(received_id, received_data) = event {
                    match received_id {
                        id if id == rx1_id => {
                            assert!(!recvd1, "i32 received twice");
                            let received_value: i32 = received_data.to().unwrap();
                            assert_eq!(received_value, 1);
                            recvd1 = true;
                        },
                        id if id == rx2_id => {
                            assert!(!recvd2, "String received twice");
                            let received_value: String = received_data.to().unwrap();
                            assert_eq!(received_value, "test".to_string());
                            recvd2 = true;
                        },
                        _ => panic!("unexpected id"),
                    }
                } else {
                    panic!("Unexpected SubSelectionResult");
                }
            }
        }
    }

    #[test]
    // A heterogeneous SubReceiverSet is one with SubReceivers having distinct underlying IpcChannels.
    fn receiver_set_heterogeneous() {
        let channel1 = SelectableChannel::new().unwrap();
        let (tx1, rx1) = channel1.sub_channel::<i32>();

        let mut rx_set = SubReceiverSet::new().unwrap();
        let rx1_id = rx_set.add(rx1).unwrap();

        let channel2 = SelectableChannel::new().unwrap();
        let (tx2, rx2) = channel2.sub_channel::<String>();
        let rx2_id = rx_set.add(rx2).unwrap();

        tx1.send(1).unwrap();
        tx2.send("test".to_string()).unwrap();

        let mut recvd1 = false;
        let mut recvd2 = false;
        while !recvd1 || !recvd2 {
            for event in rx_set.select().unwrap() {
                if let SubSelectionResult::MessageReceived(received_id, received_data) = event {
                    match received_id {
                        id if id == rx1_id => {
                            assert!(!recvd1, "i32 received twice");
                            let received_value: i32 = received_data.to().unwrap();
                            assert_eq!(received_value, 1);
                            recvd1 = true;
                        },
                        id if id == rx2_id => {
                            assert!(!recvd2, "String received twice");
                            let received_value: String = received_data.to().unwrap();
                            assert_eq!(received_value, "test".to_string());
                            recvd2 = true;
                        },
                        _ => panic!("unexpected id"),
                    }
                } else {
                    panic!("Unexpected SubSelectionResult");
                }
            }
        }
    }

    #[test]
    fn receiver_set_disconnect() {
        let channel = SelectableChannel::new().unwrap();
        let (tx, rx) = channel.sub_channel::<i32>();

        let mut rx_set = SubReceiverSet::new().unwrap();
        let rx_id = rx_set.add(rx).unwrap();

        drop(tx);
        if let SubSelectionResult::ChannelClosed(received_id) =
            rx_set.select().unwrap().into_iter().next().unwrap()
        {
            assert_eq!(received_id, rx_id);
        } else {
            panic!("unexpected result");
        }
    }

    #[test]
    fn receiver_set_homogeneous_blocking() {
        // this will be used to receive from the spawned thread
        let (bootstrap_server, bootstrap_token) = SubOneShotServer::new().unwrap();

        let thread = thread::spawn(move || {
            let bootstrap_sub_sender: SubSender<SubSender<i32>> =
                SubSender::connect(bootstrap_token).unwrap();

            let channel = SelectableChannel::new().unwrap();
            let (tx1, rx1) = channel.sub_channel();
            bootstrap_sub_sender.send(tx1).unwrap();
            let (tx2, rx2) = channel.sub_channel();
            bootstrap_sub_sender.send(tx2).unwrap();

            let mut rx_set = SubReceiverSet::new().unwrap();
            let rx1_id = rx_set.add(rx1).unwrap();
            let rx2_id = rx_set.add(rx2).unwrap();

            let mut recvd1 = false;
            let mut recvd2 = false;
            while !recvd1 || !recvd2 {
                for event in rx_set.select().unwrap() {
                    if let SubSelectionResult::MessageReceived(received_id, received_data) = event {
                        match received_id {
                            id if id == rx1_id => {
                                assert!(!recvd1, "1 received twice");
                                let received_value: i32 = received_data.to().unwrap();
                                assert_eq!(received_value, 1);
                                recvd1 = true;
                            },
                            id if id == rx2_id => {
                                assert!(!recvd2, "2 received twice");
                                let received_value: i32 = received_data.to().unwrap();
                                assert_eq!(received_value, 2);
                                recvd2 = true;
                            },
                            _ => panic!("unexpected id"),
                        }
                    } else {
                        panic!("Unexpected SubSelectionResult");
                    }
                }
            }
        });

        let (bootstrap_sub_receiver, tx1): (SubReceiver<SubSender<i32>>, SubSender<i32>) =
            bootstrap_server.accept().unwrap();

        let tx2 = bootstrap_sub_receiver.recv().unwrap();
        tx1.send(1).unwrap();
        tx2.send(2).unwrap();

        thread.join().expect("the spawned thread panicked");
    }

    #[test]
    fn receiver_set_heterogeneous_blocking() {
        // this will be used to receive from the spawned thread
        let (bootstrap_server, bootstrap_token) = SubOneShotServer::new().unwrap();

        let thread = thread::spawn(move || {
            let bootstrap_sub_sender: SubSender<SubSender<i32>> =
                SubSender::connect(bootstrap_token).unwrap();

            let channel1 = SelectableChannel::new().unwrap();
            let (tx1, rx1) = channel1.sub_channel();
            bootstrap_sub_sender.send(tx1).unwrap();

            let channel2 = SelectableChannel::new().unwrap();
            let (tx2, rx2) = channel2.sub_channel();
            bootstrap_sub_sender.send(tx2).unwrap();

            let mut rx_set = SubReceiverSet::new().unwrap();
            let rx1_id = rx_set.add(rx1).unwrap();
            let rx2_id = rx_set.add(rx2).unwrap();

            let mut recvd1 = false;
            let mut recvd2 = false;
            while !recvd1 || !recvd2 {
                for event in rx_set.select().unwrap() {
                    if let SubSelectionResult::MessageReceived(received_id, received_data) = event {
                        match received_id {
                            id if id == rx1_id => {
                                assert!(!recvd1, "1 received twice");
                                let received_value: i32 = received_data.to().unwrap();
                                assert_eq!(received_value, 1);
                                recvd1 = true;
                            },
                            id if id == rx2_id => {
                                assert!(!recvd2, "2 received twice");
                                let received_value: i32 = received_data.to().unwrap();
                                assert_eq!(received_value, 2);
                                recvd2 = true;
                            },
                            _ => panic!("unexpected id"),
                        }
                    } else {
                        panic!("Unexpected SubSelectionResult");
                    }
                }
            }
        });

        let (bootstrap_sub_receiver, tx1): (SubReceiver<SubSender<i32>>, SubSender<i32>) =
            bootstrap_server.accept().unwrap();

        let tx2 = bootstrap_sub_receiver.recv().unwrap();
        tx1.send(1).unwrap();
        tx2.send(2).unwrap();

        thread.join().expect("the spawned thread panicked");
    }

    #[test]
    // Test SubReceivers sharing an IPC channel belonging to distinct SubReceiverSets with distinct IpcReceiverSets.
    fn subreceivers_sharing_ipc_channel_cannot_belong_to_distinct_subreceiversets_with_distinct_ipcreceiversets()
     {
        let channel = SelectableChannel::new().unwrap();
        let (_tx1, rx1) = channel.sub_channel::<i32>();
        let (_tx2, rx2) = channel.sub_channel::<i32>();

        let mut rx_set1 = SubReceiverSet::new().unwrap();
        let _rx1_id = rx_set1.add(rx1).unwrap();

        let mut rx_set2 = SubReceiverSet::new().unwrap();

        // Ensure rx_set2 has a non-empty IpcReceiverSet.
        let channel2 = SelectableChannel::new().unwrap();
        let (_tx3, rx3) = channel2.sub_channel::<i32>();
        let _rx3_id = rx_set2.add(rx3).unwrap();

        assert_eq!(
            format!("{:?}", rx_set2.add(rx2)),
            "Err(InternalError(\"Cannot merge non-empty MultiReceiverSets\"))"
        );
    }

    #[test]
    // A homogeneous SubReceiverSet is one whose SubReceivers all have the same underlying IpcChannel.
    fn receiver_sets_with_subreceivers_sharing_ipc_channel() {
        let channel = SelectableChannel::new().unwrap();
        let (tx1, rx1) = channel.sub_channel::<i32>();

        let mut rx_set1 = SubReceiverSet::new().unwrap();
        let rx1_id = rx_set1.add(rx1).unwrap();

        let mut rx_set2 = SubReceiverSet::new().unwrap();
        let (tx2, rx2) = channel.sub_channel::<String>();
        let rx2_id = rx_set2.add(rx2).unwrap();

        tx1.send(1).unwrap();

        if let SubSelectionResult::MessageReceived(received_id, received_data) =
            rx_set1.select().unwrap().into_iter().next().unwrap()
        {
            match received_id {
                id if id == rx1_id => {
                    let received_value: i32 = received_data.to().unwrap();
                    assert_eq!(received_value, 1);
                },
                _ => panic!("unexpected id"),
            }
        } else {
            panic!("Unexpected SubSelectionResult");
        }

        tx2.send("test".to_string()).unwrap();

        if let SubSelectionResult::MessageReceived(received_id, received_data) =
            rx_set2.select().unwrap().into_iter().next().unwrap()
        {
            match received_id {
                id if id == rx2_id => {
                    let received_value: String = received_data.to().unwrap();
                    assert_eq!(received_value, "test".to_string());
                },
                _ => panic!("unexpected id"),
            }
        } else {
            panic!("Unexpected SubSelectionResult");
        }
    }
}
