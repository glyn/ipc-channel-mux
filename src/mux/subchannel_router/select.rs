// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashMap;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use std::{fmt, io};

use ipc_channel::IpcError;
use ipc_channel::ipc::{self, IpcReceiver, IpcReceiverSet, IpcSelectionResult};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use uuid::Uuid;

use crate::mux::demux::ResolvedMessageOrDisconnect;
use crate::mux::protocol::{MultiMessage, SubChannelId};
use crate::mux::{
    MuxError, SubSender,
    channel_identification::Source,
    demux::Demuxer,
    protocol::ClientId,
    sender::{MultiSender, SubChannelSender},
    subchannel_lifecycle::SubReceiverProxy,
};

/// SelectableChannel wraps an IPC channel and is used to construct subchannels.
pub struct SelectableChannel {
    multi_sender: Arc<Mutex<MultiSender>>,
    multi_receiver: Arc<SelectableMultiReceiver>,
}

impl SelectableChannel {
    /// Construct a new [SelectableChannel].
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn new() -> Result<SelectableChannel, MuxError> {
        let (ms, mr) = selectable_multi_channel()?;
        Ok(SelectableChannel {
            multi_sender: ms,
            multi_receiver: mr,
        })
    }

    /// Construct a new subchannel of a [Channel]. The subchannel has
    /// a [SubSender] and a [SubReceiver].
    #[instrument(level = "debug", skip(self))]
    pub fn sub_channel<T>(&self) -> (SubSender<T>, SelectableSubReceiver<T>)
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let scs = SubChannelSender::new(Arc::clone(&self.multi_sender));
        let scid = scs.sub_channel_id();
        self.multi_sender
            .lock()
            .unwrap()
            .insert_sub_receiver_proxy(scid, SubReceiverProxy::new());
        let scr = SelectableMultiReceiver::attach(&self.multi_receiver, scid);
        (
            SubSender::from_sender(scs),
            SelectableSubReceiver::from_receiver(scr),
        )
    }
}

#[instrument(level = "debug", ret, err(level = "debug"))]
fn selectable_multi_channel()
-> Result<(Arc<Mutex<MultiSender>>, Arc<SelectableMultiReceiver>), io::Error> {
    let (ipc_sender, ipc_receiver) = ipc::channel()?;
    let (ipc_response_sender, ipc_response_receiver) = ipc::channel()?;
    let client_id = ClientId::new();
    #[allow(clippy::arc_with_non_send_sync)]
    let mrs = Arc::new(Mutex::new(MultiReceiverSet::new()?));
    let multi_receiver_rc = Arc::new(SelectableMultiReceiver::new(
        Uuid::new_v4(),
        SelectableReceiverDemuxer::new(
            Arc::clone(&mrs),
            Arc::new(Mutex::new(Demuxer::with_sender(
                client_id,
                ipc_response_sender,
            ))),
        ),
    ));
    mrs.lock()
        .unwrap()
        .set_pending(ipc_receiver, Arc::clone(&multi_receiver_rc));
    let multi_sender = MultiSender::new(
        client_id,
        Arc::new(ipc_sender),
        Uuid::new_v4(),
        Arc::new(Mutex::new(Source::new())),
        ipc_response_receiver,
    );
    Ok((Arc::new(Mutex::new(multi_sender)), multi_receiver_rc))
}

/// SelectableSubReceiver is the receiving end of a subchannel which will be added to a SubReceiverSet.
#[derive(Debug)]
pub struct SelectableSubReceiver<T>
where
    T: for<'x> Deserialize<'x> + Serialize,
{
    sub_channel_receiver: SelectableSubChannelReceiver,
    phantom: PhantomData<T>,
}

impl<T> SelectableSubReceiver<T>
where
    T: for<'x> Deserialize<'x> + Serialize,
{
    fn from_receiver(sub_channel_receiver: SelectableSubChannelReceiver) -> Self {
        SelectableSubReceiver {
            sub_channel_receiver,
            phantom: PhantomData,
        }
    }
}

impl<T> SelectableSubReceiver<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    /// Convert a SelectableSubReceiver to an OpaqueSelectableSubReceiver by erasing its message type.
    ///
    /// Useful for adding routes to a `RouterProxy`.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_opaque(self) -> OpaqueSelectableSubReceiver {
        OpaqueSelectableSubReceiver {
            sub_channel_receiver: self.sub_channel_receiver,
        }
    }
}

/// OpaqueSelectableSubReceiver is a SelectableSubReceiver with the message type erased. It can be
/// passed around in a message type independent manner, but must be converted
/// into a SelectableSubReceiver before it can be used to receive messages.
pub struct OpaqueSelectableSubReceiver {
    sub_channel_receiver: SelectableSubChannelReceiver,
}

impl OpaqueSelectableSubReceiver {
    pub fn multi_receiver_set(&self) -> Arc<Mutex<MultiReceiverSet>> {
        self.sub_channel_receiver.multi_receiver_set()
    }

    pub fn demuxer(&self) -> Arc<Mutex<crate::mux::demux::Demuxer>> {
        self.sub_channel_receiver.demuxer()
    }

    pub fn sub_channel_id(&self) -> crate::mux::protocol::SubChannelId {
        self.sub_channel_receiver.sub_channel_id()
    }

    pub fn into_inner(self) -> SelectableSubChannelReceiver {
        self.sub_channel_receiver
    }
}

pub struct SelectableSubChannelReceiver {
    multi_receiver: Arc<SelectableMultiReceiver>,
    sub_channel_id: SubChannelId,
}

// No custom Drop for SelectableSubChannelReceiver: SubReceiverSet::drop calls
// MultiReceiverSet::close() which clears the demuxer's ipc_senders,
// closing the response channels. is_receiver_connected detects this
// closure via IpcError and returns false.

impl SelectableSubChannelReceiver {
    fn multi_receiver_set(&self) -> Arc<Mutex<MultiReceiverSet>> {
        Arc::clone(&self.multi_receiver.receiver_demuxer.multi_receiver_set)
    }

    fn demuxer(&self) -> Arc<Mutex<Demuxer>> {
        Arc::clone(&self.multi_receiver.receiver_demuxer.demuxer)
    }

    fn sub_channel_id(&self) -> SubChannelId {
        self.sub_channel_id
    }
}

unsafe impl Send for SelectableSubChannelReceiver {}
unsafe impl Sync for SelectableSubChannelReceiver {}

impl fmt::Debug for SelectableSubChannelReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SelectableSubChannelReceiver")
            .field("sub_channel_id", &self.sub_channel_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct SelectableReceiverDemuxer {
    multi_receiver_set: Arc<Mutex<MultiReceiverSet>>,
    demuxer: Arc<Mutex<Demuxer>>,
}

impl SelectableReceiverDemuxer {
    fn new(multi_receiver_set: Arc<Mutex<MultiReceiverSet>>, demuxer: Arc<Mutex<Demuxer>>) -> Self {
        SelectableReceiverDemuxer {
            multi_receiver_set,
            demuxer,
        }
    }
}

#[derive(Debug)]
pub struct SelectableMultiReceiver {
    ipc_receiver_uuid: Uuid,
    receiver_demuxer: SelectableReceiverDemuxer,
}

unsafe impl Send for SelectableMultiReceiver {}
unsafe impl Sync for SelectableMultiReceiver {}

impl SelectableMultiReceiver {
    fn new(ipc_receiver_uuid: Uuid, receiver_demuxer: SelectableReceiverDemuxer) -> Self {
        SelectableMultiReceiver {
            ipc_receiver_uuid,
            receiver_demuxer,
        }
    }

    #[instrument(level = "debug", ret)]
    pub fn attach(
        mr: &Arc<SelectableMultiReceiver>,
        sub_channel_id: SubChannelId,
    ) -> SelectableSubChannelReceiver {
        let (tx, _rx): (
            Sender<ResolvedMessageOrDisconnect>,
            Receiver<ResolvedMessageOrDisconnect>,
        ) = mpsc::channel();
        mr.receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .insert_state_machine(sub_channel_id, tx);
        SelectableSubChannelReceiver {
            multi_receiver: Arc::clone(mr),
            sub_channel_id,
        }
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn handle(mr: Arc<SelectableMultiReceiver>, msg: MultiMessage) -> Result<(), MuxError> {
        let demuxer = &mut mr.receiver_demuxer.demuxer.lock().unwrap();
        if let MultiMessage::Data(scid, payload, ipc_senders) = msg {
            demuxer.send(scid, payload, &ipc_senders, mr.ipc_receiver_uuid)
        } else {
            demuxer.handle(msg, mr.ipc_receiver_uuid)
        }
    }

    // poll returns true if and only if a probe failed.
    #[instrument(level = "trace", ret)]
    fn poll(&self) -> bool {
        Demuxer::poll_all_subchannels(&self.receiver_demuxer.demuxer)
    }
}

pub struct MultiReceiverSet {
    ipc_receiver_set: IpcReceiverSet,
    // Weak refs to avoid a reference cycle: SelectableMultiReceiver → MultiReceiverSet → SelectableMultiReceiver.
    // The strong refs are held by SelectableSubChannelReceiver instances.
    multi_receivers: HashMap<u64, Weak<SelectableMultiReceiver>>,
    // IPC receiver not yet registered in ipc_receiver_set, waiting to be registered
    // when the first subreceiver from this channel is added to a SubReceiverSet.
    // Uses a strong Arc to keep SelectableMultiReceiver alive until subchannels are created
    // (this creates a temporary reference cycle that is broken by register_pending).
    pending: Option<(IpcReceiver<MultiMessage>, Arc<SelectableMultiReceiver>)>,
    // After this MRS is merged into another, records which MRS it was merged into.
    merged_into: Option<Weak<Mutex<MultiReceiverSet>>>,
}

impl fmt::Debug for MultiReceiverSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiReceiverSet")
            .field("multi_receivers", &self.multi_receivers)
            .finish_non_exhaustive()
    }
}

impl MultiReceiverSet {
    // Create a new empty MultiReceiverSet.
    //
    // Receivers may then be added to the set with the add method.
    pub fn new() -> Result<MultiReceiverSet, io::Error> {
        Ok(MultiReceiverSet {
            ipc_receiver_set: IpcReceiverSet::new()?,
            multi_receivers: HashMap::new(),
            pending: None,
            merged_into: None,
        })
    }

    // Register a pending IPC receiver into the IpcReceiverSet.
    // This breaks the temporary reference cycle by dropping the strong Arc<SelectableMultiReceiver>
    // from pending and storing only a Weak ref in multi_receivers.
    pub fn register_pending(&mut self) -> Result<(), MuxError> {
        if let Some((ipc_receiver, multi_receiver)) = self.pending.take() {
            let id = self.ipc_receiver_set.add(ipc_receiver)?;
            self.multi_receivers
                .insert(id, Arc::downgrade(&multi_receiver));
        }
        Ok(())
    }

    // Close all SelectableMultiReceivers in this set by clearing their demuxer's ipc_senders.
    // This causes is_receiver_connected to detect IpcError on the response channel,
    // signalling disconnection to MultiSenders without needing a SubReceiverDisconnected broadcast.
    // Called from SubReceiverSet::drop to handle router shutdown.
    pub fn close(&self) {
        for mr in self.multi_receivers.values() {
            if let Some(mr) = mr.upgrade() {
                mr.receiver_demuxer
                    .demuxer
                    .lock()
                    .unwrap()
                    .clear_ipc_senders();
            }
        }
        if let Some((_, ref mr)) = self.pending {
            mr.receiver_demuxer
                .demuxer
                .lock()
                .unwrap()
                .clear_ipc_senders();
        }
    }

    pub fn set_pending(
        &mut self,
        ipc_receiver: IpcReceiver<MultiMessage>,
        multi_receiver: Arc<SelectableMultiReceiver>,
    ) {
        self.pending = Some((ipc_receiver, multi_receiver));
    }

    pub fn is_merged_into(&self, target: &Arc<Mutex<MultiReceiverSet>>) -> bool {
        #[allow(clippy::map_unwrap_or)]
        self.merged_into
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .map(|arc| Arc::ptr_eq(&arc, target))
            .unwrap_or(false)
    }

    pub fn take_pending(
        &mut self,
    ) -> Option<(IpcReceiver<MultiMessage>, Arc<SelectableMultiReceiver>)> {
        self.pending.take()
    }

    pub fn merge_receiver(
        &mut self,
        ipc_receiver: IpcReceiver<MultiMessage>,
        multi_receiver: &Arc<SelectableMultiReceiver>,
    ) -> Result<(), MuxError> {
        let id = self.ipc_receiver_set.add(ipc_receiver)?;
        self.multi_receivers
            .insert(id, Arc::downgrade(multi_receiver));
        Ok(())
    }

    pub fn set_merged_into(&mut self, target: &Arc<Mutex<MultiReceiverSet>>) {
        self.merged_into = Some(Arc::downgrade(target));
    }

    // Return true if and only if the MultiReceiverSet is empty (no IPC receivers
    // registered or pending registration).
    pub fn is_empty(mrs: &Arc<Mutex<MultiReceiverSet>>) -> bool {
        let mrs_locked = mrs.lock().unwrap();
        mrs_locked.multi_receivers.is_empty() && mrs_locked.pending.is_none()
    }

    // Obtain one or more incoming messages and handle them.
    #[instrument(level = "trace", ret, err(level = "trace"))]
    pub fn select(mrs: &Arc<Mutex<MultiReceiverSet>>) -> Result<(), MuxError> {
        let polling_interval = Duration::new(1, 0);
        let mut mrs_mut = mrs.lock().unwrap();
        loop {
            let results = mrs_mut
                .ipc_receiver_set
                .try_select_timeout(polling_interval);
            match results {
                Ok(results) => {
                    log::trace!(
                        "MultiReceiverSet::select processing {} results",
                        results.len()
                    );
                    for result in results {
                        match result {
                            IpcSelectionResult::MessageReceived(id, ipc_message) => {
                                if let Some(multi_receiver) =
                                    mrs_mut.multi_receivers.get(&id).and_then(Weak::upgrade)
                                {
                                    SelectableMultiReceiver::handle(
                                        multi_receiver,
                                        ipc_message.to().map_err(|e| {
                                            MuxError::IpcError(IpcError::SerializationError(e))
                                        })?,
                                    )?;
                                }
                            },
                            IpcSelectionResult::ChannelClosed(id) => {
                                mrs_mut.multi_receivers.remove(&id);
                            },
                        }
                    }
                    break;
                },
                Err(ipc_channel::TrySelectError::Empty) => {
                    let mut probe_failed = false;
                    for weak_mr in mrs_mut.multi_receivers.values() {
                        if let Some(mr) = weak_mr.upgrade() {
                            if mr.poll() {
                                probe_failed = true;
                            }
                        }
                    }
                    if probe_failed {
                        // At least one probe failed, so return to caller.
                        return Ok(());
                    }
                },
                Err(ipc_channel::TrySelectError::IoError(e)) => {
                    return Err(e.into());
                },
            }
        }
        Ok(())
    }
}
