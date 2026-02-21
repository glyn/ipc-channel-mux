// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module multiplexes _subchannels_ over IPC channels.
//!
//! A subchannel is a multi-producer, single-consumer (MPSC) FIFO queue with
//! an unbounded buffer.
//!
//! ## Multiplexing
//!
//! A subchannel uses an underlying IPC channel for its communication. More than
//! one subchannel may use the same underlying IPC channel and messages are
//! multiplexed at the sender and demultiplexed at the receiver. This reduces the
//! number of IPC channels (and corresponding operating system IPC resources) needed
//! to send messages between processes.
//!
//! ## Disconnection
//!
//! The send and receive operations on subchannels return a Result indicating
//! whether or not the operation succeeded. An unsuccessful operation normally
//! indicates that the other "half" of a channel has "disconnected" by being dropped
//! or by the process(es) containing the other half terminating or by subchannel
//! senders being lost in transmission (e.g. because the receiver of a subchannel
//! used to transmit a subchannel sender was dropped or its containing process
//! terminated).
//!
//! Once half of a channel has been dropped, most operations can no longer
//! continue to make progress, so Err will be returned.
//!
//! ## Examples
//!
//! Simple usage:
//! ```
//! # use ipc_channel_mux::mux;
//! # fn main() -> Result<(), mux::MuxError> {
//!    let channel = mux::Channel::new().unwrap();
//!
//!    let (tx, rx) = channel.sub_channel();
//!    tx.send(1729).unwrap();
//!    assert_eq!(rx.recv().unwrap(), 1729);
//!
//!    let (tx2, rx2) = channel.sub_channel();
//!    let taxi = "taxi".to_string();
//!    tx2.send(taxi.clone()).unwrap();
//!    assert_eq!(rx2.recv().unwrap(), taxi);
//! #  Ok(())
//! # }
//! ```
//!
//! Inter-process bootstrapping:
//! ```
//! # use ipc_channel_mux::mux;
//! # use std::thread;
//! # fn main() -> Result<(), mux::MuxError> {
//!    let (server, name) = mux::SubOneShotServer::<i32>::new().unwrap();
//!
//!    thread::spawn(move || {
//!        let tx = mux::SubSender::connect(name).unwrap();
//!        tx.send(1729).unwrap();
//!        tx.send(1730).unwrap();
//!    });
//!
//!    let (rx, val) = server.accept().unwrap();
//!    assert_eq!(val, 1729);
//!    assert_eq!(rx.recv().unwrap(), 1730);
//! #  Ok(())
//! # }
//! ```
//!
//! Subchannel sender transmission:
//! ```
//! # use ipc_channel_mux::mux;
//! # fn main() -> Result<(), mux::MuxError> {
//!    let channel = mux::Channel::new().unwrap();
//!    let (tx, rx) = channel.sub_channel();
//!
//!    let (sender, receiver) = channel.sub_channel();
//!    sender.send(tx).unwrap();
//!
//!    let received_tx = receiver.recv().unwrap();
//!    received_tx.send(1729);
//!    assert_eq!(rx.recv().unwrap(), 1729);
//! #  Ok(())
//! # }
//! ```
//!
//! Subchannel sender transmission failure:
//! ```
//! # use ipc_channel_mux::mux;
//! # fn main() -> Result<(), mux::MuxError> {
//!    let channel = mux::Channel::new().unwrap();
//!    let (tx, rx) = channel.sub_channel::<i32>();
//!
//!    let (sender, receiver) = channel.sub_channel();
//!    sender.send(tx).unwrap();
//!
//!    drop(receiver);
//!    
//!    match rx.recv().unwrap_err() {
//!        mux::MuxError::Disconnected => (),
//!        e => panic!("unexpected error"),
//!    }
//! #  Ok(())
//! # }
//! ```
//!
//! Opaque subchannel sender:
//! ```
//! # use ipc_channel_mux::mux;
//! # fn main() -> Result<(), mux::MuxError> {
//! let channel = mux::Channel::new().unwrap();
//! let (tx, rx) = channel.sub_channel::<i32>();
//!
//! let opaque_tx = tx.to_opaque();
//! let tx: mux::SubSender<i32> = opaque_tx.to();
//!
//! tx.send(1).unwrap();
//! assert_eq!(rx.recv().unwrap(), 1);
//! #  Ok(())
//! # }
//! ```

#![warn(missing_docs)]

use channel_identification::{Source, Target};
use ipc_channel::ipc::{self, IpcOneShotServer, IpcReceiver};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use tracing::instrument;
use uuid::Uuid;
use weak_table::WeakValueHashMap;

mod channel_identification;
mod demux;
mod error;
mod protocol;
mod sender;
mod subchannel_endpoint;
mod subchannel_lifecycle;
pub mod subchannel_router;

use demux::{
    Demuxer, MultiReceiver, MultiReceiverSet, ProtoSender, ReceiverDemuxer, ResolvedMessage,
    ResolvedMessageOrDisconnect, SelectableMultiReceiver, SelectableReceiverDemuxer,
    SelectableSubChannelReceiver, clear_deserialization_context,
    establish_deserialization_context,
};
pub use error::MuxError;
pub use subchannel_endpoint::{
    OpaqueSubReceiver, OpaqueSubSender, SubReceiver, SubSender,
};
pub(crate) use subchannel_endpoint::{
    OpaqueSelectableSubReceiver, SelectableSubReceiver,
};
use protocol::{ClientId, MultiMessage, SubChannelId};
use sender::{MultiSender, SubChannelSender};

/// Channel wraps an IPC channel and is used to construct subchannels.
pub struct Channel {
    multi_sender: Arc<Mutex<MultiSender>>,
    multi_receiver: Arc<MultiReceiver>,
}

/// SelectableChannel wraps an IPC channel and is used to construct subchannels.
pub(crate) struct SelectableChannel {
    multi_sender: Arc<Mutex<MultiSender>>,
    multi_receiver: Arc<SelectableMultiReceiver>,
}

impl Channel {
    /// Construct a new [Channel].
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn new() -> Result<Channel, MuxError> {
        let (ms, mr) = multi_channel()?;
        Ok(Channel {
            multi_sender: ms,
            multi_receiver: mr,
        })
    }

    /// Construct a new subchannel of a [Channel]. The subchannel has
    /// a [SubSender] and a [SubReceiver].
    #[instrument(level = "debug", skip(self))]
    pub fn sub_channel<T>(&self) -> (SubSender<T>, SubReceiver<T>)
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let scs = SubChannelSender::new(Arc::clone(&self.multi_sender));
        let scid = scs.sub_channel_id();
        self.multi_sender
            .lock()
            .unwrap()
            .sub_receiver_proxies
            .lock()
            .unwrap()
            .insert(scid, subchannel_lifecycle::SubReceiverProxy::new());
        let scr = MultiReceiver::attach(&self.multi_receiver, scid);
        (
            SubSender {
                sub_channel_sender: scs,
                phantom: PhantomData,
            },
            SubReceiver {
                sub_channel_receiver: scr,
                phantom: PhantomData,
            },
        )
    }
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
            .sub_receiver_proxies
            .lock()
            .unwrap()
            .insert(scid, subchannel_lifecycle::SubReceiverProxy::new());
        let scr = SelectableMultiReceiver::attach(&self.multi_receiver, scid);
        (
            SubSender {
                sub_channel_sender: scs,
                phantom: PhantomData,
            },
            SelectableSubReceiver {
                sub_channel_receiver: scr,
                phantom: PhantomData,
            },
        )
    }
}

/// SubOneShotServer together with its generated name can be used to establish a subchannel
/// between processes.
///
/// On the server side, call [accept] against the server to obtain the subchannel receiver
/// and receive the first message.
///
/// On the client side, call [connect], passing the server name, to obtain the subchannel
/// sender. The server is “one-shot” because it accepts only one connect request from a client.
///
/// [accept]: crate::mux::SubOneShotServer::accept
/// [connect]: crate::mux::SubSender::connect
pub struct SubOneShotServer<T> {
    one_shot_multi_server: OneShotMultiServer,
    name: String,
    phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for SubOneShotServer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubOneShotServer")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<T> SubOneShotServer<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    /// Construct a new server with a generated name in order to establish a subchannel
    /// between processes.
    ///
    /// Call accept on the server to obtain a subchannel receiver and receive the first message.
    ///
    /// Call connect passing the server name to obtain the subchannel sender.
    #[instrument(level = "debug", ret, err(level = "debug"))]
    pub fn new() -> Result<(SubOneShotServer<T>, String), MuxError> {
        let (one_shot_multi_server, name) = OneShotMultiServer::new()?;
        Ok((
            SubOneShotServer {
                one_shot_multi_server,
                name: name.clone(),
                phantom: PhantomData,
            },
            name,
        ))
    }

    /// Obtain a [SubReceiver] from a server and receive the first message.
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn accept(self) -> Result<(SubReceiver<T>, T), MuxError> {
        let multi_receiver = self.one_shot_multi_server.accept()?;
        let (subchannel_id, name) = MultiReceiver::receive_sub_channel(&multi_receiver)
            .expect("receive sub channel failed");
        if name != self.name {
            return Err(MuxError::InternalError(format!(
                "unexpected sub channel name {}",
                name
            )));
        }
        let sub_receiver = MultiReceiver::attach(&multi_receiver, subchannel_id);
        let msg: T = sub_receiver.recv()?;
        Ok((
            SubReceiver {
                sub_channel_receiver: sub_receiver,
                phantom: PhantomData,
            },
            msg,
        ))
    }
}

/// Create a multiplexing channel that can be used to establish subchannels
/// between processes. The subchannels flow across the multichannel.
/// A multiplexing channel represents a fixed collection of IPC resources.
/// Subchannels consume no further IPC resources.
/// Each subchannel is allocated an identity and this identity can flow
/// across the multiplexing channel.
///
/// [MultiSender]: struct.MultiSender.html
/// [MultiReceiver]: struct.MultiReceiver.html
#[instrument(level = "debug", ret, err(level = "debug"))]
fn multi_channel() -> Result<(Arc<Mutex<MultiSender>>, Arc<MultiReceiver>), io::Error> {
    let (ipc_sender, ipc_receiver) = ipc::channel()?;
    let (ipc_response_sender, ipc_response_receiver) = ipc::channel()?;
    let client_id = ClientId(Uuid::new_v4());
    let mut senders = HashMap::new();
    senders.insert(client_id, ipc_response_sender);
    let multi_receiver = MultiReceiver {
        ipc_receiver_uuid: Uuid::new_v4(),
        receiver_demuxer: ReceiverDemuxer {
            ipc_receiver,
            demuxer: Arc::new(Mutex::new(Demuxer {
                ipc_senders: senders,
                sub_channels: HashMap::new(),
                disconnectors: WeakValueHashMap::new(),
                ipc_senders_by_id: Target::new(),
            })),
        },
    };
    let multi_receiver_rc = Arc::new(multi_receiver);
    let multi_sender = MultiSender {
        client_id,
        ipc_sender: Arc::new(ipc_sender),
        uuid: Uuid::new_v4(),
        sender_id: Arc::new(Mutex::new(Source::new())),
        response_receiver: ipc_response_receiver,
        sub_receiver_proxies: Mutex::new(HashMap::new()),
    };
    Ok((Arc::new(Mutex::new(multi_sender)), multi_receiver_rc))
}

#[instrument(level = "debug", ret, err(level = "debug"))]
fn selectable_multi_channel()
-> Result<(Arc<Mutex<MultiSender>>, Arc<SelectableMultiReceiver>), io::Error> {
    let (ipc_sender, ipc_receiver) = ipc::channel()?;
    let (ipc_response_sender, ipc_response_receiver) = ipc::channel()?;
    let client_id = ClientId(Uuid::new_v4());
    let mut senders = HashMap::new();
    senders.insert(client_id, ipc_response_sender);
    #[allow(clippy::arc_with_non_send_sync)]
    let mrs = Arc::new(Mutex::new(MultiReceiverSet::new()?));
    let multi_receiver_rc = Arc::new(SelectableMultiReceiver {
        ipc_receiver_uuid: Uuid::new_v4(),
        receiver_demuxer: SelectableReceiverDemuxer {
            multi_receiver_set: Arc::clone(&mrs),
            demuxer: Arc::new(Mutex::new(Demuxer {
                ipc_senders: senders,
                sub_channels: HashMap::new(),
                disconnectors: WeakValueHashMap::new(),
                ipc_senders_by_id: Target::new(),
            })),
        },
    });
    {
        let mut mrs_mut = mrs.lock().unwrap();
        mrs_mut.pending = Some((ipc_receiver, Arc::clone(&multi_receiver_rc)));
    }
    let multi_sender = MultiSender {
        client_id,
        ipc_sender: Arc::new(ipc_sender),
        uuid: Uuid::new_v4(),
        sender_id: Arc::new(Mutex::new(Source::new())),
        response_receiver: ipc_response_receiver,
        sub_receiver_proxies: Mutex::new(HashMap::new()),
    };
    Ok((Arc::new(Mutex::new(multi_sender)), multi_receiver_rc))
}

struct OneShotMultiServer {
    multi_server: IpcOneShotServer<MultiMessage>,
}

impl OneShotMultiServer {
    #[instrument(level = "debug", err(level = "debug"))]
    fn new() -> Result<(OneShotMultiServer, String), io::Error> {
        let (multi_server, name) = IpcOneShotServer::new()?;
        Ok((OneShotMultiServer { multi_server }, name))
    }

    #[instrument(level = "debug", skip(self), ret, err(level = "debug"))]
    fn accept(self) -> Result<Arc<MultiReceiver>, MuxError> {
        let (ipc_receiver, multi_message): (IpcReceiver<MultiMessage>, MultiMessage) =
            self.multi_server.accept()?;

        let mr = MultiReceiver {
            ipc_receiver_uuid: Uuid::new_v4(),
            receiver_demuxer: ReceiverDemuxer {
                ipc_receiver,
                demuxer: Arc::new(Mutex::new(Demuxer {
                    ipc_senders: HashMap::new(),
                    sub_channels: HashMap::new(),
                    disconnectors: WeakValueHashMap::new(),
                    ipc_senders_by_id: Target::new(),
                })),
            },
        };
        let mr_rc = Arc::new(mr);
        mr_rc
            .receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .handle(multi_message, mr_rc.ipc_receiver_uuid)?;
        Ok(mr_rc)
    }
}

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
pub(crate) struct SubReceiverSet {
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
}

impl RawMessage {
    /// Deserialise the raw message into the inferred type.
    pub fn to<T>(self) -> Result<T, MuxError>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        establish_deserialization_context(self.senders, self.scid);

        let result = postcard::from_bytes::<T>(self.payload.as_slice());

        clear_deserialization_context();

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
        let incoming_multi_receiver_set = Arc::clone(
            &receiver
                .sub_channel_receiver
                .multi_receiver
                .receiver_demuxer
                .multi_receiver_set,
        );

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
            let already_merged = {
                let incoming_locked = incoming_multi_receiver_set.lock().unwrap();
                #[allow(clippy::map_unwrap_or)]
                incoming_locked
                    .merged_into
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade)
                    .map(|arc| Arc::ptr_eq(&arc, &self.multi_receiver_set))
                    .unwrap_or(false)
            };
            if !already_merged {
                // Merge: take the pending IPC receiver from the incoming MRS and register it in
                // ours. IpcReceiverSets cannot be merged, so we only support merging an incoming
                // MRS that has a pending (not yet registered) IPC receiver.
                let mut incoming_locked = incoming_multi_receiver_set.lock().unwrap();
                if let Some((ipc_receiver, multi_receiver)) = incoming_locked.pending.take() {
                    {
                        let mut self_locked = self.multi_receiver_set.lock().unwrap();
                        let id = self_locked.ipc_receiver_set.add(ipc_receiver)?;
                        self_locked
                            .multi_receivers
                            .insert(id, Arc::downgrade(&multi_receiver));
                    }
                    incoming_locked.merged_into = Some(Arc::downgrade(&self.multi_receiver_set));
                } else {
                    return Err(MuxError::InternalError(
                        "Cannot merge non-empty MultiReceiverSets".to_string(),
                    ));
                }
            }
        }

        // Modify MultiReceiver so that messages for the subchannel are sent to the set.
        {
            let demuxer = receiver
                .sub_channel_receiver
                .multi_receiver
                .receiver_demuxer
                .demuxer
                .lock()
                .unwrap();
            let sub_sender_state_machine = demuxer
                .sub_channels
                .get(&receiver.sub_channel_receiver.sub_channel_id)
                .ok_or_else(|| {
                    MuxError::InternalError(format!(
                        "missing sub_channel for {}",
                        receiver.sub_channel_receiver.sub_channel_id
                    ))
                })?;
            sub_sender_state_machine.switch_sender(self.tx.clone());
        }

        let id = self.next_id;
        self.next_id += 1;
        self.ids
            .insert(receiver.sub_channel_receiver.sub_channel_id, id);
        self.rxs.insert(id, receiver.sub_channel_receiver);
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
                Ok(ResolvedMessageOrDisconnect::ResolvedMessage(ResolvedMessage {
                    scid,
                    payload,
                    senders,
                })) => {
                    let id = *self.ids.get(&scid).ok_or_else(|| {
                        MuxError::InternalError(format!("missing id for subchannel {}", scid))
                    })?;
                    results.push(SubSelectionResult::MessageReceived(
                        id,
                        RawMessage {
                            payload,
                            senders,
                            scid,
                        },
                    ));
                },
                Ok(ResolvedMessageOrDisconnect::Disconnect(scid)) => {
                    let id = self.ids.remove(&scid).ok_or_else(|| {
                        MuxError::InternalError(format!(
                            "missing id for disconnected subchannel {}",
                            scid
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
