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
use ipc_channel::IpcError;
use ipc_channel::ipc::{
    self, IpcOneShotServer, IpcReceiver, IpcReceiverSet, IpcSelectionResult, IpcSender,
};
use log;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug, Display, Formatter};
use std::io;
use std::marker::PhantomData;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use subchannel_lifecycle::SubSenderTracker;
use thiserror::Error;
use tracing::instrument;
use uuid::Uuid;
use weak_table::WeakValueHashMap;

mod channel_identification;
mod subchannel_lifecycle;
pub mod subchannel_router;

const EMPTY_SUBCHANNEL_ID: SubChannelId =
    SubChannelId(uuid::uuid!("11111111-10b1-428f-9447-cb680e5fe0c8"));
const ORIGIN: Uuid = uuid::uuid!("00000000-10b1-428f-9447-cb680e5fe0c8");

/// Channel wraps an IPC channel and is used to construct subchannels.
pub struct Channel {
    multi_sender: Arc<Mutex<MultiSender>>,
    multi_receiver: Arc<MultiReceiver>,
}

/// Channel2 wraps an IPC channel and is used to construct subchannels.
pub struct Channel2 {
    multi_sender: Arc<Mutex<MultiSender>>,
    multi_receiver: Arc<MultiReceiver2>,
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
impl Channel2 {
    /// Construct a new [Channel2].
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn new() -> Result<Channel2, MuxError> {
        let (ms, mr) = multi_channel2()?;
        Ok(Channel2 {
            multi_sender: ms,
            multi_receiver: mr,
        })
    }

    /// Construct a new subchannel of a [Channel]. The subchannel has
    /// a [SubSender] and a [SubReceiver].
    #[instrument(level = "debug", skip(self))]
    pub fn sub_channel<T>(&self) -> (SubSender<T>, SubReceiver2<T>)
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
        let scr = MultiReceiver2::attach(&self.multi_receiver, scid);
        (
            SubSender {
                sub_channel_sender: scs,
                phantom: PhantomData,
            },
            SubReceiver2 {
                sub_channel_receiver: scr,
                phantom: PhantomData,
            },
        )
    }
}

/// SubSender is the sending end of a subchannel, used to serialize and send messages of a given type.
///
/// SubSenders can be sent in messages on other subchannels and can be cloned.
#[derive(Debug, Deserialize, Serialize)]
pub struct SubSender<T>
where
    T: Serialize,
{
    sub_channel_sender: SubChannelSender,
    phantom: PhantomData<T>,
}

impl<T> Clone for SubSender<T>
where
    T: Serialize,
{
    fn clone(&self) -> SubSender<T> {
        SubSender {
            sub_channel_sender: self.sub_channel_sender.clone(),
            phantom: PhantomData,
        }
    }
}

impl<T> SubSender<T>
where
    T: Serialize,
{
    /// Connect to a server, passing the server name returned from [new], to construct a [SubSender].
    ///
    /// This function must not be called more than once per [SubOneShotServer],
    /// otherwise the behaviour is unpredictable.
    /// For more information, see [issue 378](https://github.com/servo/ipc-channel/issues/378).
    ///
    /// [new]: crate::mux::SubOneShotServer::new
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn connect(name: String) -> Result<SubSender<T>, MuxError> {
        let multi_sender: Arc<Mutex<MultiSender>> = MultiSender::connect(name.clone())?;
        let sub_channel_sender: SubChannelSender = SubChannelSender::new(Arc::clone(&multi_sender));
        MultiSender::notify_sub_channel(multi_sender, sub_channel_sender.sub_channel_id(), name)?;
        Ok(SubSender {
            sub_channel_sender,
            phantom: PhantomData,
        })
    }

    /// Send a message across the subchannel to the [SubReceiver].
    ///
    /// A successful send occurs when the corresponding [SubReceiver] has not already been deallocated and the process
    /// containing the [SubReceiver] has not already terminated. Ok is returned, but the message will not necessarily be
    /// received: [recv] might not to be called to receive the message or the corresponding [SubReceiver] might be
    /// deallocated, or the [SubReceiver]'s process might terminate, before [recv] is called to receive the message.
    ///
    /// An unsuccessful send occurs when the corresponding [SubReceiver] has already been deallocated or the
    /// [SubReceiver]'s process has already terminated. Err is returned and the message will never be received.
    ///
    /// This method will never block the current thread.
    ///
    /// [recv]: crate::mux::SubReceiver::recv
    #[instrument(level = "debug", skip(self, data), err(level = "debug"))]
    pub fn send(&self, data: T) -> Result<(), MuxError> {
        self.sub_channel_sender.send(data)
    }

    /// Convert a SubSender to an OpaqueSubSender by erasing its message type.
    pub fn to_opaque(self) -> OpaqueSubSender {
        OpaqueSubSender {
            sub_channel_sender: self.sub_channel_sender,
        }
    }
}

/// OpaqueSubSender is a SubSender with the message type erased. It can be
/// passed around in a message type independent manner, but must be converted
/// into a SubSender before it can be used to send messages.
#[derive(Deserialize, Serialize)]
pub struct OpaqueSubSender {
    sub_channel_sender: SubChannelSender,
}

impl OpaqueSubSender {
    /// Convert an OpaqueSubSender to a SubSender by restoring its message type.
    /// If the message type and the original message type have incompatible
    /// serial representations, deserialization may produce errors or unexpected
    /// deserialized values.
    pub fn to<'de, T>(self) -> SubSender<T>
    where
        T: Deserialize<'de> + Serialize,
    {
        SubSender {
            sub_channel_sender: self.sub_channel_sender,
            phantom: PhantomData,
        }
    }
}

/// SubReceiver is the receiving end of a subchannel, used to receive and deserialize messages of a given type.
#[derive(Debug)]
pub struct SubReceiver<T>
where
    T: for<'x> Deserialize<'x> + Serialize,
{
    sub_channel_receiver: SubChannelReceiver,
    phantom: PhantomData<T>,
}

/// SubReceiver2 is the receiving end of a subchannel which will be added to a SubReceiverSet.
#[derive(Debug)]
pub struct SubReceiver2<T>
where
    T: for<'x> Deserialize<'x> + Serialize,
{
    sub_channel_receiver: SubChannelReceiver2,
    phantom: PhantomData<T>,
}

impl<T> SubReceiver<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    /// Waits for, and returns, a message from the channel or returns an error if all corresponding [SubSender]s have
    /// disconnected (have been deallocated, their processes have terminated, or they have been lost in transmission).
    ///
    /// This method will always block the current thread if no messages are available and it’s possible for more messages
    /// to be sent (at least one [SubSender] still exists). Once a message is sent to a corresponding [SubSender],
    /// this method will wake up and return a message.
    ///
    /// If all the corresponding [SubSender]s have disconnected while this method is blocking, this method will wake up
    /// and return Err to indicate that no more messages can ever be received on this subchannel. However, since
    /// subchannels are buffered, messages sent before the [SubSender]s disconnect can still be properly received.
    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    pub fn recv(&self) -> Result<T, MuxError> {
        self.sub_channel_receiver.recv()
    }

    // pub fn try_recv(&self) -> Result<T, TryRecvError> {
    // }

    // pub fn try_recv_timeout(&self, duration: Duration) -> Result<T, TryRecvError> {
    // }

    /// Convert a SubReceiver to an OpaqueSubReceiver by erasing its message type.
    ///
    /// Useful for adding routes to a `RouterProxy`.
    pub fn to_opaque(self) -> OpaqueSubReceiver {
        OpaqueSubReceiver {
            sub_channel_receiver: self.sub_channel_receiver,
        }
    }
}

impl<T> SubReceiver2<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    /// Convert a SubReceiver2 to an OpaqueSubReceiver2 by erasing its message type.
    ///
    /// Useful for adding routes to a `RouterProxy`.
    pub fn to_opaque(self) -> OpaqueSubReceiver2 {
        OpaqueSubReceiver2 {
            sub_channel_receiver: self.sub_channel_receiver,
        }
    }
}

/// OpaqueSubReceiver is a SubReceiver with the message type erased. It can be
/// passed around in a message type independent manner, but must be converted
/// into a SubReceiver before it can be used to receive messages.
pub struct OpaqueSubReceiver {
    sub_channel_receiver: SubChannelReceiver,
}

/// OpaqueSubReceiver2 is a SubReceiver2 with the message type erased. It can be
/// passed around in a message type independent manner, but must be converted
/// into a SubReceiver2 before it can be used to receive messages.
pub struct OpaqueSubReceiver2 {
    sub_channel_receiver: SubChannelReceiver2,
}

impl OpaqueSubReceiver {
    /// Convert an OpaqueSubReceiver to a SubReceiver by restoring its message type.
    /// If the message type and the original message type have incompatible
    /// serial representations, deserialization may produce errors or unexpected
    /// deserialized values.
    pub fn to<'de, T>(self) -> SubReceiver<T>
    where
        T: for<'x> Deserialize<'x> + Serialize,
    {
        SubReceiver {
            sub_channel_receiver: self.sub_channel_receiver,
            phantom: PhantomData,
        }
    }
}

impl OpaqueSubReceiver2 {
    /// Convert an OpaqueSubReceiver2 to a SubReceiver2 by restoring its message type.
    /// If the message type and the original message type have incompatible
    /// serial representations, deserialization may produce errors or unexpected
    /// deserialized values.
    pub fn to<'de, T>(self) -> SubReceiver2<T>
    where
        T: for<'x> Deserialize<'x> + Serialize,
    {
        SubReceiver2 {
            sub_channel_receiver: self.sub_channel_receiver,
            phantom: PhantomData,
        }
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

#[derive(Eq, Clone, Copy, Debug, Hash, PartialEq, Serialize, Deserialize)]
struct ClientId(Uuid);

#[derive(Eq, Clone, Copy, Debug, Hash, PartialEq)]
struct SubChannelId(Uuid);

impl SubChannelId {
    fn new() -> SubChannelId {
        SubChannelId(Uuid::new_v4())
    }
}

impl Serialize for SubChannelId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.to_string().serialize(serializer)
    }
}

impl Display for SubChannelId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}

impl<'de> Deserialize<'de> for SubChannelId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let content: String = String::deserialize(deserializer)?;
        let uuid = Uuid::parse_str(&content).unwrap(); // FIXME: percolate this error
        Ok(SubChannelId(uuid))
    }
}

/// Sending end of a multiplexed channel.
///
/// [MultiSender]: struct.MultiSender.html
struct MultiSender {
    client_id: ClientId,
    ipc_sender: Arc<IpcSender<MultiMessage>>,
    uuid: Uuid,
    sender_id: Arc<Mutex<Source<Weak<IpcSender<MultiMessage>>>>>,
    response_receiver: IpcReceiver<MultiResponse>,
    sub_receiver_proxies: Mutex<HashMap<SubChannelId, subchannel_lifecycle::SubReceiverProxy>>,
}

impl fmt::Debug for MultiSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiSender")
            .field("client_id", &self.client_id)
            .field("uuid", &self.uuid)
            .finish_non_exhaustive()
    }
}

/// This enumeration lists the possible reasons for failure of functions and methods in the [mux]
/// module.
///
/// [mux]: crate::mux
#[derive(Debug, Error)]
pub enum MuxError {
    /// An error has occurred while receiving a message from the IPC channel underlying a subchannel.
    #[error("IPC error: {0}")]
    IpcError(#[from] IpcError),
    /// No more messages may be received.
    ///
    /// Returned from [send] when the subchannel's [SubReceiver] has disconnected (has been
    /// deallocated or its process has terminated) and no more messages can be received.
    ///
    /// Returned from [recv] or [accept] when all the subchannel’s [SubSender]s have disconnected (have been
    /// deallocated or their processes have terminated) and no more messages are available to be received.
    ///
    /// [send]: crate::mux::SubSender::send
    /// [recv]: crate::mux::SubReceiver::recv
    /// [accept]: crate::mux::SubOneShotServer::accept
    #[error("Disconnected")]
    Disconnected,
    /// An internal logic error has occurred.
    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<std::io::Error> for MuxError {
    fn from(err: std::io::Error) -> MuxError {
        MuxError::IpcError(IpcError::Io(err))
    }
}

impl From<postcard::Error> for MuxError {
    fn from(err: postcard::Error) -> MuxError {
        MuxError::IpcError(IpcError::SerializationError(err.into()))
    }
}

impl MultiSender {
    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn connect(name: String) -> Result<Arc<Mutex<MultiSender>>, MuxError> {
        let sender = Arc::new(IpcSender::connect(name)?);
        Self::connect_sender(sender, Uuid::new_v4())
    }

    #[instrument(level = "trace", ret, err(level = "trace"))]
    fn connect_sender(
        sender: Arc<IpcSender<MultiMessage>>,
        ipc_sender_uuid: Uuid,
    ) -> Result<Arc<Mutex<MultiSender>>, MuxError> {
        let (response_sender, response_receiver) = ipc::channel()?;
        let client_id = ClientId(Uuid::new_v4());
        sender.send(MultiMessage::Connect(response_sender, client_id))?;
        Ok(Arc::new(Mutex::new(MultiSender {
            client_id,
            ipc_sender: sender,
            uuid: ipc_sender_uuid,
            sender_id: Arc::new(Mutex::new(Source::new())),
            response_receiver,
            sub_receiver_proxies: Mutex::new(HashMap::new()),
        })))
    }

    #[instrument(level = "debug", err(level = "debug"))]
    fn notify_sub_channel(
        raw_self: Arc<Mutex<MultiSender>>,
        sub_channel_id: SubChannelId,
        name: String,
    ) -> Result<(), MuxError> {
        Ok(raw_self
            .lock()
            .unwrap()
            .ipc_sender
            .send(MultiMessage::SubChannelId(sub_channel_id, name))?)
    }

    #[instrument(level = "trace", ret)]
    fn is_receiver_connected(&self, scid: SubChannelId) -> bool {
        while let Ok(MultiResponse::SubReceiverDisconnected(disconnected_scid)) =
            self.response_receiver.try_recv()
        {
            if let Some(proxy) = self
                .sub_receiver_proxies
                .lock()
                .unwrap()
                .get(&disconnected_scid)
            {
                proxy.disconnect();
            }
        }
        if let Some(proxy) = self.sub_receiver_proxies.lock().unwrap().get(&scid) {
            !proxy.disconnected()
        } else {
            true
        }
    }
}

/// Receiving end of a multiplexed channel.
///
/// [MultiReceiver]: struct.MultiReceiver.html
#[derive(Debug)]
struct MultiReceiver {
    ipc_receiver_uuid: Uuid,
    receiver_demuxer: ReceiverDemuxer,
}

#[derive(Debug)]
struct MultiReceiver2 {
    ipc_receiver_uuid: Uuid,
    receiver_demuxer: ReceiverDemuxer2,
}

#[derive(Debug)]
struct ReceiverDemuxer {
    // When receiving from the IPC receiver, the Demuxer must be locked to
    // ensure messages are received in order.
    ipc_receiver_or_multi_receiver_set: IpcReceiverOrMultiReceiverSet,
    demuxer: Arc<Mutex<Demuxer>>,
}

#[derive(Debug)]
struct ReceiverDemuxer2 {
    // When receiving from the IPC receiver, the Demuxer must be locked to
    // ensure messages are received in order.
    ipc_receiver_or_multi_receiver_set: Mutex<IpcReceiverOrMultiReceiverSet>,
    demuxer: Arc<Mutex<Demuxer>>,
}

unsafe impl Send for MultiReceiver {}
unsafe impl Sync for MultiReceiver {}

type ProtoSender = (
    SubChannelId,
    Arc<Mutex<MultiSender>>,
    Uuid,
    Arc<SubSenderTracker<dyn Fn() + Send + Sync>>,
);

struct ResolvedMessage {
    scid: SubChannelId,
    payload: Vec<u8>,
    senders: VecDeque<ProtoSender>,
}

#[derive(Debug)]
enum IpcReceiverOrMultiReceiverSet {
    IpcReceiver(IpcReceiver<MultiMessage>),
    MultiReceiverSet(Arc<Mutex<MultiReceiverSet>>),
}

#[derive(Debug)]
enum TryRecvError {
    MuxError(MuxError),
    Empty,
    Handled,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::MuxError(multiplex_error) => {
                write!(f, "TryRecvError::MuxError({})", multiplex_error)
            },
            TryRecvError::Empty => write!(f, "TryRecvError::Empty"),
            TryRecvError::Handled => write!(f, "TryRecvError::Handled"),
        }
    }
}

impl IpcReceiverOrMultiReceiverSet {
    #[instrument(level = "trace", ret)]
    fn try_recv_timeout(&self, duration: Duration) -> Result<MultiMessage, TryRecvError> {
        match self {
            IpcReceiverOrMultiReceiverSet::IpcReceiver(ipc_receiver) => {
                match ipc_receiver.try_recv_timeout(duration) {
                    Ok(multi_message) => return Ok(multi_message),
                    Err(ipc_channel::TryRecvError::IpcError(ipc_error)) => {
                        return Err(TryRecvError::MuxError(MuxError::IpcError(ipc_error)));
                    },
                    Err(ipc_channel::TryRecvError::Empty) => return Err(TryRecvError::Empty),
                }
            },
            IpcReceiverOrMultiReceiverSet::MultiReceiverSet(_multi_receiver_set) => {
                unimplemented!(
                    "calling this from a locked MultiReceiver mutator results in deadlock"
                );
            },
        }
    }

    #[instrument(level = "trace", ret)]
    fn try_recv(&self) -> Result<MultiMessage, TryRecvError> {
        match self {
            IpcReceiverOrMultiReceiverSet::IpcReceiver(ipc_receiver) => {
                match ipc_receiver.try_recv() {
                    Ok(multi_message) => return Ok(multi_message),
                    Err(ipc_channel::TryRecvError::IpcError(ipc_error)) => {
                        return Err(TryRecvError::MuxError(MuxError::IpcError(ipc_error)));
                    },
                    Err(ipc_channel::TryRecvError::Empty) => return Err(TryRecvError::Empty),
                }
            },
            IpcReceiverOrMultiReceiverSet::MultiReceiverSet(_) => {
                unimplemented!();
            },
        }
    }

    #[instrument(level = "trace", ret)]
    fn recv(&self) -> Result<MultiMessage, MuxError> {
        match self {
            IpcReceiverOrMultiReceiverSet::IpcReceiver(ipc_receiver) => match ipc_receiver.recv() {
                Ok(multi_message) => return Ok(multi_message),
                Err(e) => return Err(MuxError::IpcError(e)),
            },
            IpcReceiverOrMultiReceiverSet::MultiReceiverSet(_) => {
                panic!("IpcReceiver not set");
            },
        }
    }

    fn is_ipc_receiver(&self) -> bool {
        match self {
            IpcReceiverOrMultiReceiverSet::IpcReceiver(_) => true,
            IpcReceiverOrMultiReceiverSet::MultiReceiverSet(_) => false,
        }
    }

    fn swap(&mut self, mrs: Arc<Mutex<MultiReceiverSet>>) -> IpcReceiver<MultiMessage> {
        let prev = std::mem::replace(self, IpcReceiverOrMultiReceiverSet::MultiReceiverSet(mrs));
        match prev {
            IpcReceiverOrMultiReceiverSet::IpcReceiver(ipc_receiver) => ipc_receiver,
            IpcReceiverOrMultiReceiverSet::MultiReceiverSet(_) => panic!("already swapped"),
        }
    }

    fn multi_receiver_set(&self) -> Option<Arc<Mutex<MultiReceiverSet>>> {
        match self {
            IpcReceiverOrMultiReceiverSet::MultiReceiverSet(mrs) => Some(Arc::clone(mrs)),
            IpcReceiverOrMultiReceiverSet::IpcReceiver(_) => None,
        }
    }
}

type SubSenderStateMachine = subchannel_lifecycle::SubSenderStateMachine<
    mpsc::Sender<ResolvedMessageOrDisconnect>,
    ResolvedMessageOrDisconnect,
    mpsc::SendError<ResolvedMessageOrDisconnect>,
    Uuid,
    SubChannelId,
    dyn Fn() -> bool + Send,
>;

struct Demuxer {
    ipc_senders: HashMap<ClientId, IpcSender<MultiResponse>>,
    sub_channels: HashMap<SubChannelId, SubSenderStateMachine>,
    disconnectors: WeakValueHashMap<SubChannelId, Weak<SubSenderTracker<dyn Fn() + Send + Sync>>>,
    ipc_senders_by_id: Target<Arc<Mutex<MultiSender>>>,
}

impl std::fmt::Debug for Demuxer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiReceiverMutator")
            .field("ipc_senders", &self.ipc_senders)
            .field("sub_channels", &self.sub_channels)
            .finish_non_exhaustive()
    }
}

impl Demuxer {
    #[instrument(level = "debug", ret, err(level = "debug"))]
    #[allow(clippy::too_many_lines)]
    fn handle(
        self: &mut Demuxer,
        msg: MultiMessage,
        multi_receiver_uuid: Uuid,
    ) -> Result<(), MuxError> {
        match msg {
            MultiMessage::Connect(sender, client_id) => {
                self.ipc_senders.insert(client_id, sender);
                Ok(())
            },

            MultiMessage::Data(scid, payload, ipc_senders) => {
                let srs: VecDeque<ProtoSender> = ipc_senders
                    .clone()
                    .iter()
                    .map(|(scid, s)| {
                        let ipc_sender = Self::ipcsender_from_sender_and_or_id(self, s).unwrap();
                        let i = {
                            let l = ipc_sender.lock().unwrap();
                            l.ipc_sender.clone()
                        };
                        let j = ipc_sender.clone();
                        let disc = if let Some(disc) = self.disconnectors.get(scid) {
                            disc
                        } else {
                            let scid_copy = *scid;
                            let ipc_sender_clone = i.clone();
                            let source_copy = multi_receiver_uuid;
                            let multi_sender_clone = ipc_sender.clone();
                            let disconnector: Arc<
                                SubSenderTracker<dyn Fn() + Send + Sync + 'static>,
                            > = Arc::new(SubSenderTracker::new(Box::new(move || {
                                let d = SubChannelDisconnector {
                                    sub_channel_id: scid_copy,
                                    ipc_sender: ipc_sender_clone.clone(),
                                    source: source_copy,
                                    multi_sender: multi_sender_clone.clone(),
                                };
                                d.dropped();
                            })));
                            self.disconnectors.insert(*scid, Arc::clone(&disconnector));
                            disconnector.clone()
                        };

                        (*scid, j, multi_receiver_uuid, disc)
                    })
                    .collect();

                let result: Option<Result<(), mpsc::SendError<ResolvedMessageOrDisconnect>>> =
                    if let Some(sm) = self.sub_channels.get(&scid) {
                        sm.send(ResolvedMessageOrDisconnect::ResolvedMessage(
                            ResolvedMessage {
                                scid,
                                payload,
                                senders: srs,
                            },
                        ))
                    } else {
                        // Send ReceiveFailed to members of srs
                        // TODO: Need to test this path
                        for (recv_scid, recv_multi_sender, _, _) in srs {
                            {
                                let _ = recv_multi_sender.lock().unwrap().ipc_sender.send(
                                    MultiMessage::ReceiveFailed {
                                        scid: recv_scid,
                                        via: scid,
                                    },
                                );
                            }
                        }
                        Err(MuxError::InternalError(format!(
                            "invalid subchannel id {}",
                            scid
                        )))?
                    };

                if let Some(Ok(())) = result {
                    Ok(())
                } else {
                    Err(MuxError::Disconnected)
                }
            },
            MultiMessage::Disconnect(scid, source) => {
                log::trace!("Processing MultiMessage::Disconnect");
                #[allow(clippy::collapsible_if)] // unstable in MRSV
                if let Some(sm) = self.sub_channels.get(&scid) {
                    log::trace!("About to send disconnect to SubSenderStateMachine");
                    if let Some(sender) = sm.disconnect(source) {
                        let _ = sender.send(ResolvedMessageOrDisconnect::Disconnect(scid));
                    }
                }

                Ok(())
            },
            MultiMessage::Sending {
                scid,
                via,
                via_chan,
            } => {
                let ipc_sender = Self::ipcsender_from_sender_and_or_id(self, &via_chan);

                if let Some(sm) = self.sub_channels.get(&scid) {
                    if let Ok(ipc_sender) = ipc_sender {
                        sm.to_be_sent(
                            via,
                            Box::new(move || probe(ipc_sender.lock().unwrap().ipc_sender.clone())),
                        );
                    } else {
                        log::trace!(
                            "Error processing Sending message: {:?}",
                            ipc_sender.err().unwrap()
                        );
                        sm.to_be_sent(via, Box::new(|| false));
                        if let Some(sender) = sm.receive_failed(&via) {
                            let _ = sender.send(ResolvedMessageOrDisconnect::Disconnect(scid));
                        }
                    }
                }

                Ok(())
            },
            MultiMessage::ReceiveFailed { scid, via } => {
                if let Some(sm) = self.sub_channels.get(&scid) {
                    if let Some(sender) = sm.receive_failed(&via) {
                        let _ = sender.send(ResolvedMessageOrDisconnect::Disconnect(scid));
                    }
                }

                Ok(())
            },
            MultiMessage::Received {
                scid,
                via,
                new_source,
            } => {
                if let Some(sm) = self.sub_channels.get(&scid) {
                    sm.received(&via, new_source);
                }

                Ok(())
            },
            MultiMessage::Probe() => Ok(()), // ignore probe messages
            m @ MultiMessage::SubChannelId(..) => Err(MuxError::InternalError(format!(
                "unexpected multi message {:?}",
                m
            ))),
        }
    }

    fn process_results(results: IdSenderResults) -> Result<IdSenders, MuxError> {
        let mut srs: IdSenders = VecDeque::new();
        for (scid, res, uuid, disc) in results {
            srs.push_back((scid, res?, uuid, disc));
        }
        Ok(srs)
    }

    fn ipcsender_from_sender_and_or_id(
        self: &mut Demuxer,
        s: &IpcSenderAndOrId,
    ) -> Result<Arc<Mutex<MultiSender>>, MuxError> {
        match s {
            IpcSenderAndOrId::IpcSender(s, id) => {
                let uuid = Uuid::parse_str(id).unwrap();
                let multi_sender = MultiSender::connect_sender(Arc::new(s.clone()), uuid)?;
                log::trace!("associating {} with a MultiSender", uuid);
                self.ipc_senders_by_id.add(uuid, &multi_sender);
                log::trace!("association complete");
                Ok(multi_sender)
            },
            IpcSenderAndOrId::IpcSenderId(id) => {
                let uuid = Uuid::parse_str(id).unwrap();
                log::trace!("looking up MultiSender associated with {}", uuid);
                let maybe_sender: Option<Arc<Mutex<MultiSender>>> =
                    self.ipc_senders_by_id.look_up(uuid);
                log::trace!("result of looking up MultiSender is {:?}", maybe_sender);
                if let Some(sender) = maybe_sender {
                    Ok(sender)
                } else {
                    Err(MuxError::Disconnected)
                }
            },
        }
    }

    fn send(
        self: &mut Demuxer,
        scid: SubChannelId,
        payload: Vec<u8>,
        ipc_senders: &[(SubChannelId, IpcSenderAndOrId)],
        mr_clone: &Arc<MultiReceiver2>,
    ) -> Result<(), MuxError> {
        let srs = Demuxer::process_results(
            ipc_senders
                .iter()
                .map(|(scid, s)| {
                    (
                        *scid,
                        self.ipcsender_from_sender_and_or_id(s),
                        mr_clone.ipc_receiver_uuid,
                        self.disconnectors.get(scid).unwrap(), // FIXME: can this panic?
                    )
                })
                .collect(),
        );
        if let Ok(srs) = srs {
            if let Some(sm) = self.sub_channels.get(&scid) {
                let sent = sm.send(ResolvedMessageOrDisconnect::ResolvedMessage(
                    ResolvedMessage {
                        scid,
                        payload,
                        senders: srs,
                    },
                ));
                if let Some(result) = sent {
                    result.map_err(|_| MuxError::Disconnected)
                } else {
                    Err(MuxError::Disconnected)
                }
            } else {
                // Send ReceiveFailed to members of srs
                // TODO: Need to test this path
                for (recv_scid, recv_multi_sender, _, _) in srs {
                    {
                        let _ = recv_multi_sender.lock().unwrap().ipc_sender.send(
                            MultiMessage::ReceiveFailed {
                                scid: recv_scid,
                                via: scid,
                            },
                        );
                    }
                }
                Err(MuxError::Disconnected)
            }
        } else {
            srs.map(|_| ())
        }
    }
}

thread_local! {
    static IPC_SENDERS_RECEIVED: Mutex<VecDeque<ProtoSender>> = const { Mutex::new(VecDeque::new()) };
    static VIA: Mutex<SubChannelId> = const { Mutex::new(EMPTY_SUBCHANNEL_ID) };
}

fn establish_deserialization_context(mut multi_senders: VecDeque<ProtoSender>, via: SubChannelId) {
    IPC_SENDERS_RECEIVED.with(|senders| {
        senders.lock().unwrap().clear();
        senders.lock().unwrap().append(&mut multi_senders);
    });
    VIA.with(|via_val| {
        let mut v = via_val.lock().unwrap();
        *v = via;
    });
}

fn clear_deserialization_context() {
    VIA.with(|via| {
        let mut v = via.lock().unwrap();
        *v = EMPTY_SUBCHANNEL_ID;
    });
    IPC_SENDERS_RECEIVED.with(|senders| {
        senders.lock().unwrap().clear();
    });
}

impl
    subchannel_lifecycle::Sender<
        ResolvedMessageOrDisconnect,
        mpsc::SendError<ResolvedMessageOrDisconnect>,
    > for Sender<ResolvedMessageOrDisconnect>
{
    fn send(
        &self,
        msg: ResolvedMessageOrDisconnect,
    ) -> Result<(), mpsc::SendError<ResolvedMessageOrDisconnect>> {
        self.send(msg)
    }
}

enum ResolvedMessageOrDisconnect {
    ResolvedMessage(ResolvedMessage),
    Disconnect(SubChannelId),
}

type IdSenders = VecDeque<(
    SubChannelId,
    Arc<Mutex<MultiSender>>,
    Uuid,
    Arc<SubSenderTracker<dyn Fn() + Send + Sync>>,
)>;

type IdSenderResults = VecDeque<(
    SubChannelId,
    Result<Arc<Mutex<MultiSender>>, MuxError>,
    Uuid,
    Arc<SubSenderTracker<dyn Fn() + Send + Sync>>,
)>;

impl MultiReceiver {
    #[instrument(level = "debug", ret)]
    fn attach(mr: &Arc<MultiReceiver>, sub_channel_id: SubChannelId) -> SubChannelReceiver {
        let (tx, rx): (
            Sender<ResolvedMessageOrDisconnect>,
            Receiver<ResolvedMessageOrDisconnect>,
        ) = mpsc::channel();
        mr.receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .sub_channels
            .insert(
                sub_channel_id,
                subchannel_lifecycle::SubSenderStateMachine::new(tx, ORIGIN),
            );
        SubChannelReceiver {
            multi_receiver: Arc::clone(mr),
            sub_channel_id,
            channel: rx,
        }
    }

    #[instrument(level = "debug", err(level = "debug"))]
    fn try_recv(mr: &Arc<MultiReceiver>) -> Result<(), TryRecvError> {
        // The following is inlined to avoid deadlock.
        let maybe_mrs = mr
            .receiver_demuxer
            .ipc_receiver_or_multi_receiver_set
            .multi_receiver_set();
        if maybe_mrs.is_some() {
            // No need to attempt to receive messages because
            // SubChannelReceiver::drop in this case can only occur
            // when the router is shutting down, in which case
            // message receiving is irrelevant.
            return Err(TryRecvError::Empty);
        }
        let result = mr
            .receiver_demuxer
            .ipc_receiver_or_multi_receiver_set
            .try_recv();
        match result {
            Ok(msg) => {
                mr.receiver_demuxer
                    .demuxer
                    .lock()
                    .unwrap()
                    .handle(msg, mr.ipc_receiver_uuid)
                    .map_err(TryRecvError::MuxError)?;
                Err(TryRecvError::Handled)
            },
            Err(TryRecvError::Handled) => Ok(()),
            Err(e) => Err(e),
        }
    }

    //#[instrument(level = "debug", err(level = "debug"))]
    #[inline(always)]
    fn try_recv_timeout(mr: &Arc<MultiReceiver>, duration: Duration) -> Result<(), TryRecvError> {
        let result = mr
            .receiver_demuxer
            .ipc_receiver_or_multi_receiver_set
            .try_recv_timeout(duration);
        match result {
            Ok(msg) => mr
                .receiver_demuxer
                .demuxer
                .lock()
                .unwrap()
                .handle(msg, mr.ipc_receiver_uuid)
                .map_err(TryRecvError::MuxError),
            Err(TryRecvError::Empty) => {
                mr.poll();
                Ok(())
            },
            Err(TryRecvError::Handled) => Ok(()),
            Err(e) => Err(e),
        }
    }

    #[instrument(level = "debug")]
    fn drain(mr: &Arc<MultiReceiver>) {
        loop {
            let result = Self::try_recv(mr);
            match result {
                Ok(()) => {},
                Err(_) => break,
            }
        }
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn receive_sub_channel(mr: &Arc<MultiReceiver>) -> Result<(SubChannelId, String), MuxError> {
        let msg = mr
            .receiver_demuxer
            .ipc_receiver_or_multi_receiver_set
            .recv()?;
        match msg {
            MultiMessage::SubChannelId(sub_channel_id, name) => Ok((sub_channel_id, name)),
            m => Err(MuxError::InternalError(format!(
                "unexpected multi message {:?}",
                m
            ))),
        }
    }

    // poll returns true if and only if a probe failed.
    #[instrument(level = "trace", ret)]
    fn poll(&self) -> bool {
        let probe_failed = Mutex::new(false);
        self.receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .sub_channels
            .iter()
            .for_each(|(scid, subsender_state_machine)| {
                let (poll, v) = subsender_state_machine.poll();
                if !poll {
                    let _ = v
                        .unwrap()
                        .send(ResolvedMessageOrDisconnect::Disconnect(*scid));
                    let mut p = probe_failed.lock().unwrap();
                    *p = true;
                }
            });
        #[allow(clippy::clone_on_copy)]
        let result = probe_failed.lock().unwrap().clone();
        result
    }
}

impl MultiReceiver2 {
    #[instrument(level = "debug", ret)]
    fn attach(mr: &Arc<MultiReceiver2>, sub_channel_id: SubChannelId) -> SubChannelReceiver2 {
        let (tx, rx): (
            Sender<ResolvedMessageOrDisconnect>,
            Receiver<ResolvedMessageOrDisconnect>,
        ) = mpsc::channel();
        mr.receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .sub_channels
            .insert(
                sub_channel_id,
                subchannel_lifecycle::SubSenderStateMachine::new(tx, ORIGIN),
            );
        SubChannelReceiver2 {
            multi_receiver: Arc::clone(mr),
            sub_channel_id,
            channel: rx,
        }
    }

    #[instrument(level = "debug", err(level = "debug"))]
    fn try_recv(mr: &Arc<MultiReceiver2>) -> Result<(), TryRecvError> {
        // The following is inlined to avoid deadlock.
        let maybe_mrs = mr
            .receiver_demuxer
            .ipc_receiver_or_multi_receiver_set
            .lock()
            .unwrap()
            .multi_receiver_set();
        if maybe_mrs.is_some() {
            // No need to attempt to receive messages because
            // SubChannelReceiver::drop in this case can only occur
            // when the router is shutting down, in which case
            // message receiving is irrelevant.
            return Err(TryRecvError::Empty);
        }
        let result = mr
            .receiver_demuxer
            .ipc_receiver_or_multi_receiver_set
            .lock()
            .unwrap()
            .try_recv();
        match result {
            Ok(msg) => {
                Self::handle(Arc::clone(mr), msg).map_err(TryRecvError::MuxError)?;
                Err(TryRecvError::Handled)
            },
            Err(TryRecvError::Handled) => Ok(()),
            Err(e) => Err(e),
        }
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn handle(mr: Arc<MultiReceiver2>, msg: MultiMessage) -> Result<(), MuxError> {
        let demuxer = &mut mr.receiver_demuxer.demuxer.lock().unwrap();
        if let MultiMessage::Data(scid, payload, ipc_senders) = msg {
            demuxer.send(scid, payload, &ipc_senders, &mr)
        } else {
            demuxer.handle(msg, mr.ipc_receiver_uuid)
        }
    }

    #[instrument(level = "debug")]
    fn drain(mr: &Arc<MultiReceiver2>) {
        loop {
            let result = Self::try_recv(mr);
            match result {
                Ok(()) => {},
                Err(_) => break,
            }
        }
    }

    // poll returns true if and only if a probe failed.
    #[instrument(level = "trace", ret)]
    fn poll(&self) -> bool {
        let probe_failed = Mutex::new(false);
        self.receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .sub_channels
            .iter()
            .for_each(|(scid, subsender_state_machine)| {
                let (poll, v) = subsender_state_machine.poll();
                if !poll {
                    let _ = v
                        .unwrap()
                        .send(ResolvedMessageOrDisconnect::Disconnect(*scid));
                    let mut p = probe_failed.lock().unwrap();
                    *p = true;
                }
            });
        #[allow(clippy::clone_on_copy)]
        let result = probe_failed.lock().unwrap().clone();
        result
    }
}

#[instrument(level = "trace", ret)]
fn probe(ipc_sender: Arc<IpcSender<MultiMessage>>) -> bool {
    ipc_sender.send(MultiMessage::Probe()).is_ok()
}

struct SubChannelDisconnector {
    sub_channel_id: SubChannelId,
    ipc_sender: Arc<IpcSender<MultiMessage>>,
    source: Uuid,
    multi_sender: Arc<Mutex<MultiSender>>,
}

impl SubChannelDisconnector {
    fn dropped(&self) {
        if self
            .multi_sender
            .lock()
            .unwrap()
            .is_receiver_connected(self.sub_channel_id)
        {
            // Ignore any error sending disconnect message as it is not needed if the other end has hung up.
            let _ = self
                .ipc_sender
                .send(MultiMessage::Disconnect(self.sub_channel_id, self.source));
        }
    }
}

struct SubChannelSender {
    sub_channel_id: SubChannelId,
    ipc_sender: Arc<IpcSender<MultiMessage>>,
    disconnector: Arc<subchannel_lifecycle::SubSenderTracker<dyn Fn() + Send + Sync>>,
    ipc_sender_uuid: Uuid,
    sender_id: Arc<Mutex<Source<Weak<IpcSender<MultiMessage>>>>>,
    multi_sender: Arc<Mutex<MultiSender>>,
}

impl Clone for SubChannelSender {
    fn clone(&self) -> SubChannelSender {
        SubChannelSender {
            sub_channel_id: self.sub_channel_id,
            ipc_sender: Arc::clone(&self.ipc_sender),
            disconnector: Arc::clone(&self.disconnector),
            ipc_sender_uuid: self.ipc_sender_uuid,
            sender_id: Arc::clone(&self.sender_id),
            multi_sender: Arc::clone(&self.multi_sender),
        }
    }
}

impl SubChannelSender {
    #[instrument(level = "debug", ret)]
    fn new(raw_self: Arc<Mutex<MultiSender>>) -> Self {
        let locked_self = raw_self.lock().unwrap();
        let scid = SubChannelId::new();
        let sender_clone = locked_self.ipc_sender.clone();
        let multi_sender_clone = raw_self.clone();
        SubChannelSender {
            sub_channel_id: scid,
            ipc_sender: locked_self.ipc_sender.clone(),
            disconnector: Arc::new(SubSenderTracker::new(Box::new(move || {
                let d = SubChannelDisconnector {
                    sub_channel_id: scid,
                    ipc_sender: sender_clone.clone(),
                    source: ORIGIN,
                    multi_sender: multi_sender_clone.clone(),
                };
                d.dropped();
            }))),
            ipc_sender_uuid: locked_self.uuid,
            sender_id: Arc::clone(&locked_self.sender_id),
            multi_sender: Arc::clone(&raw_self),
        }
    }

    #[instrument(level = "debug", skip(msg), err(level = "debug"))]
    fn send<T>(&self, msg: T) -> Result<(), MuxError>
    where
        T: Serialize,
    {
        log::debug!(">SubChannelSender::send");
        if !self
            .multi_sender
            .lock()
            .unwrap()
            .is_receiver_connected(self.sub_channel_id)
        {
            return Err(MuxError::Disconnected);
        }
        clear_serialization_context();

        let payload = postcard::to_stdvec(&msg).map_err(MuxError::from)?;

        let (serialized_subchannel_senders, ipc_senders_to_send) = take_serialization_context();

        // Notify transmission of any subchannel senders so that they are counted during transmission.
        for (subchannel_id, ipc_sender, sender_id) in serialized_subchannel_senders {
            {
                let _ = ipc_sender.send(MultiMessage::Sending {
                    scid: subchannel_id,
                    via: self.sub_channel_id,
                    via_chan: Self::ipc_sender_and_or_uuid(
                        &sender_id,
                        &self.ipc_sender,
                        self.ipc_sender_uuid,
                    ),
                });
            }
        }

        let srs: Vec<(SubChannelId, IpcSenderAndOrId)> = ipc_senders_to_send
            .iter()
            .map(|ipc_sender_and_uuid| {
                (
                    ipc_sender_and_uuid.0,
                    Self::ipc_sender_and_or_uuid(
                        &self.sender_id,
                        &ipc_sender_and_uuid.2,
                        ipc_sender_and_uuid.1,
                    ),
                )
            })
            .collect();
        let result = self
            .ipc_sender
            .send(MultiMessage::Data(self.sub_channel_id, payload, srs));
        log::debug!("<SubChannelSender::send -> {:#?}", result.as_ref());
        result.map_err(From::from)
    }

    fn ipc_sender_and_or_uuid(
        sender_id: &Arc<Mutex<Source<Weak<IpcSender<MultiMessage>>>>>,
        ipc_sender: &Arc<IpcSender<MultiMessage>>,
        ipc_sender_uuid: Uuid,
    ) -> IpcSenderAndOrId {
        /* If this SubChannelSender has sent the given IpcSender
        before, send just the UUID associated with the IpcSender.
        Otherwise this is the first time this SubChannelSender
        has sent the given IpcSender, so send both the IpcSender
        and the UUID. */
        let already_sent = sender_id.lock().unwrap().insert(ipc_sender.clone());
        if already_sent {
            log::trace!(
                "sending UUID {} associated with previously sent IpcSender",
                ipc_sender_uuid
            );
            IpcSenderAndOrId::IpcSenderId(ipc_sender_uuid.to_string())
        } else {
            log::trace!("sending IpcSender with UUID {}", ipc_sender_uuid);
            IpcSenderAndOrId::IpcSender(
                Arc::<IpcSender<MultiMessage>>::unwrap_or_clone(ipc_sender.clone()),
                ipc_sender_uuid.to_string(),
            )
        }
    }

    #[instrument(level = "trace", ret)]
    fn sub_channel_id(&self) -> SubChannelId {
        self.sub_channel_id
    }
}

impl<'de> Deserialize<'de> for SubChannelSender {
    #[instrument(level = "trace", ret, skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let scsi = SubChannelSenderIds::deserialize(deserializer).unwrap(); // FIXME: handle this error gracefully

        let multi_sender = IPC_SENDERS_RECEIVED
            .with(|senders| {
                let mut binding = senders.lock().unwrap();
                let result = binding
                    .pop_front()
                    .ok_or(MuxError::InternalError(
                        "IpcSender missing from message".to_string(),
                    ))?
                    .clone();
                Ok(result)
            })
            .map_err(serde::de::Error::custom::<MuxError>)?;

        let new_source = multi_sender.2;

        let via = VIA.with(|via| *via.lock().unwrap());

        multi_sender
            .1
            .lock()
            .unwrap()
            .ipc_sender
            .send(MultiMessage::Received {
                scid: scsi.sub_channel_id,
                via,
                new_source,
            })
            .unwrap();

        let disc = multi_sender.3;
        let ipc_sender = Arc::clone(&multi_sender.1.lock().unwrap().ipc_sender);
        let ipc_sender_uuid = multi_sender.1.lock().unwrap().uuid;

        Ok(SubChannelSender {
            sub_channel_id: scsi.sub_channel_id,
            ipc_sender,
            disconnector: disc,
            ipc_sender_uuid,
            sender_id: Arc::new(Mutex::new(Source::new())),
            multi_sender: multi_sender.1,
        })
    }
}

impl fmt::Debug for SubChannelSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubChannelSender")
            .field("sub_channel_id", &self.sub_channel_id)
            .field("ipc_sender", &self.ipc_sender)
            .finish_non_exhaustive()
    }
}

type SubChannelSenderSendDetails = (SubChannelId, Uuid, Arc<IpcSender<MultiMessage>>);

type SubChannelSenderSerializedDetails = (
    SubChannelId,
    Arc<IpcSender<MultiMessage>>,
    Arc<Mutex<Source<Weak<IpcSender<MultiMessage>>>>>,
);

// TODO: rationalise the following to avoid duplication of data
thread_local! {
    static IPC_SENDERS_TO_SEND: Mutex<Vec<SubChannelSenderSendDetails>> = const {Mutex::new(vec!()) };
    static SERIALIZED_SUBCHANNEL_SENDERS: Mutex<Vec<SubChannelSenderSerializedDetails>> = const { Mutex::new(vec!()) };
}

fn clear_serialization_context() {
    IPC_SENDERS_TO_SEND.with(|senders| {
        senders.lock().unwrap().clear();
    });

    SERIALIZED_SUBCHANNEL_SENDERS.with(|subchannel_senders| {
        subchannel_senders.lock().unwrap().clear();
    });
}

fn take_serialization_context() -> (
    Vec<SubChannelSenderSerializedDetails>,
    Vec<SubChannelSenderSendDetails>, // SubChannelId, IPC sender UUID, and IpcSender of serialized SubChannelSenders
) {
    let serialized_subchannel_senders = SERIALIZED_SUBCHANNEL_SENDERS.with(|subchannel_senders| {
        let empty = Mutex::new(vec![]);
        let mut v = subchannel_senders.lock().unwrap();
        let w = empty.lock().unwrap();
        std::mem::replace(&mut v, w).to_vec()
    });

    let ipc_senders_to_send: Vec<SubChannelSenderSendDetails> =
        IPC_SENDERS_TO_SEND.with(|ipc_senders: &Mutex<Vec<SubChannelSenderSendDetails>>| {
            let empty = Mutex::new(vec![]);
            let mut v = ipc_senders.lock().unwrap();
            let w = empty.lock().unwrap();
            std::mem::replace(&mut v, w).to_vec()
        });

    (serialized_subchannel_senders, ipc_senders_to_send)
}

impl Serialize for SubChannelSender {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        log::trace!(
            "Adding SubChannelSender with SubChannelId {} to IPC_SENDERS_TO_SEND and SERIALIZED_SUBCHANNEL_SENDERS",
            self.sub_channel_id
        );

        IPC_SENDERS_TO_SEND.with(|ipc_senders| {
            ipc_senders.lock().unwrap().push((
                self.sub_channel_id,
                self.ipc_sender_uuid,
                self.ipc_sender.clone(),
            ));
        });

        SERIALIZED_SUBCHANNEL_SENDERS.with(|subchannel_senders| {
            subchannel_senders.lock().unwrap().push((
                self.sub_channel_id,
                self.ipc_sender.clone(),
                self.sender_id.clone(),
            ));
        });

        let scsi = SubChannelSenderIds {
            sub_channel_id: self.sub_channel_id,
            ipc_sender_uuid: self.ipc_sender_uuid.to_string(),
        };
        log::trace!("Serializing {:?}", scsi);
        Ok(scsi.serialize(serializer).unwrap()) // FIXME: handle this error gracefully
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SubChannelSenderIds {
    sub_channel_id: SubChannelId,
    ipc_sender_uuid: String,
}

struct SubChannelReceiver {
    multi_receiver: Arc<MultiReceiver>,
    sub_channel_id: SubChannelId,
    channel: Receiver<ResolvedMessageOrDisconnect>,
}
struct SubChannelReceiver2 {
    multi_receiver: Arc<MultiReceiver2>,
    sub_channel_id: SubChannelId,
    channel: Receiver<ResolvedMessageOrDisconnect>,
}

unsafe impl Send for SubChannelReceiver {}
unsafe impl Sync for SubChannelReceiver {}

impl Drop for SubChannelReceiver {
    fn drop(&mut self) {
        // Clear any messages in MultiReceiver (which could cause sending to block).
        let _ = MultiReceiver::try_recv(&self.multi_receiver);

        // Broadcast disconnection to all MultiSenders connected to the MultiReceiver for this SubChannelReceiver.
        // Note: This may be overkill as not all MultiSenders will have a SubChannelSender corresponding to this
        // SubChannelReceiver.
        for (client_id, sender) in &self
            .multi_receiver
            .receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .ipc_senders
        {
            log::trace!(
                "SubChannelReceiver::drop sending SubReceiverDisconnected for subchannel {:?} to client {:?}",
                self.sub_channel_id,
                client_id
            );
            let result = sender.send(MultiResponse::SubReceiverDisconnected(self.sub_channel_id));
            log::trace!("Result of sending SubReceiverDisconnected was {:?}", result);
        }

        // Drain the multireceiver.
        MultiReceiver::drain(&self.multi_receiver);

        // Drain the SubChannelReceiver and mark any subsenders as "receive failed". This is equivalent to receiving and then dropping
        // the subsenders.
        while let Ok(ResolvedMessageOrDisconnect::ResolvedMessage(ResolvedMessage {
            scid: via,
            payload: _,
            senders: scids_and_multi_senders,
        })) = self.channel.try_recv()
        {
            // log::trace!(
            //     "SubChannelReceiver::drop draining = {:#?}",
            //     scids_and_multi_senders
            // );
            for (scid, ms, _, _) in scids_and_multi_senders {
                {
                    let _ = ms
                        .lock()
                        .unwrap()
                        .ipc_sender
                        .send(MultiMessage::ReceiveFailed { scid, via });
                }
            }
        }
    }
}

impl Drop for SubChannelReceiver2 {
    fn drop(&mut self) {
        // Clear any messages in MultiReceiver (which could cause sending to block).
        let _ = MultiReceiver2::try_recv(&self.multi_receiver);

        // Broadcast disconnection to all MultiSenders connected to the MultiReceiver for this SubChannelReceiver.
        // Note: This may be overkill as not all MultiSenders will have a SubChannelSender corresponding to this
        // SubChannelReceiver.
        for (client_id, sender) in &self
            .multi_receiver
            .receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .ipc_senders
        {
            log::trace!(
                "SubChannelReceiver::drop sending SubReceiverDisconnected for subchannel {:?} to client {:?}",
                self.sub_channel_id,
                client_id
            );
            let result = sender.send(MultiResponse::SubReceiverDisconnected(self.sub_channel_id));
            log::trace!("Result of sending SubReceiverDisconnected was {:?}", result);
        }

        // Drain the multireceiver.
        MultiReceiver2::drain(&self.multi_receiver);

        // Drain the SubChannelReceiver and mark any subsenders as "receive failed". This is equivalent to receiving and then dropping
        // the subsenders.
        while let Ok(ResolvedMessageOrDisconnect::ResolvedMessage(ResolvedMessage {
            scid: via,
            payload: _,
            senders: scids_and_multi_senders,
        })) = self.channel.try_recv()
        {
            // log::trace!(
            //     "SubChannelReceiver::drop draining = {:#?}",
            //     scids_and_multi_senders
            // );
            for (scid, ms, _, _) in scids_and_multi_senders {
                {
                    let _ = ms
                        .lock()
                        .unwrap()
                        .ipc_sender
                        .send(MultiMessage::ReceiveFailed { scid, via });
                }
            }
        }
    }
}

impl fmt::Debug for SubChannelReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubChannelReceiver")
            .field("sub_channel_id", &self.sub_channel_id)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for SubChannelReceiver2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubChannelReceiver2")
            .field("sub_channel_id", &self.sub_channel_id)
            .finish_non_exhaustive()
    }
}

const POLLING_INTERVAL: Duration = Duration::from_millis(100);
const CONTENDED_WAIT_INTERVAL: Duration = Duration::from_micros(100);

impl SubChannelReceiver {
    #[instrument(level = "debug", err(level = "debug"))]
    fn recv<T>(&self) -> Result<T, MuxError>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let mut wait_interval: Option<Duration> = None;
        loop {
            let result = if let Some(interval) = wait_interval {
                self.channel.recv_timeout(interval).map_err(|e| match e {
                    mpsc::RecvTimeoutError::Timeout => mpsc::TryRecvError::Empty,
                    mpsc::RecvTimeoutError::Disconnected => mpsc::TryRecvError::Disconnected,
                })
            } else {
                self.channel.try_recv()
            };
            match result {
                Ok(ResolvedMessageOrDisconnect::ResolvedMessage(ResolvedMessage {
                    scid,
                    payload,
                    senders: multi_senders,
                })) => {
                    log::trace!("SubChannelReceiver::recv received = {:#?}", payload);

                    establish_deserialization_context(multi_senders, scid);

                    let result = postcard::from_bytes::<T>(payload.as_slice());

                    clear_deserialization_context();

                    return result.map_err(From::from);
                },
                Err(mpsc::TryRecvError::Empty) => {
                    // If the mutex is locked, wait on the local channel.
                    if self
                        .multi_receiver
                        .receiver_demuxer
                        .demuxer
                        .try_lock()
                        .is_err()
                    {
                        wait_interval = Some(CONTENDED_WAIT_INTERVAL);
                        continue;
                    }
                    wait_interval = None;
                    // receive another message, possibly for another subchannel
                    let multi_receiver_result =
                        MultiReceiver::try_recv_timeout(&self.multi_receiver, POLLING_INTERVAL);
                    log::trace!(
                        "SubChannelReceiver::recv multi_receiver_result = {:#?}",
                        multi_receiver_result.as_ref()
                    );
                    if let Err(TryRecvError::MuxError(e)) = multi_receiver_result {
                        return Err(e);
                    }
                },
                _ => {
                    return Err(MuxError::Disconnected);
                },
            }
        }
    }
}

/// MultiMessage is used to communicate across multiplexing channels.
#[derive(Serialize, Deserialize, Debug)]
enum MultiMessage {
    Connect(IpcSender<MultiResponse>, ClientId),
    Data(SubChannelId, Vec<u8>, Vec<(SubChannelId, IpcSenderAndOrId)>),
    SubChannelId(SubChannelId, String),
    Sending {
        scid: SubChannelId,
        via: SubChannelId,
        via_chan: IpcSenderAndOrId,
    },
    ReceiveFailed {
        scid: SubChannelId,
        via: SubChannelId,
    },
    Received {
        scid: SubChannelId,
        via: SubChannelId,
        new_source: Uuid,
    },
    Disconnect(SubChannelId, Uuid),
    Probe(),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
enum IpcSenderAndOrId {
    IpcSender(IpcSender<MultiMessage>, String),
    IpcSenderId(String),
}

/// MultiResponse is used to communicate from the receiver of a multiplexing channel to the sender
/// via an additional channel in the reverse direction.
#[derive(Serialize, Deserialize, Debug)]
enum MultiResponse {
    /// The SubReceiver for the subchannel identified by the given subchannel id. has disconnected (been dropped).
    SubReceiverDisconnected(SubChannelId),
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
            ipc_receiver_or_multi_receiver_set: IpcReceiverOrMultiReceiverSet::IpcReceiver(
                ipc_receiver,
            ),
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
fn multi_channel2() -> Result<(Arc<Mutex<MultiSender>>, Arc<MultiReceiver2>), io::Error> {
    let (ipc_sender, ipc_receiver) = ipc::channel()?;
    let (ipc_response_sender, ipc_response_receiver) = ipc::channel()?;
    let client_id = ClientId(Uuid::new_v4());
    let mut senders = HashMap::new();
    senders.insert(client_id, ipc_response_sender);
    let multi_receiver = MultiReceiver2 {
        ipc_receiver_uuid: Uuid::new_v4(),
        receiver_demuxer: ReceiverDemuxer2 {
            ipc_receiver_or_multi_receiver_set: Mutex::new(
                IpcReceiverOrMultiReceiverSet::IpcReceiver(ipc_receiver),
            ),
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
                ipc_receiver_or_multi_receiver_set: IpcReceiverOrMultiReceiverSet::IpcReceiver(
                    ipc_receiver,
                ),
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
pub struct SubReceiverSet {
    next_id: u64,
    rxs: HashMap<u64, SubChannelReceiver2>,
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
    pub fn add<T>(&mut self, subreceiver: SubReceiver2<T>) -> Result<u64, MuxError>
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
    pub fn add_opaque(&mut self, receiver: OpaqueSubReceiver2) -> Result<u64, MuxError> {
        if receiver
            .sub_channel_receiver
            .multi_receiver
            .receiver_demuxer
            .ipc_receiver_or_multi_receiver_set
            .lock()
            .unwrap()
            .is_ipc_receiver()
        {
            MultiReceiverSet::add(
                &self.multi_receiver_set,
                Arc::clone(&receiver.sub_channel_receiver.multi_receiver),
            )?;
        } else {
            // The subreceiver being added is already associated with a non-empty MultiReceiverSet.
            let incoming_multi_receiver_set = receiver
                .sub_channel_receiver
                .multi_receiver
                .receiver_demuxer
                .ipc_receiver_or_multi_receiver_set
                .lock()
                .unwrap()
                .multi_receiver_set()
                .unwrap();

            // If this SubReceiverSet's MultiReceiverSet is the same as the MultiReceiverSet associated
            // with the subreceiver being added, do nothing.
            if Arc::ptr_eq(&self.multi_receiver_set, &incoming_multi_receiver_set) {
            } else
            // Otherwise, if this SubReceiverSet's MultiReceiverSet is empty, we can replace it with the incoming
            // MultiReceiverSet. Otherwise, the add operation fails because we cannot merge non-empty
            // MultiReceiverSets (since IpcReceiverSets cannot be merged).
            if MultiReceiverSet::is_empty(&self.multi_receiver_set) {
                self.multi_receiver_set = receiver
                    .sub_channel_receiver
                    .multi_receiver
                    .receiver_demuxer
                    .ipc_receiver_or_multi_receiver_set
                    .lock()
                    .unwrap()
                    .multi_receiver_set()
                    .unwrap();
            } else {
                return Err(MuxError::InternalError(
                    "Cannot merge non-empty MultiReceiverSets".to_string(),
                ));
            }
        }

        // Modify MultiReceiver so that messages for the subchannel are sent to the set.
        {
            let mut demuxer = receiver
                .sub_channel_receiver
                .multi_receiver
                .receiver_demuxer
                .demuxer
                .lock()
                .unwrap();
            let sub_sender_state_machine = demuxer
                .sub_channels
                .get_mut(&receiver.sub_channel_receiver.sub_channel_id)
                .unwrap();
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
        // TODO: relax the current restriction of returning at most one SubSelectionResult.
        loop {
            match self.rx.try_recv() {
                Ok(ResolvedMessageOrDisconnect::ResolvedMessage(ResolvedMessage {
                    scid,
                    payload,
                    senders,
                })) => {
                    let id = self.ids.get(&scid).unwrap();
                    return Ok(vec![SubSelectionResult::MessageReceived(
                        *id,
                        RawMessage {
                            payload,
                            senders,
                            scid,
                        },
                    )]);
                },
                Ok(ResolvedMessageOrDisconnect::Disconnect(scid)) => {
                    let id = self.ids.get(&scid).unwrap();
                    return Ok(vec![SubSelectionResult::ChannelClosed(*id)]);
                },
                Err(mpsc::TryRecvError::Empty) => {
                    MultiReceiverSet::select(&self.multi_receiver_set)?;
                },
                Err(_) => {
                    return Err(MuxError::Disconnected);
                },
            }
        }
    }
}

struct MultiReceiverSet {
    ipc_receiver_set: IpcReceiverSet,
    multi_receivers: HashMap<u64, Arc<MultiReceiver2>>,
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
    fn new() -> Result<MultiReceiverSet, io::Error> {
        Ok(MultiReceiverSet {
            ipc_receiver_set: IpcReceiverSet::new()?,
            multi_receivers: HashMap::new(),
        })
    }

    // Add a MultiReceiver to the MultiReceiverSet.
    fn add(
        mrs: &Arc<Mutex<MultiReceiverSet>>,
        multi_receiver: Arc<MultiReceiver2>,
    ) -> Result<(), MuxError> {
        let ipc_receiver = multi_receiver
            .receiver_demuxer
            .ipc_receiver_or_multi_receiver_set
            .lock()
            .unwrap()
            .swap(Arc::clone(mrs));
        let mut multi_receiver_set_mut = mrs.lock().unwrap();
        let id = multi_receiver_set_mut.ipc_receiver_set.add(ipc_receiver)?;
        multi_receiver_set_mut
            .multi_receivers
            .insert(id, multi_receiver);
        Ok(())
    }

    // Return true if and only if the MultiReceiverSet is empty.
    fn is_empty(mrs: &Arc<Mutex<MultiReceiverSet>>) -> bool {
        mrs.lock().unwrap().multi_receivers.is_empty()
    }

    // Obtain one or more incoming messages and handle them.
    #[instrument(level = "trace", ret, err(level = "trace"))]
    fn select(mrs: &Arc<Mutex<MultiReceiverSet>>) -> Result<(), MuxError> {
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
                                if let Some(multi_receiver) = mrs_mut.multi_receivers.get(&id) {
                                    MultiReceiver2::handle(
                                        Arc::clone(multi_receiver),
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
                    for mr in mrs_mut.multi_receivers.values() {
                        if mr.poll() {
                            probe_failed = true;
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
