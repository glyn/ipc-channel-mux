// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Multiplex _subchannels_ over IPC channels.
//! 
//! A subchannel is similar to an IPC channel except that:
//! 1. Subchannel senders may be sent and received without consuming
//!    scarce operating system resources, such as file descriptors on Unix variants.
//! 2. Subchannel receivers may not be sent or received.
//! 
//! In the current multiplexing prototype, subchannels do not support:
//! * Opaque messages.
//! * Non-blocking receives and timeouts.

#![warn(missing_docs)]

use crate::ipc::{self, IpcError, IpcOneShotServer, IpcReceiver, IpcSender};
use bincode;
use channel_identification::{Source, Target};
use log;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug, Display, Formatter};
use std::io;
use std::marker::PhantomData;
use std::rc::{Rc, Weak};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::LazyLock;
use std::time::Duration;
use subchannel_lifecycle::SubSenderTracker;
use tracing::instrument;
use uuid::Uuid;
use weak_table::WeakValueHashMap;

mod channel_identification;
mod subchannel_lifecycle;
 
static EMPTY_SUBCHANNEL_ID: LazyLock<SubChannelId> = LazyLock::new(|| SubChannelId(Uuid::new_v4()));
static ORIGIN: LazyLock<Uuid> = LazyLock::new(|| Uuid::new_v4());

/// Channel wraps an IPC channel and is used to construct subchannels.
pub struct Channel {
    multi_sender: Rc<MultiSender>,
    multi_receiver: Rc<RefCell<MultiReceiver>>,
}

impl Channel {
    /// Construct a new [Channel].
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn new() -> Result<Channel, MultiplexError> {
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
        let scs = MultiSender::new(Rc::clone(&self.multi_sender));
        let scid = scs.sub_channel_id();
        self.multi_sender
            .sub_receiver_proxies
            .borrow_mut()
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

/// SubSender is the sending end of a subchannel, used to serialize and send messages of a given type.
/// 
/// SubSenders can be sent in messages on other subchannels and can be cloned.
#[derive(Debug, Deserialize, Serialize)]
pub struct SubSender<T>
where
    T: Serialize,
{
    sub_channel_sender: SubChannelSender<T>,
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
    /// [new]: crate::multiplex::SubOneShotServer::new
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn connect(name: String) -> Result<SubSender<T>, MultiplexError> {
        let multi_sender: Rc<MultiSender> = MultiSender::connect(name.to_string())?;
        let sub_channel_sender: SubChannelSender<T> = MultiSender::new(Rc::clone(&multi_sender));
        MultiSender::notify_sub_channel(multi_sender, sub_channel_sender.sub_channel_id(), name)?;
        Ok(SubSender {
            sub_channel_sender: sub_channel_sender,
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
    /// [recv]: crate::multiplex::SubReceiver::recv
    #[instrument(level = "debug", skip(self, data), err(level = "debug"))]
    pub fn send(&self, data: T) -> Result<(), MultiplexError> {
        self.sub_channel_sender.send(data)
    }

    // pub fn to_opaque(self) -> OpaqueIpcSender {
    // }
}

/// SubReceiver is the receiving end of a subchannel, used to receive and deserialize messages of a given type.
#[derive(Debug)]
pub struct SubReceiver<T>
where
    T: for<'x> Deserialize<'x> + Serialize,
{
    sub_channel_receiver: SubChannelReceiver<T>,
    phantom: PhantomData<T>,
}

impl<T> SubReceiver<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    /// Waits for, and returns, a message from the channel or returns an error if all corresponding [SubSender]s have
    /// disconnected (have been deallocated or their processes have terminated).
    /// 
    /// This method will always block the current thread if no messages are available and it’s possible for more messages
    /// to be sent (at least one [SubSender] still exists). Once a message is sent to a corresponding [SubSender],
    /// this method will wake up and return a message.
    ///
    /// If all the corresponding [SubSender]s have disconnected while this method is blocking, this method will wake up
    /// and return Err to indicate that no more messages can ever be received on this subchannel. However, since
    /// subchannels are buffered, messages sent before the [SubSender]s disconnect can still be properly received.
    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    pub fn recv(&self) -> Result<T, MultiplexError> {
        self.sub_channel_receiver.recv()
    }

    // pub fn try_recv(&self) -> Result<T, TryRecvError> {
    // }

    // pub fn try_recv_timeout(&self, duration: Duration) -> Result<T, TryRecvError> {
    // }

    // pub fn to_opaque(self) -> OpaqueIpcReceiver {
    // }
}

/// A server with a generated name which can be used to establish a subchannel
/// between processes.
/// 
/// On the server side, call [accept] against the server to obtain the subchannel receiver
/// and receive the first message.
/// 
/// On the client side, call [connect], passing the server name, to obtain the subchannel
/// sender. The server is “one-shot” because it accepts only one connect request from a client.
/// 
/// [accept]: crate::multiplex::SubOneShotServer::accept
/// [connect]: crate::multiplex::SubSender::connect
pub struct SubOneShotServer<T> {
    one_shot_multi_server: OneShotMultiServer,
    name: String,
    phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for SubOneShotServer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubOneShotServer")
            .field("name", &self.name)
            .finish()
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
    pub fn new() -> Result<(SubOneShotServer<T>, String), MultiplexError> {
        let (one_shot_multi_server, name) = OneShotMultiServer::new()?;
        Ok((
            SubOneShotServer {
                one_shot_multi_server: one_shot_multi_server,
                name: name.to_string(),
                phantom: PhantomData,
            },
            name,
        ))
    }

    /// Obtain a [SubReceiver] from a server and receive the first message.
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn accept(self) -> Result<(SubReceiver<T>, T), MultiplexError> {
        let multi_receiver = self.one_shot_multi_server.accept()?;
        let (subchannel_id, name) = MultiReceiver::receive_sub_channel(&multi_receiver)
            .expect("receive sub channel failed");
        if name != self.name {
            return Err(MultiplexError::InternalError(format!(
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
    ipc_sender: Rc<IpcSender<MultiMessage>>,
    uuid: Uuid,
    sender_id: Rc<RefCell<Source<Weak<IpcSender<MultiMessage>>>>>,
    response_receiver: Rc<IpcReceiver<MultiResponse>>,
    sub_receiver_proxies: RefCell<HashMap<SubChannelId, subchannel_lifecycle::SubReceiverProxy>>,
}

impl fmt::Debug for MultiSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiSender")
            .field("client_id", &self.client_id)
            .field("uuid", &self.uuid)
            .finish()
    }
}

/// This enumeration lists the possible reasons for failure of functions and methods in the [multiplex]
/// module.
/// 
/// [multiplex]: crate::multiplex
#[derive(Debug)]
pub enum MultiplexError {
    /// An error has occurred while receiving a message from the IPC channel underlying a subchannel.
    IpcError(IpcError),
    /// No more messages may be received.
    /// 
    /// Returned from [send] when the subchannel's [SubReceiver] has disconnected (has been
    /// deallocated or its process has terminated) and no more messages can be received.
    /// 
    /// Returned from [recv] or [accept] when all the subchannel’s [SubSender]s have disconnected (have been
    /// deallocated or their processes have terminated) and no more messages are available to be received.
    ///
    /// [send]: crate::multiplex::SubSender::send
    /// [recv]: crate::multiplex::SubReceiver::recv
    /// [accept]: crate::multiplex::SubOneShotServer::accept
    Disconnected,
    /// An internal logic error has occurred.
    InternalError(String),
}

impl fmt::Display for MultiplexError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MultiplexError::IpcError(ref err) => write!(fmt, "IPC error: {}", err),
            MultiplexError::Disconnected => write!(fmt, "disconnected"),
            MultiplexError::InternalError(s) => write!(fmt, "internal logic error: {s}"),
        }
    }
}

impl From<IpcError> for MultiplexError {
    fn from(err: IpcError) -> MultiplexError {
        match err {
            IpcError::Disconnected => MultiplexError::Disconnected,
            _ => MultiplexError::IpcError(err),
        }
    }
}

impl From<std::io::Error> for MultiplexError {
    fn from(err: std::io::Error) -> MultiplexError {
        MultiplexError::IpcError(IpcError::Io(err))
    }
}

impl From<bincode::Error> for MultiplexError {
    fn from(err: bincode::Error) -> MultiplexError {
        MultiplexError::IpcError(IpcError::Bincode(err))
    }
}

impl<'a> MultiSender {
    #[instrument(level = "debug", ret)]
    fn new<T>(self: Rc<MultiSender>) -> SubChannelSender<T> {
        let scid = SubChannelId::new();
        let sender_clone = self.ipc_sender.clone();
        SubChannelSender {
            sub_channel_id: scid,
            ipc_sender: self.ipc_sender.clone(),
            disconnector: Rc::new(SubSenderTracker::new(Box::new(move || {
                let d = SubChannelDisconnector {
                    sub_channel_id: scid,
                    ipc_sender: sender_clone.clone(),
                    source: *ORIGIN,
                };
                d.dropped();
            }))),
            ipc_sender_uuid: self.uuid,
            sender_id: Rc::clone(&self.sender_id),
            multi_sender: self,
            phantom: PhantomData,
        }
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn connect(name: String) -> Result<Rc<MultiSender>, MultiplexError> {
        let sender = Rc::new(IpcSender::connect(name)?);
        Self::connect_sender(sender, Uuid::new_v4())
    }

    #[instrument(level = "trace", ret, err(level = "trace"))]
    fn connect_sender(
        sender: Rc<IpcSender<MultiMessage>>,
        ipc_sender_uuid: Uuid,
    ) -> Result<Rc<MultiSender>, MultiplexError> {
        let (response_sender, response_receiver) = ipc::channel()?;
        let client_id = ClientId(Uuid::new_v4());
        sender.send(MultiMessage::Connect(response_sender, client_id))?;
        Ok(Rc::new(MultiSender {
            client_id: client_id,
            ipc_sender: sender,
            uuid: ipc_sender_uuid,
            sender_id: Rc::new(RefCell::new(Source::new())),
            response_receiver: Rc::new(response_receiver),
            sub_receiver_proxies: RefCell::new(HashMap::new()),
        }))
    }

    #[instrument(level = "debug", err(level = "debug"))]
    fn notify_sub_channel(
        self: Rc<MultiSender>,
        sub_channel_id: SubChannelId,
        name: String,
    ) -> Result<(), MultiplexError> {
        Ok(self
            .ipc_sender
            .send(MultiMessage::SubChannelId(sub_channel_id, name))?)
    }

    #[instrument(level = "trace", ret)]
    fn is_receiver_connected(&self, scid: SubChannelId) -> bool {
        loop {
            match self.response_receiver.try_recv() {
                Ok(MultiResponse::SubReceiverDisconnected(disconnected_scid)) => {
                    if let Some(proxy) = self.sub_receiver_proxies.borrow().get(&disconnected_scid)
                    {
                        proxy.disconnect();
                    };
                },
                _ => break,
            }
        }
        if let Some(proxy) = self.sub_receiver_proxies.borrow().get(&scid) {
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
    ipc_receiver: Rc<IpcReceiver<MultiMessage>>,
    ipc_receiver_uuid: Uuid,
    mutator: RefCell<MultiReceiverMutator>,
}

struct MultiReceiverMutator {
    ipc_senders: HashMap<ClientId, IpcSender<MultiResponse>>,
    sub_channels: HashMap<
        SubChannelId,
        subchannel_lifecycle::SubSenderStateMachine<
            mpsc::Sender<Vec<u8>>,
            Vec<u8>,
            mpsc::SendError<Vec<u8>>,
            Uuid,
            SubChannelId,
            dyn Fn() -> bool,
        >,
    >,
    disconnectors: WeakValueHashMap<SubChannelId, Weak<SubSenderTracker<dyn Fn()>>>,
    ipc_senders_by_id: Target<Weak<MultiSender>>,
}

impl std::fmt::Debug for MultiReceiverMutator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiReceiverMutator")
            .field("ipc_senders", &self.ipc_senders)
            .field("sub_channels", &self.sub_channels)
            .finish()
    }
}

thread_local! {
    static IPC_SENDERS_RECEIVED: RefCell<VecDeque<Rc<MultiSender>>> = RefCell::new(VecDeque::new());
    static FROM_VIA: RefCell<(Uuid, SubChannelId)> = RefCell::new((*ORIGIN, *EMPTY_SUBCHANNEL_ID));
}

impl subchannel_lifecycle::Sender<Vec<u8>, mpsc::SendError<Vec<u8>>> for Sender<Vec<u8>> {
    fn send(&self, msg: Vec<u8>) -> Result<(), mpsc::SendError<Vec<u8>>> {
        self.send(msg)
    }
}

impl MultiReceiver {
    #[instrument(level = "debug", ret)]
    fn attach<T: for<'de> Deserialize<'de> + Serialize>(
        mr: &Rc<RefCell<MultiReceiver>>,
        sub_channel_id: SubChannelId,
    ) -> SubChannelReceiver<T> {
        let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::channel();
        mr.borrow().mutator.borrow_mut().sub_channels.insert(
            sub_channel_id,
            subchannel_lifecycle::SubSenderStateMachine::new(tx, *ORIGIN),
        );
        SubChannelReceiver {
            multi_receiver: Rc::clone(mr),
            sub_channel_id: sub_channel_id,
            ipc_receiver_uuid: mr.borrow().ipc_receiver_uuid,
            channel: rx,
            phantom: PhantomData,
        }
    }

    #[instrument(level = "debug", err(level = "debug"))]
    fn receive(mr: &Rc<RefCell<MultiReceiver>>) -> Result<(), MultiplexError> {
        let msg = loop {
            let polling_interval = Duration::new(1, 0);
            match mr.borrow().ipc_receiver.try_recv_timeout(polling_interval) {
                Ok(msg) => break Ok(msg),
                Err(crate::ipc::TryRecvError::Empty) => {
                    if mr.borrow().poll() {
                        // At least one probe failed, so return to caller.
                        return Ok(());
                    }
                },
                Err(crate::ipc::TryRecvError::IpcError(e)) => {
                    break Err(MultiplexError::IpcError(e))
                },
            }
        }?;
        Self::handle(Rc::clone(&mr), msg)
    }

    #[instrument(level = "debug")]
    fn drain(mr: &Rc<RefCell<MultiReceiver>>) {
        loop {
            match mr.borrow().ipc_receiver.try_recv() {
                Ok(msg) => {
                    let _ = Self::handle(Rc::clone(mr), msg);
                },
                _ => {
                    break;
                },
            }
        }
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn handle(mr: Rc<RefCell<MultiReceiver>>, msg: MultiMessage) -> Result<(), MultiplexError> {
        match msg {
            MultiMessage::Connect(sender, client_id) => {
                mr.borrow()
                    .mutator
                    .borrow_mut()
                    .ipc_senders
                    .insert(client_id, sender);
                Ok(())
            },
            MultiMessage::Data(scid, data, ipc_senders, from) => {
                IPC_SENDERS_RECEIVED.with(|senders| {
                    let mut srs: VecDeque<Rc<MultiSender>> = ipc_senders
                        .iter()
                        .map(|s| Self::ipcsender_from_sender_and_or_id(&mr, s))
                        .collect();
                    senders.borrow_mut().clear();
                    senders.borrow_mut().append(&mut srs);
                    Ok::<(), MultiplexError>(())
                })?;
                CURRENT_MULTI_RECEIVER.with(|multi_receiver| {
                    multi_receiver.borrow_mut().replace(Rc::clone(&mr));
                });
                FROM_VIA.with(|from_via| from_via.replace((from, scid)));
                let result = mr
                    .borrow()
                    .mutator
                    .borrow()
                    .sub_channels
                    .get(&scid)
                    .ok_or(MultiplexError::InternalError(format!(
                        "invalid subchannel id {}",
                        scid
                    )))?
                    .send(data);

                // FIXME: where to clear IPC_SENDERS_RECEIVED. If the following is uncommented, tests fail.
                // IPC_SENDERS_RECEIVED.with(|senders| {
                //     senders.borrow_mut().clear();
                // });

                // FIXME: where to clear CURRENT_MULTI_RECEIVER. If the following is uncommented, tests fail.
                // CURRENT_MULTI_RECEIVER.with(|multi_receiver| {
                //     multi_receiver.borrow_mut().take();
                // });

                if let Some(Ok(())) = result {
                    Ok(())
                } else {
                    Err(MultiplexError::Disconnected)
                }
            },
            MultiMessage::Disconnect(scid, source) => {
                // FIXME: all senders (the original, its clones, and any transmitted copies) need to disconnect
                // before the receiver should stop blocking to receive messages.
                if let Some(sm) = mr.borrow().mutator.borrow_mut().sub_channels.get(&scid) {
                    sm.disconnect(source);
                }

                Ok(())
            },
            MultiMessage::Sending {
                scid,
                from,
                via,
                via_chan,
            } => {
                let ipc_sender = Self::ipcsender_from_sender_and_or_id(&mr, &via_chan);

                if let Some(sm) = mr.borrow().mutator.borrow_mut().sub_channels.get(&scid) {
                    sm.to_be_sent(
                        from,
                        via,
                        Box::new(move || probe(ipc_sender.ipc_sender.clone())),
                    );
                }

                Ok(())
            },
            MultiMessage::Received {
                scid,
                from,
                via,
                new_source,
            } => {
                if let Some(sm) = mr.borrow().mutator.borrow_mut().sub_channels.get(&scid) {
                    // Each subsender is serialised twice and results in to_be_sent() being called twice. Balance this out by
                    // calling received() twice.
                    sm.received(from.clone(), via.clone(), new_source.clone());
                    sm.received(from, via, new_source);
                }

                Ok(())
            },
            MultiMessage::Probe() => Ok(()), // ignore probe messages
            m => Err(MultiplexError::InternalError(format!(
                "unexpected multi message {:?}",
                m
            ))),
        }
    }

    fn ipcsender_from_sender_and_or_id(
        mr: &Rc<RefCell<MultiReceiver>>,
        s: &IpcSenderAndOrId,
    ) -> Rc<MultiSender> {
        match s {
            IpcSenderAndOrId::IpcSender(s, id) => {
                let uuid = Uuid::parse_str(&id).unwrap();
                let multi_sender = MultiSender::connect_sender(Rc::new(s.clone()), uuid).unwrap(); // return an error instead of panicking
                log::trace!("associating {} with a MultiSender", uuid);
                mr.borrow()
                    .mutator
                    .borrow_mut()
                    .ipc_senders_by_id
                    .add(uuid, &multi_sender);
                log::trace!("association complete");
                multi_sender
            },
            IpcSenderAndOrId::IpcSenderId(id) => {
                let uuid = Uuid::parse_str(&id).unwrap();
                log::trace!("looking up MultiSender associated with {}", uuid);
                let maybe_sender = mr.borrow().mutator.borrow().ipc_senders_by_id.look_up(uuid);
                log::trace!("result of looking up MultiSender is {:?}", maybe_sender);
                maybe_sender.unwrap()
            },
        }
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn receive_sub_channel(
        mr: &Rc<RefCell<MultiReceiver>>,
    ) -> Result<(SubChannelId, String), MultiplexError> {
        let msg = mr.borrow().ipc_receiver.recv()?;
        match msg {
            MultiMessage::SubChannelId(sub_channel_id, name) => Ok((sub_channel_id, name)),
            m => Err(MultiplexError::InternalError(format!(
                "unexpected multi message {:?}",
                m
            ))),
        }
    }

    // poll returns true if and only if a probe failed.
    #[instrument(level = "trace")]
    fn poll(&self) -> bool {
        let probe_failed = RefCell::new(false);
        self.mutator
            .borrow()
            .sub_channels
            .iter()
            .for_each(|(_, subsender_state_machine)| {
                if !subsender_state_machine.poll() {
                    probe_failed.replace(true);
                }
            });
        let result = probe_failed.borrow().clone();
        result
    }
}

#[instrument(level = "trace", ret)]
fn probe(ipc_sender: Rc<IpcSender<MultiMessage>>) -> bool {
    ipc_sender.send(MultiMessage::Probe()).is_ok()
}

struct SubChannelDisconnector {
    sub_channel_id: SubChannelId,
    ipc_sender: Rc<IpcSender<MultiMessage>>,
    source: Uuid,
}

impl SubChannelDisconnector {
    fn dropped(&self) {
        // Ignore any error sending disconnect message as it is not needed if the other end has hung up.
        let _ = self
            .ipc_sender
            .send(MultiMessage::Disconnect(self.sub_channel_id, self.source));
    }
}

struct SubChannelSender<T> {
    sub_channel_id: SubChannelId,
    ipc_sender: Rc<IpcSender<MultiMessage>>,
    disconnector: Rc<subchannel_lifecycle::SubSenderTracker<dyn Fn()>>,
    ipc_sender_uuid: Uuid,
    sender_id: Rc<RefCell<Source<Weak<IpcSender<MultiMessage>>>>>,
    multi_sender: Rc<MultiSender>,
    phantom: PhantomData<T>,
}

impl<T> Clone for SubChannelSender<T> {
    fn clone(&self) -> SubChannelSender<T> {
        SubChannelSender {
            sub_channel_id: self.sub_channel_id,
            ipc_sender: Rc::clone(&self.ipc_sender),
            disconnector: Rc::clone(&self.disconnector),
            ipc_sender_uuid: self.ipc_sender_uuid,
            sender_id: Rc::clone(&self.sender_id),
            multi_sender: Rc::clone(&self.multi_sender),
            phantom: self.phantom,
        }
    }
}

impl<T> SubChannelSender<T>
where
    T: Serialize,
{
    #[instrument(level = "debug", skip(msg), err(level = "debug"))]
    fn send(&self, msg: T) -> Result<(), MultiplexError> {
        log::debug!(">SubChannelSender::send");
        if !self.multi_sender.is_receiver_connected(self.sub_channel_id) {
            return Err(MultiplexError::Disconnected);
        }
        IPC_SENDERS_TO_SEND.with(|senders| {
            senders.borrow_mut().clear();
            Ok::<(), MultiplexError>(())
        })?;

        SERIALIZED_SUBCHANNEL_SENDERS.with(|subchannel_senders| {
            subchannel_senders.borrow_mut().clear();
            Ok::<(), MultiplexError>(())
        })?;

        let data = bincode::serialize(&msg)?;

        // Notify transmission of any subchannel senders so that they are counted during transmission.
        SERIALIZED_SUBCHANNEL_SENDERS.with(|subchannel_senders| {
            subchannel_senders.borrow().iter().for_each(
                |(subchannel_id, ipc_sender, sender_id)| {
                    // Note: each subsender is serialised twice and so Sending will be sent twice for each subsender.
                    let _ = ipc_sender.send(MultiMessage::Sending {
                        scid: subchannel_id.clone(),
                        from: *ORIGIN, // TODO: do we need to send the actual sender source?
                        via: self.sub_channel_id,
                        via_chan: Self::ipc_sender_and_or_uuid(
                            sender_id.clone(),
                            self.ipc_sender.clone(),
                            self.ipc_sender_uuid.clone(),
                        ),
                    });
                },
            );
            Ok::<(), MultiplexError>(())
        })?;

        SERIALIZED_SUBCHANNEL_SENDERS.with(|subchannel_senders| {
            subchannel_senders.borrow_mut().clear();
            Ok::<(), MultiplexError>(())
        })?;

        IPC_SENDERS_TO_SEND.with(|ipc_senders| {
            let srs = ipc_senders
                .borrow()
                .iter()
                .map(|ipc_sender_and_uuid| {
                    Self::ipc_sender_and_or_uuid(
                        self.sender_id.clone(),
                        ipc_sender_and_uuid.1.clone(),
                        ipc_sender_and_uuid.0,
                    )
                })
                .collect();
            let result =
                self.ipc_sender
                    .send(MultiMessage::Data(self.sub_channel_id, data, srs, *ORIGIN));
            log::debug!("<SubChannelSender::send -> {:#?}", result.as_ref());
            result.map_err(From::from)
        })
    }

    fn ipc_sender_and_or_uuid(
        sender_id: Rc<RefCell<Source<Weak<IpcSender<MultiMessage>>>>>,
        ipc_sender: Rc<IpcSender<MultiMessage>>,
        ipc_sender_uuid: Uuid,
    ) -> IpcSenderAndOrId {
        /* If this SubChannelSender has sent the given IpcSender
        before, send just the UUID associated with the IpcSender.
        Otherwise this is the first time this SubChannelSender
        has sent the given IpcSender, so send both the IpcSender
        and the UUID. */
        let already_sent = sender_id.borrow_mut().insert(ipc_sender.clone());
        if already_sent {
            log::trace!(
                "sending UUID {} associated with previously sent IpcSender",
                ipc_sender_uuid
            );
            IpcSenderAndOrId::IpcSenderId(ipc_sender_uuid.to_string())
        } else {
            log::trace!("sending IpcSender with UUID {}", ipc_sender_uuid);
            IpcSenderAndOrId::IpcSender(
                Rc::<IpcSender<MultiMessage>>::unwrap_or_clone(ipc_sender.clone()),
                ipc_sender_uuid.to_string(),
            )
        }
    }

    #[instrument(level = "trace", ret)]
    fn sub_channel_id(&self) -> SubChannelId {
        self.sub_channel_id
    }

    #[allow(dead_code)]
    fn disconnect(&self) -> Result<(), MultiplexError> {
        Ok(self
            .ipc_sender
            .send(MultiMessage::Disconnect(self.sub_channel_id, *ORIGIN))?)
    }
}

impl<'de, T> Deserialize<'de> for SubChannelSender<T> {
    #[instrument(level = "trace", ret, skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let scsi = SubChannelSenderIds::deserialize(deserializer).unwrap(); // FIXME: handle this error gracefully

        let multi_sender = IPC_SENDERS_RECEIVED
            .with(|senders| {
                let mut binding = senders.borrow_mut();
                let result = binding
                    .pop_front()
                    .ok_or(MultiplexError::InternalError(
                        "IpcSender missing from message".to_string(),
                    ))?
                    .clone();
                Ok(result)
            })
            .map_err(serde::de::Error::custom::<MultiplexError>)?;

        let new_source = CURRENT_MULTI_RECEIVER.with(|maybe_mr| {
            maybe_mr
                .borrow()
                .as_ref()
                .expect("CURRENT_MULTI_RECEIVER not set")
                .borrow()
                .ipc_receiver_uuid
        });

        let (from, via) = FROM_VIA.with(|from_via| from_via.borrow().clone());

        multi_sender
            .ipc_sender
            .send(MultiMessage::Received {
                scid: scsi.sub_channel_id,
                from: from,
                via: via,
                new_source,
            })
            .unwrap();

        let ipc_sender_clone = multi_sender.ipc_sender.clone();

        let disc = CURRENT_MULTI_RECEIVER.with(|maybe_mr| {
            let maybe_mr = maybe_mr.borrow();
            let mr = maybe_mr
                .as_ref()
                .expect("CURRENT_MULTI_RECEIVER not set")
                .borrow();
            let mut mutator = mr.mutator.borrow_mut();
            if let Some(disc) = mutator.disconnectors.get(&scsi.sub_channel_id) {
                disc
            } else {
                let disconnector: Rc<SubSenderTracker<dyn Fn()>> =
                    Rc::new(SubSenderTracker::new(Box::new(move || {
                        let d = SubChannelDisconnector {
                            sub_channel_id: scsi.sub_channel_id,
                            ipc_sender: ipc_sender_clone.clone(),
                            source: new_source,
                        };
                        d.dropped();
                    })));
                mutator
                    .disconnectors
                    .insert(scsi.sub_channel_id, Rc::clone(&disconnector));
                disconnector
            }
        });

        Ok(SubChannelSender {
            sub_channel_id: scsi.sub_channel_id,
            ipc_sender: Rc::clone(&multi_sender.ipc_sender),
            disconnector: disc,
            ipc_sender_uuid: multi_sender.uuid,
            sender_id: Rc::new(RefCell::new(Source::new())),
            multi_sender: multi_sender,
            phantom: PhantomData,
        })
    }
}

impl<'a, T> fmt::Debug for SubChannelSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubChannelSender")
            .field("sub_channel_id", &self.sub_channel_id)
            .field("ipc_sender", &self.ipc_sender)
            .finish()
    }
}

// FIXME: ensure this is cleared after use
thread_local! {
    static IPC_SENDERS_TO_SEND: RefCell<Vec<(Uuid, Rc<IpcSender<MultiMessage>>)>> = RefCell::new(vec!());
    static SERIALIZED_SUBCHANNEL_SENDERS: RefCell<Vec<(SubChannelId, Rc<IpcSender<MultiMessage>>, Rc<RefCell<Source<Weak<IpcSender<MultiMessage>>>>>)>> = RefCell::new(vec!());
}

impl<T> Serialize for SubChannelSender<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        log::trace!(
            "Adding SubChannelSender with SubChannelId {} to IPC_SENDERS_TO_SEND and SERIALIZED_SUBCHANNEL_SENDERS",
            self.sub_channel_id
        );

        IPC_SENDERS_TO_SEND.with(|ipc_senders| {
            ipc_senders
                .borrow_mut()
                .push((self.ipc_sender_uuid, self.ipc_sender.clone()))
        });

        SERIALIZED_SUBCHANNEL_SENDERS.with(|subchannel_senders| {
            subchannel_senders.borrow_mut().push((
                self.sub_channel_id,
                self.ipc_sender.clone(),
                self.sender_id.clone(),
            ))
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

struct SubChannelReceiver<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    multi_receiver: Rc<RefCell<MultiReceiver>>,
    sub_channel_id: SubChannelId,
    ipc_receiver_uuid: Uuid,
    channel: Receiver<Vec<u8>>,
    phantom: PhantomData<T>,
}

impl<T> Drop for SubChannelReceiver<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    fn drop(&mut self) {
        // Broadcast disconnection to all SubChannelSenders
        for (client_id, sender) in self
            .multi_receiver
            .borrow()
            .mutator
            .borrow()
            .ipc_senders
            .iter()
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

        // Drain the SubChannelReceiver. This ensures that any in-flight subsenders in messages
        // in the subchannel are received then dropped.
        loop {
            match self.channel.try_recv() {
                Ok(payload) => {
                    log::trace!("SubChannelReceiver::drop draining = {:#?}", payload);
                    let _ = bincode::deserialize::<T>(payload.as_slice());
                    IPC_SENDERS_RECEIVED.with(|senders| {
                        senders.borrow_mut().clear();
                    });
                },
                Err(_) => {
                    break;
                },
            }
        }
    }
}

impl<T> fmt::Debug for SubChannelReceiver<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubChannelReceiver")
            .field("sub_channel_id", &self.sub_channel_id)
            .field("ipc_receiver_uuid", &self.ipc_receiver_uuid)
            .finish()
    }
}

impl<T> SubChannelReceiver<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    #[instrument(level = "debug", err(level = "debug"))]
    fn recv(&self) -> Result<T, MultiplexError> {
        loop {
            match self.channel.try_recv() {
                Ok(payload) => {
                    log::trace!("SubChannelReceiver::recv received = {:#?}", payload);
                    let result = bincode::deserialize(payload.as_slice());
                    IPC_SENDERS_RECEIVED.with(|senders| {
                        senders.borrow_mut().clear();
                        Ok::<(), MultiplexError>(())
                    })?;
                    return result.map_err(From::from);
                },
                Err(mpsc::TryRecvError::Empty) => {
                    // receive another message, possibly for another subchannel
                    let multi_receiver_result = MultiReceiver::receive(&self.multi_receiver);
                    log::trace!(
                        "SubChannelReceiver::recv multi_receiver_result = {:#?}",
                        multi_receiver_result.as_ref()
                    );
                    multi_receiver_result?;
                },
                Err(_) => {
                    return Err(MultiplexError::Disconnected);
                },
            }
        }
    }
}

thread_local! {
    static CURRENT_MULTI_RECEIVER: RefCell<Option<Rc<RefCell<MultiReceiver>>>> = RefCell::new(None);
}

#[derive(Serialize, Deserialize, Debug)]
struct SubChannelReceiverIds {
    sub_channel_id: SubChannelId,
    ipc_receiver_uuid: String,
}

/// MultiMessage is used to communicate across multiplexing channels.
#[derive(Serialize, Deserialize, Debug)]
enum MultiMessage {
    Connect(IpcSender<MultiResponse>, ClientId),
    Data(SubChannelId, Vec<u8>, Vec<IpcSenderAndOrId>, Uuid),
    SubChannelId(SubChannelId, String),
    Sending {
        scid: SubChannelId,
        from: Uuid,
        via: SubChannelId,
        via_chan: IpcSenderAndOrId,
    },
    Received {
        scid: SubChannelId,
        from: Uuid,
        via: SubChannelId,
        new_source: Uuid,
    },
    Disconnect(SubChannelId, Uuid),
    Probe(),
}

#[derive(Serialize, Deserialize, Debug)]
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
fn multi_channel() -> Result<(Rc<MultiSender>, Rc<RefCell<MultiReceiver>>), io::Error> {
    let (ipc_sender, ipc_receiver) = ipc::channel()?;
    let (ipc_response_sender, ipc_response_receiver) = ipc::channel()?;
    let client_id = ClientId(Uuid::new_v4());
    let mut senders = HashMap::new();
    senders.insert(client_id, ipc_response_sender);
    let multi_receiver = MultiReceiver {
        ipc_receiver: Rc::new(ipc_receiver),
        ipc_receiver_uuid: Uuid::new_v4(),
        mutator: RefCell::new(MultiReceiverMutator {
            ipc_senders: senders,
            sub_channels: HashMap::new(),
            disconnectors: WeakValueHashMap::new(),
            ipc_senders_by_id: Target::new(),
        }),
    };
    let multi_receiver_rc = Rc::new(RefCell::new(multi_receiver));
    let multi_sender = MultiSender {
        client_id: client_id,
        ipc_sender: Rc::new(ipc_sender),
        uuid: Uuid::new_v4(),
        sender_id: Rc::new(RefCell::new(Source::new())),
        response_receiver: Rc::new(ipc_response_receiver),
        sub_receiver_proxies: RefCell::new(HashMap::new()),
    };
    Ok((Rc::new(multi_sender), multi_receiver_rc))
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
    fn accept(self) -> Result<Rc<RefCell<MultiReceiver>>, MultiplexError> {
        let (multi_receiver, multi_message): (IpcReceiver<MultiMessage>, MultiMessage) =
            self.multi_server.accept()?;

        let mr = MultiReceiver {
            ipc_receiver: Rc::new(multi_receiver),
            ipc_receiver_uuid: Uuid::new_v4(),
            mutator: RefCell::new(MultiReceiverMutator {
                ipc_senders: HashMap::new(),
                sub_channels: HashMap::new(),
                disconnectors: WeakValueHashMap::new(),
                ipc_senders_by_id: Target::new(),
            }),
        };
        let mr_rc = Rc::new(RefCell::new(mr));
        MultiReceiver::handle(Rc::clone(&mr_rc), multi_message)?;
        Ok(mr_rc)
    }
}
