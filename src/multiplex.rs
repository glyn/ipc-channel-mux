// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::ipc::{self, IpcError, IpcOneShotServer, IpcReceiver, IpcSender};
use bincode;
use log;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug, Display, Formatter};
use std::io;
use std::marker::PhantomData;
use std::ops::Deref;
use std::rc::{Rc, Weak};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, MutexGuard, PoisonError};
use tracing::instrument;
use uuid::Uuid;
use weak_table::traits::WeakElement;
use weak_table::{PtrWeakHashSet, WeakValueHashMap};

pub struct Channel {
    multi_sender: MultiSender,
    multi_receiver: Rc<RefCell<MultiReceiver>>,
}

impl Channel {
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn new() -> Result<Channel, MultiplexError> {
        // TODO: should this go in plex impl?
        let (ms, mr) = multi_channel()?;
        Ok(Channel {
            multi_sender: ms,
            multi_receiver: mr,
        })
    }

    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    pub fn sub_channel<T>(&self) -> Result<(SubSender<T>, SubReceiver<T>), MultiplexError>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let scs = self.multi_sender.new();
        let scid = scs.sub_channel_id();
        let scr = MultiReceiver::attach(&self.multi_receiver, scid)?;
        Ok((
            SubSender {
                sub_channel_sender: scs,
                phantom: PhantomData,
            },
            SubReceiver {
                sub_channel_receiver: scr,
                phantom: PhantomData,
            },
        ))
    }
}

#[derive(Deserialize, Serialize)]
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

impl<T> Drop for SubSender<T>
where
    T: Serialize,
{
    fn drop(&mut self) {
        // TODO: the following is insufficient because disconnecting
        // when the first sender is dropped can be premature.
        //self.sub_channel_sender.disconnect().unwrap();
    }
}

impl<T> SubSender<T>
where
    T: Serialize,
{
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn connect(name: String) -> Result<SubSender<T>, MultiplexError> {
        let multi_sender: MultiSender = MultiSender::connect(name.to_string())?;
        let sub_channel_sender: SubChannelSender<T> = multi_sender.new();
        multi_sender
        .notify_sub_channel(
            sub_channel_sender.sub_channel_id(),
            name,
        )?;
        Ok(SubSender {
            sub_channel_sender: sub_channel_sender,
            phantom: PhantomData,
        })
    }

    #[instrument(level = "debug", skip(self, data), err(level = "debug"))]
    pub fn send(&self, data: T) -> Result<(), MultiplexError> {
        self.sub_channel_sender.send(data)
    }

    // pub fn to_opaque(self) -> OpaqueIpcSender {
    // }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SubReceiver<T> {
    sub_channel_receiver: SubChannelReceiver<T>,
    phantom: PhantomData<T>,
}

// impl<T> fmt::debug for SubReceiver<T> {

// }

impl<T> SubReceiver<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
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

    #[instrument(level = "debug", err(level = "debug"))]
    pub fn accept(self) -> Result<(SubReceiver<T>, T), MultiplexError> {
        let multi_receiver = self.one_shot_multi_server.accept()?;
        let (subchannel_id, name) = MultiReceiver::receive_sub_channel(&multi_receiver)
            .expect("receive sub channel failed");
        if name != self.name {
            return Err(MultiplexError::InternalError(format!("unexpected sub channel name {}", name)));
        }
        let sub_receiver = MultiReceiver::attach(&multi_receiver, subchannel_id).unwrap();
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

type ClientId = u64;

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
    receiver_id: Rc<RefCell<Source<Weak<IpcReceiver<MultiMessage>>>>>,
}

impl fmt::Debug for MultiSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiSender")
            .field("client_id", &self.client_id)
            .field("uuid", &self.uuid)
            .finish()
    }
}

#[derive(Debug)]
pub enum MultiplexError {
    IpcError(IpcError),
    MpmcSendError, // FIXME: add error details once std::sync::mpmc::SendError is stable
    MpmcRecvError, // FIXME: add error details once std::sync::mpmc::RecvError is stable
    Disconnected,
    PoisonError,
    InternalError(String),
}

impl fmt::Display for MultiplexError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MultiplexError::IpcError(ref err) => write!(fmt, "IPC error: {}", err),
            MultiplexError::MpmcSendError => write!(
                fmt,
                "std::sync::mpmc::SenderError: receiver may have hung up or been dropped"
            ),
            MultiplexError::MpmcRecvError => write!(
                fmt,
                "std::sync::mpmc::RecvError: sender may have hung up or been dropped"
            ),
            MultiplexError::Disconnected => write!(fmt, "disconnected"),
            MultiplexError::PoisonError => write!(fmt, "poisoned mutex"),
            MultiplexError::InternalError(s) => write!(fmt, "internal logic error: {s}"),
        }
    }
}

impl From<IpcError> for MultiplexError {
    fn from(err: IpcError) -> MultiplexError {
        MultiplexError::IpcError(err)
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

impl From<PoisonError<MutexGuard<'_, MultiReceiverMutator>>> for MultiplexError {
    fn from(_err: PoisonError<MutexGuard<'_, MultiReceiverMutator>>) -> MultiplexError {
        MultiplexError::PoisonError
    }
}

impl From<PoisonError<std::sync::MutexGuard<'_, Vec<IpcSender<MultiMessage>>>>> for MultiplexError {
    fn from(
        _err: PoisonError<std::sync::MutexGuard<'_, Vec<IpcSender<MultiMessage>>>>,
    ) -> MultiplexError {
        MultiplexError::PoisonError
    }
}

impl From<PoisonError<std::sync::MutexGuard<'_, Vec<Rc<IpcSender<MultiMessage>>>>>>
    for MultiplexError
{
    fn from(
        _err: PoisonError<std::sync::MutexGuard<'_, Vec<Rc<IpcSender<MultiMessage>>>>>,
    ) -> MultiplexError {
        MultiplexError::PoisonError
    }
}

impl From<PoisonError<std::sync::MutexGuard<'_, Vec<(Uuid, Rc<IpcSender<MultiMessage>>)>>>>
    for MultiplexError
{
    fn from(
        _err: PoisonError<std::sync::MutexGuard<'_, Vec<(Uuid, Rc<IpcSender<MultiMessage>>)>>>,
    ) -> MultiplexError {
        MultiplexError::PoisonError
    }
}

impl From<PoisonError<std::sync::MutexGuard<'_, VecDeque<Rc<IpcSender<MultiMessage>>>>>>
    for MultiplexError
{
    fn from(
        _err: PoisonError<std::sync::MutexGuard<'_, VecDeque<Rc<IpcSender<MultiMessage>>>>>,
    ) -> MultiplexError {
        MultiplexError::PoisonError
    }
}

impl From<PoisonError<std::sync::MutexGuard<'_, VecDeque<Rc<IpcReceiver<MultiMessage>>>>>>
    for MultiplexError
{
    fn from(
        _err: PoisonError<std::sync::MutexGuard<'_, VecDeque<Rc<IpcReceiver<MultiMessage>>>>>,
    ) -> MultiplexError {
        MultiplexError::PoisonError
    }
}

impl<'a> MultiSender {
    #[instrument(level = "debug", ret)]
    fn new<T>(&self) -> SubChannelSender<T> {
        SubChannelSender {
            sub_channel_id: SubChannelId::new(),
            ipc_sender: self.ipc_sender.clone(),
            ipc_sender_uuid: self.uuid,
            sender_id: Rc::clone(&self.sender_id),
            receiver_id: Rc::clone(&self.receiver_id),
            phantom: PhantomData,
        }
    }

    /// Create a [MultiSender] connected to a previously defined [IpcOneShotMultiServer].
    ///
    /// This function should not be called more than once per [IpcOneShotMultiServer],
    /// otherwise the behaviour is unpredictable.
    /// Compare [issue 378](https://github.com/servo/ipc-channel/issues/378).
    ///
    /// [MultiSender]: struct.MultiSender.html
    /// [OneShotMultiServer]: struct.OneShotMultiServer.html
    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn connect(name: String) -> Result<MultiSender, MultiplexError> {
        let sender = Rc::new(IpcSender::connect(name)?);
        let (response_sender, response_receiver) = ipc::channel()?;
        sender.send(MultiMessage::Connect(response_sender))?;
        let resp = response_receiver.recv()?;
        let client_id = match resp {
            MultiResponse::ClientId(client_id) => Ok(client_id),
            m => Err(MultiplexError::InternalError(format!(
                "unexpected multi response {:?}",
                m
            ))),
        }?;
        Ok(MultiSender {
            client_id: client_id,
            ipc_sender: sender,
            uuid: Uuid::new_v4(),
            sender_id: Rc::new(RefCell::new(Source::new())),
            receiver_id: Rc::new(RefCell::new(Source::new())),
        })
    }

    #[instrument(level = "debug", err(level = "debug"))]
    fn notify_sub_channel(
        &self,
        sub_channel_id: SubChannelId,
        name: String,
    ) -> Result<(), MultiplexError> {
        Ok(self
            .ipc_sender
            .send(MultiMessage::SubChannelId(sub_channel_id, name))?)
    }
}

/// Receiving end of a multiplexed channel.
///
/// [MultiReceiver]: struct.MultiReceiver.html
#[derive(Debug)]
struct MultiReceiver {
    ipc_receiver: Rc<RefCell<Option<IpcReceiver<MultiMessage>>>>, // sending this MultiReceiver removes its IpcReceiver
    ipc_receiver_uuid: Uuid,
    mutex: Mutex<MultiReceiverMutator>,
}

struct MultiReceiverMutator {
    ipc_senders: HashMap<ClientId, IpcSender<MultiResponse>>,
    next_client_id: ClientId,
    sub_channels: HashMap<SubChannelId, Sender<Vec<u8>>>,
    ipc_senders_by_id: Target<Weak<IpcSender<MultiMessage>>>,
    ipc_receivers_by_id: Target<Weak<RefCell<Option<IpcReceiver<MultiMessage>>>>>,
    multi_receiver_grid: Rc<RefCell<HashMap<Uuid, Rc<RefCell<MultiReceiver>>>>>,
}

impl std::fmt::Debug for MultiReceiverMutator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiReceiverMutator")
            .field("ipc_senders", &self.ipc_senders)
            .field("next_client_id", &self.next_client_id)
            .field("sub_channels", &self.sub_channels)
            .finish()
    }
}

thread_local! {
    static IPC_SENDERS_RECEIVED: Mutex<VecDeque<Rc<IpcSender<MultiMessage>>>> = Mutex::new(VecDeque::new());
}

impl MultiReceiver {
    #[instrument(level = "debug", ret)]
    fn attach<T>(
        mr: &Rc<RefCell<MultiReceiver>>,
        sub_channel_id: SubChannelId,
    ) -> Result<SubChannelReceiver<T>, MultiplexError> {
        let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::channel();
        mr.borrow()
            .mutex
            .lock()?
            .sub_channels
            .insert(sub_channel_id, tx);
        Ok(SubChannelReceiver {
            multi_receiver: Rc::clone(
                &mr.borrow()
                    .mutex
                    .lock()
                    .unwrap()
                    .multi_receiver_grid
                    .borrow()
                    .get(&mr.borrow().ipc_receiver_uuid)
                    .unwrap(),
            ),
            sub_channel_id: sub_channel_id,
            ipc_receiver_uuid: mr.borrow().ipc_receiver_uuid,
            channel: rx,
            phantom: PhantomData,
        })
    }

    #[instrument(level = "debug", err(level = "debug"))]
    fn receive(&self) -> Result<(), MultiplexError> {
        let msg = if let Some(ipc_receiver) = self.ipc_receiver.borrow().as_ref() {
            ipc_receiver.recv()?
        } else {
            panic!("Attempted to use IpcReceiver after it was sent"); // FIXME: convert this to an error
        };
        //let msg = self.ipc_receiver.borrow().as_ref().unwrap().recv()?;
        let mr_rc = Rc::clone(
            &self
                .mutex
                .lock()
                .unwrap()
                .multi_receiver_grid
                .borrow()
                .get(&self.ipc_receiver_uuid)
                .unwrap(),
        );
        Self::handle(mr_rc, msg)
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn handle(mr: Rc<RefCell<MultiReceiver>>, msg: MultiMessage) -> Result<(), MultiplexError> {
        match msg {
            MultiMessage::Connect(sender) => {
                mr.borrow().mutex.lock()?.next_client_id += 1;
                let client_id = mr.borrow().mutex.lock()?.next_client_id;
                sender.send(MultiResponse::ClientId(client_id))?;
                mr.borrow()
                    .mutex
                    .lock()?
                    .ipc_senders
                    .insert(client_id, sender);
                Ok(())
            },
            MultiMessage::Data(scid, data, ipc_senders, mut ipc_receivers) => {
                IPC_SENDERS_RECEIVED.with(|senders| {
                    let mut srs: VecDeque<Rc<IpcSender<MultiMessage>>> = ipc_senders
                        .iter()
                        .map(|s| match s {
                            IpcSenderAndOrId::IpcSender(s, id) => {
                                let uuid = Uuid::parse_str(&id).unwrap();
                                log::trace!("associating {} with an IpcSender", uuid);
                                let result = Rc::new(s.clone());
                                mr.borrow()
                                    .mutex
                                    .lock()
                                    .unwrap()
                                    .ipc_senders_by_id
                                    .add(uuid, &result);
                                log::trace!("association complete");
                                result
                            },
                            IpcSenderAndOrId::IpcSenderId(id) => {
                                let uuid = Uuid::parse_str(&id).unwrap();
                                log::trace!("looking up IpcSender associated with {}", uuid);
                                let maybe_sender = mr
                                    .borrow()
                                    .mutex
                                    .lock()
                                    .unwrap()
                                    .ipc_senders_by_id
                                    .look_up(uuid);
                                log::trace!("result of looking up IpcSender is {:?}", maybe_sender);
                                maybe_sender.unwrap()
                            },
                        })
                        .collect();
                    senders.lock().unwrap().clear();
                    senders.lock().unwrap().append(&mut srs);
                    Ok::<(), MultiplexError>(())
                })?;
                IPC_RECEIVERS_RECEIVED.with(|receivers| {
                    let mut mrs: VecDeque<Rc<RefCell<MultiReceiver>>> = ipc_receivers
                        .iter_mut()
                        .map(|r| {
                            let taken_r = std::mem::replace(
                                &mut *r,
                                IpcReceiverAndOrId::IpcReceiverId("taken".to_string()),
                            );

                            match taken_r {
                                IpcReceiverAndOrId::IpcReceiver(r, id) => {
                                    let uuid = Uuid::parse_str(&id).unwrap();
                                    log::trace!("associating {} with an MultiReceiver", uuid);

                                    let r_ref = Rc::new(RefCell::new(Some(r)));

                                    let new_mr = MultiReceiver {
                                        ipc_receiver: Rc::clone(&r_ref),
                                        ipc_receiver_uuid: uuid,
                                        mutex: Mutex::new(MultiReceiverMutator {
                                            ipc_senders: HashMap::new(),
                                            next_client_id: 0,
                                            sub_channels: HashMap::new(),
                                            ipc_senders_by_id: Target::new(),
                                            ipc_receivers_by_id: Target::new(),
                                            multi_receiver_grid: Rc::clone(
                                                &mr.borrow()
                                                    .mutex
                                                    .lock()
                                                    .unwrap()
                                                    .multi_receiver_grid,
                                            ),
                                        }),
                                    };

                                    let result = Rc::new(RefCell::new(new_mr));

                                    mr.borrow()
                                        .mutex
                                        .lock()
                                        .unwrap()
                                        .multi_receiver_grid
                                        .borrow_mut()
                                        .insert(uuid, Rc::clone(&result));

                                    // Add new MultiReceiver to *current* MultiReceiver's ipc_receivers_by_id.
                                    mr.borrow()
                                        .mutex
                                        .lock()
                                        .unwrap()
                                        .ipc_receivers_by_id
                                        .add(uuid, &Rc::clone(&r_ref));

                                    log::trace!("association complete");
                                    result
                                },
                                IpcReceiverAndOrId::IpcReceiverId(id) => {
                                    let uuid = Uuid::parse_str(&id).unwrap();
                                    log::trace!(
                                        "looking up MultiReceiver associated with {}",
                                        uuid
                                    );

                                    let borrow = mr.borrow();
                                    let borrow2 = borrow.mutex.lock().unwrap();
                                    let borrow3 = borrow2.multi_receiver_grid.borrow();
                                    let found_mr = borrow3.get(&uuid).unwrap();

                                    log::trace!("result of looking up IpcSender is {:?}", found_mr);
                                    Rc::clone(found_mr)
                                },
                            }
                        })
                        .collect();
                    receivers.lock().unwrap().clear();
                    receivers.lock().unwrap().append(&mut mrs);
                    Ok::<(), MultiplexError>(())
                })?;
                CURRENT_MULTI_RECEIVER.with(|multi_receiver| {
                    multi_receiver.borrow_mut().replace(Rc::clone(&mr));
                });
                let result = mr
                    .borrow()
                    .mutex
                    .lock()?
                    .sub_channels
                    .get(&scid)
                    .ok_or(MultiplexError::InternalError(format!(
                        "invalid subchannel id {}",
                        scid
                    )))?
                    .send(data)
                    .map_err(|_| MultiplexError::MpmcSendError);
                // FIXME: where to clear IPC_SENDERS_RECEIVED. If the following is uncommented, IpcSenders go AWOL.
                // IPC_SENDERS_RECEIVED.with(|senders| {
                //     senders.lock().unwrap().clear();
                // });

                // FIXME: similar concern for the following.
                // IPC_RECEIVERS_RECEIVED.with(|receivers| {
                //     receivers.lock().unwrap().clear();
                // });

                // FIXME: similar concern for the following.
                // CURRENT_MULTI_RECEIVER.with(|multi_receiver| {
                //     multi_receiver.borrow_mut().take();
                // });
                result
            },
            MultiMessage::Disconnect(scid) => {
                // FIXME: all senders (the original, its clones, and any transmitted copies) need to disconnect
                // before the receiver should stop blocking to receive messages.
                mr.borrow().mutex.lock().unwrap().sub_channels.remove(&scid);
                Ok(())
            },
            m => Err(MultiplexError::InternalError(format!(
                "unexpected multi message {:?}",
                m
            ))),
        }
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn receive_sub_channel(
        mr: &Rc<RefCell<MultiReceiver>>,
    ) -> Result<(SubChannelId, String), MultiplexError> {
        let msg = mr.borrow().ipc_receiver.borrow().as_ref().unwrap().recv()?;
        match msg {
            MultiMessage::SubChannelId(sub_channel_id, name) => Ok((sub_channel_id, name)),
            m => Err(MultiplexError::InternalError(format!(
                "unexpected multi message {:?}",
                m
            ))),
        }
    }
}

struct SubChannelSender<T> {
    sub_channel_id: SubChannelId,
    ipc_sender: Rc<IpcSender<MultiMessage>>,
    ipc_sender_uuid: Uuid,
    sender_id: Rc<RefCell<Source<Weak<IpcSender<MultiMessage>>>>>,
    receiver_id: Rc<RefCell<Source<Weak<IpcReceiver<MultiMessage>>>>>,
    phantom: PhantomData<T>,
}

impl<T> Clone for SubChannelSender<T> {
    fn clone(&self) -> SubChannelSender<T> {
        SubChannelSender {
            sub_channel_id: self.sub_channel_id,
            ipc_sender: Rc::clone(&self.ipc_sender),
            ipc_sender_uuid: self.ipc_sender_uuid,
            sender_id: Rc::clone(&self.sender_id),
            receiver_id: Rc::clone(&self.receiver_id),
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
        IPC_SENDERS_TO_SEND.with(|senders| {
            senders.lock()?.clear();
            Ok::<(), MultiplexError>(())
        })?;
        let data = bincode::serialize(&msg)?;
        IPC_SENDERS_TO_SEND.with(|ipc_senders| {
            IPC_RECEIVERS_TO_SEND.with(|ipc_receivers| {
                let srs = ipc_senders
                    .lock()?
                    .iter()
                    .map(|ipc_sender_and_uuid| {
                        let ipc_sender_uuid = ipc_sender_and_uuid.0;
                        let ipc_sender = ipc_sender_and_uuid.1.clone();
                        /* If this SubChannelSender has sent the given IpcSender
                        before, send just the UUID associated with the IpcSender.
                        Otherwise this is the first time this SubChannelSender
                        has sent the given IpcSender, so associate it with a UUID
                        and send both the IpcSender and the UUID. */
                        let already_sent = self.sender_id.borrow_mut().insert(ipc_sender.clone()); // FIXME: change identify to not generate and return UUIDs
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
                    })
                    .collect();
                let rrs = ipc_receivers
                    .lock()
                    .unwrap() // FIXME: convert panic to error
                    .iter()
                    .map(|ipc_receiver_and_uuid| {
                        let ipc_receiver_uuid = ipc_receiver_and_uuid.0;
                        //let ipc_receiver = ipc_receiver_and_uuid.1;
                        /* If this SubChannelSender has sent the given IpcReceiver
                        before, send just the UUID associated with the IpcReceiver.
                        Otherwise this is the first time this SubChannelSender
                        has sent the given IpcReceiver, so associate it with a UUID
                        and send both the IpcReceiver and the UUID. */
                        let already_sent = false;
                        // self.receiver_id.borrow_mut().insert(ipc_receiver.clone()); // FIXME: change identify to not generate and return UUIDs
                        if already_sent {
                            log::trace!(
                                "sending UUID {} associated with previously sent IpcReceiver",
                                ipc_receiver_uuid
                            );
                            IpcReceiverAndOrId::IpcReceiverId(ipc_receiver_uuid.to_string())
                        } else {
                            log::trace!("sending IpcReceiver with UUID {}", ipc_receiver_uuid);
                            if let Some(ipc_receiver) = ipc_receiver_and_uuid.1.take() {
                                IpcReceiverAndOrId::IpcReceiver(
                                    ipc_receiver,
                                    ipc_receiver_uuid.to_string(),
                                )
                            } else {
                                IpcReceiverAndOrId::IpcReceiverId(ipc_receiver_uuid.to_string())
                            }
                        }
                    })
                    .collect();
                let result =
                    self.ipc_sender
                        .send(MultiMessage::Data(self.sub_channel_id, data, srs, rrs));
                log::debug!("<SubChannelSender::send -> {:#?}", result.as_ref());
                result.map_err(From::from)
            })
        })
    }

    #[instrument(level = "trace", ret)]
    fn sub_channel_id(&self) -> SubChannelId {
        self.sub_channel_id
    }

    fn disconnect(&self) -> Result<(), MultiplexError> {
        Ok(self
            .ipc_sender
            .send(MultiMessage::Disconnect(self.sub_channel_id))?)
    }
}

impl<'de, T> Deserialize<'de> for SubChannelSender<T> {
    #[instrument(level = "trace", ret, skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let scsi = SubChannelSenderIds::deserialize(deserializer).unwrap(); // FIXME: handle this error gracefully

        let ipc_sender = IPC_SENDERS_RECEIVED
            .with(|senders| {
                let mut binding = senders.lock()?;
                let result = binding
                    .pop_front()
                    .ok_or(MultiplexError::InternalError(
                        "IpcSender missing from message".to_string(),
                    ))?
                    .clone();
                Ok(result)
            })
            .map_err(serde::de::Error::custom::<MultiplexError>)?;

        Ok(SubChannelSender {
            sub_channel_id: scsi.sub_channel_id,
            ipc_sender: ipc_sender,
            ipc_sender_uuid: Uuid::parse_str(&scsi.ipc_sender_uuid).unwrap(), // FIXME: handle this error gracefully
            sender_id: Rc::new(RefCell::new(Source::new())),
            receiver_id: Rc::new(RefCell::new(Source::new())), // FIXME: copy from MultiSender
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
    static IPC_SENDERS_TO_SEND: Mutex<Vec<(Uuid, Rc<IpcSender<MultiMessage>>)>> = Mutex::new(vec!());
}

impl<T> Serialize for SubChannelSender<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        log::trace!(
            "Adding SubChannelSender with SubChannelId {} to IPC_SENDERS_TO_SEND",
            self.sub_channel_id
        );
        IPC_SENDERS_TO_SEND.with(|ipc_senders| {
            ipc_senders
                .lock()
                .unwrap()
                .push((self.ipc_sender_uuid, self.ipc_sender.clone()))
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

struct SubChannelReceiver<T> {
    multi_receiver: Rc<RefCell<MultiReceiver>>,
    sub_channel_id: SubChannelId,
    ipc_receiver_uuid: Uuid,
    channel: Receiver<Vec<u8>>,
    phantom: PhantomData<T>,
}

impl<T> fmt::Debug for SubChannelReceiver<T> {
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
        let multi_receiver_result = self.multi_receiver.borrow().receive();
        log::trace!(
            "SubChannelReceiver::recv multi_receiver_result = {:#?}",
            multi_receiver_result.as_ref()
        );
        multi_receiver_result?;
        log::trace!("SubChannelReceiver = {:#?}", self);
        let recvd = self
            .channel
            .recv() // this may block if multireceiver received a message for another subchannel
            // but if/when the other subchannel receiver recv is called, it will unblock
            // FIXME: cope with the case where the other subchannel receiver recv is not called
            // Maybe the solution is to loop receiving messages and use channel try_recv
            .map_err(|_| MultiplexError::Disconnected)?;
        log::trace!("SubChannelReceiver::recv recvd = {:#?}", recvd);
        let result = bincode::deserialize(recvd.as_slice());
        IPC_SENDERS_RECEIVED.with(|senders| {
            senders.lock()?.clear();
            Ok::<(), MultiplexError>(())
        })?;
        result.map_err(From::from)
    }
}

// FIXME: ensure this is cleared after use
thread_local! {
    static IPC_RECEIVERS_TO_SEND: Mutex<Vec<(Uuid, RefCell<Option<IpcReceiver<MultiMessage>>>)>> = Mutex::new(vec!());
}

impl<T> Serialize for SubChannelReceiver<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        log::trace!(
            "Adding SubChannelReceiver with SubChannelId {} and IpcReceiverUuid {} to IPC_RECEIVERS_TO_SEND",
            self.sub_channel_id,
            self.ipc_receiver_uuid,
        );
        IPC_RECEIVERS_TO_SEND.with(|ipc_receivers| {
            ipc_receivers.lock().unwrap().push((
                self.ipc_receiver_uuid,
                RefCell::new(self.multi_receiver.borrow().ipc_receiver.take()),
            ))
        });

        let scri = SubChannelReceiverIds {
            sub_channel_id: self.sub_channel_id,
            ipc_receiver_uuid: self.ipc_receiver_uuid.to_string(),
        };
        log::trace!("Serializing {:?}", scri);
        Ok(scri.serialize(serializer).unwrap()) // FIXME: handle this error gracefully
    }
}

thread_local! {
    static IPC_RECEIVERS_RECEIVED: Mutex<VecDeque<Rc<RefCell<MultiReceiver>>>> = Mutex::new(VecDeque::new());
    static CURRENT_MULTI_RECEIVER: RefCell<Option<Rc<RefCell<MultiReceiver>>>> = RefCell::new(None);
}

impl<'de, T> Deserialize<'de> for SubChannelReceiver<T> {
    #[instrument(level = "trace", ret, skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let scri = SubChannelReceiverIds::deserialize(deserializer).unwrap(); // FIXME: handle this error gracefully

        let ipc_receiver_uuid = Uuid::parse_str(&scri.ipc_receiver_uuid).unwrap(); // FIXME: handle this error gracefully

        let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::channel();

        let mr_rc: Rc<RefCell<MultiReceiver>> = IPC_RECEIVERS_RECEIVED.with(|receivers| {
            let mut binding = receivers.lock().unwrap();
            binding.pop_front().unwrap() // FIXME: handle this error gracefully
        });

        mr_rc
            .borrow()
            .mutex
            .lock()
            .unwrap()
            .sub_channels
            .insert(scri.sub_channel_id, tx);

        Ok(SubChannelReceiver {
            sub_channel_id: scri.sub_channel_id,
            multi_receiver: Rc::clone(&mr_rc),
            ipc_receiver_uuid: ipc_receiver_uuid,
            channel: rx,
            phantom: PhantomData,
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SubChannelReceiverIds {
    sub_channel_id: SubChannelId,
    ipc_receiver_uuid: String,
}

/// MultiMessage is used to communicate across multiplexing channels.
#[derive(Serialize, Deserialize, Debug)]
enum MultiMessage {
    Connect(IpcSender<MultiResponse>),
    Data(
        SubChannelId,
        Vec<u8>,
        Vec<IpcSenderAndOrId>,
        Vec<IpcReceiverAndOrId>,
    ),
    SubChannelId(SubChannelId, String),
    Disconnect(SubChannelId),
}

#[derive(Serialize, Deserialize, Debug)]
enum IpcSenderAndOrId {
    IpcSender(IpcSender<MultiMessage>, String),
    IpcSenderId(String),
}

#[derive(Serialize, Deserialize, Debug)]
enum IpcReceiverAndOrId {
    IpcReceiver(IpcReceiver<MultiMessage>, String),
    IpcReceiverId(String),
}

/// MultiResponse is used to communicate across multiplexing channels in response to a MultiMessage.
#[derive(Serialize, Deserialize, Debug)]
enum MultiResponse {
    ClientId(ClientId),
    SubChannel(SubChannelId),
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
fn multi_channel() -> Result<(MultiSender, Rc<RefCell<MultiReceiver>>), io::Error> {
    let (ipc_sender, ipc_receiver) = ipc::channel()?;
    let (ipc_response_sender, _) = ipc::channel()?;
    let client_id = 1;
    let mut senders = HashMap::new();
    senders.insert(client_id, ipc_response_sender);
    let multi_receiver = MultiReceiver {
        ipc_receiver: Rc::new(RefCell::new(Some(ipc_receiver))),
        ipc_receiver_uuid: Uuid::new_v4(),
        mutex: Mutex::new(MultiReceiverMutator {
            ipc_senders: senders,
            next_client_id: client_id + 1,
            sub_channels: HashMap::new(),
            ipc_senders_by_id: Target::new(),
            ipc_receivers_by_id: Target::new(),
            multi_receiver_grid: Rc::new(RefCell::new(HashMap::new())),
        }),
    };
    let multi_receiver_rc = Rc::new(RefCell::new(multi_receiver));
    multi_receiver_rc
        .borrow()
        .mutex
        .lock()
        .unwrap()
        .multi_receiver_grid
        .borrow_mut()
        .insert(
            multi_receiver_rc.borrow().ipc_receiver_uuid,
            Rc::clone(&multi_receiver_rc),
        );
    let multi_sender = MultiSender {
        client_id: client_id,
        ipc_sender: Rc::new(ipc_sender),
        uuid: Uuid::new_v4(),
        sender_id: Rc::new(RefCell::new(Source::new())),
        receiver_id: Rc::new(RefCell::new(Source::new())),
    };
    Ok((multi_sender, multi_receiver_rc))
}

/// A multiplexing server associated with a given name. The server is "one-shot" because
/// it accepts only one connect request from a client.
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
            ipc_receiver: Rc::new(RefCell::new(Some(multi_receiver))),
            ipc_receiver_uuid: Uuid::new_v4(),
            mutex: Mutex::new(MultiReceiverMutator {
                ipc_senders: HashMap::new(),
                next_client_id: 0,
                sub_channels: HashMap::new(),
                ipc_senders_by_id: Target::new(),
                ipc_receivers_by_id: Target::new(),
                multi_receiver_grid: Rc::new(RefCell::new(HashMap::new())),
            }),
        };
        let mr_rc = Rc::new(RefCell::new(mr));
        mr_rc
            .borrow()
            .mutex
            .lock()
            .unwrap()
            .multi_receiver_grid
            .borrow_mut()
            .insert(mr_rc.borrow().ipc_receiver_uuid, Rc::clone(&mr_rc));
        MultiReceiver::handle(Rc::clone(&mr_rc), multi_message)?;
        Ok(mr_rc)
    }
}

// Source and Target provide identification of generic endpoints (which turn
// out in practice to be IPC senders and receivers). The goals is to avoid
// sending the same endpoint multiple times over an IPC channel, since that
// tends to consume operating system resources such as (on Unix variants file
// descriptors.
//
// There are two "sides" to the module: source and target. The source side
// records endpoints. The intention is that the first time a source sends
// an endpoint over a channel, it will send the endpoint along with a UUID
// that identifies the endpoint. When the source side sends the same endpoint
// subsequently over the same channel, it sends just the endpoint's UUID.
//
// The target side of the module receives an endpoint along with its UUID
// and stores the UUID and the associated endpoint. When the target side
// receives just a UUID, it looks up the associated endpoint.
//
// Although the two sides of the module are decoupled, they are provided
// as a single module so that unit tests can demonstrate the intended usage.
//
// The module uses weak hashtables to allow the endpoints to be dropped.
// In the source side hashtable, the elements are endpoints held by weak
// pointers and compared by pointer. In the target side hashtable, the keys
// are UUIDs and the values are endpoints held by weak pointers.
//
// The endpoint types are abstracted to be weak pointers with a strong form
// that can be dereferenced. An example of a weak pointer type is
// Weak<IpcSender<MultiMessage>> which can be implemented using
// Rc<IpcSender<MultiMessage>>, for example. See the tests below for some
// analogous code.

/// Source is where endpoints are transmitted from. It tracks endpoints,
/// as they are sent, in a weak hashtable. Thus the hashtable does not
/// prevent an endpoint from being dropped.
#[derive(Debug)]
struct Source<T>
where
    T: Sized,
    T: WeakElement,
    <T as WeakElement>::Strong: Debug,
    <T as WeakElement>::Strong: Deref,
{
    end_points: Mutex<PtrWeakHashSet<T>>,
}

impl<T> Source<T>
where
    T: Sized,
    T: WeakElement,
    <T as WeakElement>::Strong: Debug,
    <T as WeakElement>::Strong: Deref,
{
    fn new() -> Source<T> {
        Source {
            end_points: Mutex::new(PtrWeakHashSet::new()),
        }
    }

    /// insert returns whether a given endpoint has already been sent by this Source
    /// and inserts it if it has not.
    fn insert(&mut self, e: <T as WeakElement>::Strong) -> bool {
        self.end_points.lock().unwrap().insert(e)
    }
}

/// Target is where endpoints from a Source are received. The associations
/// between UUIDs and endpoints are stored in a weak hashtable. Thus the
/// hashtable does not prevent the endpoint from being dropped. If an endpoint
/// is dropped, the association to a UUID is removed.
struct Target<T>
where
    T: WeakElement,
{
    end_points: Mutex<WeakValueHashMap<Uuid, T>>,
}

impl<T> Target<T>
where
    T: WeakElement,
    <T as WeakElement>::Strong: Clone,
{
    /// new creates a new Target with an empty hashtable.
    fn new() -> Target<T> {
        Target {
            end_points: Mutex::new(WeakValueHashMap::new()),
        }
    }

    /// add associates a UUID with an endpoint. The UUID must not already be
    /// associated with another endpoint, but this IS NOT CHECKED. As this
    /// would, with the intended usage of Source and Target here, amount to a
    /// UUID collision, the probability of this occurring is vanishingly small.
    fn add(&mut self, id: Uuid, e: &<T as WeakElement>::Strong) {
        let mut end_points = self.end_points.lock().unwrap();
        if !end_points.contains_key(&id) {
            end_points.insert(id, <T as WeakElement>::Strong::clone(&e));
        }
    }

    /// lookup looks up the endpoint associated with the given UUID.
    /// If the UUID was found return Some(e) where e is the endpoint
    /// associated with the UUID. Otherwise, there is no endpoint
    /// associated with the UUID, so return None.
    fn look_up(&self, id: Uuid) -> Option<<T as WeakElement>::Strong> {
        self.end_points.lock().unwrap().get(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::{Rc, Weak};

    #[test]
    fn test_source() {
        type Table = Source<Weak<str>>;

        let mut map = Table::new();
        let a = Rc::<str>::from("a");
        assert!(!map.insert(Rc::clone(&a)));

        let b = Rc::<str>::from("b");
        assert!(!map.insert(Rc::clone(&b)));

        assert!(map.insert(Rc::clone(&a)));

        assert!(map.insert(Rc::clone(&b)));

        drop(a);

        assert!(map.insert(Rc::clone(&b)));

        let c = Rc::<str>::from("c");
        assert!(!map.insert(c));
    }

    #[test]
    fn test_target() {
        type Table = Target<Weak<str>>;

        let mut map = Table::new();

        let u = Uuid::new_v4();

        // An unassociated UUID should return None from look_up.
        assert_eq!(map.look_up(u), None);

        // Associating an unassociated UUID with an endpoint should succeed.
        let a = Rc::<str>::from("a");
        map.add(u, &a);

        // Associating an associated UUID with the same endpoint should succeed.
        map.add(u, &a);

        // look_up should find the endpoint associated with a UUID.
        let val = map.look_up(u);
        assert_eq!(val, Some(a.clone()));

        // Associating an associated UUID with a distinct endpoint DOES NOT FAIL.
        let b = Rc::<str>::from("b");
        map.add(u, &b);

        // Associating an unassociated UUID with an unassociated endpoint should succeed.
        let u2 = Uuid::new_v4();
        map.add(u2, &b);

        // Associating another UUID with an endpoint already associated with a
        // distinct UUID should succeed.
        let u3 = Uuid::new_v4();
        map.add(u3, &a);

        // Looking up a UUID associated with a dropped endpoint should fail.
        drop(a);
        drop(val); // another strong pointer to the endpoint
        assert_eq!(map.look_up(u), None);
    }

    // The following test demonstrates the intended use of Source and Target.
    #[test]
    fn test_source_with_target() {
        type SourceTable = Source<Weak<str>>;
        let mut source_map = SourceTable::new();

        // The first time a given endpoint is sent from source to target, it is
        // sent along with a UUID newly associated with the endpoint by the
        // source. This association is recorded by the target.

        let a = Rc::<str>::from("a");
        let id = Uuid::new_v4();
        let already_sent = source_map.insert(Rc::clone(&a));
        assert!(!already_sent);

        type TargetTable = Target<Weak<str>>;
        let mut target_map = TargetTable::new();

        target_map.add(id, &a);

        // Subsequent times the endpoint is sent from source to target, just
        // the UUID associated with the endpoint is sent. The target looks up
        // the endpoint using the UUID.

        let already_sent2 = source_map.insert(Rc::clone(&a));
        assert!(already_sent2);

        let val = target_map.look_up(id);
        assert_eq!(val, Some(a.clone()));
    }
}
