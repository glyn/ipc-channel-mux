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

type ClientId = u64;

#[derive(Eq, Clone, Copy, Debug, Hash, PartialEq)]
pub struct SubChannelId(Uuid);

impl SubChannelId {
    fn new() -> SubChannelId {
        SubChannelId(Uuid::new_v4())
    }
}

impl Serialize for SubChannelId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer {
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
        D: Deserializer<'de> {
        let content: String = String::deserialize(deserializer)?;
        let uuid = Uuid::parse_str(&content).unwrap(); // FIXME: percolate this error
        Ok(SubChannelId(uuid))
    }
}

/// Sending end of a multiplexed channel.
///
/// [MultiSender]: struct.MultiSender.html
pub struct MultiSender {
    client_id: ClientId,
    ipc_sender: Rc<IpcSender<MultiMessage>>,
    uuid: Uuid,
    sender_id: Rc<RefCell<Source<Weak<IpcSender<MultiMessage>>>>>
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

impl<'a> MultiSender {
    #[instrument(level = "debug", ret)]
    pub fn new<T>(
        &self,
    ) -> SubChannelSender<T> {
        SubChannelSender {
            sub_channel_id: SubChannelId::new(),
            ipc_sender: self.ipc_sender.clone(),
            ipc_sender_uuid: self.uuid,
            sender_id: Rc::clone(&self.sender_id),
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
    pub fn connect(name: String) -> Result<MultiSender, MultiplexError> {
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
        })
    }

    #[instrument(level = "debug", err(level = "debug"))]
    pub fn notify_sub_channel(
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
pub struct MultiReceiver {
    ipc_receiver: IpcReceiver<MultiMessage>,
    mutex: Mutex<MultiReceiverMutator>,
}

struct MultiReceiverMutator {
    ipc_senders: HashMap<ClientId, IpcSender<MultiResponse>>,
    next_client_id: ClientId,
    sub_channels: HashMap<SubChannelId, Sender<Vec<u8>>>,
    ipc_senders_by_id: Target<Weak<IpcSender<MultiMessage>>>,
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
    pub fn attach<'a, T>(
        &'a self,
        sub_channel_id: SubChannelId,
    ) -> Result<SubChannelReceiver<'a, T>, MultiplexError> {
        let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::channel();
        self.mutex.lock()?.sub_channels.insert(sub_channel_id, tx);
        Ok(SubChannelReceiver {
            multi_receiver: self,
            channel: rx,
            phantom: PhantomData,
        })
    }

    #[instrument(level = "debug", err(level = "debug"))]
    pub fn receive(&self) -> Result<(), MultiplexError> {
        let msg = self.ipc_receiver.recv()?;
        self.handle(msg)
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    fn handle(&self, msg: MultiMessage) -> Result<(), MultiplexError> {
        match msg {
            MultiMessage::Connect(sender) => {
                self.mutex.lock()?.next_client_id += 1;
                let client_id = self.mutex.lock()?.next_client_id;
                sender.send(MultiResponse::ClientId(client_id))?;
                self.mutex.lock()?.ipc_senders.insert(client_id, sender);
                Ok(())
            },
            MultiMessage::Data(scid, data, ipc_senders) => {
                IPC_SENDERS_RECEIVED.with(|senders| {
                    let mut srs: VecDeque<Rc<IpcSender<MultiMessage>>> = ipc_senders
                        .iter()
                        .map(|s| match s {
                            IpcSenderAndOrId::IpcSender(s, id) => {
                                let uuid = Uuid::parse_str(&id).unwrap();
                                log::trace!("associating {} with an IpcSender", uuid);
                                let result = Rc::new(s.clone());
                                self.mutex
                                    .lock()
                                    .unwrap()
                                    .ipc_senders_by_id
                                    .add(uuid, &result);
                                result
                            },
                            IpcSenderAndOrId::IpcSenderId(id) => {
                                let uuid = Uuid::parse_str(&id).unwrap();
                                log::trace!("looking up IpcSender associated with {}", uuid);
                                let maybe_sender =
                                    self.mutex.lock().unwrap().ipc_senders_by_id.look_up(uuid);
                                maybe_sender.unwrap()
                            },
                        })
                        .collect();
                    senders.lock().unwrap().clear();
                    senders.lock().unwrap().append(&mut srs);
                    Ok::<(), MultiplexError>(())
                })?;
                self.mutex
                    .lock()?
                    .sub_channels
                    .get(&scid)
                    .ok_or(MultiplexError::InternalError(format!(
                        "invalid subchannel id {}",
                        scid
                    )))?
                    .send(data)
                    .map_err(|_| MultiplexError::MpmcSendError)
            },
            m => Err(MultiplexError::InternalError(format!(
                "unexpected multi message {:?}",
                m
            ))),
        }
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    pub fn receive_sub_channel(&mut self) -> Result<(SubChannelId, String), MultiplexError> {
        let msg = self.ipc_receiver.recv()?;
        match msg {
            MultiMessage::SubChannelId(sub_channel_id, name) => Ok((sub_channel_id, name)),
            m => Err(MultiplexError::InternalError(format!(
                "unexpected multi message {:?}",
                m
            ))),
        }
    }
}

pub struct SubChannelSender<T> {
    sub_channel_id: SubChannelId,
    ipc_sender: Rc<IpcSender<MultiMessage>>,
    ipc_sender_uuid: Uuid,
    sender_id: Rc<RefCell<Source<Weak<IpcSender<MultiMessage>>>>>,
    phantom: PhantomData<T>,
}

impl<T> Clone for SubChannelSender<T> {
    fn clone(&self) -> SubChannelSender<T> {
        SubChannelSender {
            sub_channel_id: self.sub_channel_id,
            ipc_sender: Rc::clone(&self.ipc_sender),
            ipc_sender_uuid: self.ipc_sender_uuid,
            sender_id: Rc::clone(&self.sender_id),
            phantom: self.phantom,
        }
    }
}

impl<T> SubChannelSender<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    #[instrument(level = "debug", skip(msg), err(level = "debug"))]
    pub fn send(&self, msg: T) -> Result<(), MultiplexError> {
        log::debug!(">SubChannelSender::send");
        IPC_SENDERS_TO_SEND.with(|senders| {
            senders.lock()?.clear();
            Ok::<(), MultiplexError>(())
        })?;
        let data = bincode::serialize(&msg)?;
        IPC_SENDERS_TO_SEND.with(|ipc_senders| {
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
            let result = self
                .ipc_sender
                .send(MultiMessage::Data(self.sub_channel_id, data, srs));
            log::debug!("<SubChannelSender::send -> {:#?}", result.as_ref());
            result.map_err(From::from)
        })
    }

    #[instrument(level = "trace", ret)]
    pub fn sub_channel_id(&self) -> SubChannelId {
        self.sub_channel_id
    }
}

impl<'de, T> Deserialize<'de> for SubChannelSender<T> {
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

pub struct SubChannelReceiver<'a, T> {
    multi_receiver: &'a MultiReceiver,
    channel: Receiver<Vec<u8>>,
    phantom: PhantomData<T>,
}

impl<T> SubChannelReceiver<'_, T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn recv(&mut self) -> Result<T, MultiplexError> {
        let multi_receiver_result = self.multi_receiver.receive();
        log::trace!(
            "SubChannelReceiver::recv multi_receiver_result = {:#?}",
            multi_receiver_result.as_ref()
        );
        multi_receiver_result?;
        log::trace!("SubChannelReceiver = {:#?}", self);
        let recvd = self
            .channel
            .recv()
            .map_err(|_| MultiplexError::MpmcRecvError)?;
        log::trace!("SubChannelReceiver::recv recvd = {:#?}", recvd);
        let result = bincode::deserialize(recvd.as_slice());
        IPC_SENDERS_RECEIVED.with(|senders| {
            senders.lock()?.clear();
            Ok::<(), MultiplexError>(())
        })?;
        result.map_err(From::from)
    }
}

impl<'a, T> fmt::Debug for SubChannelReceiver<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubChannelReceiver")
            .field("multi_receiver", &self.multi_receiver)
            .field("channel", &self.channel)
            .finish()
    }
}

/// MultiMessage is used to communicate across multiplexing channels.
#[derive(Serialize, Deserialize, Debug)]
enum MultiMessage {
    Connect(IpcSender<MultiResponse>),
    Data(SubChannelId, Vec<u8>, Vec<IpcSenderAndOrId>),
    SubChannelId(SubChannelId, String),
}

#[derive(Serialize, Deserialize, Debug)]
enum IpcSenderAndOrId {
    IpcSender(IpcSender<MultiMessage>, String),
    IpcSenderId(String),
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
pub fn multi_channel() -> Result<(MultiSender, MultiReceiver), io::Error> {
    let (ipc_sender, ipc_receiver) = ipc::channel()?;
    let (ipc_response_sender, _) = ipc::channel()?;
    let client_id = 1;
    let mut senders = HashMap::new();
    senders.insert(client_id, ipc_response_sender);
    let multi_receiver = MultiReceiver {
        ipc_receiver: ipc_receiver,
        mutex: Mutex::new(MultiReceiverMutator {
            ipc_senders: senders,
            next_client_id: client_id + 1,
            sub_channels: HashMap::new(),
            ipc_senders_by_id: Target::new(),
        }),
    };
    let multi_sender = MultiSender {
        client_id: client_id,
        ipc_sender: Rc::new(ipc_sender),
        uuid: Uuid::new_v4(),
        sender_id: Rc::new(RefCell::new(Source::new())),
    };
    Ok((multi_sender, multi_receiver))
}

/// A multiplexing server associated with a given name. The server is "one-shot" because
/// it accepts only one connect request from a client.
pub struct OneShotMultiServer {
    multi_server: IpcOneShotServer<MultiMessage>,
}

impl OneShotMultiServer {
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn new() -> Result<(OneShotMultiServer, String), io::Error> {
        let (multi_server, name) = IpcOneShotServer::new()?;
        Ok((OneShotMultiServer { multi_server }, name))
    }

    #[instrument(level = "debug", skip(self), ret, err(level = "debug"))]
    pub fn accept(self) -> Result<MultiReceiver, MultiplexError> {
        let (multi_receiver, multi_message): (IpcReceiver<MultiMessage>, MultiMessage) =
            self.multi_server.accept()?;

        let mr = MultiReceiver {
            ipc_receiver: multi_receiver,
            mutex: Mutex::new(MultiReceiverMutator {
                ipc_senders: HashMap::new(),
                next_client_id: 0,
                sub_channels: HashMap::new(),
                ipc_senders_by_id: Target::new(),
            }),
        };
        mr.handle(multi_message)?;
        Ok(mr)
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
