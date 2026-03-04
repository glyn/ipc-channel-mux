// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::mux::error::MuxError;
use crate::mux::protocol::{
    ClientId, EMPTY_SUBCHANNEL_ID, IpcSenderAndOrId, MultiMessage, MultiResponse, ORIGIN,
    SubChannelId, SubChannelSenderIds,
};
use crate::mux::sender::Target;
use crate::mux::sender::{MultiSender, SubChannelDisconnector, SubChannelSender};
use crate::mux::shared_memory::{clear_shmem_deserialization_context, set_shmems_for_recv};
use crate::mux::subchannel_lifecycle::SubSenderTracker;
use ipc_channel::ipc::{IpcReceiver, IpcSender, IpcSharedMemory};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tracing::instrument;
use uuid::Uuid;
use weak_table::WeakValueHashMap;

const POLLING_INTERVAL: Duration = Duration::from_millis(100);
const CONTENDED_WAIT_INTERVAL: Duration = Duration::from_micros(100);

pub type ProtoSender = (
    SubChannelId,
    Arc<Mutex<MultiSender>>,
    Uuid,
    Arc<SubSenderTracker<dyn Fn() + Send + Sync>>,
);

pub struct ResolvedMessage {
    scid: SubChannelId,
    payload: Vec<u8>,
    senders: VecDeque<ProtoSender>,
    shmems: Vec<IpcSharedMemory>,
}

impl ResolvedMessage {
    pub fn into_parts(
        self,
    ) -> (
        SubChannelId,
        Vec<u8>,
        VecDeque<ProtoSender>,
        Vec<IpcSharedMemory>,
    ) {
        (self.scid, self.payload, self.senders, self.shmems)
    }

    fn deserialize<T>(self) -> Result<T, MuxError>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        log::trace!("ResolvedMessage::deserialize payload = {:#?}", self.payload);
        establish_deserialization_context(self.senders, self.scid);
        set_shmems_for_recv(self.shmems);

        let result = postcard::from_bytes::<T>(self.payload.as_slice());

        clear_deserialization_context();
        clear_shmem_deserialization_context();

        result.map_err(From::from)
    }
}

pub enum ResolvedMessageOrDisconnect {
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
                write!(f, "TryRecvError::MuxError({multiplex_error})")
            },
            TryRecvError::Empty => write!(f, "TryRecvError::Empty"),
            TryRecvError::Handled => write!(f, "TryRecvError::Handled"),
        }
    }
}

type SubSenderStateMachine = crate::mux::subchannel_lifecycle::SubSenderStateMachine<
    mpsc::Sender<ResolvedMessageOrDisconnect>,
    ResolvedMessageOrDisconnect,
    mpsc::SendError<ResolvedMessageOrDisconnect>,
    Uuid,
    SubChannelId,
    dyn Fn() -> bool + Send,
>;

pub struct Demuxer {
    ipc_senders: HashMap<ClientId, IpcSender<MultiResponse>>,
    sub_channels: HashMap<SubChannelId, Arc<SubSenderStateMachine>>,
    disconnectors: WeakValueHashMap<SubChannelId, Weak<SubSenderTracker<dyn Fn() + Send + Sync>>>,
    ipc_senders_by_id: Target,
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
    pub fn empty() -> Self {
        Demuxer {
            ipc_senders: HashMap::new(),
            sub_channels: HashMap::new(),
            disconnectors: WeakValueHashMap::new(),
            ipc_senders_by_id: Target::new(),
        }
    }

    pub fn with_sender(client_id: ClientId, sender: IpcSender<MultiResponse>) -> Self {
        let mut ipc_senders = HashMap::new();
        ipc_senders.insert(client_id, sender);
        Demuxer {
            ipc_senders,
            sub_channels: HashMap::new(),
            disconnectors: WeakValueHashMap::new(),
            ipc_senders_by_id: Target::new(),
        }
    }

    pub fn switch_sub_channel_sender(
        &self,
        scid: SubChannelId,
        sender: Sender<ResolvedMessageOrDisconnect>,
    ) -> Result<(), MuxError> {
        let sub_sender_state_machine = self
            .sub_channels
            .get(&scid)
            .ok_or_else(|| MuxError::InternalError(format!("missing sub_channel for {scid}")))?;
        sub_sender_state_machine.switch_sender(sender);
        Ok(())
    }

    pub fn insert_state_machine(
        &mut self,
        sub_channel_id: SubChannelId,
        tx: Sender<ResolvedMessageOrDisconnect>,
    ) {
        self.sub_channels.insert(
            sub_channel_id,
            Arc::new(crate::mux::subchannel_lifecycle::SubSenderStateMachine::new(tx, ORIGIN)),
        );
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    #[allow(clippy::too_many_lines)]
    pub fn handle(
        self: &mut Demuxer,
        msg: MultiMessage,
        multi_receiver_uuid: Uuid,
    ) -> Result<(), MuxError> {
        match msg {
            MultiMessage::Connect(sender, client_id) => {
                self.ipc_senders.insert(client_id, sender);
                Ok(())
            },

            MultiMessage::Data(scid, payload, ipc_senders, shmems) => {
                let srs: VecDeque<ProtoSender> = ipc_senders
                    .clone()
                    .iter()
                    .map(|(scid, s)| {
                        let ipc_sender = Self::ipcsender_from_sender_and_or_id(self, s)?;
                        let i = ipc_sender.lock().unwrap().clone_ipc_sender();
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
                                SubChannelDisconnector::new(
                                    scid_copy,
                                    ipc_sender_clone.clone(),
                                    source_copy,
                                    multi_sender_clone.clone(),
                                )
                                .dropped();
                            })));
                            self.disconnectors.insert(*scid, Arc::clone(&disconnector));
                            disconnector.clone()
                        };

                        Ok((*scid, j, multi_receiver_uuid, disc))
                    })
                    .collect::<Result<VecDeque<ProtoSender>, MuxError>>()?;

                let result: Option<Result<(), mpsc::SendError<ResolvedMessageOrDisconnect>>> =
                    if let Some(sm) = self.sub_channels.get(&scid) {
                        sm.send(ResolvedMessageOrDisconnect::ResolvedMessage(
                            ResolvedMessage {
                                scid,
                                payload,
                                senders: srs,
                                shmems,
                            },
                        ))
                    } else {
                        // Send ReceiveFailed to members of srs
                        for (recv_scid, recv_multi_sender, _, _) in srs {
                            if let Err(e) = recv_multi_sender.lock().unwrap().send_message(
                                MultiMessage::ReceiveFailed {
                                    scid: recv_scid,
                                    via: scid,
                                },
                            ) {
                                log::debug!("Failed to send ReceiveFailed: {e}");
                            }
                        }
                        Err(MuxError::InternalError(format!(
                            "invalid subchannel id {scid}"
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
                        if let Err(e) = sender.send(ResolvedMessageOrDisconnect::Disconnect(scid)) {
                            log::debug!("Failed to send disconnect: {e}");
                        }
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
                    match ipc_sender {
                        Ok(ipc_sender) => {
                            sm.to_be_sent(
                                via,
                                Box::new(move || ipc_sender.lock().unwrap().probe()),
                            );
                        },
                        Err(e) => {
                            log::trace!("Error processing Sending message: {e:?}");
                            sm.to_be_sent(via, Box::new(|| false));
                            if let Some(sender) = sm.receive_failed(&via) {
                                if let Err(e) =
                                    sender.send(ResolvedMessageOrDisconnect::Disconnect(scid))
                                {
                                    log::debug!(
                                        "Failed to send disconnect after receive_failed: {e}"
                                    );
                                }
                            }
                        },
                    }
                }

                Ok(())
            },
            MultiMessage::ReceiveFailed { scid, via } => {
                if let Some(sm) = self.sub_channels.get(&scid) {
                    if let Some(sender) = sm.receive_failed(&via) {
                        if let Err(e) = sender.send(ResolvedMessageOrDisconnect::Disconnect(scid)) {
                            log::debug!("Failed to send disconnect after receive_failed: {e}");
                        }
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
            m @ MultiMessage::SubChannelId(..) => Err(MuxError::InternalError(format!(
                "unexpected multi message {m:?}"
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
                let uuid = Uuid::parse_str(id)
                    .map_err(|e| MuxError::InternalError(format!("invalid UUID: {e}")))?;
                let multi_sender = MultiSender::connect_sender(Arc::new(s.clone()), uuid)?;
                log::trace!("associating {uuid} with a MultiSender");
                self.ipc_senders_by_id.add(uuid, &multi_sender);
                log::trace!("association complete");
                Ok(multi_sender)
            },
            IpcSenderAndOrId::IpcSenderId(id) => {
                let uuid = Uuid::parse_str(id)
                    .map_err(|e| MuxError::InternalError(format!("invalid UUID: {e}")))?;
                log::trace!("looking up MultiSender associated with {uuid}");
                let maybe_sender: Option<Arc<Mutex<MultiSender>>> =
                    self.ipc_senders_by_id.look_up(uuid);
                log::trace!("result of looking up MultiSender is {maybe_sender:?}");
                if let Some(sender) = maybe_sender {
                    Ok(sender)
                } else {
                    Err(MuxError::Disconnected)
                }
            },
        }
    }

    pub fn send(
        self: &mut Demuxer,
        scid: SubChannelId,
        payload: Vec<u8>,
        ipc_senders: &[(SubChannelId, IpcSenderAndOrId)],
        ipc_receiver_uuid: Uuid,
        shmems: Vec<IpcSharedMemory>,
    ) -> Result<(), MuxError> {
        let mut id_sender_results: IdSenderResults = VecDeque::new();
        for (scid, s) in ipc_senders {
            let ipc_sender = self.ipcsender_from_sender_and_or_id(s)?;
            let disc = if let Some(disc) = self.disconnectors.get(scid) {
                disc
            } else {
                let scid_copy = *scid;
                let ipc_sender_clone = ipc_sender.lock().unwrap().clone_ipc_sender();
                let source_copy = ipc_receiver_uuid;
                let multi_sender_clone = ipc_sender.clone();
                let disconnector: Arc<SubSenderTracker<dyn Fn() + Send + Sync + 'static>> =
                    Arc::new(SubSenderTracker::new(Box::new(move || {
                        SubChannelDisconnector::new(
                            scid_copy,
                            ipc_sender_clone.clone(),
                            source_copy,
                            multi_sender_clone.clone(),
                        )
                        .dropped();
                    })));
                self.disconnectors.insert(*scid, Arc::clone(&disconnector));
                disconnector
            };
            id_sender_results.push_back((
                *scid,
                Ok(ipc_sender),
                ipc_receiver_uuid,
                disc,
            ));
        }
        let srs = Demuxer::process_results(id_sender_results);
        if let Ok(srs) = srs {
            if let Some(sm) = self.sub_channels.get(&scid) {
                let sent = sm.send(ResolvedMessageOrDisconnect::ResolvedMessage(
                    ResolvedMessage {
                        scid,
                        payload,
                        senders: srs,
                        shmems,
                    },
                ));
                match sent {
                    // In the case of an error, SubSenderStateMachine's internal channel
                    // disconnected. This can happen transiently between attach() and
                    // switch_sender() when the receiver hasn't been added to a
                    // SubReceiverSet yet. Drop the message rather than propagating a fatal error.
                    Some(_) => Ok(()),
                    // SubSenderStateMachine was already disconnected (maybe taken by poll).
                    None => Err(MuxError::Disconnected),
                }
            } else {
                // Send ReceiveFailed to members of srs
                for (recv_scid, recv_multi_sender, _, _) in srs {
                    if let Err(e) = recv_multi_sender.lock().unwrap().send_message(
                        MultiMessage::ReceiveFailed {
                            scid: recv_scid,
                            via: scid,
                        },
                    ) {
                        log::debug!("Failed to send ReceiveFailed: {e}");
                    }
                }
                Err(MuxError::Disconnected)
            }
        } else {
            srs.map(|_| ())
        }
    }

    pub fn poll_all_subchannels(demuxer: &Arc<Mutex<Demuxer>>) -> bool {
        // Snapshot Arc refs while holding the lock, then drop the lock before
        // running probes to minimise lock hold time.
        let state_machines: Vec<(SubChannelId, Arc<SubSenderStateMachine>)> = demuxer
            .lock()
            .unwrap()
            .sub_channels
            .iter()
            .map(|(scid, sm)| (*scid, Arc::clone(sm)))
            .collect();

        let mut probe_failed = false;
        for (scid, subsender_state_machine) in state_machines {
            let (poll, v) = subsender_state_machine.poll();
            if !poll {
                if let Some(sender) = v {
                    if let Err(e) = sender.send(ResolvedMessageOrDisconnect::Disconnect(scid)) {
                        log::debug!("Failed to send disconnect after poll: {e}");
                    }
                }
                probe_failed = true;
            }
        }
        probe_failed
    }

    pub fn clear_ipc_senders(self: &mut Demuxer) {
        self.ipc_senders.clear();
    }
}

thread_local! {
    static IPC_SENDERS_RECEIVED: Mutex<VecDeque<ProtoSender>> = const { Mutex::new(VecDeque::new()) };
    static VIA: Mutex<SubChannelId> = const { Mutex::new(EMPTY_SUBCHANNEL_ID) };
}

pub fn establish_deserialization_context(
    mut multi_senders: VecDeque<ProtoSender>,
    via: SubChannelId,
) {
    IPC_SENDERS_RECEIVED.with(|senders| {
        senders.lock().unwrap().clear();
        senders.lock().unwrap().append(&mut multi_senders);
    });
    VIA.with(|via_val| {
        let mut v = via_val.lock().unwrap();
        *v = via;
    });
}

pub fn clear_deserialization_context() {
    VIA.with(|via| {
        let mut v = via.lock().unwrap();
        *v = EMPTY_SUBCHANNEL_ID;
    });
    IPC_SENDERS_RECEIVED.with(|senders| {
        senders.lock().unwrap().clear();
    });
}

impl
    crate::mux::subchannel_lifecycle::Sender<
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

impl<'de> Deserialize<'de> for SubChannelSender {
    #[instrument(level = "trace", ret, skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let scsi = SubChannelSenderIds::deserialize(deserializer)?;

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
            .send_message(MultiMessage::Received {
                scid: scsi.sub_channel_id(),
                via,
                new_source,
            })
            .map_err(serde::de::Error::custom)?;

        let disc = multi_sender.3;
        let locked = multi_sender.1.lock().unwrap();
        let ipc_sender = locked.clone_ipc_sender();
        let ipc_sender_uuid = locked.uuid();
        drop(locked);

        Ok(SubChannelSender::from_deserialized(
            scsi.sub_channel_id(),
            ipc_sender,
            disc,
            ipc_sender_uuid,
            multi_sender.1,
        ))
    }
}

/// Receiving end of a multiplexed channel.
///
/// [MultiReceiver]: struct.MultiReceiver.html
#[derive(Debug)]
pub struct MultiReceiver {
    ipc_receiver_uuid: Uuid,
    receiver_demuxer: ReceiverDemuxer,
}

#[derive(Debug)]
pub struct ReceiverDemuxer {
    // When receiving from the IPC receiver, the Demuxer must be locked to
    // ensure messages are received in order.
    ipc_receiver: IpcReceiver<MultiMessage>,
    demuxer: Arc<Mutex<Demuxer>>,
}

impl ReceiverDemuxer {
    pub fn new(ipc_receiver: IpcReceiver<MultiMessage>, demuxer: Arc<Mutex<Demuxer>>) -> Self {
        ReceiverDemuxer {
            ipc_receiver,
            demuxer,
        }
    }
}

unsafe impl Send for MultiReceiver {}
unsafe impl Sync for MultiReceiver {}

impl MultiReceiver {
    pub fn new(ipc_receiver_uuid: Uuid, receiver_demuxer: ReceiverDemuxer) -> Self {
        MultiReceiver {
            ipc_receiver_uuid,
            receiver_demuxer,
        }
    }

    pub fn handle_initial_message(&self, msg: MultiMessage) -> Result<(), MuxError> {
        self.receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .handle(msg, self.ipc_receiver_uuid)
    }

    #[instrument(level = "debug", ret)]
    pub fn attach(mr: &Arc<MultiReceiver>, sub_channel_id: SubChannelId) -> SubChannelReceiver {
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
                Arc::new(crate::mux::subchannel_lifecycle::SubSenderStateMachine::new(tx, ORIGIN)),
            );
        SubChannelReceiver {
            multi_receiver: Arc::clone(mr),
            sub_channel_id,
            channel: rx,
        }
    }

    #[instrument(level = "debug", err(level = "debug"))]
    fn try_recv(mr: &Arc<MultiReceiver>) -> Result<(), TryRecvError> {
        let mut demuxer = mr.receiver_demuxer.demuxer.lock().unwrap();
        let result = mr.receiver_demuxer.ipc_receiver.try_recv();
        match result {
            Ok(msg) => {
                demuxer
                    .handle(msg, mr.ipc_receiver_uuid)
                    .map_err(TryRecvError::MuxError)?;
                Err(TryRecvError::Handled)
            },
            Err(e) => Err(match e {
                ipc_channel::TryRecvError::IpcError(ipc_error) => {
                    TryRecvError::MuxError(ipc_error.into())
                },
                ipc_channel::TryRecvError::Empty => TryRecvError::Empty,
            }),
        }
    }

    //#[instrument(level = "debug", err(level = "debug"))]
    #[inline(always)]
    fn try_recv_timeout(
        mr: &Arc<MultiReceiver>,
        mut demuxer: MutexGuard<'_, Demuxer>,
        duration: Duration,
    ) -> Result<(), TryRecvError> {
        let result = mr.receiver_demuxer.ipc_receiver.try_recv_timeout(duration);
        match result {
            Ok(msg) => demuxer
                .handle(msg, mr.ipc_receiver_uuid)
                .map_err(TryRecvError::MuxError),
            Err(ipc_channel::TryRecvError::Empty) => {
                mr.poll(demuxer);
                Ok(())
            },
            Err(ipc_channel::TryRecvError::IpcError(ipc_error)) => {
                Err(TryRecvError::MuxError(ipc_error.into()))
            },
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
    pub fn receive_sub_channel(
        mr: &Arc<MultiReceiver>,
    ) -> Result<(SubChannelId, String), MuxError> {
        let _unused = mr.receiver_demuxer.demuxer.lock().unwrap();
        let msg = mr.receiver_demuxer.ipc_receiver.recv()?;
        match msg {
            MultiMessage::SubChannelId(sub_channel_id, name) => Ok((sub_channel_id, name)),
            m => Err(MuxError::InternalError(format!(
                "unexpected multi message {m:?}"
            ))),
        }
    }

    // poll returns true if and only if a probe failed.
    #[instrument(level = "trace", ret)]
    fn poll(&self, demuxer: MutexGuard<'_, Demuxer>) -> bool {
        // Snapshot Arc refs while holding the lock, then drop the lock before
        // running probes to minimise lock hold time.
        let state_machines: Vec<(SubChannelId, Arc<SubSenderStateMachine>)> = demuxer
            .sub_channels
            .iter()
            .map(|(scid, sm)| (*scid, Arc::clone(sm)))
            .collect();
        drop(demuxer);

        let mut probe_failed = false;
        for (scid, subsender_state_machine) in state_machines {
            let (poll, v) = subsender_state_machine.poll();
            if !poll {
                if let Some(sender) = v {
                    if let Err(e) = sender.send(ResolvedMessageOrDisconnect::Disconnect(scid)) {
                        log::debug!("Failed to send disconnect after poll: {e}");
                    }
                }
                probe_failed = true;
            }
        }
        probe_failed
    }
}

pub struct SubChannelReceiver {
    multi_receiver: Arc<MultiReceiver>,
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
            log::trace!("Result of sending SubReceiverDisconnected was {result:?}");
        }

        // Drain the multireceiver.
        MultiReceiver::drain(&self.multi_receiver);

        // Drain the SubChannelReceiver and mark any subsenders as "receive failed". This is equivalent to receiving and then dropping
        // the subsenders.
        while let Ok(ResolvedMessageOrDisconnect::ResolvedMessage(ResolvedMessage {
            scid: via,
            payload: _,
            senders: scids_and_multi_senders,
            shmems: _,
        })) = self.channel.try_recv()
        {
            // log::trace!(
            //     "SubChannelReceiver::drop draining = {:#?}",
            //     scids_and_multi_senders
            // );
            for (scid, ms, _, _) in scids_and_multi_senders {
                if let Err(e) = ms
                    .lock()
                    .unwrap()
                    .send_message(MultiMessage::ReceiveFailed { scid, via })
                {
                    log::debug!("Failed to send ReceiveFailed during drop: {e}");
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

impl SubChannelReceiver {
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn recv<T>(&self) -> Result<T, MuxError>
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
                Ok(ResolvedMessageOrDisconnect::ResolvedMessage(resolved)) => {
                    return resolved.deserialize();
                },
                Err(mpsc::TryRecvError::Empty) => {
                    // If the mutex is locked, wait on the local channel.
                    let Ok(demuxer) = self.multi_receiver.receiver_demuxer.demuxer.try_lock()
                    else {
                        wait_interval = Some(CONTENDED_WAIT_INTERVAL);
                        continue;
                    };
                    wait_interval = None;
                    // receive another message, possibly for another subchannel
                    let multi_receiver_result = MultiReceiver::try_recv_timeout(
                        &self.multi_receiver,
                        demuxer,
                        POLLING_INTERVAL,
                    );
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

    #[instrument(level = "debug", err(level = "debug"))]
    pub fn try_recv<T>(&self) -> Result<T, crate::mux::error::TryRecvError>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        // First check the local mpsc channel for an already-demuxed message.
        match self.channel.try_recv() {
            Ok(ResolvedMessageOrDisconnect::ResolvedMessage(resolved)) => {
                return resolved.deserialize().map_err(Into::into);
            },
            Err(mpsc::TryRecvError::Empty) => {
                // Fall through to try the IPC channel.
            },
            _ => {
                return Err(crate::mux::error::TryRecvError::MuxError(
                    MuxError::Disconnected,
                ));
            },
        }

        // Do a non-blocking receive from the IPC channel to demux any pending messages.
        match MultiReceiver::try_recv(&self.multi_receiver) {
            Ok(()) | Err(TryRecvError::Empty | TryRecvError::Handled) => {},
            Err(TryRecvError::MuxError(e)) => {
                return Err(crate::mux::error::TryRecvError::MuxError(e));
            },
        }

        // Check the local channel again after demuxing.
        match self.channel.try_recv() {
            Ok(ResolvedMessageOrDisconnect::ResolvedMessage(resolved)) => {
                resolved.deserialize().map_err(Into::into)
            },
            Err(mpsc::TryRecvError::Empty) => Err(crate::mux::error::TryRecvError::Empty),
            _ => Err(crate::mux::error::TryRecvError::MuxError(
                MuxError::Disconnected,
            )),
        }
    }

    #[instrument(level = "debug", err(level = "debug"))]
    pub fn try_recv_timeout<T>(
        &self,
        duration: Duration,
    ) -> Result<T, crate::mux::error::TryRecvError>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let deadline = std::time::Instant::now() + duration;

        loop {
            // Check the local mpsc channel for an already-demuxed message.
            match self.channel.try_recv() {
                Ok(ResolvedMessageOrDisconnect::ResolvedMessage(resolved)) => {
                    return resolved.deserialize().map_err(Into::into);
                },
                Err(mpsc::TryRecvError::Empty) => {
                    // Fall through to try the IPC channel.
                },
                _ => {
                    return Err(crate::mux::error::TryRecvError::MuxError(
                        MuxError::Disconnected,
                    ));
                },
            }

            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(crate::mux::error::TryRecvError::Empty);
            }

            // Try to acquire the demuxer lock.
            let Ok(demuxer) = self.multi_receiver.receiver_demuxer.demuxer.try_lock() else {
                // Another thread holds the lock; wait briefly on the local channel.
                let wait = remaining.min(CONTENDED_WAIT_INTERVAL);
                match self.channel.recv_timeout(wait) {
                    Ok(ResolvedMessageOrDisconnect::ResolvedMessage(resolved)) => {
                        return resolved.deserialize().map_err(Into::into);
                    },
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    _ => {
                        return Err(crate::mux::error::TryRecvError::MuxError(
                            MuxError::Disconnected,
                        ));
                    },
                }
            };

            // Receive from the IPC channel with a bounded wait.
            let wait = remaining.min(POLLING_INTERVAL);
            match MultiReceiver::try_recv_timeout(&self.multi_receiver, demuxer, wait) {
                Err(TryRecvError::MuxError(e)) => {
                    return Err(crate::mux::error::TryRecvError::MuxError(e));
                },
                Ok(()) | Err(TryRecvError::Empty | TryRecvError::Handled) => {},
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::protocol::{IpcSenderAndOrId, SubChannelId};
    use ipc_channel::ipc;

    #[test]
    fn handle_data_for_unregistered_subchannel_sends_receive_failed() {
        let unknown_scid = SubChannelId::new();
        let sender_scid = SubChannelId::new();

        // Create an IPC channel for the embedded sender to communicate over.
        let (ipc_tx, ipc_rx) =
            ipc::channel::<MultiMessage>().expect("failed to create IPC channel");

        let sender_uuid = Uuid::new_v4();
        let mut demuxer = Demuxer::empty();

        // Build a Data message addressed to an unregistered subchannel,
        // containing one embedded sender.
        let msg = MultiMessage::Data(
            unknown_scid,
            vec![],
            vec![(
                sender_scid,
                IpcSenderAndOrId::IpcSender(ipc_tx, sender_uuid.to_string()),
            )],
            vec![],
        );

        let result = demuxer.handle(msg, Uuid::new_v4());

        // The handle call should return an InternalError for the invalid subchannel id.
        match result {
            Err(MuxError::InternalError(ref s)) if s.contains("invalid subchannel id") => (),
            other => panic!("expected InternalError about invalid subchannel id, got {other:?}"),
        }

        // connect_sender sends a Connect message first, then the else branch
        // sends ReceiveFailed — drain the Connect and verify ReceiveFailed.
        let connect_msg = ipc_rx.recv().expect("expected Connect message");
        assert!(
            matches!(connect_msg, MultiMessage::Connect(..)),
            "expected Connect, got {connect_msg:?}"
        );

        let rf_msg = ipc_rx.recv().expect("expected ReceiveFailed message");
        match rf_msg {
            MultiMessage::ReceiveFailed { scid, via } => {
                assert_eq!(scid, sender_scid);
                assert_eq!(via, unknown_scid);
            },
            other => panic!("expected ReceiveFailed, got {other:?}"),
        }
    }

    #[test]
    fn send_to_unregistered_subchannel_returns_disconnected() {
        let unknown_scid = SubChannelId::new();
        let mut demuxer = Demuxer::empty();

        // Call send() with no embedded senders and an unregistered destination scid.
        let result = demuxer.send(unknown_scid, vec![], &[], Uuid::new_v4(), vec![]);

        match result {
            Err(MuxError::Disconnected) => (),
            other => panic!("expected Disconnected, got {other:?}"),
        }
    }
}
