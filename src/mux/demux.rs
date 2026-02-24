// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::mux::channel_identification::Target;
use crate::mux::error::MuxError;
use crate::mux::protocol::{
    ClientId, EMPTY_SUBCHANNEL_ID, IpcSenderAndOrId, MultiMessage, MultiResponse, ORIGIN,
    SubChannelId, SubChannelSenderIds,
};
use crate::mux::sender::{MultiSender, SubChannelDisconnector, SubChannelSender};
use crate::mux::subchannel_lifecycle::SubSenderTracker;
use ipc_channel::IpcError;
use ipc_channel::ipc::{IpcReceiver, IpcReceiverSet, IpcSelectionResult, IpcSender};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Formatter};
use std::io;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tracing::instrument;
use uuid::Uuid;
use weak_table::WeakValueHashMap;

pub(crate) const POLLING_INTERVAL: Duration = Duration::from_millis(100);
pub(crate) const CONTENDED_WAIT_INTERVAL: Duration = Duration::from_micros(100);

pub(crate) type ProtoSender = (
    SubChannelId,
    Arc<Mutex<MultiSender>>,
    Uuid,
    Arc<SubSenderTracker<dyn Fn() + Send + Sync>>,
);

pub(crate) struct ResolvedMessage {
    scid: SubChannelId,
    payload: Vec<u8>,
    senders: VecDeque<ProtoSender>,
}

impl ResolvedMessage {
    pub(crate) fn into_parts(self) -> (SubChannelId, Vec<u8>, VecDeque<ProtoSender>) {
        (self.scid, self.payload, self.senders)
    }
}

pub(crate) enum ResolvedMessageOrDisconnect {
    ResolvedMessage(ResolvedMessage),
    Disconnect(SubChannelId),
}

pub(crate) type IdSenders = VecDeque<(
    SubChannelId,
    Arc<Mutex<MultiSender>>,
    Uuid,
    Arc<SubSenderTracker<dyn Fn() + Send + Sync>>,
)>;

pub(crate) type IdSenderResults = VecDeque<(
    SubChannelId,
    Result<Arc<Mutex<MultiSender>>, MuxError>,
    Uuid,
    Arc<SubSenderTracker<dyn Fn() + Send + Sync>>,
)>;

#[derive(Debug)]
pub(crate) enum TryRecvError {
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

pub(crate) type SubSenderStateMachine = crate::mux::subchannel_lifecycle::SubSenderStateMachine<
    mpsc::Sender<ResolvedMessageOrDisconnect>,
    ResolvedMessageOrDisconnect,
    mpsc::SendError<ResolvedMessageOrDisconnect>,
    Uuid,
    SubChannelId,
    dyn Fn() -> bool + Send,
>;

pub(crate) struct Demuxer {
    ipc_senders: HashMap<ClientId, IpcSender<MultiResponse>>,
    sub_channels: HashMap<SubChannelId, Arc<SubSenderStateMachine>>,
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
    pub(crate) fn empty() -> Self {
        Demuxer {
            ipc_senders: HashMap::new(),
            sub_channels: HashMap::new(),
            disconnectors: WeakValueHashMap::new(),
            ipc_senders_by_id: Target::new(),
        }
    }

    pub(crate) fn with_sender(client_id: ClientId, sender: IpcSender<MultiResponse>) -> Self {
        let mut ipc_senders = HashMap::new();
        ipc_senders.insert(client_id, sender);
        Demuxer {
            ipc_senders,
            sub_channels: HashMap::new(),
            disconnectors: WeakValueHashMap::new(),
            ipc_senders_by_id: Target::new(),
        }
    }

    pub(crate) fn switch_sub_channel_sender(
        &self,
        scid: SubChannelId,
        sender: Sender<ResolvedMessageOrDisconnect>,
    ) -> Result<(), MuxError> {
        let sub_sender_state_machine = self
            .sub_channels
            .get(&scid)
            .ok_or_else(|| MuxError::InternalError(format!("missing sub_channel for {}", scid)))?;
        sub_sender_state_machine.switch_sender(sender);
        Ok(())
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    #[allow(clippy::too_many_lines)]
    pub(crate) fn handle(
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
                            },
                        ))
                    } else {
                        // Send ReceiveFailed to members of srs
                        // TODO: Need to test this path
                        for (recv_scid, recv_multi_sender, _, _) in srs {
                            if let Err(e) = recv_multi_sender.lock().unwrap().send_message(
                                MultiMessage::ReceiveFailed {
                                    scid: recv_scid,
                                    via: scid,
                                },
                            ) {
                                log::debug!("Failed to send ReceiveFailed: {}", e);
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
                        if let Err(e) = sender.send(ResolvedMessageOrDisconnect::Disconnect(scid)) {
                            log::debug!("Failed to send disconnect: {}", e);
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
                            log::trace!("Error processing Sending message: {:?}", e);
                            sm.to_be_sent(via, Box::new(|| false));
                            if let Some(sender) = sm.receive_failed(&via) {
                                if let Err(e) =
                                    sender.send(ResolvedMessageOrDisconnect::Disconnect(scid))
                                {
                                    log::debug!(
                                        "Failed to send disconnect after receive_failed: {}",
                                        e
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
                            log::debug!("Failed to send disconnect after receive_failed: {}", e);
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
            MultiMessage::Probe() => Ok(()), // ignore probe messages
            m @ MultiMessage::SubChannelId(..) => Err(MuxError::InternalError(format!(
                "unexpected multi message {:?}",
                m
            ))),
        }
    }

    pub(crate) fn process_results(results: IdSenderResults) -> Result<IdSenders, MuxError> {
        let mut srs: IdSenders = VecDeque::new();
        for (scid, res, uuid, disc) in results {
            srs.push_back((scid, res?, uuid, disc));
        }
        Ok(srs)
    }

    pub(crate) fn ipcsender_from_sender_and_or_id(
        self: &mut Demuxer,
        s: &IpcSenderAndOrId,
    ) -> Result<Arc<Mutex<MultiSender>>, MuxError> {
        match s {
            IpcSenderAndOrId::IpcSender(s, id) => {
                let uuid = Uuid::parse_str(id)
                    .map_err(|e| MuxError::InternalError(format!("invalid UUID: {}", e)))?;
                let multi_sender = MultiSender::connect_sender(Arc::new(s.clone()), uuid)?;
                log::trace!("associating {} with a MultiSender", uuid);
                self.ipc_senders_by_id.add(uuid, &multi_sender);
                log::trace!("association complete");
                Ok(multi_sender)
            },
            IpcSenderAndOrId::IpcSenderId(id) => {
                let uuid = Uuid::parse_str(id)
                    .map_err(|e| MuxError::InternalError(format!("invalid UUID: {}", e)))?;
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

    pub(crate) fn send(
        self: &mut Demuxer,
        scid: SubChannelId,
        payload: Vec<u8>,
        ipc_senders: &[(SubChannelId, IpcSenderAndOrId)],
        mr_clone: &Arc<SelectableMultiReceiver>,
    ) -> Result<(), MuxError> {
        let mut id_sender_results: IdSenderResults = VecDeque::new();
        for (scid, s) in ipc_senders {
            let disc = self.disconnectors.get(scid).ok_or_else(|| {
                MuxError::InternalError(format!("missing disconnector for subchannel {}", scid))
            })?;
            id_sender_results.push_back((
                *scid,
                self.ipcsender_from_sender_and_or_id(s),
                mr_clone.ipc_receiver_uuid,
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
                // TODO: Need to test this path
                for (recv_scid, recv_multi_sender, _, _) in srs {
                    if let Err(e) = recv_multi_sender.lock().unwrap().send_message(
                        MultiMessage::ReceiveFailed {
                            scid: recv_scid,
                            via: scid,
                        },
                    ) {
                        log::debug!("Failed to send ReceiveFailed: {}", e);
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

pub(crate) fn establish_deserialization_context(
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

pub(crate) fn clear_deserialization_context() {
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
pub(crate) struct MultiReceiver {
    ipc_receiver_uuid: Uuid,
    receiver_demuxer: ReceiverDemuxer,
}

#[derive(Debug)]
pub(crate) struct SelectableMultiReceiver {
    ipc_receiver_uuid: Uuid,
    receiver_demuxer: SelectableReceiverDemuxer,
}

#[derive(Debug)]
pub(crate) struct ReceiverDemuxer {
    // When receiving from the IPC receiver, the Demuxer must be locked to
    // ensure messages are received in order.
    ipc_receiver: IpcReceiver<MultiMessage>,
    demuxer: Arc<Mutex<Demuxer>>,
}

#[derive(Debug)]
pub(crate) struct SelectableReceiverDemuxer {
    multi_receiver_set: Arc<Mutex<MultiReceiverSet>>,
    demuxer: Arc<Mutex<Demuxer>>,
}

impl ReceiverDemuxer {
    pub(crate) fn new(
        ipc_receiver: IpcReceiver<MultiMessage>,
        demuxer: Arc<Mutex<Demuxer>>,
    ) -> Self {
        ReceiverDemuxer {
            ipc_receiver,
            demuxer,
        }
    }
}

impl SelectableReceiverDemuxer {
    pub(crate) fn new(
        multi_receiver_set: Arc<Mutex<MultiReceiverSet>>,
        demuxer: Arc<Mutex<Demuxer>>,
    ) -> Self {
        SelectableReceiverDemuxer {
            multi_receiver_set,
            demuxer,
        }
    }
}

unsafe impl Send for MultiReceiver {}
unsafe impl Sync for MultiReceiver {}

unsafe impl Send for SelectableMultiReceiver {}
unsafe impl Sync for SelectableMultiReceiver {}

impl MultiReceiver {
    pub(crate) fn new(ipc_receiver_uuid: Uuid, receiver_demuxer: ReceiverDemuxer) -> Self {
        MultiReceiver {
            ipc_receiver_uuid,
            receiver_demuxer,
        }
    }

    pub(crate) fn handle_initial_message(&self, msg: MultiMessage) -> Result<(), MuxError> {
        self.receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .handle(msg, self.ipc_receiver_uuid)
    }

    #[instrument(level = "debug", ret)]
    pub(crate) fn attach(
        mr: &Arc<MultiReceiver>,
        sub_channel_id: SubChannelId,
    ) -> SubChannelReceiver {
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
    pub(crate) fn try_recv(mr: &Arc<MultiReceiver>) -> Result<(), TryRecvError> {
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
    pub(crate) fn try_recv_timeout(
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
    pub(crate) fn drain(mr: &Arc<MultiReceiver>) {
        loop {
            let result = Self::try_recv(mr);
            match result {
                Ok(()) => {},
                Err(_) => break,
            }
        }
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    pub(crate) fn receive_sub_channel(
        mr: &Arc<MultiReceiver>,
    ) -> Result<(SubChannelId, String), MuxError> {
        let _unused = mr.receiver_demuxer.demuxer.lock().unwrap();
        let msg = mr.receiver_demuxer.ipc_receiver.recv()?;
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
    pub(crate) fn poll(&self, demuxer: MutexGuard<'_, Demuxer>) -> bool {
        // Snapshot Arc refs while holding the lock, then drop the lock before
        // running probes. This prevents a probe from blocking indefinitely
        // (e.g. when the remote socket buffer is full) while holding the
        // demuxer mutex.
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
                        log::debug!("Failed to send disconnect after poll: {}", e);
                    }
                }
                probe_failed = true;
            }
        }
        probe_failed
    }
}

impl SelectableMultiReceiver {
    pub(crate) fn new(
        ipc_receiver_uuid: Uuid,
        receiver_demuxer: SelectableReceiverDemuxer,
    ) -> Self {
        SelectableMultiReceiver {
            ipc_receiver_uuid,
            receiver_demuxer,
        }
    }

    #[instrument(level = "debug", ret)]
    pub(crate) fn attach(
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
            .sub_channels
            .insert(
                sub_channel_id,
                Arc::new(crate::mux::subchannel_lifecycle::SubSenderStateMachine::new(tx, ORIGIN)),
            );
        SelectableSubChannelReceiver {
            multi_receiver: Arc::clone(mr),
            sub_channel_id,
        }
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    pub(crate) fn handle(
        mr: Arc<SelectableMultiReceiver>,
        msg: MultiMessage,
    ) -> Result<(), MuxError> {
        let demuxer = &mut mr.receiver_demuxer.demuxer.lock().unwrap();
        if let MultiMessage::Data(scid, payload, ipc_senders) = msg {
            demuxer.send(scid, payload, &ipc_senders, &mr)
        } else {
            demuxer.handle(msg, mr.ipc_receiver_uuid)
        }
    }

    // poll returns true if and only if a probe failed.
    #[instrument(level = "trace", ret)]
    pub(crate) fn poll(&self) -> bool {
        // Snapshot Arc refs while holding the lock, then drop the lock before
        // running probes. This prevents a probe from blocking indefinitely
        // (e.g. when the remote socket buffer is full) while holding the
        // demuxer mutex.
        let state_machines: Vec<(SubChannelId, Arc<SubSenderStateMachine>)> = self
            .receiver_demuxer
            .demuxer
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
                        log::debug!("Failed to send disconnect after poll: {}", e);
                    }
                }
                probe_failed = true;
            }
        }
        probe_failed
    }
}

pub(crate) struct SubChannelReceiver {
    multi_receiver: Arc<MultiReceiver>,
    sub_channel_id: SubChannelId,
    channel: Receiver<ResolvedMessageOrDisconnect>,
}

unsafe impl Send for SubChannelReceiver {}
unsafe impl Sync for SubChannelReceiver {}

pub(crate) struct SelectableSubChannelReceiver {
    multi_receiver: Arc<SelectableMultiReceiver>,
    sub_channel_id: SubChannelId,
}

impl SelectableSubChannelReceiver {
    pub(crate) fn multi_receiver_set(&self) -> Arc<Mutex<MultiReceiverSet>> {
        Arc::clone(&self.multi_receiver.receiver_demuxer.multi_receiver_set)
    }

    pub(crate) fn demuxer(&self) -> Arc<Mutex<Demuxer>> {
        Arc::clone(&self.multi_receiver.receiver_demuxer.demuxer)
    }

    pub(crate) fn sub_channel_id(&self) -> SubChannelId {
        self.sub_channel_id
    }
}

unsafe impl Send for SelectableSubChannelReceiver {}
unsafe impl Sync for SelectableSubChannelReceiver {}

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
                if let Err(e) = ms
                    .lock()
                    .unwrap()
                    .send_message(MultiMessage::ReceiveFailed { scid, via })
                {
                    log::debug!("Failed to send ReceiveFailed during drop: {}", e);
                }
            }
        }
    }
}

// No custom Drop for SelectableSubChannelReceiver: SubReceiverSet::drop calls
// MultiReceiverSet::close() which clears the demuxer's ipc_senders,
// closing the response channels. is_receiver_connected detects this
// closure via IpcError and returns false.

impl fmt::Debug for SubChannelReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubChannelReceiver")
            .field("sub_channel_id", &self.sub_channel_id)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for SelectableSubChannelReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SelectableSubChannelReceiver")
            .field("sub_channel_id", &self.sub_channel_id)
            .finish_non_exhaustive()
    }
}

impl SubChannelReceiver {
    #[instrument(level = "debug", err(level = "debug"))]
    pub(crate) fn recv<T>(&self) -> Result<T, MuxError>
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
}

pub(crate) struct MultiReceiverSet {
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
    pub(crate) fn new() -> Result<MultiReceiverSet, io::Error> {
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
    pub(crate) fn register_pending(&mut self) -> Result<(), MuxError> {
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
    pub(crate) fn close(&self) {
        for mr in self.multi_receivers.values() {
            if let Some(mr) = mr.upgrade() {
                mr.receiver_demuxer
                    .demuxer
                    .lock()
                    .unwrap()
                    .ipc_senders
                    .clear();
            }
        }
        if let Some((_, ref mr)) = self.pending {
            mr.receiver_demuxer
                .demuxer
                .lock()
                .unwrap()
                .ipc_senders
                .clear();
        }
    }

    pub(crate) fn set_pending(
        &mut self,
        ipc_receiver: IpcReceiver<MultiMessage>,
        multi_receiver: Arc<SelectableMultiReceiver>,
    ) {
        self.pending = Some((ipc_receiver, multi_receiver));
    }

    pub(crate) fn is_merged_into(&self, target: &Arc<Mutex<MultiReceiverSet>>) -> bool {
        #[allow(clippy::map_unwrap_or)]
        self.merged_into
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .map(|arc| Arc::ptr_eq(&arc, target))
            .unwrap_or(false)
    }

    pub(crate) fn take_pending(
        &mut self,
    ) -> Option<(IpcReceiver<MultiMessage>, Arc<SelectableMultiReceiver>)> {
        self.pending.take()
    }

    pub(crate) fn merge_receiver(
        &mut self,
        ipc_receiver: IpcReceiver<MultiMessage>,
        multi_receiver: &Arc<SelectableMultiReceiver>,
    ) -> Result<(), MuxError> {
        let id = self.ipc_receiver_set.add(ipc_receiver)?;
        self.multi_receivers
            .insert(id, Arc::downgrade(multi_receiver));
        Ok(())
    }

    pub(crate) fn set_merged_into(&mut self, target: &Arc<Mutex<MultiReceiverSet>>) {
        self.merged_into = Some(Arc::downgrade(target));
    }

    // Return true if and only if the MultiReceiverSet is empty (no IPC receivers
    // registered or pending registration).
    pub(crate) fn is_empty(mrs: &Arc<Mutex<MultiReceiverSet>>) -> bool {
        let mrs_locked = mrs.lock().unwrap();
        mrs_locked.multi_receivers.is_empty() && mrs_locked.pending.is_none()
    }

    // Obtain one or more incoming messages and handle them.
    #[instrument(level = "trace", ret, err(level = "trace"))]
    pub(crate) fn select(mrs: &Arc<Mutex<MultiReceiverSet>>) -> Result<(), MuxError> {
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
