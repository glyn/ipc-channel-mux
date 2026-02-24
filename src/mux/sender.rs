// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::mux::channel_identification::Source;
use crate::mux::error::MuxError;
use crate::mux::protocol::{
    ClientId, IpcSenderAndOrId, MultiMessage, MultiResponse, ORIGIN, SubChannelId,
    SubChannelSenderIds,
};
use crate::mux::subchannel_lifecycle::{SubReceiverProxy, SubSenderTracker};
use ipc_channel::ipc::{self, IpcReceiver, IpcSender};
use serde::{Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use tracing::instrument;
use uuid::Uuid;

/// Sending end of a multiplexed channel.
///
/// [MultiSender]: struct.MultiSender.html
pub(crate) struct MultiSender {
    client_id: ClientId,
    ipc_sender: Arc<IpcSender<MultiMessage>>,
    uuid: Uuid,
    sender_id: Arc<Mutex<Source<Weak<IpcSender<MultiMessage>>>>>,
    response_receiver: IpcReceiver<MultiResponse>,
    sub_receiver_proxies: Mutex<HashMap<SubChannelId, SubReceiverProxy>>,
}

impl fmt::Debug for MultiSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiSender")
            .field("client_id", &self.client_id)
            .field("uuid", &self.uuid)
            .finish_non_exhaustive()
    }
}

impl MultiSender {
    pub(crate) fn new(
        client_id: ClientId,
        ipc_sender: Arc<IpcSender<MultiMessage>>,
        uuid: Uuid,
        sender_id: Arc<Mutex<Source<Weak<IpcSender<MultiMessage>>>>>,
        response_receiver: IpcReceiver<MultiResponse>,
    ) -> Self {
        MultiSender {
            client_id,
            ipc_sender,
            uuid,
            sender_id,
            response_receiver,
            sub_receiver_proxies: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn clone_ipc_sender(&self) -> Arc<IpcSender<MultiMessage>> {
        Arc::clone(&self.ipc_sender)
    }

    pub(crate) fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub(crate) fn send_message(&self, msg: MultiMessage) -> Result<(), MuxError> {
        self.ipc_sender.send(msg).map_err(From::from)
    }

    pub(crate) fn probe(&self) -> bool {
        self.ipc_sender.send(MultiMessage::Probe()).is_ok()
    }

    pub(crate) fn insert_sub_receiver_proxy(&self, scid: SubChannelId, proxy: SubReceiverProxy) {
        self.sub_receiver_proxies
            .lock()
            .unwrap()
            .insert(scid, proxy);
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    pub(crate) fn connect(name: String) -> Result<Arc<Mutex<MultiSender>>, MuxError> {
        let sender = Arc::new(IpcSender::connect(name)?);
        Self::connect_sender(sender, Uuid::new_v4())
    }

    #[instrument(level = "trace", ret, err(level = "trace"))]
    pub(crate) fn connect_sender(
        sender: Arc<IpcSender<MultiMessage>>,
        ipc_sender_uuid: Uuid,
    ) -> Result<Arc<Mutex<MultiSender>>, MuxError> {
        let (response_sender, response_receiver) = ipc::channel()?;
        let client_id = ClientId::new();
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
    pub(crate) fn notify_sub_channel(
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
    pub(crate) fn is_receiver_connected(&self, scid: SubChannelId) -> bool {
        loop {
            match self.response_receiver.try_recv() {
                Ok(MultiResponse::SubReceiverDisconnected(disconnected_scid)) => {
                    if let Some(proxy) = self
                        .sub_receiver_proxies
                        .lock()
                        .unwrap()
                        .get(&disconnected_scid)
                    {
                        proxy.disconnect();
                    }
                },
                Err(ipc_channel::TryRecvError::Empty) => break,
                Err(ipc_channel::TryRecvError::IpcError(_)) => return false,
            }
        }
        if let Some(proxy) = self.sub_receiver_proxies.lock().unwrap().get(&scid) {
            !proxy.disconnected()
        } else {
            true
        }
    }
}

pub(crate) struct SubChannelDisconnector {
    sub_channel_id: SubChannelId,
    ipc_sender: Arc<IpcSender<MultiMessage>>,
    source: Uuid,
    multi_sender: Arc<Mutex<MultiSender>>,
}

impl SubChannelDisconnector {
    pub(crate) fn new(
        sub_channel_id: SubChannelId,
        ipc_sender: Arc<IpcSender<MultiMessage>>,
        source: Uuid,
        multi_sender: Arc<Mutex<MultiSender>>,
    ) -> Self {
        SubChannelDisconnector {
            sub_channel_id,
            ipc_sender,
            source,
            multi_sender,
        }
    }

    pub(crate) fn dropped(&self) {
        if self
            .multi_sender
            .lock()
            .unwrap()
            .is_receiver_connected(self.sub_channel_id)
        {
            if let Err(e) = self
                .ipc_sender
                .send(MultiMessage::Disconnect(self.sub_channel_id, self.source))
            {
                log::debug!(
                    "Failed to send disconnect (other end may have hung up): {}",
                    e
                );
            }
        }
    }
}

pub(crate) struct SubChannelSender {
    sub_channel_id: SubChannelId,
    ipc_sender: Arc<IpcSender<MultiMessage>>,
    disconnector: Arc<SubSenderTracker<dyn Fn() + Send + Sync>>,
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
    pub(crate) fn new(raw_self: Arc<Mutex<MultiSender>>) -> Self {
        let locked_self = raw_self.lock().unwrap();
        let scid = SubChannelId::new();
        let sender_clone = locked_self.clone_ipc_sender();
        let multi_sender_clone = raw_self.clone();
        SubChannelSender {
            sub_channel_id: scid,
            ipc_sender: locked_self.clone_ipc_sender(),
            disconnector: Arc::new(SubSenderTracker::new(Box::new(move || {
                SubChannelDisconnector::new(
                    scid,
                    sender_clone.clone(),
                    ORIGIN,
                    multi_sender_clone.clone(),
                )
                .dropped();
            }))),
            ipc_sender_uuid: locked_self.uuid(),
            sender_id: Arc::clone(&locked_self.sender_id),
            multi_sender: Arc::clone(&raw_self),
        }
    }

    pub(crate) fn from_deserialized(
        sub_channel_id: SubChannelId,
        ipc_sender: Arc<IpcSender<MultiMessage>>,
        disconnector: Arc<SubSenderTracker<dyn Fn() + Send + Sync>>,
        ipc_sender_uuid: Uuid,
        multi_sender: Arc<Mutex<MultiSender>>,
    ) -> Self {
        SubChannelSender {
            sub_channel_id,
            ipc_sender,
            disconnector,
            ipc_sender_uuid,
            sender_id: Arc::new(Mutex::new(Source::new())),
            multi_sender,
        }
    }

    #[instrument(level = "debug", skip(msg), err(level = "debug"))]
    pub(crate) fn send<T>(&self, msg: T) -> Result<(), MuxError>
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
                if let Err(e) = ipc_sender.send(MultiMessage::Sending {
                    scid: subchannel_id,
                    via: self.sub_channel_id,
                    via_chan: Self::ipc_sender_and_or_uuid(
                        &sender_id,
                        &self.ipc_sender,
                        self.ipc_sender_uuid,
                    ),
                }) {
                    log::debug!("Failed to send Sending notification: {}", e);
                }
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
    pub(crate) fn sub_channel_id(&self) -> SubChannelId {
        self.sub_channel_id
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

pub(crate) type SubChannelSenderSendDetails = (SubChannelId, Uuid, Arc<IpcSender<MultiMessage>>);

pub(crate) type SubChannelSenderSerializedDetails = (
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

        let scsi = SubChannelSenderIds::new(self.sub_channel_id, self.ipc_sender_uuid.to_string());
        log::trace!("Serializing {:?}", scsi);
        scsi.serialize(serializer)
    }
}
