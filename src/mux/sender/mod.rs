// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

mod sender_id;

use crate::mux::error::MuxError;
use crate::mux::ipc_channel::SyncOpaqueIpcReceiver;
use crate::mux::ipc_channel::{
    clear_ipc_receiver_serialization_context, clear_ipc_sender_serialization_context,
    take_ipc_receivers_for_send, take_ipc_senders_for_send,
};
use crate::mux::protocol::{
    ClientId, EMPTY_SUBCHANNEL_ID, IpcSenderAndOrId, MultiMessage, MultiResponse, ORIGIN,
    SubChannelId, SubChannelSenderIds,
};
use crate::mux::shared_memory::{clear_shmem_serialization_context, take_shmems_for_send};
use crate::mux::subchannel_lifecycle::{SubReceiverProxy, SubSenderTracker};
use ipc_channel::ipc::{self, IpcReceiver, IpcSender};
use sender_id::Source;
use serde::{Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tracing::instrument;
use uuid::Uuid;

/// Sending end of a multiplexed channel.
///
/// [MultiSender]: struct.MultiSender.html
pub struct MultiSender {
    client_id: ClientId,
    ipc_sender: Arc<IpcSender<MultiMessage>>,
    uuid: Uuid,
    sender_id: Arc<Mutex<Source<Weak<IpcSender<MultiMessage>>>>>,
    response_receiver: Option<IpcReceiver<MultiResponse>>,
    disconnected: AtomicBool,
    sub_receiver_proxies: Mutex<HashMap<SubChannelId, SubReceiverProxy>>,
}

pub type Target = sender_id::Target<Arc<Mutex<MultiSender>>>;

impl fmt::Debug for MultiSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiSender")
            .field("client_id", &self.client_id)
            .field("uuid", &self.uuid)
            .finish_non_exhaustive()
    }
}

impl MultiSender {
    pub fn new(
        client_id: ClientId,
        ipc_sender: Arc<IpcSender<MultiMessage>>,
        response_receiver: IpcReceiver<MultiResponse>,
    ) -> Self {
        MultiSender {
            client_id,
            ipc_sender,
            uuid: Uuid::new_v4(),
            sender_id: Arc::new(Mutex::new(Source::new())),
            response_receiver: Some(response_receiver),
            disconnected: AtomicBool::new(false),
            sub_receiver_proxies: Mutex::new(HashMap::new()),
        }
    }

    pub fn clone_ipc_sender(&self) -> Arc<IpcSender<MultiMessage>> {
        Arc::clone(&self.ipc_sender)
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn send_message(&self, msg: MultiMessage) -> Result<(), MuxError> {
        self.ipc_sender.send(msg).map_err(From::from)
    }

    pub fn probe(&self) -> bool {
        self.drain_responses()
    }

    pub fn insert_sub_receiver_proxy(&self, scid: SubChannelId, proxy: SubReceiverProxy) {
        self.sub_receiver_proxies
            .lock()
            .unwrap()
            .insert(scid, proxy);
    }

    #[instrument(level = "debug", ret, err(level = "debug"))]
    pub fn connect(name: String) -> Result<Arc<Mutex<MultiSender>>, MuxError> {
        let sender = Arc::new(IpcSender::connect(name)?);
        Self::connect_sender(sender, Uuid::new_v4())
    }

    #[instrument(level = "trace", ret, err(level = "trace"))]
    pub fn connect_sender(
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
            response_receiver: Some(response_receiver),
            disconnected: AtomicBool::new(false),
            sub_receiver_proxies: Mutex::new(HashMap::new()),
        })))
    }

    #[instrument(level = "debug", err(level = "debug"))]
    pub fn notify_sub_channel(
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
    pub fn is_receiver_connected(&self, scid: SubChannelId) -> bool {
        self.drain_responses();
        if let Some(proxy) = self.sub_receiver_proxies.lock().unwrap().get(&scid) {
            !proxy.disconnected()
        } else {
            true
        }
    }

    /// Drains all pending messages from the response channel, processing
    /// `SubReceiverDisconnected` notifications and caching IPC disconnection.
    /// Returns `true` if the response channel is still alive, `false` if
    /// the channel is disconnected.
    fn drain_responses(&self) -> bool {
        if self.disconnected.load(Ordering::Relaxed) {
            return false;
        }
        let Some(ref response_receiver) = self.response_receiver else {
            return true;
        };
        loop {
            match response_receiver.try_recv() {
                Ok(MultiResponse::SubReceiverDisconnected(scid)) => {
                    if let Some(proxy) = self.sub_receiver_proxies.lock().unwrap().get(&scid) {
                        proxy.disconnect();
                    }
                },
                Err(ipc_channel::TryRecvError::Empty) => return true,
                Err(ipc_channel::TryRecvError::IpcError(_)) => {
                    self.disconnected.store(true, Ordering::Relaxed);
                    return false;
                },
            }
        }
    }
}

pub struct SubChannelDisconnector {
    sub_channel_id: SubChannelId,
    ipc_sender: Arc<IpcSender<MultiMessage>>,
    source: Uuid,
    multi_sender: Arc<Mutex<MultiSender>>,
}

impl SubChannelDisconnector {
    pub fn new(
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

    pub fn dropped(&self) {
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
                log::debug!("Failed to send disconnect (other end may have hung up): {e}");
            }
        }
    }
}

/// Components returned by [`SubChannelSender::begin_ipc_channel_transport`]:
/// the subchannel ID, raw IPC sender, sender UUID, and optional keepalive sender.
pub(crate) type IpcChannelTransportParts = (
    SubChannelId,
    IpcSender<MultiMessage>,
    Uuid,
    Option<ipc_channel::ipc::IpcSender<()>>,
);

pub struct SubChannelSender {
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
    pub fn new(raw_self: Arc<Mutex<MultiSender>>) -> Self {
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

    pub fn from_deserialized(
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

    /// Extract the components needed to reconstruct this sender in another process
    /// via an IPC channel transport. Sends a `SendingViaIpcChannel` lifecycle
    /// notification (with a keepalive sender included in the returned value) so the
    /// state machine does not signal premature disconnection while the transport is
    /// in transit, and can detect a crash of the receiving process.
    pub fn begin_ipc_channel_transport(self) -> Result<IpcChannelTransportParts, MuxError> {
        let raw_ipc_sender = (*self.ipc_sender).clone();
        match ipc::channel::<()>() {
            Ok((keepalive_tx, keepalive_rx)) => {
                if let Err(e) = self.multi_sender.lock().unwrap().send_message(
                    MultiMessage::SendingViaIpcChannel {
                        scid: self.sub_channel_id,
                        keepalive: SyncOpaqueIpcReceiver::new(keepalive_rx.to_opaque()),
                    },
                ) {
                    log::debug!(
                        "Failed to send SendingViaIpcChannel notification: {e}; \
                         crash detection unavailable for this transport"
                    );
                }
                Ok((
                    self.sub_channel_id,
                    raw_ipc_sender,
                    self.ipc_sender_uuid,
                    Some(keepalive_tx),
                ))
            },
            Err(e) => {
                log::debug!(
                    "Failed to create keepalive channel for IPC channel transport: {e}; \
                     falling back to Sending without crash detection"
                );
                self.multi_sender
                    .lock()
                    .unwrap()
                    .send_message(MultiMessage::Sending {
                        scid: self.sub_channel_id,
                        // EMPTY_SUBCHANNEL_ID is the sentinel for "not via any subchannel"; the
                        // matching Received notification in into_sub_sender uses the same key.
                        via: EMPTY_SUBCHANNEL_ID,
                        via_chan: IpcSenderAndOrId::IpcSender(
                            raw_ipc_sender.clone(),
                            self.ipc_sender_uuid.to_string(),
                        ),
                    })?;
                Ok((
                    self.sub_channel_id,
                    raw_ipc_sender,
                    self.ipc_sender_uuid,
                    None,
                ))
            },
        }
    }

    #[instrument(level = "debug", skip(msg), err(level = "debug"))]
    pub fn send<T>(&self, msg: T) -> Result<(), MuxError>
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
        clear_shmem_serialization_context();
        clear_ipc_sender_serialization_context();
        clear_ipc_receiver_serialization_context();

        let payload = postcard::to_stdvec(&msg).map_err(MuxError::from)?;

        let shmems = take_shmems_for_send();
        let ipc_channel_senders = take_ipc_senders_for_send();
        let ipc_channel_receivers = take_ipc_receivers_for_send();
        let serialized_senders = take_serialization_context();

        // Notify transmission of any subchannel senders so that they are counted during transmission.
        // Also build the list of IPC sender references for the Data message.
        let mut srs: Vec<(SubChannelId, IpcSenderAndOrId)> =
            Vec::with_capacity(serialized_senders.len());
        for ctx in &serialized_senders {
            if let Err(e) = ctx.ipc_sender.send(MultiMessage::Sending {
                scid: ctx.sub_channel_id,
                via: self.sub_channel_id,
                via_chan: Self::ipc_sender_and_or_uuid(
                    &ctx.sender_id,
                    &self.ipc_sender,
                    self.ipc_sender_uuid,
                ),
            }) {
                log::debug!("Failed to send Sending notification: {e}");
            }

            srs.push((
                ctx.sub_channel_id,
                Self::ipc_sender_and_or_uuid(&self.sender_id, &ctx.ipc_sender, ctx.ipc_sender_uuid),
            ));
        }

        let result = self.ipc_sender.send(MultiMessage::Data(
            self.sub_channel_id,
            payload,
            srs,
            shmems,
            ipc_channel_senders,
            ipc_channel_receivers,
        ));
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
            log::trace!("sending UUID {ipc_sender_uuid} associated with previously sent IpcSender");
            IpcSenderAndOrId::IpcSenderId(ipc_sender_uuid.to_string())
        } else {
            log::trace!("sending IpcSender with UUID {ipc_sender_uuid}");
            IpcSenderAndOrId::IpcSender(
                Arc::<IpcSender<MultiMessage>>::unwrap_or_clone(ipc_sender.clone()),
                ipc_sender_uuid.to_string(),
            )
        }
    }

    #[instrument(level = "trace", ret)]
    pub fn sub_channel_id(&self) -> SubChannelId {
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

struct SerializedSenderContext {
    sub_channel_id: SubChannelId,
    ipc_sender_uuid: Uuid,
    ipc_sender: Arc<IpcSender<MultiMessage>>,
    sender_id: Arc<Mutex<Source<Weak<IpcSender<MultiMessage>>>>>,
}

thread_local! {
    static SERIALIZED_SENDERS: Mutex<Vec<SerializedSenderContext>> = const { Mutex::new(vec!()) };
}

fn clear_serialization_context() {
    SERIALIZED_SENDERS.with(|senders| {
        senders.lock().unwrap().clear();
    });
}

fn take_serialization_context() -> Vec<SerializedSenderContext> {
    SERIALIZED_SENDERS.with(|senders| std::mem::take(&mut *senders.lock().unwrap()))
}

impl Serialize for SubChannelSender {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        log::trace!(
            "Adding SubChannelSender with SubChannelId {} to SERIALIZED_SENDERS",
            self.sub_channel_id
        );

        SERIALIZED_SENDERS.with(|senders| {
            senders.lock().unwrap().push(SerializedSenderContext {
                sub_channel_id: self.sub_channel_id,
                ipc_sender_uuid: self.ipc_sender_uuid,
                ipc_sender: self.ipc_sender.clone(),
                sender_id: self.sender_id.clone(),
            });
        });

        let scsi = SubChannelSenderIds::new(self.sub_channel_id, self.ipc_sender_uuid.to_string());
        log::trace!("Serializing {scsi:?}");
        scsi.serialize(serializer)
    }
}
