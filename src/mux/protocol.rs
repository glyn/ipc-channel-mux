// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::mux::ipc_channel::SyncOpaqueIpcReceiver;
use ipc_channel::ipc::{IpcSender, IpcSharedMemory, OpaqueIpcSender};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter};
use uuid::Uuid;

pub const EMPTY_SUBCHANNEL_ID: SubChannelId =
    SubChannelId(uuid::uuid!("11111111-10b1-428f-9447-cb680e5fe0c8"));
pub const ORIGIN: Uuid = uuid::uuid!("00000000-10b1-428f-9447-cb680e5fe0c8");

#[derive(Eq, Clone, Copy, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct ClientId(Uuid);

impl ClientId {
    pub fn new() -> ClientId {
        ClientId(Uuid::new_v4())
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Eq, Clone, Copy, Debug, Hash, PartialEq)]
pub struct SubChannelId(Uuid);

impl SubChannelId {
    pub fn new() -> SubChannelId {
        SubChannelId(Uuid::new_v4())
    }
}

impl Default for SubChannelId {
    fn default() -> Self {
        Self::new()
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
        let uuid = Uuid::parse_str(&content).map_err(serde::de::Error::custom)?;
        Ok(SubChannelId(uuid))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubChannelSenderIds {
    sub_channel_id: SubChannelId,
    ipc_sender_uuid: String,
}

impl SubChannelSenderIds {
    pub fn new(sub_channel_id: SubChannelId, ipc_sender_uuid: String) -> Self {
        SubChannelSenderIds {
            sub_channel_id,
            ipc_sender_uuid,
        }
    }

    pub fn sub_channel_id(&self) -> SubChannelId {
        self.sub_channel_id
    }
}

/// MultiMessage is used to communicate across multiplexing channels.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum MultiMessage {
    Connect(IpcSender<MultiResponse>, ClientId),
    Data(
        SubChannelId,
        Vec<u8>,
        Vec<(SubChannelId, IpcSenderAndOrId)>,
        Vec<IpcSharedMemory>,
        Vec<OpaqueIpcSender>,
        Vec<SyncOpaqueIpcReceiver>,
    ),
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
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IpcSenderAndOrId {
    IpcSender(IpcSender<MultiMessage>, String),
    IpcSenderId(String),
}

/// MultiResponse is used to communicate from the receiver of a multiplexing channel to the sender
/// via an additional channel in the reverse direction.
#[derive(Serialize, Deserialize, Debug)]
pub enum MultiResponse {
    /// The SubReceiver for the subchannel identified by the given subchannel id. has disconnected (been dropped).
    SubReceiverDisconnected(SubChannelId),
}
