// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Wrapper for transmitting a [`SubSender<T>`] over a raw `ipc-channel` IPC channel.
//!
//! [`SubSender<T>`]: super::subchannel_endpoint::SubSender

use crate::mux::protocol::{MultiMessage, SubChannelId};
use ipc_channel::ipc::IpcSender as RawIpcSender;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use uuid::Uuid;

/// Wrapper for transmitting a [`SubSender<T>`] over a raw `ipc-channel` IPC channel.
///
/// Unlike [`SubSender<T>`], which uses a mux-specific thread-local serialization
/// mechanism, `IpcChannelSubSender<T>` implements [`Serialize`]/[`Deserialize`] in
/// a way that lets ipc-channel transport the embedded OS handle natively, so it
/// can be included as a field in any message type sent over an
/// [`ipc_channel::ipc::IpcSender`].
///
/// # Usage
///
/// ```
/// use ipc_channel::ipc;
/// use ipc_channel_mux::mux;
///
/// let channel = mux::Channel::new().unwrap();
/// let (tx, rx) = channel.sub_channel::<u32>();
///
/// // Wrap the SubSender for IPC channel transport (consuming it).
/// let transport = mux::IpcChannelSubSender::from(tx);
///
/// // Send over a raw IPC channel, then reconstruct on the receiving side.
/// let (raw_tx, raw_rx) = ipc::channel().unwrap();
/// raw_tx.send(transport).unwrap();
/// let received: mux::IpcChannelSubSender<u32> = raw_rx.recv().unwrap();
/// let tx: mux::SubSender<u32> = received.into_sub_sender().unwrap();
///
/// tx.send(42).unwrap();
/// assert_eq!(rx.recv().unwrap(), 42);
/// ```
///
/// # Lifecycle
///
/// Converting a [`SubSender<T>`] to `IpcChannelSubSender` sends a
/// `SendingViaIpcChannel` lifecycle notification so the subchannel state
/// machine does not signal premature disconnection while the transport is in
/// transit and can detect a crash of the receiving process. Calling
/// [`into_sub_sender`] sends a `Connect` message to establish a response
/// channel (enabling subreceiver-disconnection detection) and a `Received`
/// notification to register the new process as a sender source. Dropping the
/// reconstructed [`SubSender<T>`] sends `Disconnect` as usual.
///
/// [`SubSender<T>`]: super::subchannel_endpoint::SubSender
/// [`into_sub_sender`]: IpcChannelSubSender::into_sub_sender
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct IpcChannelSubSender<T> {
    sub_channel_id: SubChannelId,
    ipc_sender: RawIpcSender<MultiMessage>,
    ipc_sender_uuid: Uuid,
    /// Keepalive sender. Held by the receiving process as long as this wrapper
    /// (or the `SubSender` reconstructed from it) is alive; dropping it signals
    /// the parent's probe that the receiver has crashed or finished.
    /// `None` when keepalive channel creation failed at transport time.
    keepalive_tx: Option<RawIpcSender<()>>,
    #[serde(skip)]
    _phantom: PhantomData<T>,
}

impl<T> IpcChannelSubSender<T> {
    pub(crate) fn new(
        sub_channel_id: SubChannelId,
        ipc_sender: RawIpcSender<MultiMessage>,
        ipc_sender_uuid: Uuid,
        keepalive_tx: Option<RawIpcSender<()>>,
    ) -> Self {
        IpcChannelSubSender {
            sub_channel_id,
            ipc_sender,
            ipc_sender_uuid,
            keepalive_tx,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SubChannelId,
        RawIpcSender<MultiMessage>,
        Uuid,
        Option<RawIpcSender<()>>,
    ) {
        (
            self.sub_channel_id,
            self.ipc_sender,
            self.ipc_sender_uuid,
            self.keepalive_tx,
        )
    }
}
