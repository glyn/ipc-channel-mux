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
/// let tx: mux::SubSender<u32> = received.into_sub_sender();
///
/// tx.send(42).unwrap();
/// assert_eq!(rx.recv().unwrap(), 42);
/// ```
///
/// # Lifecycle
///
/// Converting a [`SubSender<T>`] to `IpcChannelSubSender` sends a `Sending`
/// lifecycle notification so the subchannel state machine does not signal
/// premature disconnection while the transport is in transit. Calling
/// [`into_sub_sender`] sends a `Received` notification to register the new
/// process as a sender source. Dropping the reconstructed [`SubSender<T>`]
/// sends `Disconnect` as usual.
///
/// # Limitation
///
/// The reconstructed [`SubSender<T>`] cannot detect subreceiver disconnection:
/// `is_receiver_connected` always returns `true` because no response channel is
/// available to the new process. Full support requires a future protocol
/// extension.
///
/// [`SubSender<T>`]: super::subchannel_endpoint::SubSender
/// [`into_sub_sender`]: IpcChannelSubSender::into_sub_sender
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct IpcChannelSubSender<T> {
    pub(crate) sub_channel_id: SubChannelId,
    pub(crate) ipc_sender: RawIpcSender<MultiMessage>,
    pub(crate) ipc_sender_uuid: Uuid,
    /// Keepalive sender. Held by the receiving process as long as this wrapper
    /// (or the `SubSender` reconstructed from it) is alive; dropping it signals
    /// the parent's probe that the receiver has crashed or finished.
    /// `None` when keepalive channel creation failed at transport time.
    pub(crate) keepalive_tx: Option<RawIpcSender<()>>,
    #[serde(skip)]
    pub(crate) _phantom: PhantomData<T>,
}
