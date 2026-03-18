// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Wrappers for transmitting [`ipc_channel::ipc::IpcSender`] and
//! [`ipc_channel::ipc::IpcReceiver`] values over subchannels.
//!
//! [`IpcSender`] and [`IpcReceiver`] use the same thread-local mechanism as
//! [`SharedMemory`]: during inner (postcard) serialization the OS handle is
//! captured in a thread-local and only an index is written into the payload
//! bytes. After serialization the captured handles are lifted into the outer
//! [`MultiMessage::Data`] fields, where ipc-channel's outer serialization
//! transports them via OS handle passing.
//!
//! [`SharedMemory`]: super::shared_memory::SharedMemory
//! [`MultiMessage::Data`]: super::protocol::MultiMessage::Data

use crate::mux::error::MuxError;
use ipc_channel::ipc::{
    IpcReceiver as RawIpcReceiver, IpcSender as RawIpcSender, OpaqueIpcReceiver, OpaqueIpcSender,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cell::RefCell;
use std::marker::PhantomData;

/// Wrapper around [`OpaqueIpcReceiver`] that adds `Sync`.
///
/// # Safety
/// [`MultiMessage`] values are serialised and sent immediately after
/// construction and are never shared between threads, so the `!Sync` bound
/// from `OsIpcReceiver`'s `Cell<i32>` is not observable in practice.
///
/// [`MultiMessage`]: super::protocol::MultiMessage
pub(crate) struct SyncOpaqueIpcReceiver(pub(crate) OpaqueIpcReceiver);

// SAFETY: see doc comment above.
unsafe impl Sync for SyncOpaqueIpcReceiver {}

impl std::fmt::Debug for SyncOpaqueIpcReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SyncOpaqueIpcReceiver").finish()
    }
}

impl Serialize for SyncOpaqueIpcReceiver {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SyncOpaqueIpcReceiver {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        OpaqueIpcReceiver::deserialize(deserializer).map(SyncOpaqueIpcReceiver)
    }
}

thread_local! {
    static IPC_SENDERS_FOR_SER: RefCell<Vec<OpaqueIpcSender>> = const { RefCell::new(Vec::new()) };
    static IPC_RECEIVERS_FOR_SER: RefCell<Vec<SyncOpaqueIpcReceiver>> = const { RefCell::new(Vec::new()) };
    static IPC_SENDERS_FOR_DE: RefCell<Vec<Option<OpaqueIpcSender>>> = const { RefCell::new(Vec::new()) };
    static IPC_RECEIVERS_FOR_DE: RefCell<Vec<Option<OpaqueIpcReceiver>>> = const { RefCell::new(Vec::new()) };
}

/// Wrapper for transmitting an [`ipc_channel::ipc::IpcSender`] over a subchannel.
///
/// Analogous to [`SharedMemory`] for [`IpcSharedMemory`], this type wraps
/// [`ipc_channel::ipc::IpcSender`] and implements [`Serialize`]/[`Deserialize`]
/// using a thread-local mechanism so OS handles are transported via the outer
/// ipc-channel layer rather than the inner postcard serialization.
///
/// Use [`into_inner`] to recover the underlying [`ipc_channel::ipc::IpcSender`]
/// after receiving.
///
/// [`SharedMemory`]: super::shared_memory::SharedMemory
/// [`IpcSharedMemory`]: ipc_channel::ipc::IpcSharedMemory
/// [`into_inner`]: IpcSender::into_inner
pub struct IpcSender<T>(OpaqueIpcSender, PhantomData<T>);

impl<T> Clone for IpcSender<T> {
    fn clone(&self) -> Self {
        IpcSender(self.0.clone(), PhantomData)
    }
}

impl<T: Serialize + for<'de> Deserialize<'de>> From<RawIpcSender<T>> for IpcSender<T> {
    fn from(sender: RawIpcSender<T>) -> Self {
        IpcSender(sender.to_opaque(), PhantomData)
    }
}

impl<T: Serialize + for<'de> Deserialize<'de>> IpcSender<T> {
    /// Recover the underlying [`ipc_channel::ipc::IpcSender<T>`].
    pub fn into_inner(self) -> RawIpcSender<T> {
        self.0.to::<T>()
    }
}

impl<T> Serialize for IpcSender<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let index = IPC_SENDERS_FOR_SER.with(|senders| {
            let mut senders = senders.borrow_mut();
            let idx = senders.len();
            senders.push(self.0.clone());
            idx
        });
        index.serialize(serializer)
    }
}

impl<'de, T: Serialize + Deserialize<'de>> Deserialize<'de> for IpcSender<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let index: usize = Deserialize::deserialize(deserializer)?;
        let opaque = IPC_SENDERS_FOR_DE
            .with(|senders| {
                let mut senders = senders.borrow_mut();
                senders
                    .get_mut(index)
                    .and_then(Option::take)
                    .ok_or_else(|| {
                        format!(
                            "IpcSender {index} not available \
                             (have {} senders)",
                            senders.len()
                        )
                    })
            })
            .map_err(serde::de::Error::custom)?;
        Ok(IpcSender(opaque, PhantomData))
    }
}

/// Wrapper for transmitting an [`ipc_channel::ipc::IpcReceiver`] over a subchannel.
///
/// Analogous to [`SharedMemory`] for [`IpcSharedMemory`], this type wraps
/// [`ipc_channel::ipc::IpcReceiver`] and implements [`Serialize`]/[`Deserialize`]
/// using a thread-local mechanism so OS handles are transported via the outer
/// ipc-channel layer rather than the inner postcard serialization.
///
/// An `IpcReceiver` may only be serialized once; attempting to serialize it a
/// second time returns an error.
///
/// Use [`into_inner`] to recover the underlying [`ipc_channel::ipc::IpcReceiver`]
/// after receiving.
///
/// [`SharedMemory`]: super::shared_memory::SharedMemory
/// [`IpcSharedMemory`]: ipc_channel::ipc::IpcSharedMemory
/// [`into_inner`]: IpcReceiver::into_inner
pub struct IpcReceiver<T>(RefCell<Option<OpaqueIpcReceiver>>, PhantomData<T>);

impl<T: Serialize + for<'de> Deserialize<'de>> From<RawIpcReceiver<T>> for IpcReceiver<T> {
    fn from(receiver: RawIpcReceiver<T>) -> Self {
        IpcReceiver(RefCell::new(Some(receiver.to_opaque())), PhantomData)
    }
}

impl<T: Serialize + for<'de> Deserialize<'de>> IpcReceiver<T> {
    /// Recover the underlying [`ipc_channel::ipc::IpcReceiver<T>`].
    ///
    /// Returns an error if the receiver has already been serialized.
    pub fn into_inner(self) -> Result<RawIpcReceiver<T>, MuxError> {
        self.0
            .into_inner()
            .ok_or_else(|| MuxError::InternalError("IpcReceiver already serialized".to_string()))
            .map(OpaqueIpcReceiver::to::<T>)
    }
}

impl<T> Serialize for IpcReceiver<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let opaque = self
            .0
            .borrow_mut()
            .take()
            .ok_or_else(|| serde::ser::Error::custom("IpcReceiver already serialized"))?;
        let index = IPC_RECEIVERS_FOR_SER.with(|receivers| {
            let mut receivers = receivers.borrow_mut();
            let idx = receivers.len();
            receivers.push(SyncOpaqueIpcReceiver(opaque));
            idx
        });
        index.serialize(serializer)
    }
}

impl<'de, T: Serialize + Deserialize<'de>> Deserialize<'de> for IpcReceiver<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let index: usize = Deserialize::deserialize(deserializer)?;
        let opaque = IPC_RECEIVERS_FOR_DE
            .with(|receivers| {
                let mut receivers = receivers.borrow_mut();
                receivers
                    .get_mut(index)
                    .and_then(Option::take)
                    .ok_or_else(|| {
                        format!(
                            "IpcReceiver {index} not available \
                             (have {} receivers)",
                            receivers.len()
                        )
                    })
            })
            .map_err(serde::de::Error::custom)?;
        Ok(IpcReceiver(RefCell::new(Some(opaque)), PhantomData))
    }
}

// Context management functions used by the sender and demuxer.

pub(crate) fn clear_ipc_sender_serialization_context() {
    IPC_SENDERS_FOR_SER.with(|s| s.borrow_mut().clear());
}

pub(crate) fn clear_ipc_receiver_serialization_context() {
    IPC_RECEIVERS_FOR_SER.with(|r| r.borrow_mut().clear());
}

pub(crate) fn take_ipc_senders_for_send() -> Vec<OpaqueIpcSender> {
    IPC_SENDERS_FOR_SER.with(|s| std::mem::take(&mut *s.borrow_mut()))
}

pub(crate) fn take_ipc_receivers_for_send() -> Vec<SyncOpaqueIpcReceiver> {
    IPC_RECEIVERS_FOR_SER.with(|r| std::mem::take(&mut *r.borrow_mut()))
}

pub(crate) fn set_ipc_senders_for_recv(senders: Vec<OpaqueIpcSender>) {
    IPC_SENDERS_FOR_DE.with(|s| {
        *s.borrow_mut() = senders.into_iter().map(Some).collect();
    });
}

pub(crate) fn set_ipc_receivers_for_recv(receivers: Vec<SyncOpaqueIpcReceiver>) {
    IPC_RECEIVERS_FOR_DE.with(|r| {
        *r.borrow_mut() = receivers.into_iter().map(|r| Some(r.0)).collect();
    });
}

pub(crate) fn clear_ipc_sender_deserialization_context() {
    IPC_SENDERS_FOR_DE.with(|s| s.borrow_mut().clear());
}

pub(crate) fn clear_ipc_receiver_deserialization_context() {
    IPC_RECEIVERS_FOR_DE.with(|r| r.borrow_mut().clear());
}
