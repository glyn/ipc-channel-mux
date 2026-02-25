// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Shared memory for multiplexed IPC subchannels.
//!
//! [`SharedMemory`] is a thin wrapper around [`IpcSharedMemory`] with custom
//! serialization that works with the mux's two-stage serialization model.
//! During inner (postcard) serialization, the wrapper captures
//! [`IpcSharedMemory`] values into a thread-local and writes only an index
//! into the payload bytes. The captured values are then included in the
//! protocol message so that ipc-channel's outer serialization transports them
//! efficiently via OS shared memory.

use ipc_channel::ipc::IpcSharedMemory;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cell::RefCell;
use std::ops::Deref;

thread_local! {
    static SHMEM_FOR_SERIALIZATION: RefCell<Vec<IpcSharedMemory>> = const { RefCell::new(Vec::new()) };
    static SHMEM_FOR_DESERIALIZATION: RefCell<Vec<Option<IpcSharedMemory>>> = const { RefCell::new(Vec::new()) };
}

/// Shared memory region for use with multiplexed IPC subchannels.
///
/// Analogous to [`IpcSharedMemory`] but designed for transmission over
/// multiplexed subchannels. Convert to/from [`IpcSharedMemory`] using the
/// [`From`] implementations.
#[derive(Clone, Debug, PartialEq)]
pub struct SharedMemory(IpcSharedMemory);

impl SharedMemory {
    /// Create a shared memory region initialized with the provided bytes.
    pub fn from_bytes(bytes: &[u8]) -> SharedMemory {
        SharedMemory(IpcSharedMemory::from_bytes(bytes))
    }

    /// Create a shared memory region filled with a single byte value.
    pub fn from_byte(byte: u8, length: usize) -> SharedMemory {
        SharedMemory(IpcSharedMemory::from_byte(byte, length))
    }
}

impl Deref for SharedMemory {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl From<IpcSharedMemory> for SharedMemory {
    fn from(shmem: IpcSharedMemory) -> Self {
        SharedMemory(shmem)
    }
}

impl From<SharedMemory> for IpcSharedMemory {
    fn from(shmem: SharedMemory) -> Self {
        shmem.0
    }
}

impl Serialize for SharedMemory {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let index = SHMEM_FOR_SERIALIZATION.with(|regions| {
            let mut regions = regions.borrow_mut();
            let idx = regions.len();
            regions.push(self.0.clone());
            idx
        });
        index.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SharedMemory {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let index: usize = Deserialize::deserialize(deserializer)?;
        let shmem = SHMEM_FOR_DESERIALIZATION
            .with(|regions| {
                let mut regions = regions.borrow_mut();
                regions
                    .get_mut(index)
                    .and_then(Option::take)
                    .ok_or_else(|| {
                        format!(
                            "shared memory region {index} not available \
                             (have {} regions)",
                            regions.len()
                        )
                    })
            })
            .map_err(serde::de::Error::custom)?;
        Ok(SharedMemory(shmem))
    }
}

// Context management functions used by the sender and demuxer.

pub(crate) fn clear_shmem_serialization_context() {
    SHMEM_FOR_SERIALIZATION.with(|regions| {
        regions.borrow_mut().clear();
    });
}

pub(crate) fn take_shmems_for_send() -> Vec<IpcSharedMemory> {
    SHMEM_FOR_SERIALIZATION.with(std::cell::RefCell::take)
}

pub(crate) fn set_shmems_for_recv(shmems: Vec<IpcSharedMemory>) {
    SHMEM_FOR_DESERIALIZATION.with(|regions| {
        *regions.borrow_mut() = shmems.into_iter().map(Some).collect();
    });
}

pub(crate) fn clear_shmem_deserialization_context() {
    SHMEM_FOR_DESERIALIZATION.with(|regions| {
        regions.borrow_mut().clear();
    });
}
