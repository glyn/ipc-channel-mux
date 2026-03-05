// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::mux::demux::SubChannelReceiver;
use crate::mux::error::{MuxError, TryRecvError};
use crate::mux::sender::SubChannelSender;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::time::Duration;
use tracing::instrument;

/// SubSender is the sending end of a subchannel, used to serialize and send messages of a given type.
///
/// SubSenders can be sent in messages on other subchannels and can be cloned.
#[derive(Debug, Deserialize, Serialize)]
pub struct SubSender<T>
where
    T: Serialize,
{
    sub_channel_sender: SubChannelSender,
    phantom: PhantomData<T>,
}

impl<T> SubSender<T>
where
    T: Serialize,
{
    /// Convert a SubChannelSender to a SubSender.
    pub fn from_sender(sub_channel_sender: SubChannelSender) -> Self {
        SubSender {
            sub_channel_sender,
            phantom: PhantomData,
        }
    }
}

impl<T> Clone for SubSender<T>
where
    T: Serialize,
{
    fn clone(&self) -> SubSender<T> {
        SubSender {
            sub_channel_sender: self.sub_channel_sender.clone(),
            phantom: PhantomData,
        }
    }
}

impl<T> SubSender<T>
where
    T: Serialize,
{
    /// Connect to a server, passing the server name returned from [new], to construct a [SubSender].
    ///
    /// This function must not be called more than once per [SubOneShotServer],
    /// otherwise the behaviour is unpredictable.
    /// For more information, see [issue 378](https://github.com/servo/ipc-channel/issues/378).
    ///
    /// [new]: crate::mux::channel::SubOneShotServer::new
    /// [SubOneShotServer]: crate::mux::channel::SubOneShotServer
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn connect(name: String) -> Result<SubSender<T>, MuxError> {
        let multi_sender = super::sender::MultiSender::connect(name.clone())?;
        let sub_channel_sender = SubChannelSender::new(std::sync::Arc::clone(&multi_sender));
        super::sender::MultiSender::notify_sub_channel(
            multi_sender,
            sub_channel_sender.sub_channel_id(),
            name,
        )?;
        Ok(SubSender {
            sub_channel_sender,
            phantom: PhantomData,
        })
    }

    /// Send a message across the subchannel to the [SubReceiver].
    ///
    /// A successful send occurs when the corresponding [SubReceiver] has not already been deallocated and the process
    /// containing the [SubReceiver] has not already terminated. Ok is returned, but the message will not necessarily be
    /// received: [recv] might not to be called to receive the message or the corresponding [SubReceiver] might be
    /// deallocated, or the [SubReceiver]'s process might terminate, before [recv] is called to receive the message.
    ///
    /// An unsuccessful send occurs when the corresponding [SubReceiver] has already been deallocated or the
    /// [SubReceiver]'s process has already terminated. Err is returned and the message will never be received.
    ///
    /// This method will never block the current thread.
    ///
    /// [recv]: crate::mux::SubReceiver::recv
    #[instrument(level = "debug", skip(self, data), err(level = "debug"))]
    pub fn send(&self, data: T) -> Result<(), MuxError> {
        self.sub_channel_sender.send(data)
    }

    /// Convert a SubSender to an OpaqueSubSender by erasing its message type.
    pub fn to_opaque(self) -> OpaqueSubSender {
        OpaqueSubSender {
            sub_channel_sender: self.sub_channel_sender,
        }
    }
}

/// OpaqueSubSender is a SubSender with the message type erased. It can be
/// passed around in a message type independent manner, but must be converted
/// into a SubSender before it can be used to send messages.
#[derive(Deserialize, Serialize)]
pub struct OpaqueSubSender {
    sub_channel_sender: SubChannelSender,
}

impl OpaqueSubSender {
    /// Convert an OpaqueSubSender to a SubSender by restoring its message type.
    /// If the message type and the original message type have incompatible
    /// serial representations, deserialization may produce errors or unexpected
    /// deserialized values.
    pub fn to<'de, T>(self) -> SubSender<T>
    where
        T: Deserialize<'de> + Serialize,
    {
        SubSender {
            sub_channel_sender: self.sub_channel_sender,
            phantom: PhantomData,
        }
    }

    /// Convert an OpaqueSubSender to a BytesSubSender.
    pub fn to_bytes(self) -> BytesSubSender {
        BytesSubSender {
            sub_channel_sender: self.sub_channel_sender,
        }
    }
}

/// SubReceiver is the receiving end of a subchannel, used to receive and deserialize messages of a given type.
#[derive(Debug)]
pub struct SubReceiver<T>
where
    T: for<'x> Deserialize<'x> + Serialize,
{
    sub_channel_receiver: SubChannelReceiver,
    phantom: PhantomData<T>,
}

impl<T> SubReceiver<T>
where
    T: for<'x> Deserialize<'x> + Serialize,
{
    /// Convert a SubChannelReceiver to a SubReceiver.
    pub fn from_receiver(sub_channel_receiver: SubChannelReceiver) -> Self {
        SubReceiver {
            sub_channel_receiver,
            phantom: PhantomData,
        }
    }
}

impl<T> SubReceiver<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    /// Waits for, and returns, a message from the channel or returns an error if all corresponding [SubSender]s have
    /// disconnected (have been deallocated, their processes have terminated, or they have been lost in transmission).
    ///
    /// This method will always block the current thread if no messages are available and it's possible for more messages
    /// to be sent (at least one [SubSender] still exists). Once a message is sent to a corresponding [SubSender],
    /// this method will wake up and return a message.
    ///
    /// If all the corresponding [SubSender]s have disconnected while this method is blocking, this method will wake up
    /// and return Err to indicate that no more messages can ever be received on this subchannel. However, since
    /// subchannels are buffered, messages sent before the [SubSender]s disconnect can still be properly received.
    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    pub fn recv(&self) -> Result<T, MuxError> {
        self.sub_channel_receiver.recv()
    }

    /// Attempts to return a pending message on this subchannel without blocking.
    ///
    /// This method will never block the caller in order to wait for data to become available.
    /// Instead, it will always return immediately with a possible option of pending data on the
    /// subchannel.
    ///
    /// This is useful for an optimistic check before deciding to block on a [SubReceiver].
    ///
    /// Compared with [`recv`](SubReceiver::recv), this function has two failure cases instead of
    /// one (one for disconnection, one for an empty buffer).
    ///
    /// If the subchannel is empty but at least one corresponding [SubSender] still exists
    /// (including in-flight subsenders that have been sent over a subchannel but not yet
    /// received), this method returns `Err(TryRecvError::Empty)`.
    ///
    /// If all corresponding [SubSender]s have disconnected (have been deallocated, their
    /// processes have terminated, or they have been lost in transmission) and no buffered
    /// messages remain, this method returns `Err(TryRecvError::MuxError(MuxError::Disconnected))`.
    ///
    /// If a message is available, it is returned as `Ok(T)`.
    ///
    /// If an error occurs while receiving a message from the underlying IPC channel, this method
    /// returns `Err(TryRecvError::MuxError(...))` wrapping the underlying error.
    ///
    /// # Note on disconnection detection
    ///
    /// Unlike [`recv`](SubReceiver::recv), which blocks until disconnection is detected through
    /// polling, `try_recv` may return `Empty` even when all senders have been dropped if the
    /// disconnection has not yet been observed. A subsequent call to `try_recv` or `recv` will
    /// detect the disconnection and return the appropriate error. This matches the behavior of
    /// [`IpcReceiver::try_recv`](ipc_channel::ipc::IpcReceiver::try_recv) and
    /// [`Receiver::try_recv`](std::sync::mpsc::Receiver::try_recv).
    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.sub_channel_receiver.try_recv()
    }

    /// Blocks for up to the specified duration attempting to receive a message.
    ///
    /// This method will block the caller for at most `duration` waiting for a message to become
    /// available. If a message becomes available before the timeout expires, it is returned as
    /// `Ok(T)`.
    ///
    /// This may block for longer than the specified duration if the underlying IPC channel is
    /// busy.
    ///
    /// If the timeout expires without a message becoming available and at least one corresponding
    /// [SubSender] still exists (including in-flight subsenders), this method returns
    /// `Err(TryRecvError::Empty)`.
    ///
    /// If all corresponding [SubSender]s have disconnected and no buffered messages remain, this
    /// method returns `Err(TryRecvError::MuxError(MuxError::Disconnected))`. This may be
    /// detected before the timeout expires.
    ///
    /// If an error occurs while receiving a message from the underlying IPC channel, this method
    /// returns `Err(TryRecvError::MuxError(...))` wrapping the underlying error.
    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    pub fn try_recv_timeout(&self, duration: Duration) -> Result<T, TryRecvError> {
        self.sub_channel_receiver.try_recv_timeout(duration)
    }

    /// Convert a SubReceiver to an OpaqueSubReceiver by erasing its message type.
    ///
    /// Useful for adding routes to a `RouterProxy`.
    pub fn to_opaque(self) -> OpaqueSubReceiver {
        OpaqueSubReceiver {
            sub_channel_receiver: self.sub_channel_receiver,
        }
    }
}

/// OpaqueSubReceiver is a SubReceiver with the message type erased. It can be
/// passed around in a message type independent manner, but must be converted
/// into a SubReceiver before it can be used to receive messages.
pub struct OpaqueSubReceiver {
    sub_channel_receiver: SubChannelReceiver,
}

impl OpaqueSubReceiver {
    /// Convert an OpaqueSubReceiver to a SubReceiver by restoring its message type.
    /// If the message type and the original message type have incompatible
    /// serial representations, deserialization may produce errors or unexpected
    /// deserialized values.
    pub fn to<'de, T>(self) -> SubReceiver<T>
    where
        T: for<'x> Deserialize<'x> + Serialize,
    {
        SubReceiver {
            sub_channel_receiver: self.sub_channel_receiver,
            phantom: PhantomData,
        }
    }

    /// Convert an OpaqueSubReceiver to a BytesSubReceiver.
    pub fn to_bytes(self) -> BytesSubReceiver {
        BytesSubReceiver {
            sub_channel_receiver: self.sub_channel_receiver,
        }
    }
}

/// BytesSubSender is the sending end of a bytes subchannel, used to send raw byte data
/// with reduced serialization overhead.
///
/// BytesSubSenders can be sent in messages on other subchannels and can be cloned.
///
/// This is the mux equivalent of [ipc_channel::ipc::IpcBytesSender].
#[derive(Debug, Deserialize, Serialize)]
pub struct BytesSubSender {
    sub_channel_sender: SubChannelSender,
}

impl BytesSubSender {
    /// Convert a SubChannelSender to a BytesSubSender.
    pub fn from_sender(sub_channel_sender: SubChannelSender) -> Self {
        BytesSubSender { sub_channel_sender }
    }
}

impl Clone for BytesSubSender {
    fn clone(&self) -> BytesSubSender {
        BytesSubSender {
            sub_channel_sender: self.sub_channel_sender.clone(),
        }
    }
}

impl BytesSubSender {
    /// Send raw bytes across the subchannel to the [BytesSubReceiver].
    ///
    /// Takes a byte slice rather than owned data, matching the
    /// [IpcBytesSender::send](ipc_channel::ipc::IpcBytesSender::send) signature.
    ///
    /// This method will never block the current thread.
    #[instrument(level = "debug", skip(self, data), err(level = "debug"))]
    pub fn send(&self, data: &[u8]) -> Result<(), MuxError> {
        self.sub_channel_sender.send(data.to_vec())
    }

    /// Convert a BytesSubSender to an OpaqueSubSender by erasing its type.
    pub fn to_opaque(self) -> OpaqueSubSender {
        OpaqueSubSender {
            sub_channel_sender: self.sub_channel_sender,
        }
    }
}

/// BytesSubReceiver is the receiving end of a bytes subchannel, used to receive raw byte data
/// with reduced deserialization overhead.
///
/// This is the mux equivalent of [ipc_channel::ipc::IpcBytesReceiver].
#[derive(Debug)]
pub struct BytesSubReceiver {
    sub_channel_receiver: SubChannelReceiver,
}

impl BytesSubReceiver {
    /// Convert a SubChannelReceiver to a BytesSubReceiver.
    pub fn from_receiver(sub_channel_receiver: SubChannelReceiver) -> Self {
        BytesSubReceiver {
            sub_channel_receiver,
        }
    }

    /// Waits for, and returns, bytes from the channel or returns an error if all
    /// corresponding [BytesSubSender]s have disconnected.
    ///
    /// This method will always block the current thread if no messages are available
    /// and it's possible for more messages to be sent.
    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    pub fn recv(&self) -> Result<Vec<u8>, MuxError> {
        self.sub_channel_receiver.recv()
    }

    /// Attempts to return pending bytes on this subchannel without blocking.
    ///
    /// Returns `Err(TryRecvError::Empty)` if no data is available but senders exist.
    /// Returns `Err(TryRecvError::MuxError(MuxError::Disconnected))` if all senders
    /// have disconnected and no buffered messages remain.
    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    pub fn try_recv(&self) -> Result<Vec<u8>, TryRecvError> {
        self.sub_channel_receiver.try_recv()
    }

    /// Blocks for up to the specified duration attempting to receive bytes.
    ///
    /// Returns `Err(TryRecvError::Empty)` if the timeout expires without data.
    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    pub fn try_recv_timeout(&self, duration: Duration) -> Result<Vec<u8>, TryRecvError> {
        self.sub_channel_receiver.try_recv_timeout(duration)
    }

    /// Convert a BytesSubReceiver to an OpaqueSubReceiver by erasing its type.
    ///
    /// Useful for adding routes to a `RouterProxy`.
    pub fn to_opaque(self) -> OpaqueSubReceiver {
        OpaqueSubReceiver {
            sub_channel_receiver: self.sub_channel_receiver,
        }
    }
}
