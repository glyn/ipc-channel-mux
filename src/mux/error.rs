// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use ipc_channel::IpcError;
use thiserror::Error;

/// This enumeration lists the possible reasons for failure of functions and methods in the [mux]
/// module.
///
/// [mux]: crate::mux
#[derive(Debug, Error)]
pub enum MuxError {
    /// An error has occurred while receiving a message from the IPC channel underlying a subchannel.
    #[error("IPC error: {0}")]
    IpcError(#[from] IpcError),
    /// No more messages may be received.
    ///
    /// Returned from [send] when the subchannel's [SubReceiver] has disconnected (has been
    /// deallocated or its process has terminated) and no more messages can be received.
    ///
    /// Returned from [recv] or [accept] when all the subchannel's [SubSender]s have disconnected (have been
    /// deallocated or their processes have terminated) and no more messages are available to be received.
    ///
    /// [accept]: crate::mux::SubOneShotServer::accept
    /// [recv]: crate::mux::SubReceiver::recv
    /// [send]: crate::mux::SubSender::send
    /// [SubReceiver]: crate::mux::SubReceiver
    /// [SubSender]: crate::mux::SubSender
    #[error("Disconnected")]
    Disconnected,
    /// An internal logic error has occurred.
    #[error("Internal error: {0}")]
    InternalError(String),
}

/// This enumeration lists the possible reasons for failure of non-blocking receive operations on
/// subchannels.
///
/// Compared with [MuxError], which is returned by blocking [recv], this type has an additional
/// [Empty] variant indicating that no message is currently available.
///
/// [recv]: crate::mux::SubReceiver::recv
/// [Empty]: TryRecvError::Empty
#[derive(Debug, Error)]
pub enum TryRecvError {
    /// An error has occurred while receiving a message.
    ///
    /// Wraps a [MuxError] which may indicate disconnection ([MuxError::Disconnected]),
    /// an IPC error ([MuxError::IpcError]), or an internal error ([MuxError::InternalError]).
    #[error("{0}")]
    MuxError(#[from] MuxError),
    /// No message is currently available on the subchannel.
    ///
    /// At least one corresponding [SubSender] may still exist, so messages may yet arrive.
    /// This variant is only returned by non-blocking operations.
    ///
    /// [SubSender]: crate::mux::SubSender
    #[error("Channel empty")]
    Empty,
}

impl From<std::io::Error> for MuxError {
    fn from(err: std::io::Error) -> MuxError {
        MuxError::IpcError(IpcError::Io(err))
    }
}

impl From<postcard::Error> for MuxError {
    fn from(err: postcard::Error) -> MuxError {
        MuxError::IpcError(IpcError::SerializationError(err.into()))
    }
}
