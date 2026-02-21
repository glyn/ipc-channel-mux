// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::mux::channel_identification::{Source, Target};
use crate::mux::demux::{
    Demuxer, MultiReceiver, MultiReceiverSet, ReceiverDemuxer, SelectableMultiReceiver,
    SelectableReceiverDemuxer,
};
use crate::mux::error::MuxError;
use crate::mux::protocol::{ClientId, MultiMessage};
use crate::mux::sender::{MultiSender, SubChannelSender};
use crate::mux::subchannel_endpoint::{SelectableSubReceiver, SubReceiver, SubSender};
use crate::mux::subchannel_lifecycle;
use ipc_channel::ipc::{self, IpcOneShotServer, IpcReceiver};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use tracing::instrument;
use uuid::Uuid;
use weak_table::WeakValueHashMap;

/// Channel wraps an IPC channel and is used to construct subchannels.
pub struct Channel {
    multi_sender: Arc<Mutex<MultiSender>>,
    multi_receiver: Arc<MultiReceiver>,
}

/// SelectableChannel wraps an IPC channel and is used to construct subchannels.
pub(crate) struct SelectableChannel {
    multi_sender: Arc<Mutex<MultiSender>>,
    multi_receiver: Arc<SelectableMultiReceiver>,
}

impl Channel {
    /// Construct a new [Channel].
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn new() -> Result<Channel, MuxError> {
        let (ms, mr) = multi_channel()?;
        Ok(Channel {
            multi_sender: ms,
            multi_receiver: mr,
        })
    }

    /// Construct a new subchannel of a [Channel]. The subchannel has
    /// a [SubSender] and a [SubReceiver].
    #[instrument(level = "debug", skip(self))]
    pub fn sub_channel<T>(&self) -> (SubSender<T>, SubReceiver<T>)
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let scs = SubChannelSender::new(Arc::clone(&self.multi_sender));
        let scid = scs.sub_channel_id();
        self.multi_sender
            .lock()
            .unwrap()
            .sub_receiver_proxies
            .lock()
            .unwrap()
            .insert(scid, subchannel_lifecycle::SubReceiverProxy::new());
        let scr = MultiReceiver::attach(&self.multi_receiver, scid);
        (
            SubSender {
                sub_channel_sender: scs,
                phantom: PhantomData,
            },
            SubReceiver {
                sub_channel_receiver: scr,
                phantom: PhantomData,
            },
        )
    }
}
impl SelectableChannel {
    /// Construct a new [SelectableChannel].
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn new() -> Result<SelectableChannel, MuxError> {
        let (ms, mr) = selectable_multi_channel()?;
        Ok(SelectableChannel {
            multi_sender: ms,
            multi_receiver: mr,
        })
    }

    /// Construct a new subchannel of a [Channel]. The subchannel has
    /// a [SubSender] and a [SubReceiver].
    #[instrument(level = "debug", skip(self))]
    pub fn sub_channel<T>(&self) -> (SubSender<T>, SelectableSubReceiver<T>)
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let scs = SubChannelSender::new(Arc::clone(&self.multi_sender));
        let scid = scs.sub_channel_id();
        self.multi_sender
            .lock()
            .unwrap()
            .sub_receiver_proxies
            .lock()
            .unwrap()
            .insert(scid, subchannel_lifecycle::SubReceiverProxy::new());
        let scr = SelectableMultiReceiver::attach(&self.multi_receiver, scid);
        (
            SubSender {
                sub_channel_sender: scs,
                phantom: PhantomData,
            },
            SelectableSubReceiver {
                sub_channel_receiver: scr,
                phantom: PhantomData,
            },
        )
    }
}

/// SubOneShotServer together with its generated name can be used to establish a subchannel
/// between processes.
///
/// On the server side, call [accept] against the server to obtain the subchannel receiver
/// and receive the first message.
///
/// On the client side, call [connect], passing the server name, to obtain the subchannel
/// sender. The server is "one-shot" because it accepts only one connect request from a client.
///
/// [accept]: crate::mux::SubOneShotServer::accept
/// [connect]: crate::mux::SubSender::connect
pub struct SubOneShotServer<T> {
    one_shot_multi_server: OneShotMultiServer,
    name: String,
    phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for SubOneShotServer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubOneShotServer")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<T> SubOneShotServer<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    /// Construct a new server with a generated name in order to establish a subchannel
    /// between processes.
    ///
    /// Call accept on the server to obtain a subchannel receiver and receive the first message.
    ///
    /// Call connect passing the server name to obtain the subchannel sender.
    #[instrument(level = "debug", ret, err(level = "debug"))]
    pub fn new() -> Result<(SubOneShotServer<T>, String), MuxError> {
        let (one_shot_multi_server, name) = OneShotMultiServer::new()?;
        Ok((
            SubOneShotServer {
                one_shot_multi_server,
                name: name.clone(),
                phantom: PhantomData,
            },
            name,
        ))
    }

    /// Obtain a [SubReceiver] from a server and receive the first message.
    #[instrument(level = "debug", err(level = "debug"))]
    pub fn accept(self) -> Result<(SubReceiver<T>, T), MuxError> {
        let multi_receiver = self.one_shot_multi_server.accept()?;
        let (subchannel_id, name) = MultiReceiver::receive_sub_channel(&multi_receiver)
            .expect("receive sub channel failed");
        if name != self.name {
            return Err(MuxError::InternalError(format!(
                "unexpected sub channel name {}",
                name
            )));
        }
        let sub_receiver = MultiReceiver::attach(&multi_receiver, subchannel_id);
        let msg: T = sub_receiver.recv()?;
        Ok((
            SubReceiver {
                sub_channel_receiver: sub_receiver,
                phantom: PhantomData,
            },
            msg,
        ))
    }
}

/// Create a multiplexing channel that can be used to establish subchannels
/// between processes. The subchannels flow across the multichannel.
/// A multiplexing channel represents a fixed collection of IPC resources.
/// Subchannels consume no further IPC resources.
/// Each subchannel is allocated an identity and this identity can flow
/// across the multiplexing channel.
///
/// [MultiSender]: struct.MultiSender.html
/// [MultiReceiver]: struct.MultiReceiver.html
#[instrument(level = "debug", ret, err(level = "debug"))]
fn multi_channel() -> Result<(Arc<Mutex<MultiSender>>, Arc<MultiReceiver>), io::Error> {
    let (ipc_sender, ipc_receiver) = ipc::channel()?;
    let (ipc_response_sender, ipc_response_receiver) = ipc::channel()?;
    let client_id = ClientId(Uuid::new_v4());
    let mut senders = HashMap::new();
    senders.insert(client_id, ipc_response_sender);
    let multi_receiver = MultiReceiver {
        ipc_receiver_uuid: Uuid::new_v4(),
        receiver_demuxer: ReceiverDemuxer {
            ipc_receiver,
            demuxer: Arc::new(Mutex::new(Demuxer {
                ipc_senders: senders,
                sub_channels: HashMap::new(),
                disconnectors: WeakValueHashMap::new(),
                ipc_senders_by_id: Target::new(),
            })),
        },
    };
    let multi_receiver_rc = Arc::new(multi_receiver);
    let multi_sender = MultiSender {
        client_id,
        ipc_sender: Arc::new(ipc_sender),
        uuid: Uuid::new_v4(),
        sender_id: Arc::new(Mutex::new(Source::new())),
        response_receiver: ipc_response_receiver,
        sub_receiver_proxies: Mutex::new(HashMap::new()),
    };
    Ok((Arc::new(Mutex::new(multi_sender)), multi_receiver_rc))
}

#[instrument(level = "debug", ret, err(level = "debug"))]
fn selectable_multi_channel()
-> Result<(Arc<Mutex<MultiSender>>, Arc<SelectableMultiReceiver>), io::Error> {
    let (ipc_sender, ipc_receiver) = ipc::channel()?;
    let (ipc_response_sender, ipc_response_receiver) = ipc::channel()?;
    let client_id = ClientId(Uuid::new_v4());
    let mut senders = HashMap::new();
    senders.insert(client_id, ipc_response_sender);
    #[allow(clippy::arc_with_non_send_sync)]
    let mrs = Arc::new(Mutex::new(MultiReceiverSet::new()?));
    let multi_receiver_rc = Arc::new(SelectableMultiReceiver {
        ipc_receiver_uuid: Uuid::new_v4(),
        receiver_demuxer: SelectableReceiverDemuxer {
            multi_receiver_set: Arc::clone(&mrs),
            demuxer: Arc::new(Mutex::new(Demuxer {
                ipc_senders: senders,
                sub_channels: HashMap::new(),
                disconnectors: WeakValueHashMap::new(),
                ipc_senders_by_id: Target::new(),
            })),
        },
    });
    {
        let mut mrs_mut = mrs.lock().unwrap();
        mrs_mut.pending = Some((ipc_receiver, Arc::clone(&multi_receiver_rc)));
    }
    let multi_sender = MultiSender {
        client_id,
        ipc_sender: Arc::new(ipc_sender),
        uuid: Uuid::new_v4(),
        sender_id: Arc::new(Mutex::new(Source::new())),
        response_receiver: ipc_response_receiver,
        sub_receiver_proxies: Mutex::new(HashMap::new()),
    };
    Ok((Arc::new(Mutex::new(multi_sender)), multi_receiver_rc))
}

struct OneShotMultiServer {
    multi_server: IpcOneShotServer<MultiMessage>,
}

impl OneShotMultiServer {
    #[instrument(level = "debug", err(level = "debug"))]
    fn new() -> Result<(OneShotMultiServer, String), io::Error> {
        let (multi_server, name) = IpcOneShotServer::new()?;
        Ok((OneShotMultiServer { multi_server }, name))
    }

    #[instrument(level = "debug", skip(self), ret, err(level = "debug"))]
    fn accept(self) -> Result<Arc<MultiReceiver>, MuxError> {
        let (ipc_receiver, multi_message): (IpcReceiver<MultiMessage>, MultiMessage) =
            self.multi_server.accept()?;

        let mr = MultiReceiver {
            ipc_receiver_uuid: Uuid::new_v4(),
            receiver_demuxer: ReceiverDemuxer {
                ipc_receiver,
                demuxer: Arc::new(Mutex::new(Demuxer {
                    ipc_senders: HashMap::new(),
                    sub_channels: HashMap::new(),
                    disconnectors: WeakValueHashMap::new(),
                    ipc_senders_by_id: Target::new(),
                })),
            },
        };
        let mr_rc = Arc::new(mr);
        mr_rc
            .receiver_demuxer
            .demuxer
            .lock()
            .unwrap()
            .handle(multi_message, mr_rc.ipc_receiver_uuid)?;
        Ok(mr_rc)
    }
}
