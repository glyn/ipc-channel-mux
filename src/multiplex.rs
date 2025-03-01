// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use bincode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::io;
use std::sync::mpsc::{self, Sender, Receiver};
use crate::ipc::{self, IpcError, IpcOneShotServer, IpcSender, IpcReceiver};

type ClientId = u64;
type SubChannelId = u64;

/// Sending end of a multiplexed channel.
///
/// [MultiSender]: struct.MultiSender.html
pub struct MultiSender {
    client_id: ClientId,
    ipc_sender: IpcSender<MultiMessage>,
    ipc_receiver: IpcReceiver<MultiResponse>,
}

impl MultiSender {
    // This function takes a MultiReceiver as it depends on running the MultiReceiver synchronously.
    pub fn new<T>(&mut self, multi_receiver: &mut MultiReceiver) -> SubChannelSender<T> {
        self.ipc_sender.send(MultiMessage::AllocateSubChannel(self.client_id)).expect("send failed");
        multi_receiver.receive().expect("receive failed");
        let response = self.ipc_receiver.recv().expect("receive failed");
        let sub_channel_id = match response {
            MultiResponse::ClientId(_) => panic!("unexpected response"),
            MultiResponse::SubChannel(sub_channel_id) => {sub_channel_id},
        };
        let scs = SubChannelSender {
            sub_channel: sub_channel_id,
            ipc_sender: self.ipc_sender.clone(),
            phantom: PhantomData,
        };
        scs
    }
    
    // This function depends on the MultiReceiver being run asynchronously.
    pub fn new2<T>(&mut self) -> SubChannelSender<T> {
        self.ipc_sender.send(MultiMessage::AllocateSubChannel(self.client_id)).expect("send failed");
        let response = self.ipc_receiver.recv().expect("receive failed");
        let sub_channel_id = match response {
            MultiResponse::ClientId(_) => panic!("unexpected response"),
            MultiResponse::SubChannel(sub_channel_id) => {sub_channel_id},
        };
        let scs = SubChannelSender {
            sub_channel: sub_channel_id,
            ipc_sender: self.ipc_sender.clone(),
            phantom: PhantomData,
        };
        scs
    }

    /// Create a [MultiSender] connected to a previously defined [IpcOneShotMultiServer].
    ///
    /// This function should not be called more than once per [IpcOneShotMultiServer],
    /// otherwise the behaviour is unpredictable.
    /// Compare [issue 378](https://github.com/servo/ipc-channel/issues/378).
    ///
    /// [MultiSender]: struct.MultiSender.html
    /// [OneShotMultiServer]: struct.OneShotMultiServer.html
    pub fn connect(name: String) -> Result<MultiSender, io::Error> {
        let sender = IpcSender::connect(name)?;
        let (response_sender, response_receiver) = ipc::channel().expect("failed to create response channel");
        sender.send(MultiMessage::Connect(response_sender)).expect("send failed");
        let resp = response_receiver.recv().expect("failed to receive response to connect");
        let client_id = match resp {
            MultiResponse::ClientId(client_id) => client_id,
            MultiResponse::SubChannel(_) => panic!("unexpected response"),
        };
        Ok(MultiSender {
            client_id: client_id,
            ipc_sender: sender,
            ipc_receiver: response_receiver,
        })
    }

    pub fn notify_sub_channel(&self, sub_channel_id: SubChannelId, name: String) -> Result<(), io::Error> {
        self.ipc_sender.send(MultiMessage::SubChannelId(sub_channel_id, name)).expect("send failed");
        Ok(())
    }
}

/// Receiving end of a multiplexed channel.
///
/// [MultiReceiver]: struct.MultiReceiver.html
pub struct MultiReceiver {
    ipc_receiver: IpcReceiver<MultiMessage>,
    ipc_senders: HashMap<ClientId, IpcSender<MultiResponse>>,
    next_client_id: ClientId,
    sub_channels: HashMap<SubChannelId, Sender<Vec<u8>>>,
    next_sub_channel_id: SubChannelId,
}

impl MultiReceiver {
    pub fn attach<'a, T>(&'a mut self, sub_channel_id: SubChannelId) -> SubChannelReceiver<'a, T> {
        let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::channel();
        self.sub_channels.insert(sub_channel_id, tx);
        SubChannelReceiver {
            multi_receiver: self,
            channel: rx,
            phantom: PhantomData,
        }
    }

    pub fn receive(&mut self) -> Result<(), IpcError> {
        let msg = self.ipc_receiver.recv()?;
        self.handle(msg)
    }

    fn handle(&mut self, msg: MultiMessage) -> Result<(), IpcError> {
        match msg {
            MultiMessage::Connect(sender) => {
                self.next_client_id += 1;
                let client_id = self.next_client_id;
                sender.send(MultiResponse::ClientId(client_id)).expect("send failed");
                self.ipc_senders.insert(client_id, sender);
                Ok(())
            },
            MultiMessage::AllocateSubChannel(client_id) => {
                self.next_sub_channel_id += 1;
                let response_sender = self.ipc_senders.get(&client_id).expect("invalid client id");
                response_sender.send(MultiResponse::SubChannel(self.next_client_id)).expect("send failed");
                Ok(())
            }
            MultiMessage::Data(scid, data) => {
                self.sub_channels.get(&scid).expect("subchannel not found").send(data).unwrap();
                Ok(())
            },
            m => {
                panic!("unexpected multi message {:?}", m);
            }
        }
    }

    pub fn receive_sub_channel(&mut self) -> Result<(SubChannelId, String), IpcError> {
        let msg = self.ipc_receiver.recv()?;
        match msg {
            MultiMessage::SubChannelId(sub_channel_id, name) => {
                Ok((sub_channel_id, name))
            },
            m => {
                panic!("unexpected multi message {:?}", m);
            }
        }
    }
}

pub struct SubChannelSender<T> {
    sub_channel: SubChannelId,
    ipc_sender: IpcSender<MultiMessage>,
    phantom: PhantomData<T>,
}

impl<T> SubChannelSender<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    pub fn send(&self, msg: T) -> Result<(), bincode::Error> {
        self.ipc_sender.send(MultiMessage::Data(self.sub_channel, bincode::serialize(&msg)?))
    }

    pub fn sub_channel_id(&self) -> SubChannelId {
        self.sub_channel
    }
}

pub struct SubChannelReceiver<'a, T> {
    multi_receiver: &'a mut MultiReceiver,
    channel: Receiver<Vec<u8>>,
    phantom: PhantomData<T>,
}

impl<T> SubChannelReceiver<'_, T> where
    T: for<'de> Deserialize<'de> + Serialize,
{
    pub fn recv(&mut self) -> Result<T, bincode::Error> {
        self.multi_receiver.receive().unwrap();
        bincode::deserialize(self.channel.recv().unwrap().as_slice())
    }
}

/// MultiMessage is used to communicate across multiplexing channels.
#[derive(Serialize, Deserialize, Debug)]
enum MultiMessage {
    Connect(IpcSender<MultiResponse>),
    AllocateSubChannel(ClientId),
    Data(SubChannelId, Vec<u8>),
    SubChannelId(SubChannelId, String),
}

/// MultiResponse is used to communicate across multiplexing channels in response to a MultiMessage.
#[derive(Serialize, Deserialize, Debug)]
enum MultiResponse {
    ClientId(ClientId),
    SubChannel(SubChannelId),
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
pub fn multi_channel() -> Result<(MultiSender, MultiReceiver), io::Error>
{
    let (ipc_sender, ipc_receiver) = ipc::channel()?;
    let (ipc_response_sender, ipc_response_receiver) = ipc::channel()?;
    let client_id = 1;
    let mut senders = HashMap::new();
    senders.insert(client_id, ipc_response_sender);
    let multi_receiver = MultiReceiver {
        ipc_receiver: ipc_receiver,
        ipc_senders: senders,
        next_client_id: client_id + 1,
        sub_channels: HashMap::new(),
        next_sub_channel_id: 0,
    };
    let multi_sender = MultiSender {
        client_id: client_id,
        ipc_sender: ipc_sender,
        ipc_receiver: ipc_response_receiver,
    };
    Ok((multi_sender, multi_receiver))
}

/// A multiplexing server associated with a given name. The server is "one-shot" because
/// it accepts only one connect request from a client.
pub struct OneShotMultiServer {
    multi_server: IpcOneShotServer<MultiMessage>,
}

impl OneShotMultiServer
{
    pub fn new() -> Result<(OneShotMultiServer, String), io::Error> {
        let (multi_server, name) = IpcOneShotServer::new()?;
        Ok((
            OneShotMultiServer {
                multi_server,
            },
            name,
        ))
    }

    pub fn accept(self) -> Result<MultiReceiver, bincode::Error> {
        let (multi_receiver, multi_message) : (IpcReceiver<MultiMessage>, MultiMessage) = self.multi_server.accept()?;

        let mut mr = MultiReceiver {
            ipc_receiver: multi_receiver,
            ipc_senders: HashMap::new(),
            next_client_id: 0,
            sub_channels: HashMap::new(),
            next_sub_channel_id: 0,
        };
        mr.handle(multi_message).unwrap();
        Ok(mr)
    }
}