// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module provides lifecycle management for subchannel senders.
//! It consists of two types:
//! * Source: a sourceId and a set of IpcSenderIds at the source
//! * Target: Keeps track of IpcSenderIds at rest and in flight
//! WIP: flesh this out concretely in multiplex.rs and then generalise
//!      it here?
//!
//! IDEA: Model this on counter.rs.

use hashbag::HashBag;
use std::cell::RefCell;
use std::{collections::HashMap, hash::Hash};
use uuid::Uuid;

pub enum Message {

}

pub enum Error {
    SubChannelAlreadyExists,
    SubChannelNotFound,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct SourceId(Uuid);

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct TargetId(Uuid);

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct IpcSenderId(Uuid);

// SubSender, MultiSender contribution

struct Sender {
    sender_id: IpcSenderId,
}

// There is one Source associated with each multiplexing IpcSender.
pub struct Source<SubChannelId> {
    source_id: SourceId,
    senders: HashMap<SubChannelId, Sender>,
}

impl<SubChannelId> Source<SubChannelId>
where
    SubChannelId: Eq + Hash,
{
    pub fn new(tx: mpsc::Sender<Message>) -> Source<SubChannelId> {
        Source {
            source_id: SourceId(Uuid::new_v4()),
            senders: HashMap::new(),
        }
    }

    pub fn add_subchannel(&mut self, scid: SubChannelId, sender_id: IpcSenderId) -> Result<(), Error> {
        unimplemented!();
        if self.senders.contains_key(&scid) {
            Err(Error::SubChannelAlreadyExists)
        } else {
            self.senders.insert(
                scid,
                Sender {
                    sender_id: sender_id,
                },
            );
            Ok(())
        }
    }

    pub fn prepare_to_send(&self, scid: SubChannelId, ) -> Result<(SourceId, IpcSenderId), Error> {
        unimplemented!();
        if let Some(sender) = self.senders.get(&scid) {
            Ok((self.source_id, sender.sender_id))
        } else {
            Err(Error::SubChannelNotFound)
        }
    }
}

// SubReceiver, MultiReceiver contribution

// Track an in-flight sender. If transmission fails, returns false. If transmission has succeeded,
// or could succeed, returns true.
type Tracker = fn() -> bool;

struct Receiver {
    receiver_id: Uuid,
    sources: HashBag<SourceId>,
    in_flight: HashBag<(SourceId, IpcSenderId)>,
    trackers: HashMap<(SourceId, IpcSenderId), Tracker>
}

// There is one Target associated with each multiplexing IpcReceiver.
pub struct Target<SubChannelId> {
    target_id: TargetId,
    receivers: HashMap<SubChannelId, RefCell<Receiver>>,
}

impl<SubChannelId> Target<SubChannelId>
where
    SubChannelId: Eq + Hash,
{
    pub fn new(rx: mpsc::Receiver<Message>) -> Target<SubChannelId> {
        Target {
            target_id: TargetId(Uuid::new_v4()),
            receivers: HashMap::new(),
        }
    }

    pub fn add_subchannel(&mut self, scid: SubChannelId, receiver_id: Uuid) -> Result<(), Error> {
        unimplemented!();
        if self.receivers.contains_key(&scid) {
            Err(Error::SubChannelAlreadyExists)
        } else {
            self.receivers.insert(
                scid,
                RefCell::new(Receiver {
                    receiver_id: receiver_id,
                    sources: HashBag::new(),
                    in_flight: HashBag::new(),
                    trackers: HashMap::new(),
                }),
            );
            Ok(())
        }
    }

    pub fn send(&self, scid: SubChannelId, from: SourceId, via: IpcSenderId, tracker: Tracker) -> Result<(), Error> {
        unimplemented!();
        if let Some(receiver) = self.receivers.get(&scid) {
            let mut cell = receiver.borrow_mut();
            cell.in_flight.insert((from, via));
            cell.trackers.insert((from, via), tracker);
            Ok(())
        } else {
            Err(Error::SubChannelNotFound)
        }
    }

    pub fn is_connected(&self, scid: SubChannelId) -> Result<bool, Error> {
        unimplemented!();
        if let Some(receiver) = self.receivers.get(&scid) {
            let cell = receiver.borrow();
            Ok(!cell.in_flight.is_empty() || !cell.sources.is_empty())
        } else {
            Err(Error::SubChannelNotFound)
        }
    }

    pub fn receive(&self, scid: SubChannelId, from: SourceId, via: IpcSenderId, to: SourceId) -> Result<(), Error> {
        unimplemented!();
        if let Some(receiver) = self.receivers.get(&scid) {
            let mut cell = receiver.borrow_mut();
            if cell.in_flight.remove(&(from, via)) == 0 {
                cell.trackers.remove(&(from, via));
            }
            cell.sources.insert(to);
            Ok(())
        } else {
            Err(Error::SubChannelNotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use uuid::Uuid;

    #[test]
    fn test_add_subchannel() {
        let (tx, rx) = mpsc::channel();
        let mut src = Source::new(tx);
        let tgt = Target::new(tx);

        let scid = 1;
        let sender_id = IpcSenderId(Uuid::new_v4());
        src.add_subchannel(scid, sender_id);

        // How to test effect on tgt?
        // 1. Let tgt operate on a trait implemented by subreceiver
        // 2. Wrap subreceiver in a similar way counter.rs does

    }
}