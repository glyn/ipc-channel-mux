// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Source and Target provide identification of IPC senders. The goals is
// to avoid sending the same IPC sender multiple times over an IPC channel,
// since that tends to consume scarce operating system resources, such as
// file descriptors on Unix variants.
//
// There are two "sides" to the module: source and target. The source side
// records IPC senders. The intention is that the first time a source sends
// an IPC sender over a channel, it will send the sender along with a UUID
// that identifies the sender. When the source side sends the same IPC
// sender subsequently over the same channel, it sends just the sender's
// UUID.
//
// The target side of the module receives an IPC sender along with its UUID
// and stores the UUID and the associated sender. When the target side
// receives just a UUID, it looks up the associated sender.
//
// Although the two sides of the module are decoupled, they are provided
// as a single module so that unit tests can demonstrate the intended usage.
//
// The module uses weak hashtables to allow the senders to be dropped.
// In the source side hashtable, the elements are senders held by weak
// pointers and compared by pointer. In the target side hashtable, the keys
// are UUIDs and the values are senders held by weak pointers.
//
// The sender types are abstracted to be weak pointers with a strong form
// that can be dereferenced. An example of a weak pointer type is
// Weak<IpcSender<MultiMessage>> which can be implemented using
// Rc<IpcSender<MultiMessage>>, for example. See the tests below for some
// analogous code.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use uuid::Uuid;
use weak_table::PtrWeakHashSet;
use weak_table::traits::WeakElement;

/// Source is where IPC senders are transmitted from. It tracks senders,
/// as they are sent, in a weak hashtable. Thus the hashtable does not
/// prevent a sender from being dropped.
#[derive(Debug)]
pub struct Source<T>
where
    T: Sized,
    T: WeakElement,
    <T as WeakElement>::Strong: Debug,
    <T as WeakElement>::Strong: Deref,
{
    senders: RefCell<PtrWeakHashSet<T>>,
}

impl<T> Source<T>
where
    T: Sized,
    T: WeakElement,
    <T as WeakElement>::Strong: Debug,
    <T as WeakElement>::Strong: Deref,
{
    pub fn new() -> Source<T> {
        Source {
            senders: RefCell::new(PtrWeakHashSet::new()),
        }
    }

    /// insert returns whether a given sender has already been sent by this Source
    /// and inserts it if it has not.
    pub fn insert(&mut self, s: <T as WeakElement>::Strong) -> bool {
        self.senders.borrow_mut().insert(s)
    }
}

/// Target is where IPC senders from a Source are received. The associations
/// between UUIDs and senders are stored in a regular hashtable, which prevent the
/// sender from being dropped.
pub struct Target<T> {
    senders: RefCell<HashMap<Uuid, T>>,
}

impl<T> Target<T>
where
    T: Clone,
{
    /// new creates a new Target with an empty hashtable.
    pub fn new() -> Target<T> {
        Target {
            senders: RefCell::new(HashMap::new()),
        }
    }

    /// add associates a UUID with a sender. The UUID must not already be
    /// associated with another sender, but this IS NOT CHECKED. As this
    /// would, with the intended usage of Source and Target here, amount to a
    /// UUID collision, the probability of this occurring is vanishingly small.
    pub fn add(&mut self, id: Uuid, s: &T) {
        let mut end_points = self.senders.borrow_mut();
        end_points.entry(id).or_insert_with(|| s.clone());
    }

    /// lookup looks up the sender associated with the given UUID.
    /// If the UUID was found return Some(e) where e is the sender
    /// associated with the UUID. Otherwise, there is no sender
    /// associated with the UUID, so return None.
    pub fn look_up(&self, id: Uuid) -> Option<T> {
        self.senders.borrow().get(&id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::{Rc, Weak};

    #[test]
    fn test_source() {
        type Table = Source<Weak<str>>;

        let mut map = Table::new();
        let a = Rc::<str>::from("a");
        assert!(!map.insert(Rc::clone(&a)));

        let b = Rc::<str>::from("b");
        assert!(!map.insert(Rc::clone(&b)));

        assert!(map.insert(Rc::clone(&a)));

        assert!(map.insert(Rc::clone(&b)));

        drop(a);

        assert!(map.insert(Rc::clone(&b)));

        let c = Rc::<str>::from("c");
        assert!(!map.insert(c));
    }

    #[test]
    fn test_target() {
        type Table = Target<u32>;

        let mut map = Table::new();

        let u = Uuid::new_v4();

        // An unassociated UUID should return None from look_up.
        assert_eq!(map.look_up(u), None);

        // Associating an unassociated UUID with an sender should succeed.
        map.add(u, &1);

        // Associating an associated UUID with the same sender should succeed.
        map.add(u, &1);

        // look_up should find the sender associated with a UUID.
        let val = map.look_up(u);
        assert_eq!(val, Some(1));

        // Associating an associated UUID with a distinct sender DOES NOT FAIL.
        map.add(u, &2);

        // Associating an unassociated UUID with an unassociated sender should succeed.
        let u2 = Uuid::new_v4();
        map.add(u2, &3);

        // Associating another UUID with an sender already associated with a
        // distinct UUID should succeed.
        let u3 = Uuid::new_v4();
        map.add(u3, &1);
    }

    // The following test demonstrates the intended use of Source and Target.
    #[test]
    fn test_source_with_target() {
        type SourceTable = Source<Weak<u32>>;
        type TargetTable = Target<Rc<u32>>;

        let mut source_map = SourceTable::new();

        // The first time a given sender is sent from source to target, it is
        // sent along with a UUID newly associated with the sender by the
        // source. This association is recorded by the target.

        let a = Rc::<u32>::from(1);
        let id = Uuid::new_v4();
        let already_sent = source_map.insert(Rc::clone(&a));
        assert!(!already_sent);

        let mut target_map = TargetTable::new();

        target_map.add(id, &a);

        // Subsequent times the sender is sent from source to target, just
        // the UUID associated with the sender is sent. The target looks up
        // the sender using the UUID.

        let already_sent2 = source_map.insert(Rc::clone(&a));
        assert!(already_sent2);

        let val = target_map.look_up(id);
        assert_eq!(val, Some(a.clone()));
    }
}
