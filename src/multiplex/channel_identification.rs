// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Source and Target provide identification of generic endpoints (which turn
// out in practice to be IPC senders). The goals is to avoid sending the same
// endpoint multiple times over an IPC channel, since that tends to consume
// operating system resources, such as file descriptors on Unix variants.
//
// There are two "sides" to the module: source and target. The source side
// records endpoints. The intention is that the first time a source sends
// an endpoint over a channel, it will send the endpoint along with a UUID
// that identifies the endpoint. When the source side sends the same endpoint
// subsequently over the same channel, it sends just the endpoint's UUID.
//
// The target side of the module receives an endpoint along with its UUID
// and stores the UUID and the associated endpoint. When the target side
// receives just a UUID, it looks up the associated endpoint.
//
// Although the two sides of the module are decoupled, they are provided
// as a single module so that unit tests can demonstrate the intended usage.
//
// The module uses weak hashtables to allow the endpoints to be dropped.
// In the source side hashtable, the elements are endpoints held by weak
// pointers and compared by pointer. In the target side hashtable, the keys
// are UUIDs and the values are endpoints held by weak pointers.
//
// The endpoint types are abstracted to be weak pointers with a strong form
// that can be dereferenced. An example of a weak pointer type is
// Weak<IpcSender<MultiMessage>> which can be implemented using
// Rc<IpcSender<MultiMessage>>, for example. See the tests below for some
// analogous code.

use std::cell::RefCell;
use std::fmt::Debug;
use std::ops::Deref;
use uuid::Uuid;
use weak_table::{PtrWeakHashSet, WeakValueHashMap};
use weak_table::traits::WeakElement;

/// Source is where endpoints are transmitted from. It tracks endpoints,
/// as they are sent, in a weak hashtable. Thus the hashtable does not
/// prevent an endpoint from being dropped.
#[derive(Debug)]
pub struct Source<T>
where
    T: Sized,
    T: WeakElement,
    <T as WeakElement>::Strong: Debug,
    <T as WeakElement>::Strong: Deref,
{
    end_points: RefCell<PtrWeakHashSet<T>>,
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
            end_points: RefCell::new(PtrWeakHashSet::new()),
        }
    }

    /// insert returns whether a given endpoint has already been sent by this Source
    /// and inserts it if it has not.
    pub fn insert(&mut self, e: <T as WeakElement>::Strong) -> bool {
        self.end_points.borrow_mut().insert(e)
    }
}

/// Target is where endpoints from a Source are received. The associations
/// between UUIDs and endpoints are stored in a weak hashtable. Thus the
/// hashtable does not prevent the endpoint from being dropped. If an endpoint
/// is dropped, the association to a UUID is removed.
pub struct Target<T>
where
    T: WeakElement,
{
    end_points: RefCell<WeakValueHashMap<Uuid, T>>,
}

impl<T> Target<T>
where
    T: WeakElement,
    <T as WeakElement>::Strong: Clone,
{
    /// new creates a new Target with an empty hashtable.
    pub fn new() -> Target<T> {
        Target {
            end_points: RefCell::new(WeakValueHashMap::new()),
        }
    }

    /// add associates a UUID with an endpoint. The UUID must not already be
    /// associated with another endpoint, but this IS NOT CHECKED. As this
    /// would, with the intended usage of Source and Target here, amount to a
    /// UUID collision, the probability of this occurring is vanishingly small.
    pub fn add(&mut self, id: Uuid, e: &<T as WeakElement>::Strong) {
        let mut end_points = self.end_points.borrow_mut();
        if !end_points.contains_key(&id) {
            end_points.insert(id, <T as WeakElement>::Strong::clone(&e));
        }
    }

    /// lookup looks up the endpoint associated with the given UUID.
    /// If the UUID was found return Some(e) where e is the endpoint
    /// associated with the UUID. Otherwise, there is no endpoint
    /// associated with the UUID, so return None.
    pub fn look_up(&self, id: Uuid) -> Option<<T as WeakElement>::Strong> {
        self.end_points.borrow().get(&id)
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
        type Table = Target<Weak<str>>;

        let mut map = Table::new();

        let u = Uuid::new_v4();

        // An unassociated UUID should return None from look_up.
        assert_eq!(map.look_up(u), None);

        // Associating an unassociated UUID with an endpoint should succeed.
        let a = Rc::<str>::from("a");
        map.add(u, &a);

        // Associating an associated UUID with the same endpoint should succeed.
        map.add(u, &a);

        // look_up should find the endpoint associated with a UUID.
        let val = map.look_up(u);
        assert_eq!(val, Some(a.clone()));

        // Associating an associated UUID with a distinct endpoint DOES NOT FAIL.
        let b = Rc::<str>::from("b");
        map.add(u, &b);

        // Associating an unassociated UUID with an unassociated endpoint should succeed.
        let u2 = Uuid::new_v4();
        map.add(u2, &b);

        // Associating another UUID with an endpoint already associated with a
        // distinct UUID should succeed.
        let u3 = Uuid::new_v4();
        map.add(u3, &a);

        // Looking up a UUID associated with a dropped endpoint should fail.
        drop(a);
        drop(val); // another strong pointer to the endpoint
        assert_eq!(map.look_up(u), None);
    }

    // The following test demonstrates the intended use of Source and Target.
    #[test]
    fn test_source_with_target() {
        type SourceTable = Source<Weak<str>>;
        let mut source_map = SourceTable::new();

        // The first time a given endpoint is sent from source to target, it is
        // sent along with a UUID newly associated with the endpoint by the
        // source. This association is recorded by the target.

        let a = Rc::<str>::from("a");
        let id = Uuid::new_v4();
        let already_sent = source_map.insert(Rc::clone(&a));
        assert!(!already_sent);

        type TargetTable = Target<Weak<str>>;
        let mut target_map = TargetTable::new();

        target_map.add(id, &a);

        // Subsequent times the endpoint is sent from source to target, just
        // the UUID associated with the endpoint is sent. The target looks up
        // the endpoint using the UUID.

        let already_sent2 = source_map.insert(Rc::clone(&a));
        assert!(already_sent2);

        let val = target_map.look_up(id);
        assert_eq!(val, Some(a.clone()));
    }
}
