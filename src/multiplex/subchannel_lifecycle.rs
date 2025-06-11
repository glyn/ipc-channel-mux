// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module provides lifecycle management for subchannel senders.

use std::marker::PhantomData;

// Each subsender should have a Rc<SubSenderTracker> so that, when
// the last reference to a SubSenderTracker is dropped, the
// SubSenderTracker can notify the SubSenderStateMachine.
pub struct SubSenderTracker<T>
where
    T: Fn() + ?Sized,
{
    notify_dropped: Box<T>,
}

impl<T> Drop for SubSenderTracker<T>
where
    T: Fn() + ?Sized,
{
    fn drop(&mut self) {
        (self.notify_dropped)();
    }
}

impl<T> SubSenderTracker<T>
where
    T: Fn() + ?Sized,
{
    pub fn new(notify_dropped: Box<T>) -> SubSenderTracker<T> {
        SubSenderTracker { notify_dropped }
    }
}

trait Sender<M, Error> {
    fn send(&self, msg: M) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct SubSenderStateMachine<T, M, Error>
where
    T: Sender<M, Error>,
{
    maybe: Option<T>,
    phantom1: PhantomData<M>,
    phantom2: PhantomData<Error>,
}

impl<T, M, Error> SubSenderStateMachine<T, M, Error>
where
    T: Sender<M, Error>,
{
    pub fn new(t: T) -> SubSenderStateMachine<T, M, Error> {
        SubSenderStateMachine {
            maybe: Some(t),
            phantom1: PhantomData,
            phantom2: PhantomData,
        }
    }

    pub fn send(&self, msg: M) -> Result<(), Error> {
        self.maybe.map(|t| t.send(msg)).ok_or(Error::Disconnected); // TODO: generify this
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn sub_sender_tracker_basics() {
        let dropped = RefCell::new(false);
        let t = SubSenderTracker::new(Box::new(|| *dropped.borrow_mut() = true));
        drop(t);
        assert!(dropped.take());
    }
}
