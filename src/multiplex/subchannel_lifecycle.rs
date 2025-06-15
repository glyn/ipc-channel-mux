// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module provides lifecycle management for subchannel senders.

use std::cell::RefCell;
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

pub trait Sender<M, Error> {
    fn send(&self, msg: M) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct SubSenderStateMachine<T, M, Error>
where
    T: Sender<M, Error>,
{
    maybe: RefCell<Option<T>>,
    phantom_m: PhantomData<M>,
    phantom_e: PhantomData<Error>,
}

impl<T, M, Error> SubSenderStateMachine<T, M, Error>
where
    T: Sender<M, Error>,
{
    pub fn new(t: T) -> SubSenderStateMachine<T, M, Error> {
        SubSenderStateMachine {
            maybe: RefCell::new(Some(t)),
            phantom_m: PhantomData,
            phantom_e: PhantomData,
        }
    }

    pub fn send(&self, msg: M) -> Option<Result<(), Error>> {
        self.maybe.borrow().as_ref().map(|t| t.send(msg))
    }

    pub fn disconnect(&self) {
        self.maybe.replace(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn sub_sender_tracker_basics() {
        let dropped = RefCell::new(false);
        let t = SubSenderTracker::new(Box::new(|| *dropped.borrow_mut() = true));
        drop(t);
        assert!(dropped.take());
    }

    struct TestSender {
        sent: Rc<RefCell<Vec<char>>>,
    }

    impl TestSender {
        fn new(sent: &Rc<RefCell<Vec<char>>>) -> Self {
            Self {
                sent: Rc::clone(&sent),
            }
        }
    }

    #[derive(Debug, PartialEq)]
    struct TestError {}

    impl Sender<char, TestError> for TestSender {
        fn send(&self, msg: char) -> Result<(), TestError> {
            self.sent.borrow_mut().push(msg);
            Ok(())
        }
    }

    #[test]
    fn sub_sender_state_machine_send() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm = SubSenderStateMachine::new(TestSender::new(&sent));
        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.borrow().clone(), vec!['a']);
    }
    
    #[test]
    fn sub_sender_state_machine_disconnect() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm = SubSenderStateMachine::new(TestSender::new(&sent));     
        ssm.disconnect();
        assert_eq!(ssm.send('a'), None);
    }
}
