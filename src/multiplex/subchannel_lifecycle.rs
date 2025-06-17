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
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use tracing::instrument;

// Each subsender should have a Rc<SubSenderTracker> so that, when
// the last reference to a SubSenderTracker is dropped, SubSenderStateMachine
// disconnect can be called (via a callback).
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
pub struct SubSenderStateMachine<T, M, Error, Source>
where
    T: Sender<M, Error>,
{
    maybe: RefCell<Option<T>>,
    sources: RefCell<HashSet<Source>>,
    in_flight: RefCell<HashSet<Source>>,
    phantom_m: PhantomData<M>,
    phantom_e: PhantomData<Error>,
}

impl<T, M, Error, Source> SubSenderStateMachine<T, M, Error, Source>
where
    T: Sender<M, Error>,
    Source: Debug + Eq + Hash,
{
    pub fn new(t: T, initial_source: Source) -> SubSenderStateMachine<T, M, Error, Source> {
        let mut s = HashSet::new();
        s.insert(initial_source);
        SubSenderStateMachine {
            maybe: RefCell::new(Some(t)),
            sources: RefCell::new(s),
            in_flight: RefCell::new(HashSet::new()),
            phantom_m: PhantomData,
            phantom_e: PhantomData,
        }
    }

    pub fn send(&self, msg: M) -> Option<Result<(), Error>> {
        self.maybe.borrow().as_ref().map(|t| t.send(msg))
    }

    #[instrument(level = "debug", skip(self))]
    pub fn to_be_sent(&self, from: Source) {
        self.in_flight.borrow_mut().insert(from);
    }

    #[instrument(level = "debug", skip(self))]
    pub fn received(&self, received_at_source: Source) {
        self.sources.borrow_mut().insert(received_at_source); // FIXME: undo in flight
    }
    
    // Disconnect from the given source. Once the subsender has been disconnected
    // from all its sources, sending is disabled and receiving should return
    // Disconnected.
    #[instrument(level = "debug", skip(self))]
    pub fn disconnect(&self, source: Source) {
        let mut sources = self.sources.borrow_mut();
        sources.remove(&source);
        if sources.is_empty() && self.in_flight.borrow().is_empty() { // TEMP HACK - in flight cannot be undone
            self.maybe.replace(None);
        }
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
        err: Option<TestError>,
    }

    impl TestSender {
        fn new(sent: &Rc<RefCell<Vec<char>>>) -> Self {
            Self {
                sent: Rc::clone(&sent),
                err: None,
            }
        }

        fn set_error(&mut self, err: TestError) {
            self.err = Some(err);
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum TestError {
        AnError
    }

    impl Sender<char, TestError> for TestSender {
        fn send(&self, msg: char) -> Result<(), TestError> {
            if let Some(err) = self.err.clone() {
                return Err(err);
            }
            self.sent.borrow_mut().push(msg);
            Ok(())
        }
    }

    #[test]
    fn sub_sender_state_machine_send_ok() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm = SubSenderStateMachine::new(TestSender::new(&sent), "");
        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.borrow().clone(), vec!['a']);
    }

    #[test]
    fn sub_sender_state_machine_send_error() {
        let sent = Rc::new(RefCell::new(vec![]));
        let mut test_sender = TestSender::new(&sent);
        test_sender.set_error(TestError::AnError);
        let ssm = SubSenderStateMachine::new(test_sender, "");
        assert_eq!(ssm.send('a'), Some(Err(TestError::AnError)));
    }
    
    #[test]
    fn sub_sender_state_machine_disconnect() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        // Disconnecting an unknown source should have no effect.
        ssm.disconnect("y");
        assert_eq!(ssm.send('a'), Some(Ok(())));
        
        ssm.disconnect("x");
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_disconnect_received_first() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        // Disconnecting another source should have no effect.
        ssm.received("y");
        ssm.disconnect("y");
        assert_eq!(ssm.send('a'), Some(Ok(())));
        
        ssm.disconnect("x");
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_disconnect_original_first() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.received("y");

        // Disconnecting the original source should have no effect.
        ssm.disconnect("x");
        assert_eq!(ssm.send('a'), Some(Ok(())));
        
        ssm.disconnect("y");
        assert_eq!(ssm.send('a'), None);
    }

        #[test]
    fn sub_sender_state_machine_in_flight() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm = SubSenderStateMachine::new(TestSender::new(&sent), "x");
        ssm.to_be_sent("x");
        ssm.disconnect("x");
        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.borrow().clone(), vec!['a']);
    }
}
