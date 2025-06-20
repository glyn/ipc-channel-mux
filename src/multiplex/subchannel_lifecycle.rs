// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module provides lifecycle management for subchannel senders.

use hashbag::HashBag;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
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

pub struct SubSenderStateMachine<T, M, Error, Source, Via, Probe>
where
    Probe: ?Sized,
{
    maybe: RefCell<Option<T>>,
    sources: RefCell<HashSet<Source>>,
    in_flight: RefCell<HashBag<(Source, Via)>>,
    probes: RefCell<HashMap<Via, Box<Probe>>>,
    phantom_m: PhantomData<M>,
    phantom_e: PhantomData<Error>,
}

impl<T, M, Error, Source, Via, Probe> std::fmt::Debug
    for SubSenderStateMachine<T, M, Error, Source, Via, Probe>
where
    Source: Debug,
    Via: Debug,
    Probe: Fn() -> bool + ?Sized,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("SubSenderStateMachine")
            .field("sources", &self.sources)
            .field("in_flight", &self.in_flight)
            .finish_non_exhaustive()
    }
}

impl<T, M, Error, Source, Via, Probe> SubSenderStateMachine<T, M, Error, Source, Via, Probe>
where
    T: Sender<M, Error>,
    Source: Clone + Debug + Eq + Hash,
    Via: Clone + Debug + Eq + Hash,
    Probe: Fn() -> bool + ?Sized,
{
    pub fn new(
        t: T,
        initial_source: Source,
    ) -> SubSenderStateMachine<T, M, Error, Source, Via, Probe> {
        let mut s = HashSet::new();
        s.insert(initial_source);
        SubSenderStateMachine {
            maybe: RefCell::new(Some(t)),
            sources: RefCell::new(s),
            in_flight: RefCell::new(HashBag::new()),
            probes: RefCell::new(HashMap::new()),
            phantom_m: PhantomData,
            phantom_e: PhantomData,
        }
    }

    pub fn send(&self, msg: M) -> Option<Result<(), Error>> {
        self.maybe.borrow().as_ref().map(|t| t.send(msg))
    }

    #[instrument(level = "debug", skip(self, probe))]
    pub fn to_be_sent(&self, from: Source, via: Via, probe: Box<Probe>) {
        self.in_flight.borrow_mut().insert((from, via.clone()));
        self.probes.borrow_mut().insert(via, probe);
    }

    #[instrument(level = "debug", skip(self))]
    pub fn received(&self, from: Source, via: Via, received_at_source: Source) {
        self.in_flight.borrow_mut().remove(&(from, via));
        self.sources.borrow_mut().insert(received_at_source);
    }

    // Disconnect from the given source. Once the subsender has been disconnected
    // from all its sources, sending is disabled and receiving should return
    // Disconnected.
    #[instrument(level = "debug", skip(self))]
    pub fn disconnect(&self, source: Source) {
        let mut sources = self.sources.borrow_mut();
        sources.remove(&source);
        if sources.is_empty() && self.in_flight.borrow().is_empty() {
            self.maybe.replace(None);
        }
    }

    pub fn poll(&self) {
        // Determine Vias (which correspond to channels) which are disconnected.
        let disconnected: Vec<Via> = self
            .probes
            .borrow()
            .iter()
            .filter(|(_, probe)| !probe())
            .map(|(via, _)| via.clone())
            .collect();

        // Find all in-flight entries for disconnected Vias.
        let mut disconnected_in_flight = HashSet::new();
        self.in_flight
            .borrow()
            .iter()
            .filter(|(_, via)| disconnected.contains(via))
            .for_each(|(source, via)| {
                disconnected_in_flight.insert(((*source).clone(), via.clone()));
            });

        // Remove all in-flight entries for disconnected Vias.
        let mut in_flight = self.in_flight.borrow_mut();
        disconnected_in_flight.iter().for_each(|entry| {
            in_flight.remove_up_to(entry, usize::MAX); // Assumes the entry occurs less than usize::MAX times.
        });

        // Remove all probes for disconnected Vias.
        let mut probes = self.probes.borrow_mut();
        disconnected_in_flight.iter().for_each(|(_, via)| {
            probes.remove(via);
        });

        if self.sources.borrow().is_empty() && in_flight.is_empty() {
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
        AnError,
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
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "");
        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.borrow().clone(), vec!['a']);
    }

    #[test]
    fn sub_sender_state_machine_send_error() {
        let sent = Rc::new(RefCell::new(vec![]));
        let mut test_sender = TestSender::new(&sent);
        test_sender.set_error(TestError::AnError);
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool,
        > = SubSenderStateMachine::new(test_sender, "");
        assert_eq!(ssm.send('a'), Some(Err(TestError::AnError)));
    }

    #[test]
    fn sub_sender_state_machine_disconnect() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        // Disconnecting an unknown source should have no effect.
        ssm.disconnect("y");
        assert_eq!(ssm.send('a'), Some(Ok(())));

        ssm.disconnect("x");
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_disconnect_received_first() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("x", "scid", Box::new(|| true));
        ssm.received("x", "scid", "y");
        ssm.disconnect("y");
        assert_eq!(ssm.send('a'), Some(Ok(())));

        ssm.disconnect("x");
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_disconnect_original_first() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("x", "scid", Box::new(|| true));
        ssm.received("x", "scid", "y");

        // Disconnecting the original source should have no effect.
        ssm.disconnect("x");
        assert_eq!(ssm.send('a'), Some(Ok(())));

        ssm.disconnect("y");
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_in_flight() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");
        ssm.to_be_sent("x", "scid", Box::new(|| true));
        ssm.disconnect("x");
        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.borrow().clone(), vec!['a']);
    }

    #[test]
    fn sub_sender_state_machine_multiple_transmission() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("x", "scid", Box::new(|| true));
        ssm.to_be_sent("x", "scid", Box::new(|| true));
        ssm.disconnect("x");

        ssm.received("x", "scid", "y");
        ssm.disconnect("y");

        ssm.received("x", "scid", "y");
        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.borrow().clone(), vec!['a']);
    }

    #[test]
    fn sub_sender_state_machine_in_flight_crash() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("x", "scid", Box::new(|| false));
        ssm.disconnect("x");

        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.borrow().clone(), vec!['a']);

        ssm.poll();
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_two_in_flight_crash() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("x", "scid", Box::new(|| false));
        ssm.to_be_sent("x", "scid", Box::new(|| false));
        ssm.disconnect("x");

        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.borrow().clone(), vec!['a']);

        ssm.poll();
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_in_flight_crash_eventually() {
        let sent = Rc::new(RefCell::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        let count: Rc<RefCell<u8>> = Rc::new(RefCell::new(0));
        let count_clone = Rc::clone(&count);
        ssm.to_be_sent(
            "x",
            "scid",
            Box::new(move || {
                let mut c = count_clone.borrow().clone();
                c += 1;
                count_clone.replace(c);
                c < 2
            }),
        );
        ssm.disconnect("x");

        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.borrow().clone(), vec!['a']);
        
        ssm.poll();
        assert_eq!(ssm.send('b'), Some(Ok(())));
        assert_eq!(sent.borrow().clone(), vec!['a', 'b']);
        
        ssm.poll();
        assert_eq!(ssm.send('c'), None);
    }
}
