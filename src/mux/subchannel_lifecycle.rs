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
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Mutex;
use tracing::instrument;

// Each subsender should have an Arc<SubSenderTracker> so that, when
// the last reference to a SubSenderTracker is dropped, SubSenderStateMachine
// disconnect can be called (via a callback).
pub struct SubSenderTracker<T>
where
    T: Fn() + Send + Sync + ?Sized,
{
    notify_dropped: Box<T>,
}

impl<T> std::fmt::Debug for SubSenderTracker<T>
where
    T: Fn() + Send + Sync + ?Sized,
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl<T> Drop for SubSenderTracker<T>
where
    T: Fn() + Send + Sync + ?Sized,
{
    fn drop(&mut self) {
        (self.notify_dropped)();
    }
}

impl<T> SubSenderTracker<T>
where
    T: Fn() + Send + Sync + ?Sized,
{
    pub fn new(notify_dropped: Box<T>) -> SubSenderTracker<T> {
        SubSenderTracker { notify_dropped }
    }
}

// A SubReceiverProxy tracks whether or not a SubReceiver has disconnected.
pub struct SubReceiverProxy {
    disconnected: Mutex<bool>,
}

impl SubReceiverProxy {
    pub fn new() -> SubReceiverProxy {
        SubReceiverProxy {
            disconnected: Mutex::new(false),
        }
    }

    pub fn disconnect(&self) {
        *self.disconnected.lock().unwrap() = true;
    }

    pub fn disconnected(&self) -> bool {
        *self.disconnected.lock().unwrap()
    }
}

pub trait Sender<M, Error> {
    fn send(&self, msg: M) -> Result<(), Error>;
}

pub struct SubSenderStateMachine<T, M, Error, Source, Via, Probe>
where
    Probe: Send + ?Sized,
{
    maybe: Mutex<Option<T>>,
    sources: Mutex<HashSet<Source>>,
    in_flight: Mutex<HashBag<Via>>,
    probes: Mutex<HashMap<Via, Box<Probe>>>,
    phantom_m: PhantomData<M>,
    phantom_e: PhantomData<Error>,
}

impl<T, M, Error, Source, Via, Probe> std::fmt::Debug
    for SubSenderStateMachine<T, M, Error, Source, Via, Probe>
where
    Source: Debug,
    Via: Debug,
    Probe: Fn() -> bool + Send + ?Sized,
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
    T: Sender<M, Error> + Debug,
    Source: Clone + Debug + Eq + Hash,
    Via: Clone + Debug + Eq + Hash,
    Probe: Fn() -> bool + Send + ?Sized,
{
    #[instrument(level = "debug", skip(t))]
    pub fn new(
        t: T,
        initial_source: Source,
    ) -> SubSenderStateMachine<T, M, Error, Source, Via, Probe> {
        let mut s = HashSet::new();
        s.insert(initial_source);
        SubSenderStateMachine {
            maybe: Mutex::new(Some(t)),
            sources: Mutex::new(s),
            in_flight: Mutex::new(HashBag::new()),
            probes: Mutex::new(HashMap::new()),
            phantom_m: PhantomData,
            phantom_e: PhantomData,
        }
    }

    pub fn send(&self, msg: M) -> Option<Result<(), Error>> {
        self.maybe.lock().unwrap().as_ref().map(|t| t.send(msg))
    }

    #[instrument(level = "debug", skip(self, probe))]
    pub fn to_be_sent(&self, via: Via, probe: Box<Probe>) {
        self.in_flight.lock().unwrap().insert(via.clone());
        self.probes.lock().unwrap().insert(via, probe);
    }

    #[instrument(level = "debug", skip(self))]
    // Record the receipt of an inflight value.
    pub fn received(&self, via: &Via, received_at_source: Source) {
        self.in_flight.lock().unwrap().remove(via);
        self.sources.lock().unwrap().insert(received_at_source);
        self.probes.lock().unwrap().remove(via);
    }

    // Disconnect from the given source. Once the subsender has been disconnected
    // from all its sources, sending is disabled and receiving should return
    // Disconnected.
    // Return an optional Sender which is `None` unless all the sources have been
    // disconnected, in which case the returned value is `Some(s)` where s is the
    // Sender.
    #[instrument(level = "debug", skip(self), ret)]
    pub fn disconnect(&self, source: Source) -> Option<T> {
        let mut sources = self.sources.lock().unwrap();
        sources.remove(&source);
        if sources.is_empty() && self.in_flight.lock().unwrap().is_empty() {
            // FIXME: this leg is never taken
            self.maybe.lock().unwrap().take()
        } else {
            None
        }
    }

    // Remove the given inflight value and disconnect it.
    #[instrument(level = "debug", skip(self))]
    pub fn receive_failed(&self, via: &Via) -> Option<T> {
        self.in_flight.lock().unwrap().remove(via);
        if self.sources.lock().unwrap().is_empty() && self.in_flight.lock().unwrap().is_empty() {
            self.maybe.lock().unwrap().take()
        } else {
            None
        }
    }

    // poll returns (false, Some(s)), where s is the sender for the state machine,
    // if the state machine is disconnected. Otherwise poll returns (true, None).
    #[instrument(level = "trace", ret)]
    pub fn poll(&self) -> (bool, Option<T>) {
        // Skip polling if polling has already failed.
        if self.maybe.lock().unwrap().is_none() {
            return (true, None);
        }

        // Determine Vias (which correspond to channels) which are disconnected.
        let disconnected: Vec<Via> = self
            .probes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, probe)| !probe())
            .map(|(via, _)| via.clone())
            .collect();

        // Find all in-flight entries for disconnected Vias.
        let mut disconnected_in_flight = HashSet::new();
        self.in_flight
            .lock()
            .unwrap()
            .iter()
            .filter(|via| disconnected.contains(via))
            .for_each(|via| {
                disconnected_in_flight.insert(via.clone());
            });

        // Remove all in-flight entries for disconnected Vias.
        let mut in_flight = self.in_flight.lock().unwrap();
        disconnected_in_flight.iter().for_each(|entry| {
            in_flight.remove_up_to(entry, usize::MAX); // Assumes the entry occurs less than usize::MAX times.
        });

        // Remove all probes for disconnected Vias.
        let mut probes = self.probes.lock().unwrap();
        disconnected_in_flight.iter().for_each(|via| {
            probes.remove(via);
        });

        if self.sources.lock().unwrap().is_empty() && in_flight.is_empty() {
            (false, self.maybe.lock().unwrap().take())
        } else {
            (true, None)
        }
    }

    pub fn switch_sender(&self, t: T) {
        self.maybe.lock().unwrap().replace(t);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn sub_sender_tracker_basics() {
        let dropped = Mutex::new(false);
        let t = SubSenderTracker::new(Box::new(|| *dropped.lock().unwrap() = true));
        drop(t);
        assert!(*dropped.lock().unwrap());
    }

    #[test]
    fn sub_receiver_proxy_basics() {
        let p = SubReceiverProxy::new();
        assert!(!p.disconnected());
        p.disconnect();
        assert!(p.disconnected());
        p.disconnect();
        assert!(p.disconnected());
    }

    #[derive(Debug)]
    struct TestSender {
        sent: Arc<Mutex<Vec<char>>>,
        err: Option<TestError>,
    }

    impl TestSender {
        fn new(sent: &Arc<Mutex<Vec<char>>>) -> Self {
            Self {
                sent: Arc::clone(sent),
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
            self.sent.lock().unwrap().push(msg);
            Ok(())
        }
    }

    #[test]
    fn sub_sender_state_machine_send_ok() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "");
        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.lock().unwrap().clone(), vec!['a']);
    }

    #[test]
    fn sub_sender_state_machine_send_error() {
        let sent = Arc::new(Mutex::new(vec![]));
        let mut test_sender = TestSender::new(&sent);
        test_sender.set_error(TestError::AnError);
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(test_sender, "");
        assert_eq!(ssm.send('a'), Some(Err(TestError::AnError)));
    }

    #[test]
    fn sub_sender_state_machine_disconnect() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        // Disconnecting an unknown source should have no effect.
        let disc = ssm.disconnect("y");
        assert!(disc.is_none());
        assert_eq!(ssm.send('a'), Some(Ok(())));

        let disc = ssm.disconnect("x");
        assert!(disc.is_some());
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_disconnect_received_first() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("scid", Box::new(|| true));
        ssm.received(&"scid", "y");
        let disc = ssm.disconnect("y");
        assert!(disc.is_none());
        assert_eq!(ssm.send('a'), Some(Ok(())));

        let disc = ssm.disconnect("x");
        assert!(disc.is_some());
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_disconnect_original_first() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("scid", Box::new(|| true));
        ssm.received(&"scid", "y");

        // Disconnecting the original source should have no effect.
        let disc = ssm.disconnect("x");
        assert!(disc.is_none());
        assert_eq!(ssm.send('a'), Some(Ok(())));

        let disc = ssm.disconnect("y");
        assert!(disc.is_some());
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_in_flight() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");
        ssm.to_be_sent("scid", Box::new(|| true));
        let disc = ssm.disconnect("x");
        assert!(disc.is_none());
        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.lock().unwrap().clone(), vec!['a']);
    }

    #[test]
    fn sub_sender_state_machine_multiple_transmission() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("scid", Box::new(|| true));
        ssm.to_be_sent("scid", Box::new(|| true));
        let disc = ssm.disconnect("x");
        assert!(disc.is_none());

        ssm.received(&"scid", "y");
        let disc = ssm.disconnect("y");
        assert!(disc.is_none());

        ssm.received(&"scid", "y");
        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.lock().unwrap().clone(), vec!['a']);
    }

    #[test]
    fn sub_sender_state_machine_in_flight_crash() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("scid", Box::new(|| false));
        ssm.disconnect("x");

        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.lock().unwrap().clone(), vec!['a']);

        ssm.poll();
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_two_in_flight_crash() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("scid", Box::new(|| false));
        ssm.to_be_sent("scid", Box::new(|| false));
        let disc = ssm.disconnect("x");
        assert!(disc.is_none());

        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.lock().unwrap().clone(), vec!['a']);

        ssm.poll();
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_in_flight_crash_eventually() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        let count: Arc<Mutex<u8>> = Arc::new(Mutex::new(0));
        let count_clone = Arc::clone(&count);
        ssm.to_be_sent(
            "scid",
            Box::new(move || {
                let mut c = *count_clone.lock().unwrap();
                c += 1;
                *count_clone.lock().unwrap() = c;
                c < 2
            }),
        );
        let disc = ssm.disconnect("x");
        assert!(disc.is_none());

        assert_eq!(ssm.send('a'), Some(Ok(())));
        assert_eq!(sent.lock().unwrap().clone(), vec!['a']);

        ssm.poll();
        assert_eq!(ssm.send('b'), Some(Ok(())));
        assert_eq!(sent.lock().unwrap().clone(), vec!['a', 'b']);

        ssm.poll();
        assert_eq!(ssm.send('c'), None);
    }

    #[test]
    fn sub_sender_state_machine_receive_failed() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("scid", Box::new(|| true));

        // Disconnecting the original source should have no effect.
        let disc = ssm.disconnect("x");
        assert!(disc.is_none());
        assert_eq!(ssm.send('a'), Some(Ok(())));

        ssm.receive_failed(&"scid");
        assert_eq!(ssm.send('a'), None);
    }

    #[test]
    fn sub_sender_state_machine_remove_probe_on_disconnect() {
        let sent = Arc::new(Mutex::new(vec![]));
        let ssm: SubSenderStateMachine<
            TestSender,
            char,
            TestError,
            &'static str,
            &'static str,
            dyn Fn() -> bool + Send,
        > = SubSenderStateMachine::new(TestSender::new(&sent), "x");

        ssm.to_be_sent("scid", Box::new(|| panic!("probe should not be run")));
        ssm.received(&"scid", "y");
        ssm.poll();
    }
}
