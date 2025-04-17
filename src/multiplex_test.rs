// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//#[cfg(not(any(feature = "force-inprocess", target_os = "android", target_os = "ios")))]
//use crate::ipc::IpcReceiver;
//use crate::ipc::{self, IpcReceiverSet, IpcSender, IpcSharedMemory};
use crate::multiplex::{self, SubOneShotServer, SubReceiver, SubSender};
//use crossbeam_channel::{self, Sender};
// #[cfg(not(any(feature = "force-inprocess", target_os = "android", target_os = "ios")))]
// use std::env;
//use std::iter;
// #[cfg(not(any(feature = "force-inprocess", target_os = "android", target_os = "ios",)))]
// use std::process::{self, Command, Stdio};
// #[cfg(not(any(
//     feature = "force-inprocess",
//     target_os = "android",
//     target_os = "ios",
//     target_os = "windows",
// )))]
// use std::ptr;
//use std::rc::Rc;
use std::thread;
use test_log::test;

// #[cfg(not(any(
//     feature = "force-inprocess",
//     target_os = "android",
//     target_os = "ios",
//     target_os = "windows"
// )))]
// use crate::ipc::IpcOneShotServer;

// #[cfg(not(any(
//     feature = "force-inprocess",
//     target_os = "android",
//     target_os = "ios",
//     target_os = "windows",
// )))]
// use std::io::Error;
//use std::time::{Duration, Instant};

/*
#[cfg(not(any(
    feature = "force-inprocess",
    target_os = "windows",
    target_os = "android",
    target_os = "ios"
)))]
// I'm not actually sure invoking this is indeed unsafe -- but better safe than sorry...
pub unsafe fn fork<F: FnOnce()>(child_func: F) -> libc::pid_t {
    match libc::fork() {
        -1 => panic!("Fork failed: {}", Error::last_os_error()),
        0 => {
            child_func();
            libc::exit(0);
        },
        pid => pid,
    }
}

#[cfg(not(any(
    feature = "force-inprocess",
    target_os = "windows",
    target_os = "android",
    target_os = "ios"
)))]
pub trait Wait {
    fn wait(self);
}

#[cfg(not(any(
    feature = "force-inprocess",
    target_os = "windows",
    target_os = "android",
    target_os = "ios"
)))]
impl Wait for libc::pid_t {
    fn wait(self) {
        unsafe {
            libc::waitpid(self, ptr::null_mut(), 0);
        }
    }
}

// Helper to get a channel_name argument passed in; used for the
// cross-process spawn server tests.
#[cfg(not(any(feature = "force-inprocess", target_os = "android", target_os = "ios")))]
pub fn get_channel_name_arg(which: &str) -> Option<String> {
    for arg in env::args() {
        let arg_str = &*format!("channel_name-{}:", which);
        if let Some(arg) = arg.strip_prefix(arg_str) {
            return Some(arg.to_owned());
        }
    }
    None
}

// Helper to get a channel_name argument passed in; used for the
// cross-process spawn server tests.
#[cfg(not(any(feature = "force-inprocess", target_os = "android", target_os = "ios",)))]
pub fn spawn_server(test_name: &str, server_args: &[(&str, &str)]) -> process::Child {
    Command::new(env::current_exe().unwrap())
        .arg(test_name)
        .args(
            server_args
                .iter()
                .map(|(name, val)| format!("channel_name-{}:{}", name, val)),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to execute server process")
}
*/

// type Person = (String, u32);

#[test]
fn multiplex_simple() {
    let person = ("Patrick Walton".to_owned(), 29);
    let channel = multiplex::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel().unwrap();
    tx.send(person.clone()).unwrap();
    let received_person = rx.recv().unwrap();
    assert_eq!(person, received_person);
    // TODO: need to implement disconnection when all the copies of a
    // SubSender have been dropped.
    // drop(tx);
    // match rx.recv().unwrap_err() {
    //     multiplex::MultiplexError::Disconnected => (),
    //     e => panic!("expected disconnected error, got {:?}", e),
    // }
    // FIXME: fail if rx.recv() succeeds
}

#[test]
fn multiplex_two_subchannels() {
    let channel = multiplex::Channel::new().unwrap();
    let (tx1, rx1) = channel.sub_channel().unwrap();
    tx1.send(1).unwrap();
    assert_eq!(1, rx1.recv().unwrap());
    
    let (tx2, rx2) = channel.sub_channel().unwrap();
    tx2.send(2).unwrap();
    assert_eq!(2, rx2.recv().unwrap());
}

#[test]
fn multiplex_two_subchannels_reverse_ordered() {
    let channel = multiplex::Channel::new().unwrap();
    let (tx1, rx1) = channel.sub_channel().unwrap();
    tx1.send(1).unwrap();
    
    let (tx2, rx2) = channel.sub_channel().unwrap();
    tx2.send(2).unwrap();

    assert_eq!(2, rx2.recv().unwrap());
    assert_eq!(1, rx1.recv().unwrap());
}

#[test]
fn compare_base_transmission_dropped() {
    let channel = multiplex::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel::<i32>().unwrap();

    let (super_tx, super_rx) = channel.sub_channel().unwrap();
    super_tx.send(sub_tx).unwrap();
    drop(super_rx); // commenting this out makes sub_rx.recv() deadlock

    match sub_rx.recv().unwrap_err() {
        multiplex::MultiplexError::MpmcSendError => (),
        e => panic!("expected send error, got {:?}", e),
    }
}

// The following test deadlocks because the sub_rx is not dropped
// #[test]
// fn compare_base_transmission_dropped_distinct() {
//     let sub_channel = multiplex::Channel::new().unwrap();
//     let (sub_tx, sub_rx) = sub_channel.sub_channel::<i32>().unwrap();
    
//     let channel = multiplex::Channel::new().unwrap();
//     let (super_tx, super_rx) = channel.sub_channel().unwrap();
//     super_tx.send(sub_tx).unwrap();
//     drop(super_rx);

//     match sub_rx.recv().unwrap_err() {
//         multiplex::MultiplexError::MpmcSendError => (),
//         e => panic!("expected send error, got {:?}", e),
//     }
// }

#[test]
fn embedded_multiplexed_senders() {
    let person = ("Patrick Walton".to_owned(), 29);

    let channel = multiplex::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel().unwrap();

    let person_and_sender = (person.clone(), sub_tx);
    let (super_tx, super_rx) = channel.sub_channel().unwrap();

    super_tx.send(person_and_sender).unwrap();
    let received_person_and_sender: ((String, i32), SubSender<(String, i32)>) =
        super_rx.recv().unwrap();
    assert_eq!(received_person_and_sender.0, person);
    let sub_tx = received_person_and_sender.1;
    sub_tx.send(person.clone()).unwrap();

    let person2 = ("Arthur Dent".to_owned(), 42);
    sub_tx.send(person2.clone()).unwrap();

    let received_person = sub_rx.recv().unwrap();
    assert_eq!(received_person, person);

    let received_person2 = sub_rx.recv().unwrap();
    assert_eq!(received_person2, person2);
}

#[test]
fn embedded_multiplexed_two_senders() {
    let person = ("Patrick Walton".to_owned(), 29);

    let channel = multiplex::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel().unwrap();
    let (sub_tx2, sub_rx2) = channel.sub_channel().unwrap();

    let person_and_two_senders = (person.clone(), sub_tx, sub_tx2);
    let (super_tx, super_rx) = channel.sub_channel().unwrap();

    super_tx.send(person_and_two_senders).unwrap();
    let received_person_and_two_senders: (
        (String, i32),
        SubSender<(String, i32)>,
        SubSender<(String, i32)>,
    ) = super_rx.recv().unwrap();
    assert_eq!(received_person_and_two_senders.0, person);
    let sub_tx = received_person_and_two_senders.1;
    sub_tx.send(person.clone()).unwrap();

    let person2 = ("Arthur Dent".to_owned(), 42);
    sub_tx.send(person2.clone()).unwrap();

    let received_person = sub_rx.recv().unwrap();
    assert_eq!(received_person, person);

    let received_person2 = sub_rx.recv().unwrap();
    assert_eq!(received_person2, person2);

    let sub_tx2 = received_person_and_two_senders.2;
    sub_tx2.send(person.clone()).unwrap();

    let person2 = ("Arthur Dent".to_owned(), 42);
    sub_tx2.send(person2.clone()).unwrap();

    let received_person = sub_rx2.recv().unwrap();
    assert_eq!(received_person, person);

    let received_person2 = sub_rx2.recv().unwrap();
    assert_eq!(received_person2, person2);
}

#[test]
fn multiplexed_senders_interacting() {
    let channel = multiplex::Channel::new().unwrap();
    let (super_tx1, super_rx1) = channel.sub_channel().unwrap();
    let (sub_tx1, sub_rx1) = channel.sub_channel().unwrap();

    let channel2 = multiplex::Channel::new().unwrap();
    let (super_tx2, super_rx2) = channel2.sub_channel().unwrap();
    let (sub_tx2, sub_rx2) = channel2.sub_channel().unwrap();

    super_tx1.send(sub_tx2).unwrap();
    super_tx2.send(sub_tx1).unwrap();
    let sub_tx2_1 = super_rx1.recv().unwrap();
    let sub_tx1_2 = super_rx2.recv().unwrap();

    sub_tx2_1.send(2).unwrap();
    sub_tx1_2.send(1).unwrap();

    assert_eq!(sub_rx2.recv().unwrap(), 2);
    assert_eq!(sub_rx1.recv().unwrap(), 1);
}

#[test]
fn embedded_multiplexed_receivers() {
    let person = ("Patrick Walton".to_owned(), 29);

    let channel = multiplex::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel().unwrap();

    let person_and_receiver = (person.clone(), sub_rx);

    // Need a separate channel so that it has an IpcReceiver which will not be taken when sub_rx is sent.
    let super_channel = multiplex::Channel::new().unwrap();
    let (super_tx, super_rx) = super_channel.sub_channel().unwrap();

    super_tx.send(person_and_receiver).unwrap();
    let received_person_and_receiver = super_rx.recv().unwrap();
    assert_eq!(received_person_and_receiver.0, person);
    sub_tx.send(person.clone()).unwrap();
    let received_person = received_person_and_receiver.1.recv().unwrap();
    assert_eq!(received_person, person);
}

#[test]
fn embedded_multiplexed_receivers_used_before_and_after_transmission() {
    let person = ("Patrick Walton".to_owned(), 29);

    let channel = multiplex::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel().unwrap();

    sub_tx.send(person.clone()).unwrap();
    sub_tx.send(person.clone()).unwrap();
    let received_person1 = sub_rx.recv().unwrap();
    assert_eq!(received_person1, person);

    let person_and_receiver = (person.clone(), sub_rx);

    // Need a separate channel so that it has an IpcReceiver which will not be taken when sub_rx is sent.
    let super_channel = multiplex::Channel::new().unwrap();
    let (super_tx, super_rx) = super_channel.sub_channel().unwrap();

    super_tx.send(person_and_receiver).unwrap();
    let received_person_and_receiver = super_rx.recv().unwrap();
    assert_eq!(received_person_and_receiver.0, person);
    // sub_tx.send(person.clone()).unwrap();
    let received_person2 = received_person_and_receiver.1.recv().unwrap();
    assert_eq!(received_person2, person);
}

#[cfg(not(any(
    feature = "force-inprocess",
    target_os = "windows",
    target_os = "android",
    target_os = "ios"
)))]
// This test demonstrates the basic purpose of multiplexing. If IpcChannels were
// used, then this test would fail on Unix variants since the spawned process
// would run out of file descriptors. Using multiplexed channels, the spawned
// process does not run out of file descriptors.
#[test]
fn receiving_many_subchannels() {
    let channel = multiplex::Channel::new().unwrap();
    let (send2, recv2) = channel.sub_channel().unwrap();

    // this will be used to receive from the spawned thread
    let (bootstrap_server, bootstrap_token) = SubOneShotServer::new().unwrap();

    thread::spawn(move || {
        let bootstrap_sub_sender: SubSender<SubSender<SubSender<bool>>> =
            SubSender::connect(bootstrap_token).unwrap();

        let channel = multiplex::Channel::new().unwrap();
        let (send1, recv1) = channel.sub_channel().unwrap();

        bootstrap_sub_sender.send(send1).unwrap();

        let mut senders = vec![];
        while let Ok(send2) = recv1.recv() {
            let _ = send2.send(true);
            // The fd is private, but this transmute lets us get at it
            let fd: &std::sync::Arc<u32> = unsafe { std::mem::transmute(&send2) };
            println!("fd = {}", *fd);
            // Stop the ipc channel from being dropped
            senders.push(send2);
        }
    });

    let (_bootstrap_sub_receiver, send1): (
        SubReceiver<SubSender<SubSender<bool>>>,
        SubSender<SubSender<bool>>,
    ) = bootstrap_server.accept().unwrap();

    for _ in 0..10000 {
        let _ = send1.send(send2.clone());
        let _ = recv2.recv();
    }
}
