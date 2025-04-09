// Copyright 2015 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::multiplex::{self, OneShotMultiServer, MultiSender, SubChannelReceiver, SubChannelSender};
use std::thread;
use test_log::test;

#[test]
fn embedded_multiplexed_senders() {
    let person = ("Patrick Walton".to_owned(), 29);
    let (multi_sender, multi_receiver) = multiplex::multi_channel().unwrap();
    let sub_tx: SubChannelSender<(String, i32)> = multi_sender.new();
    let sub_scid = sub_tx.sub_channel_id();
    let mut sub_rx: SubChannelReceiver<(String, i32)> = multi_receiver.attach(sub_scid).unwrap();

    let person_and_sender = (person.clone(), sub_tx);
    let super_tx = multi_sender.new();
    let super_scid = super_tx.sub_channel_id();
    let mut super_rx: SubChannelReceiver<((String, i32), SubChannelSender<(String, i32)>)> =
        multi_receiver.attach(super_scid).unwrap();

    super_tx.send(person_and_sender).unwrap();
    let received_person_and_sender: ((String, i32), SubChannelSender<(String, i32)>) =
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
    let (multi_sender, multi_receiver) = multiplex::multi_channel().unwrap();
    let sub_tx: SubChannelSender<(String, i32)> = multi_sender.new();
    let sub_scid = sub_tx.sub_channel_id();
    let mut sub_rx: SubChannelReceiver<(String, i32)> = multi_receiver.attach(sub_scid).unwrap();
    let sub_tx2: SubChannelSender<(String, i32)> = multi_sender.new();
    let sub_scid2 = sub_tx2.sub_channel_id();
    let mut sub_rx2: SubChannelReceiver<(String, i32)> = multi_receiver.attach(sub_scid2).unwrap();

    let person_and_two_senders = (person.clone(), sub_tx, sub_tx2);
    let super_tx = multi_sender.new();
    let super_scid = super_tx.sub_channel_id();
    let mut super_rx: SubChannelReceiver<((String, i32), SubChannelSender<(String, i32)>, SubChannelSender<(String, i32)>)> =
        multi_receiver.attach(super_scid).unwrap();

    super_tx.send(person_and_two_senders).unwrap();
    let received_person_and_two_senders: ((String, i32), SubChannelSender<(String, i32)>, SubChannelSender<(String, i32)>) =
        super_rx.recv().unwrap();
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
    let (multi_sender1, multi_receiver1) = multiplex::multi_channel().unwrap();
    let super_tx1 = multi_sender1.new();
    let mut super_rx1: SubChannelReceiver<SubChannelSender<i32>> =
    multi_receiver1.attach(super_tx1.sub_channel_id()).unwrap();
    let sub_tx1: SubChannelSender<i32> = multi_sender1.new();
    let mut sub_rx1: SubChannelReceiver<i32> = multi_receiver1.attach(sub_tx1.sub_channel_id()).unwrap();

    let (multi_sender2, multi_receiver2) = multiplex::multi_channel().unwrap();
    let super_tx2 = multi_sender2.new();
    let mut super_rx2: SubChannelReceiver<SubChannelSender<i32>> =
    multi_receiver2.attach(super_tx2.sub_channel_id()).unwrap();
    let sub_tx2: SubChannelSender<i32> = multi_sender2.new();
    let mut sub_rx2: SubChannelReceiver<i32> = multi_receiver2.attach(sub_tx2.sub_channel_id()).unwrap();

    super_tx1.send(sub_tx2).unwrap();
    super_tx2.send(sub_tx1).unwrap();
    let sub_tx2_1 = super_rx1.recv().unwrap();
    let sub_tx1_2 = super_rx2.recv().unwrap();
    
    sub_tx2_1.send(2).unwrap();
    sub_tx1_2.send(1).unwrap();

    assert_eq!(sub_rx2.recv().unwrap(), 2);
    assert_eq!(sub_rx1.recv().unwrap(), 1);
}

// This test demonstrates the basic purpose of multiplexing. If IpcChannels were
// used, then this test would fail on Unix variants since the spawned process
// would run out of file descriptors. Using multiplexed channels, the spawned
// process does not run out of file descriptors. 
#[test]
fn receiving_many_subchannels() {
    let (multi_sender, multi_receiver) = multiplex::multi_channel().unwrap();
    let send2 = multi_sender.new();
    let mut recv2: SubChannelReceiver<bool> = multi_receiver.attach(send2.sub_channel_id()).unwrap();
  
    // this will be used to receive from the spawned thread
     let (bootstrap_server, bootstrap_token) = OneShotMultiServer::new().unwrap();
     
     thread::spawn(move || {
         let bootstrap_multi_sender = MultiSender::connect(bootstrap_token).unwrap();
         let bootstrap_sub_channel_sender = bootstrap_multi_sender.new();
         bootstrap_multi_sender.notify_sub_channel(bootstrap_sub_channel_sender.sub_channel_id(), "bootstrap".to_string()).unwrap();
         
        let (multi_sender, multi_receiver) = multiplex::multi_channel().unwrap();
        let send1: SubChannelSender<SubChannelSender<bool>> = multi_sender.new();
        let mut recv1: SubChannelReceiver<SubChannelSender<bool>> = multi_receiver.attach(send1.sub_channel_id()).unwrap();
        
        bootstrap_sub_channel_sender.send(send1).unwrap();

         let mut senders = vec![];
         while let Ok(send2) = recv1.recv() { // the test can panic here ...
            let _ = send2.send(true);
            // The fd is private, but this transmute lets us get at it
            let fd: &std::sync::Arc<u32> = unsafe { std::mem::transmute(&send2) };
            println!("fd = {}", *fd);
            // Stop the ipc channel from being dropped
            senders.push(send2);
        }
    });
    
    let mut bootstrap_multi_receiver = bootstrap_server.accept().unwrap();
    let (subchannel_id, name) = bootstrap_multi_receiver.receive_sub_channel().unwrap();
    assert_eq!(name, "bootstrap".to_string());
    let mut bootstrap_sub_channel_receiver = bootstrap_multi_receiver.attach(subchannel_id).unwrap();
    let send1: SubChannelSender<SubChannelSender<bool>> = bootstrap_sub_channel_receiver.recv().unwrap();
 
    for _ in 0..10000 {
        let _ = send1.send(send2.clone());
        let _ = recv2.recv();
    }
}
