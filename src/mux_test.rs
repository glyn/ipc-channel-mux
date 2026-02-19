// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::mux::{
    self, SubOneShotServer, SubReceiver, SubSelectionResult, SubSender,
    subchannel_router::{ROUTER, RouterError, RouterProxy},
};
use std::thread;
use test_log::test;

#[test]
fn multiplex_simple() {
    let person = ("Patrick Walton".to_owned(), 29);
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    tx.send(person.clone()).unwrap();
    let received_person = rx.recv().unwrap();
    assert_eq!(person, received_person);

    drop(tx);
    match rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected disconnected error, got {:?}", e),
    }
}

#[test]
fn multiplex_two_subchannels() {
    let channel = mux::Channel::new().unwrap();
    let (tx1, rx1) = channel.sub_channel();
    tx1.send(1).unwrap();
    assert_eq!(1, rx1.recv().unwrap());

    let (tx2, rx2) = channel.sub_channel();
    tx2.send(2).unwrap();
    assert_eq!(2, rx2.recv().unwrap());
}

#[test]
fn multiplex_two_subchannels_reverse_ordered() {
    let channel = mux::Channel::new().unwrap();
    let (tx1, rx1) = channel.sub_channel();
    tx1.send(1).unwrap();

    let (tx2, rx2) = channel.sub_channel();
    tx2.send(2).unwrap();

    assert_eq!(2, rx2.recv().unwrap());
    assert_eq!(1, rx1.recv().unwrap());
}

#[test]
fn embedded_multiplexed_senders() {
    let person = ("Patrick Walton".to_owned(), 29);

    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel();

    let person_and_sender = (person.clone(), sub_tx);
    let (super_tx, super_rx) = channel.sub_channel();

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
fn embedded_multiplexed_sender_lifecycle() {
    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel();

    let super_channel = mux::Channel::new().unwrap();
    let (super_tx, super_rx) = super_channel.sub_channel();

    super_tx.send(sub_tx.clone()).unwrap();
    let received_sub_tx: SubSender<i32> = super_rx.recv().unwrap();

    received_sub_tx.send(1).unwrap();
    assert_eq!(sub_rx.recv().unwrap(), 1);

    drop(received_sub_tx);

    // Send the subsender again to see if the association still exists on the receiving side.
    super_tx.send(sub_tx).unwrap();
    let received_sub_tx: SubSender<i32> = super_rx.recv().unwrap();

    received_sub_tx.send(2).unwrap();
    assert_eq!(sub_rx.recv().unwrap(), 2);
}

#[test]
fn embedded_multiplexed_two_senders() {
    let person = ("Patrick Walton".to_owned(), 29);

    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel();
    let (sub_tx2, sub_rx2) = channel.sub_channel();

    let person_and_two_senders = (person.clone(), sub_tx, sub_tx2);
    let (super_tx, super_rx) = channel.sub_channel();

    super_tx.send(person_and_two_senders).unwrap();
    #[allow(clippy::type_complexity)]
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
fn embedded_multiplexed_senders_interacting() {
    let channel = mux::Channel::new().unwrap();
    let (super_tx1, super_rx1) = channel.sub_channel();
    let (sub_tx1, sub_rx1) = channel.sub_channel();

    let channel2 = mux::Channel::new().unwrap();
    let (super_tx2, super_rx2) = channel2.sub_channel();
    let (sub_tx2, sub_rx2) = channel2.sub_channel();

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
fn embedded_multiplexed_senders_with_middleman() {
    // TODO: this test aimed to break the code that always sets MultiMessage:Sending.from to ORIGIN, but it passes.
    let channel = mux::Channel::new().unwrap();
    let (super_tx, super_rx) = channel.sub_channel();
    let (sub_tx, sub_rx) = channel.sub_channel::<i32>();

    let middleman = mux::Channel::new().unwrap();
    let (middleman_super_tx, middleman_super_rx) = middleman.sub_channel();
    let (middleman_sub_tx, middleman_sub_rx) = middleman.sub_channel();

    // Send super and sub subsenders to the middleman
    middleman_super_tx.send(super_tx).unwrap();
    let super_tx_at_middleman = middleman_super_rx.recv().unwrap();
    middleman_sub_tx.send(sub_tx).unwrap();
    let sub_tx_at_middleman = middleman_sub_rx.recv().unwrap();

    // Now send the sub subsender from the middleman
    super_tx_at_middleman.send(sub_tx_at_middleman).unwrap();

    // Cause transmission of the sub subsender to fail.
    drop(super_rx);

    // Check the subreceiver knows about the failure
    assert!(sub_rx.recv().is_err());
}

// This test demonstrates the basic purpose of multiplexing. If IpcChannels were
// used, then this test would fail on Unix variants since the spawned process
// would run out of file descriptors. Using multiplexed channels, the spawned
// process does not run out of file descriptors.
#[test]
fn receiving_many_subchannels() {
    let channel = mux::Channel::new().unwrap();
    let (send2, recv2) = channel.sub_channel();

    // this will be used to receive from the spawned thread
    let (bootstrap_server, bootstrap_token) = SubOneShotServer::new().unwrap();

    thread::spawn(move || {
        let bootstrap_sub_sender: SubSender<SubSender<SubSender<bool>>> =
            SubSender::connect(bootstrap_token).unwrap();

        let channel = mux::Channel::new().unwrap();
        let (send1, recv1) = channel.sub_channel();

        bootstrap_sub_sender.send(send1).unwrap();

        let mut senders = vec![];
        loop {
            let send2 = recv1.recv().unwrap(); // TODO: at the end of the test, this panics. Better if the thread terminated gracefully.
            send2.send(true).unwrap();
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
        send1.send(send2.clone()).unwrap();
        recv2.recv().unwrap();
    }
}

// This test demonstrates a significant benefit of multiplexing. If IpcChannels were
// used, then this test would fail on Unix variants since the creating an IpChannel
// consumes a file descriptor and the test would run out of file descriptors. Using
// multiplexed channels, the test does not run out of file descriptors.
#[test]
fn creating_many_subchannels() {
    let channel = mux::Channel::new().unwrap();
    let mut subchannels = vec![];
    for i in 0..10000 {
        let subchannel = channel.sub_channel::<i32>();
        subchannels.push(subchannel);
        println!("{}", i);
    }
}

#[test]
fn sender_transmission_dropped_in_flight() {
    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel::<i32>();

    let (super_tx, super_rx) = channel.sub_channel();
    super_tx.send(sub_tx).unwrap();

    // match sub_rx.try_recv().unwrap_err() { // try_recv not yet implemented
    //     ipc::TryRecvError::Empty => (),
    //     e => assert!(false, "unexpected error {:?}", e),
    // }

    drop(super_rx);

    match sub_rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected disconnected error, got {:?}", e),
    }
}

#[test]
fn multiplex_drop_only_subsender_for_dropped_channel() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();
    drop(channel);

    drop(tx);
    match rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected send error, got {:?}", e),
    }
}

#[test]
fn multiplex_drop_only_subsender_for_channel() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();

    drop(tx);
    match rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected disconnected error, got {:?}", e),
    }
}

#[test]
fn multiplex_drop_only_subsender_for_subchannel_of_dropped_channel() {
    let channel = mux::Channel::new().unwrap();
    let (tx1, rx1) = channel.sub_channel::<i32>();
    let (tx2, rx2) = channel.sub_channel::<i32>();

    drop(tx1);
    match rx1.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected disconnected error, got {:?}", e),
    }

    // check other subchannel is still working
    tx2.send(1).unwrap();
    assert_eq!(rx2.recv().unwrap(), 1);
}

#[test]
fn multiplex_drop_cloned_subsender() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();

    drop(tx.clone());

    tx.send(1).unwrap();
    assert_eq!(rx.recv().unwrap(), 1);
}

#[test]
fn multiplex_drop_only_subsender_for_subchannel() {
    let channel = mux::Channel::new().unwrap();
    let (tx1, rx1) = channel.sub_channel::<i32>();
    let (tx2, rx2) = channel.sub_channel::<i32>();

    drop(tx1);
    match rx1.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected disconnected error, got {:?}", e),
    }

    // check other subchannel is still working
    tx2.send(1).unwrap();
    assert_eq!(rx2.recv().unwrap(), 1);
}

#[test]
fn drop_transmitted_subsender() {
    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel::<i32>();
    let (super_tx, super_rx) = channel.sub_channel();
    super_tx.send(sub_tx).unwrap();
    let received_sub_tx = super_rx.recv().unwrap();
    drop(received_sub_tx);

    match sub_rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected Disconnected, got {:?}", e),
    }
}

#[test]
fn drop_transmitted_subsender_send_using_clone_of_original() {
    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel::<i32>();
    let (super_tx, super_rx) = channel.sub_channel();
    let sub_tx_clone = sub_tx.clone();
    super_tx.send(sub_tx).unwrap();
    let received_sub_tx = super_rx.recv().unwrap();
    drop(received_sub_tx);

    sub_tx_clone.send(1).unwrap();
    assert_eq!(sub_rx.recv().unwrap(), 1);
}

#[test]
fn drop_transmitted_subsender_send_using_another_transmitted_subsender() {
    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel::<i32>();
    let (super_tx1, super_rx1) = channel.sub_channel();
    super_tx1.send(sub_tx.clone()).unwrap();
    let received_sub_tx1 = super_rx1.recv().unwrap();

    let (super_tx2, super_rx2) = channel.sub_channel();
    super_tx2.send(sub_tx).unwrap();
    let received_sub_tx2 = super_rx2.recv().unwrap();

    drop(received_sub_tx1);

    received_sub_tx2.send(1).unwrap();
    assert_eq!(sub_rx.recv().unwrap(), 1);
}

#[test]
fn drop_transmitted_subsender_send_using_another_subsender_transmitted_over_another_ipc_channel() {
    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel::<i32>();
    let (super_tx1, super_rx1) = channel.sub_channel();
    super_tx1.send(sub_tx.clone()).unwrap();
    let received_sub_tx1 = super_rx1.recv().unwrap();

    let channel2 = mux::Channel::new().unwrap();
    let (super_tx2, super_rx2) = channel2.sub_channel();
    super_tx2.send(sub_tx).unwrap();
    let received_sub_tx2 = super_rx2.recv().unwrap();

    drop(received_sub_tx1);

    received_sub_tx2.send(1).unwrap();
    assert_eq!(sub_rx.recv().unwrap(), 1);
}

#[test]
fn multiplex_drop_only_subreceiver_for_dropped_channel() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();
    drop(channel);

    drop(rx);
    assert!(tx.send(1).is_err());
}

#[test]
fn multiplex_drop_only_subreceiver_for_channel() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();

    drop(rx);
    assert!(tx.send(1).is_err());
    assert!(tx.send(1).is_err()); // ensure second send does not block
}

#[test]
fn multiplex_drop_only_subreceiver_for_subchannel_of_dropped_channel() {
    let channel = mux::Channel::new().unwrap();
    let (tx1, rx1) = channel.sub_channel::<i32>();
    drop(channel);

    drop(rx1);
    assert!(tx1.send(1).is_err());
    assert!(tx1.send(1).is_err()); // ensure second send does not block
}

#[test]
fn compare_base_transmission_failure() {
    let channel1 = mux::Channel::new().unwrap();
    let (tx, rx) = channel1.sub_channel::<i32>();
    log::trace!("POINT A");

    let channel2 = mux::Channel::new().unwrap();
    let (via_tx, via_rx) = channel2.sub_channel();

    via_tx.send(tx).unwrap();
    log::trace!("POINT B");

    drop(via_rx);

    log::trace!("POINT D");
    match rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected Disconnected, got {:?}", e),
    }
}

#[test]
fn opaque_sender() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();

    let opaque_tx = tx.to_opaque();
    let tx: SubSender<i32> = opaque_tx.to();

    tx.send(1).unwrap();
    assert_eq!(rx.recv().unwrap(), 1);
}

#[test]
fn embedded_opaque_sender() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();

    let (via_tx, via_rx) = channel.sub_channel();
    via_tx.send(tx.to_opaque()).unwrap();
    let received_sender = via_rx.recv().unwrap();

    received_sender.to::<i32>().send(1).unwrap();
    assert_eq!(rx.recv().unwrap(), 1);
}

#[test]
fn opaque_receiver() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();

    let opaque_rx = rx.to_opaque();
    let rx: SubReceiver<i32> = opaque_rx.to();

    tx.send(1).unwrap();
    assert_eq!(rx.recv().unwrap(), 1);
}

type Person = (String, u32);

#[test]
// A homogeneous SubReceiverSet is one whose SubReceivers all have the same underlying IpcChannel.
fn receiver_set_homogeneous() {
    let channel = mux::SelectableChannel::new().unwrap();
    let (tx1, rx1) = channel.sub_channel::<i32>();

    let mut rx_set = mux::SubReceiverSet::new().unwrap();
    let rx1_id = rx_set.add(rx1).unwrap();

    let (tx2, rx2) = channel.sub_channel::<String>();
    let rx2_id = rx_set.add(rx2).unwrap();

    tx1.send(1).unwrap();
    tx2.send("test".to_string()).unwrap();

    let mut recvd1 = false;
    let mut recvd2 = false;
    while !recvd1 || !recvd2 {
        for event in rx_set.select().unwrap() {
            if let SubSelectionResult::MessageReceived(received_id, received_data) = event {
                match received_id {
                    id if id == rx1_id => {
                        assert!(!recvd1, "i32 received twice");
                        let received_value: i32 = received_data.to().unwrap();
                        assert_eq!(received_value, 1);
                        recvd1 = true;
                    },
                    id if id == rx2_id => {
                        assert!(!recvd2, "String received twice");
                        let received_value: String = received_data.to().unwrap();
                        assert_eq!(received_value, "test".to_string());
                        recvd2 = true;
                    },
                    _ => panic!("unexpected id"),
                }
            } else {
                panic!("Unexpected SubSelectionResult");
            }
        }
    }
}

#[test]
// A heterogeneous SubReceiverSet is one with SubReceivers having distinct underlying IpcChannels.
fn receiver_set_heterogeneous() {
    let channel1 = mux::SelectableChannel::new().unwrap();
    let (tx1, rx1) = channel1.sub_channel::<i32>();

    let mut rx_set = mux::SubReceiverSet::new().unwrap();
    let rx1_id = rx_set.add(rx1).unwrap();

    let channel2 = mux::SelectableChannel::new().unwrap();
    let (tx2, rx2) = channel2.sub_channel::<String>();
    let rx2_id = rx_set.add(rx2).unwrap();

    tx1.send(1).unwrap();
    tx2.send("test".to_string()).unwrap();

    let mut recvd1 = false;
    let mut recvd2 = false;
    while !recvd1 || !recvd2 {
        for event in rx_set.select().unwrap() {
            if let SubSelectionResult::MessageReceived(received_id, received_data) = event {
                match received_id {
                    id if id == rx1_id => {
                        assert!(!recvd1, "i32 received twice");
                        let received_value: i32 = received_data.to().unwrap();
                        assert_eq!(received_value, 1);
                        recvd1 = true;
                    },
                    id if id == rx2_id => {
                        assert!(!recvd2, "String received twice");
                        let received_value: String = received_data.to().unwrap();
                        assert_eq!(received_value, "test".to_string());
                        recvd2 = true;
                    },
                    _ => panic!("unexpected id"),
                }
            } else {
                panic!("Unexpected SubSelectionResult");
            }
        }
    }
}

#[test]
fn receiver_set_disconnect() {
    let channel = mux::SelectableChannel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();

    let mut rx_set = mux::SubReceiverSet::new().unwrap();
    let rx_id = rx_set.add(rx).unwrap();

    drop(tx);
    if let SubSelectionResult::ChannelClosed(received_id) =
        rx_set.select().unwrap().into_iter().next().unwrap()
    {
        assert_eq!(received_id, rx_id);
    } else {
        panic!("unexpected result");
    }
}

#[test]
fn receiver_set_homogeneous_blocking() {
    // this will be used to receive from the spawned thread
    let (bootstrap_server, bootstrap_token) = SubOneShotServer::new().unwrap();

    let thread = thread::spawn(move || {
        let bootstrap_sub_sender: SubSender<SubSender<i32>> =
            SubSender::connect(bootstrap_token).unwrap();

        let channel = mux::SelectableChannel::new().unwrap();
        let (tx1, rx1) = channel.sub_channel();
        bootstrap_sub_sender.send(tx1).unwrap();
        let (tx2, rx2) = channel.sub_channel();
        bootstrap_sub_sender.send(tx2).unwrap();

        let mut rx_set = mux::SubReceiverSet::new().unwrap();
        let rx1_id = rx_set.add(rx1).unwrap();
        let rx2_id = rx_set.add(rx2).unwrap();

        let mut recvd1 = false;
        let mut recvd2 = false;
        while !recvd1 || !recvd2 {
            for event in rx_set.select().unwrap() {
                if let SubSelectionResult::MessageReceived(received_id, received_data) = event {
                    match received_id {
                        id if id == rx1_id => {
                            assert!(!recvd1, "1 received twice");
                            let received_value: i32 = received_data.to().unwrap();
                            assert_eq!(received_value, 1);
                            recvd1 = true;
                        },
                        id if id == rx2_id => {
                            assert!(!recvd2, "2 received twice");
                            let received_value: i32 = received_data.to().unwrap();
                            assert_eq!(received_value, 2);
                            recvd2 = true;
                        },
                        _ => panic!("unexpected id"),
                    }
                } else {
                    panic!("Unexpected SubSelectionResult");
                }
            }
        }
    });

    let (bootstrap_sub_receiver, tx1): (SubReceiver<SubSender<i32>>, SubSender<i32>) =
        bootstrap_server.accept().unwrap();

    let tx2 = bootstrap_sub_receiver.recv().unwrap();
    tx1.send(1).unwrap();
    tx2.send(2).unwrap();

    thread.join().expect("the spawned thread panicked");
}

#[test]
fn receiver_set_heterogeneous_blocking() {
    // this will be used to receive from the spawned thread
    let (bootstrap_server, bootstrap_token) = SubOneShotServer::new().unwrap();

    let thread = thread::spawn(move || {
        let bootstrap_sub_sender: SubSender<SubSender<i32>> =
            SubSender::connect(bootstrap_token).unwrap();

        let channel1 = mux::SelectableChannel::new().unwrap();
        let (tx1, rx1) = channel1.sub_channel();
        bootstrap_sub_sender.send(tx1).unwrap();

        let channel2 = mux::SelectableChannel::new().unwrap();
        let (tx2, rx2) = channel2.sub_channel();
        bootstrap_sub_sender.send(tx2).unwrap();

        let mut rx_set = mux::SubReceiverSet::new().unwrap();
        let rx1_id = rx_set.add(rx1).unwrap();
        let rx2_id = rx_set.add(rx2).unwrap();

        let mut recvd1 = false;
        let mut recvd2 = false;
        while !recvd1 || !recvd2 {
            for event in rx_set.select().unwrap() {
                if let SubSelectionResult::MessageReceived(received_id, received_data) = event {
                    match received_id {
                        id if id == rx1_id => {
                            assert!(!recvd1, "1 received twice");
                            let received_value: i32 = received_data.to().unwrap();
                            assert_eq!(received_value, 1);
                            recvd1 = true;
                        },
                        id if id == rx2_id => {
                            assert!(!recvd2, "2 received twice");
                            let received_value: i32 = received_data.to().unwrap();
                            assert_eq!(received_value, 2);
                            recvd2 = true;
                        },
                        _ => panic!("unexpected id"),
                    }
                } else {
                    panic!("Unexpected SubSelectionResult");
                }
            }
        }
    });

    let (bootstrap_sub_receiver, tx1): (SubReceiver<SubSender<i32>>, SubSender<i32>) =
        bootstrap_server.accept().unwrap();

    let tx2 = bootstrap_sub_receiver.recv().unwrap();
    tx1.send(1).unwrap();
    tx2.send(2).unwrap();

    thread.join().expect("the spawned thread panicked");
}

#[test]
// Test SubReceivers sharing an IPC channel belonging to distinct SubReceiverSets with distinct IpcReceiverSets.
fn subreceivers_sharing_ipc_channel_cannot_belong_to_distinct_subreceiversets_with_distinct_ipcreceiversets()
 {
    let channel = mux::SelectableChannel::new().unwrap();
    let (_tx1, rx1) = channel.sub_channel::<i32>();
    let (_tx2, rx2) = channel.sub_channel::<i32>();

    let mut rx_set1 = mux::SubReceiverSet::new().unwrap();
    let _rx1_id = rx_set1.add(rx1).unwrap();

    let mut rx_set2 = mux::SubReceiverSet::new().unwrap();

    // Ensure rx_set2 has a non-empty IpcReceiverSet.
    let channel2 = mux::SelectableChannel::new().unwrap();
    let (_tx3, rx3) = channel2.sub_channel::<i32>();
    let _rx3_id = rx_set2.add(rx3).unwrap();

    assert_eq!(
        format!("{:?}", rx_set2.add(rx2)),
        "Err(InternalError(\"Cannot merge non-empty MultiReceiverSets\"))"
    );
}

#[test]
// A homogeneous SubReceiverSet is one whose SubReceivers all have the same underlying IpcChannel.
fn receiver_sets_with_subreceivers_sharing_ipc_channel() {
    let channel = mux::SelectableChannel::new().unwrap();
    let (tx1, rx1) = channel.sub_channel::<i32>();

    let mut rx_set1 = mux::SubReceiverSet::new().unwrap();
    let rx1_id = rx_set1.add(rx1).unwrap();

    let mut rx_set2 = mux::SubReceiverSet::new().unwrap();
    let (tx2, rx2) = channel.sub_channel::<String>();
    let rx2_id = rx_set2.add(rx2).unwrap();

    tx1.send(1).unwrap();

    if let SubSelectionResult::MessageReceived(received_id, received_data) =
        rx_set1.select().unwrap().into_iter().next().unwrap()
    {
        match received_id {
            id if id == rx1_id => {
                let received_value: i32 = received_data.to().unwrap();
                assert_eq!(received_value, 1);
            },
            _ => panic!("unexpected id"),
        }
    } else {
        panic!("Unexpected SubSelectionResult");
    }

    tx2.send("test".to_string()).unwrap();

    if let SubSelectionResult::MessageReceived(received_id, received_data) =
        rx_set2.select().unwrap().into_iter().next().unwrap()
    {
        match received_id {
            id if id == rx2_id => {
                let received_value: String = received_data.to().unwrap();
                assert_eq!(received_value, "test".to_string());
            },
            _ => panic!("unexpected id"),
        }
    } else {
        panic!("Unexpected SubSelectionResult");
    }
}

#[test]
fn router_simple_global() {
    // Note: All ROUTER operations need to run in a single test,
    // since tests running in the same process will share router
    // state.

    let channel = RouterProxy::new_router_channel(&ROUTER).unwrap();

    let (callback_fired_sender, callback_fired_receiver) = crossbeam_channel::unbounded::<usize>();
    let tx = channel
        .add_typed_route(Box::new(move |message| {
            callback_fired_sender.send(message.unwrap()).unwrap();
        }))
        .unwrap();

    let message: usize = 42;
    tx.send(message).unwrap();

    let received_message = callback_fired_receiver.recv().unwrap();
    assert_eq!(received_message, message);

    // Now shut down the router.
    ROUTER.shutdown();

    // Use router after shutdown.
    let (callback_fired_sender, _callback_fired_receiver) =
        crossbeam_channel::unbounded::<Person>();
    if let Err(RouterError::ShuttingDown) = channel.add_typed_route(Box::new(move |person| {
        callback_fired_sender.send(person.unwrap()).unwrap();
    })) {
    } else {
        panic!("router did not return ShuttingDown error");
    }

    // The sender should have been dropped.
    assert!(tx.send(43).is_err());

    // Shutdown the router, again (should be a no-op).
    ROUTER.shutdown();
}

#[test]
fn router_channel_usable_after_all_senders_dropped() {
    let proxy = RouterProxy::new();
    let channel = RouterProxy::new_router_channel(&proxy).unwrap();

    // Create a routed subchannel.
    let (callback_fired_sender, callback_fired_receiver) = crossbeam_channel::unbounded::<usize>();
    let tx = channel
        .add_typed_route(Box::new(move |message| {
            callback_fired_sender.send(message.unwrap()).unwrap();
        }))
        .unwrap();

    // Send and receive a message to confirm the route works.
    tx.send(42).unwrap();
    assert_eq!(callback_fired_receiver.recv().unwrap(), 42);

    // Drop the sender. The router will process ChannelClosed, dropping
    // the SelectableSubChannelReceiver.
    drop(tx);

    // Wait for the router to process the disconnection.
    thread::sleep(std::time::Duration::from_millis(100));

    // The RouterChannel should still be usable to add new routes
    // even though all previous senders were dropped.
    let (callback_fired_sender2, callback_fired_receiver2) =
        crossbeam_channel::unbounded::<usize>();
    let tx2 = channel
        .add_typed_route(Box::new(move |message| {
            callback_fired_sender2.send(message.unwrap()).unwrap();
        }))
        .expect("RouterChannel should still be usable after all senders dropped");

    tx2.send(99).unwrap();
    assert_eq!(callback_fired_receiver2.recv().unwrap(), 99);

    proxy.shutdown();
}
