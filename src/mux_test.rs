// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::mux::{
    self, IpcChannelSubSender, IpcReceiver, IpcSender, MuxError, SharedMemory, SubOneShotServer,
    SubReceiver, SubSender, TryRecvError,
    subchannel_router::{ROUTER, RouterError, RouterProxy},
};
use ipc_channel::ipc::{self as raw_ipc, IpcSharedMemory};
use serde::{Deserialize, Serialize};
use std::thread;
use std::time::{Duration, Instant};
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
        e => panic!("expected disconnected error, got {e:?}"),
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
            if let Ok(send2) = recv1.recv() {
                send2.send(true).unwrap();

                // The fd is private, but this transmute lets us get at it
                //let fd: &std::sync::Arc<u32> = unsafe { std::mem::transmute(&send2) };
                //println!("fd = {}", *fd);

                // Stop the ipc channel from being dropped
                senders.push(send2);
            } else {
                return;
            }
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
// used, then this test would fail on Unix variants since the creating an IpcChannel
// consumes a file descriptor and the test would run out of file descriptors. Using
// multiplexed channels, the test does not run out of file descriptors.
#[test]
fn creating_many_subchannels() {
    let channel = mux::Channel::new().unwrap();
    let mut subchannels = vec![];
    for _i in 0..10000 {
        let subchannel = channel.sub_channel::<i32>();
        subchannels.push(subchannel);
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
    //     e => assert!(false, "unexpected error {e:?}"),
    // }

    drop(super_rx);

    match sub_rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected disconnected error, got {e:?}"),
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
        e => panic!("expected send error, got {e:?}"),
    }
}

#[test]
fn multiplex_drop_only_subsender_for_channel() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();

    drop(tx);
    match rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected disconnected error, got {e:?}"),
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
        e => panic!("expected disconnected error, got {e:?}"),
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
        e => panic!("expected disconnected error, got {e:?}"),
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
        e => panic!("expected Disconnected, got {e:?}"),
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
        e => panic!("expected Disconnected, got {e:?}"),
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
    let proxy = RouterProxy::new().unwrap();
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

#[test]
fn shmem_simple() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    let data = SharedMemory::from_bytes(b"hello world");
    tx.send(data.clone()).unwrap();
    let received: SharedMemory = rx.recv().unwrap();
    assert_eq!(&*received, b"hello world");
    assert_eq!(data, received);
}

#[test]
fn shmem_from_byte() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    let data = SharedMemory::from_byte(0xAB, 1024);
    tx.send(data).unwrap();
    let received: SharedMemory = rx.recv().unwrap();
    assert_eq!(received.len(), 1024);
    assert!(received.iter().all(|&b| b == 0xAB));
}

#[test]
fn shmem_empty() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    let data = SharedMemory::from_bytes(&[]);
    tx.send(data).unwrap();
    let received: SharedMemory = rx.recv().unwrap();
    assert!(received.is_empty());
}

#[test]
fn shmem_large() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    let size = 4 * 1024 * 1024; // 4MB
    let data = SharedMemory::from_byte(0x42, size);
    tx.send(data).unwrap();
    let received: SharedMemory = rx.recv().unwrap();
    assert_eq!(received.len(), size);
    assert!(received.iter().all(|&b| b == 0x42));
}

#[test]
fn shmem_multiple_in_message() {
    #[derive(Serialize, Deserialize, Debug)]
    struct TwoRegions {
        a: SharedMemory,
        b: SharedMemory,
    }

    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    let msg = TwoRegions {
        a: SharedMemory::from_bytes(b"first"),
        b: SharedMemory::from_bytes(b"second"),
    };
    tx.send(msg).unwrap();
    let received: TwoRegions = rx.recv().unwrap();
    assert_eq!(&*received.a, b"first");
    assert_eq!(&*received.b, b"second");
}

#[test]
fn shmem_with_subsender() {
    #[derive(Serialize, Deserialize)]
    struct MsgWithShmemAndSender {
        data: SharedMemory,
        sender: SubSender<i32>,
    }

    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    let (inner_tx, inner_rx) = channel.sub_channel::<i32>();

    let msg = MsgWithShmemAndSender {
        data: SharedMemory::from_bytes(b"payload"),
        sender: inner_tx,
    };
    tx.send(msg).unwrap();

    let received: MsgWithShmemAndSender = rx.recv().unwrap();
    assert_eq!(&*received.data, b"payload");
    received.sender.send(42).unwrap();
    assert_eq!(inner_rx.recv().unwrap(), 42);
}

#[test]
fn shmem_cross_thread() {
    let (server, name) = SubOneShotServer::<SharedMemory>::new().unwrap();

    thread::spawn(move || {
        let tx = SubSender::connect(name).unwrap();
        tx.send(SharedMemory::from_bytes(b"cross-thread")).unwrap();
    });

    let (rx, first) = server.accept().unwrap();
    assert_eq!(&*first, b"cross-thread");
    drop(rx);
}

#[test]
fn shmem_deref() {
    let data = SharedMemory::from_bytes(b"abcdef");
    assert_eq!(data.len(), 6);
    assert_eq!(data[0], b'a');
    assert_eq!(&data[2..4], b"cd");
}

#[test]
fn shmem_deref_mut() {
    let mut data = SharedMemory::from_bytes(b"hello");
    assert_eq!(&*data, b"hello");
    // SAFETY: we have the only reference and don't clone or serialize.
    let bytes = unsafe { data.deref_mut() };
    bytes[0] = b'H';
    bytes[4] = b'O';
    assert_eq!(&*data, b"HellO");
}

#[test]
fn shmem_take() {
    let data = SharedMemory::from_bytes(b"take me");
    let bytes = data.take();
    assert_eq!(bytes, Some(b"take me".to_vec()));
}

#[test]
fn shmem_take_empty() {
    let data = SharedMemory::from_bytes(b"");
    let bytes = data.take();
    assert_eq!(bytes, Some(vec![]));
}

#[test]
fn shmem_ipc_conversion() {
    let original = SharedMemory::from_bytes(b"roundtrip");
    let ipc: IpcSharedMemory = original.clone().into();
    let back: SharedMemory = ipc.into();
    assert_eq!(&*original, &*back);
}

#[test]
fn shmem_via_router() {
    let proxy = RouterProxy::new().unwrap();
    let channel = RouterProxy::new_router_channel(&proxy).unwrap();

    let (callback_sender, callback_receiver) = crossbeam_channel::unbounded::<SharedMemory>();
    let tx = channel
        .add_typed_route(Box::new(move |message| {
            callback_sender.send(message.unwrap()).unwrap();
        }))
        .unwrap();

    tx.send(SharedMemory::from_bytes(b"routed")).unwrap();

    let received = callback_receiver.recv().unwrap();
    assert_eq!(&*received, b"routed");

    proxy.shutdown();
}

// --- try_recv tests ---

#[test]
fn try_recv_empty_channel() {
    let channel = mux::Channel::new().unwrap();
    let (_tx, rx) = channel.sub_channel::<i32>();
    match rx.try_recv() {
        Err(TryRecvError::Empty) => (),
        v => panic!("expected Empty, got {v:?}"),
    }
}

#[test]
fn try_recv_with_message() {
    let person = ("Patrick Walton".to_owned(), 29);
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    tx.send(person.clone()).unwrap();
    let received_person = rx.try_recv().unwrap();
    assert_eq!(person, received_person);
}

#[test]
fn try_recv_empty_after_receive() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    tx.send(1).unwrap();
    assert_eq!(rx.try_recv().unwrap(), 1);
    match rx.try_recv() {
        Err(TryRecvError::Empty) => (),
        v => panic!("expected Empty, got {v:?}"),
    }
}

#[test]
fn try_recv_multiple_messages() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();
    assert_eq!(rx.try_recv().unwrap(), 1);
    assert_eq!(rx.try_recv().unwrap(), 2);
    assert_eq!(rx.try_recv().unwrap(), 3);
    match rx.try_recv() {
        Err(TryRecvError::Empty) => (),
        v => panic!("expected Empty, got {v:?}"),
    }
}

#[test]
fn try_recv_disconnected_after_sender_drop() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();
    drop(tx);
    // Disconnection detection may be lazy, so loop until we get Disconnected.
    loop {
        match rx.try_recv() {
            Err(TryRecvError::Empty) => {
                // Disconnection not yet observed, try again after a brief pause.
                thread::sleep(Duration::from_millis(10));
            },
            Err(TryRecvError::MuxError(MuxError::Disconnected)) => break,
            v => panic!("expected Empty or Disconnected, got {v:?}"),
        }
    }
}

#[test]
fn try_recv_buffered_messages_before_disconnect() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    drop(tx);
    // Buffered messages should still be available even after sender drop.
    assert_eq!(rx.try_recv().unwrap(), 1);
    assert_eq!(rx.try_recv().unwrap(), 2);
    // After buffered messages are drained, should eventually get Disconnected.
    loop {
        match rx.try_recv() {
            Err(TryRecvError::Empty) => {
                thread::sleep(Duration::from_millis(10));
            },
            Err(TryRecvError::MuxError(MuxError::Disconnected)) => break,
            v => panic!("expected Empty or Disconnected, got {v:?}"),
        }
    }
}

#[test]
fn try_recv_two_subchannels_independent() {
    let channel = mux::Channel::new().unwrap();
    let (tx1, rx1) = channel.sub_channel();
    let (_tx2, rx2) = channel.sub_channel::<i32>();
    tx1.send(42).unwrap();
    // rx1 has a message, rx2 does not.
    assert_eq!(rx1.try_recv().unwrap(), 42);
    match rx2.try_recv() {
        Err(TryRecvError::Empty) => (),
        v => panic!("expected Empty on rx2, got {v:?}"),
    }
}

#[test]
fn try_recv_with_cloned_sender_partial_drop() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();
    let tx_clone = tx.clone();
    drop(tx);
    // One clone was dropped, but another still exists — should be Empty, not Disconnected.
    match rx.try_recv() {
        Err(TryRecvError::Empty) => (),
        v => panic!("expected Empty, got {v:?}"),
    }
    // Can still send via the surviving clone.
    tx_clone.send(7).unwrap();
    assert_eq!(rx.try_recv().unwrap(), 7);
}

#[test]
fn try_recv_with_in_flight_subsender() {
    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel::<i32>();
    let (super_tx, super_rx) = channel.sub_channel();
    super_tx.send(sub_tx).unwrap();
    // sub_tx is in-flight (sent but not yet received) — should be Empty, not Disconnected.
    match sub_rx.try_recv() {
        Err(TryRecvError::Empty) => (),
        v => panic!("expected Empty while sender is in-flight, got {v:?}"),
    }
    // Receive the in-flight sender and use it.
    let received_tx: SubSender<i32> = super_rx.recv().unwrap();
    received_tx.send(99).unwrap();
    // The message may not be immediately demuxed, so retry.
    loop {
        match sub_rx.try_recv() {
            Ok(value) => {
                assert_eq!(value, 99);
                break;
            },
            Err(TryRecvError::Empty) => {
                thread::sleep(Duration::from_millis(1));
            },
            v => panic!("expected Ok(99) or Empty, got {v:?}"),
        }
    }
}

// --- try_recv_timeout tests ---

#[test]
fn try_recv_timeout_empty_channel() {
    let channel = mux::Channel::new().unwrap();
    let (_tx, rx) = channel.sub_channel::<i32>();
    let timeout = Duration::from_millis(100);
    let start = Instant::now();
    match rx.try_recv_timeout(timeout) {
        Err(TryRecvError::Empty) => {
            assert!(
                start.elapsed() >= Duration::from_millis(50),
                "should have waited for at least part of the timeout"
            );
        },
        v => panic!("expected Empty, got {v:?}"),
    }
}

#[test]
fn try_recv_timeout_with_message() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    tx.send(42).unwrap();
    let timeout = Duration::from_secs(5);
    let start = Instant::now();
    let value = rx.try_recv_timeout(timeout).unwrap();
    assert_eq!(value, 42);
    assert!(
        start.elapsed() < timeout,
        "should have returned immediately when message is available"
    );
}

#[test]
fn try_recv_timeout_empty_after_receive() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    tx.send(1).unwrap();
    assert_eq!(rx.try_recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    let timeout = Duration::from_millis(100);
    let start = Instant::now();
    match rx.try_recv_timeout(timeout) {
        Err(TryRecvError::Empty) => {
            assert!(start.elapsed() >= Duration::from_millis(50));
        },
        v => panic!("expected Empty, got {v:?}"),
    }
}

#[test]
fn try_recv_timeout_disconnected_after_sender_drop() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<i32>();
    drop(tx);
    let timeout = Duration::from_secs(5);
    let start = Instant::now();
    match rx.try_recv_timeout(timeout) {
        Err(TryRecvError::MuxError(MuxError::Disconnected)) => {
            assert!(
                start.elapsed() < timeout,
                "should detect disconnection before full timeout"
            );
        },
        v => panic!("expected Disconnected, got {v:?}"),
    }
}

#[test]
fn try_recv_timeout_buffered_messages_before_disconnect() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    drop(tx);
    let timeout = Duration::from_secs(1);
    assert_eq!(rx.try_recv_timeout(timeout).unwrap(), 1);
    assert_eq!(rx.try_recv_timeout(timeout).unwrap(), 2);
    match rx.try_recv_timeout(timeout) {
        Err(TryRecvError::MuxError(MuxError::Disconnected)) => (),
        v => panic!("expected Disconnected, got {v:?}"),
    }
}

#[test]
fn try_recv_timeout_message_arrives_during_wait() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();
    let timeout = Duration::from_secs(5);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        tx.send(77).unwrap();
    });
    let start = Instant::now();
    let value = rx.try_recv_timeout(timeout).unwrap();
    assert_eq!(value, 77);
    assert!(
        start.elapsed() < timeout,
        "should have returned before full timeout"
    );
}

#[test]
fn send_subsender_via_router() {
    let router = RouterProxy::new().unwrap();
    let channel = RouterProxy::new_router_channel(&router).unwrap();
    let (tx, crossbeam_rx) = channel
        .route_to_new_crossbeam_receiver::<SubSender<i32>>()
        .unwrap();

    // Create a SubSender<i32> to send through the routed channel.
    let plain_channel = mux::Channel::new().unwrap();
    let (inner_tx, inner_rx) = plain_channel.sub_channel::<i32>();

    // Send the SubSender through the router's selectable path.
    tx.send(inner_tx).unwrap();

    // Receive the SubSender via the crossbeam receiver.
    let received_tx = crossbeam_rx.recv().unwrap();

    // Use the received SubSender to send a value.
    received_tx.send(42).unwrap();
    assert_eq!(inner_rx.recv().unwrap(), 42);

    router.shutdown();
}

// --- bytes subchannel tests ---

#[test]
fn bytes_simple() {
    let bytes = [1u8, 2, 3, 4, 5, 6, 7];
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    tx.send(&bytes).unwrap();
    let received = rx.recv().unwrap();
    assert_eq!(&bytes[..], &received[..]);
}

#[test]
fn bytes_empty() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    tx.send(&[]).unwrap();
    let received = rx.recv().unwrap();
    assert!(received.is_empty());
}

#[test]
fn bytes_large() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    let data: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();

    tx.send(&data).unwrap();
    let received = rx.recv().unwrap();
    assert_eq!(data, received);
}

#[test]
fn bytes_multiple_messages() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    tx.send(b"first").unwrap();
    tx.send(b"second").unwrap();
    tx.send(b"third").unwrap();
    assert_eq!(rx.recv().unwrap(), b"first");
    assert_eq!(rx.recv().unwrap(), b"second");
    assert_eq!(rx.recv().unwrap(), b"third");
}

#[test]
fn bytes_two_subchannels() {
    let channel = mux::Channel::new().unwrap();
    let (tx1, rx1) = channel.bytes_sub_channel();
    let (tx2, rx2) = channel.bytes_sub_channel();
    tx1.send(b"one").unwrap();
    tx2.send(b"two").unwrap();
    assert_eq!(rx2.recv().unwrap(), b"two");
    assert_eq!(rx1.recv().unwrap(), b"one");
}

#[test]
fn bytes_alongside_typed_subchannel() {
    let channel = mux::Channel::new().unwrap();
    let (bytes_tx, bytes_rx) = channel.bytes_sub_channel();
    let (typed_tx, typed_rx) = channel.sub_channel::<i32>();
    bytes_tx.send(b"hello").unwrap();
    typed_tx.send(42).unwrap();
    assert_eq!(typed_rx.recv().unwrap(), 42);
    assert_eq!(bytes_rx.recv().unwrap(), b"hello");
}

#[test]
fn bytes_sender_clone() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    let tx2 = tx.clone();
    tx.send(b"from original").unwrap();
    tx2.send(b"from clone").unwrap();
    assert_eq!(rx.recv().unwrap(), b"from original");
    assert_eq!(rx.recv().unwrap(), b"from clone");
}

#[test]
fn bytes_disconnect_on_sender_drop() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    drop(tx);
    match rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected Disconnected, got {e:?}"),
    }
}

#[test]
fn bytes_disconnect_on_receiver_drop() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    drop(rx);
    assert!(tx.send(b"should fail").is_err());
}

#[test]
fn bytes_disconnect_cloned_sender_partial_drop() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    let tx2 = tx.clone();
    drop(tx);
    tx2.send(b"still alive").unwrap();
    assert_eq!(rx.recv().unwrap(), b"still alive");
}

#[test]
fn bytes_disconnect_all_cloned_senders_dropped() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    let tx2 = tx.clone();
    drop(tx);
    drop(tx2);
    match rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected Disconnected, got {e:?}"),
    }
}

#[test]
fn bytes_buffered_messages_before_disconnect() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    tx.send(b"one").unwrap();
    tx.send(b"two").unwrap();
    drop(tx);
    assert_eq!(rx.recv().unwrap(), b"one");
    assert_eq!(rx.recv().unwrap(), b"two");
    match rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected Disconnected, got {e:?}"),
    }
}

#[test]
fn bytes_try_recv_empty() {
    let channel = mux::Channel::new().unwrap();
    let (_tx, rx) = channel.bytes_sub_channel();
    match rx.try_recv() {
        Err(TryRecvError::Empty) => (),
        v => panic!("expected Empty, got {v:?}"),
    }
}

#[test]
fn bytes_try_recv_with_message() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    tx.send(b"hello").unwrap();
    assert_eq!(rx.try_recv().unwrap(), b"hello");
}

#[test]
fn bytes_try_recv_disconnected() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    drop(tx);
    loop {
        match rx.try_recv() {
            Err(TryRecvError::Empty) => {
                thread::sleep(Duration::from_millis(10));
            },
            Err(TryRecvError::MuxError(MuxError::Disconnected)) => break,
            v => panic!("expected Empty or Disconnected, got {v:?}"),
        }
    }
}

#[test]
fn bytes_try_recv_timeout_empty() {
    let channel = mux::Channel::new().unwrap();
    let (_tx, rx) = channel.bytes_sub_channel();
    let timeout = Duration::from_millis(100);
    let start = Instant::now();
    match rx.try_recv_timeout(timeout) {
        Err(TryRecvError::Empty) => {
            assert!(
                start.elapsed() >= Duration::from_millis(50),
                "should have waited for at least part of the timeout"
            );
        },
        v => panic!("expected Empty, got {v:?}"),
    }
}

#[test]
fn bytes_try_recv_timeout_with_message() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    tx.send(b"hello").unwrap();
    let timeout = Duration::from_secs(5);
    let start = Instant::now();
    assert_eq!(rx.try_recv_timeout(timeout).unwrap(), b"hello");
    assert!(start.elapsed() < timeout);
}

#[test]
fn bytes_try_recv_timeout_disconnected() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    drop(tx);
    let timeout = Duration::from_secs(5);
    let start = Instant::now();
    match rx.try_recv_timeout(timeout) {
        Err(TryRecvError::MuxError(MuxError::Disconnected)) => {
            assert!(
                start.elapsed() < timeout,
                "should detect disconnection before full timeout"
            );
        },
        v => panic!("expected Disconnected, got {v:?}"),
    }
}

#[test]
fn bytes_try_recv_timeout_message_arrives_during_wait() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    let timeout = Duration::from_secs(5);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        tx.send(b"delayed").unwrap();
    });
    let start = Instant::now();
    assert_eq!(rx.try_recv_timeout(timeout).unwrap(), b"delayed");
    assert!(start.elapsed() < timeout);
}

#[test]
fn bytes_sender_embedded_in_typed_message() {
    use crate::mux::BytesSubSender;

    let channel = mux::Channel::new().unwrap();
    let (bytes_tx, bytes_rx) = channel.bytes_sub_channel();

    let (via_tx, via_rx) = channel.sub_channel();
    via_tx.send(bytes_tx).unwrap();
    let received_tx: BytesSubSender = via_rx.recv().unwrap();

    received_tx.send(b"via embedded sender").unwrap();
    assert_eq!(bytes_rx.recv().unwrap(), b"via embedded sender");
}

#[test]
fn bytes_sender_dropped_in_flight() {
    let channel = mux::Channel::new().unwrap();
    let (bytes_tx, bytes_rx) = channel.bytes_sub_channel();

    let (via_tx, via_rx) = channel.sub_channel();
    via_tx.send(bytes_tx).unwrap();

    drop(via_rx);

    match bytes_rx.recv().unwrap_err() {
        mux::MuxError::Disconnected => (),
        e => panic!("expected Disconnected, got {e:?}"),
    }
}

#[test]
fn bytes_cross_thread() {
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();

    thread::spawn(move || {
        tx.send(b"from thread").unwrap();
    });

    assert_eq!(rx.recv().unwrap(), b"from thread");
}

#[test]
fn bytes_odd_alignment() {
    // Use odd-length data to expose any alignment issues.
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    let bytes = [1u8, 2, 3, 4, 5, 6, 7];
    tx.send(&bytes).unwrap();
    assert_eq!(rx.recv().unwrap(), bytes);
}

#[test]
fn bytes_binary_data() {
    // All 256 byte values to ensure no encoding issues.
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.bytes_sub_channel();
    let data: Vec<u8> = (0u8..=255).collect();
    tx.send(&data).unwrap();
    assert_eq!(rx.recv().unwrap(), data);
}

// --- IpcSender / IpcReceiver over subchannels ---

#[test]
fn ipc_sender_simple() {
    let (raw_tx, raw_rx) = raw_ipc::channel::<i32>().unwrap();

    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();

    tx.send(IpcSender::from(raw_tx)).unwrap();
    let recovered_tx = rx.recv().unwrap().into_inner();

    recovered_tx.send(42).unwrap();
    assert_eq!(raw_rx.recv().unwrap(), 42);
}

#[test]
fn ipc_receiver_simple() {
    let (raw_tx, raw_rx) = raw_ipc::channel::<i32>().unwrap();

    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();

    tx.send(IpcReceiver::from(raw_rx)).unwrap();
    let recovered_rx = rx.recv().unwrap().into_inner().unwrap();

    raw_tx.send(99).unwrap();
    assert_eq!(recovered_rx.recv().unwrap(), 99);
}

#[test]
fn ipc_sender_clone() {
    let (raw_tx, raw_rx) = raw_ipc::channel::<i32>().unwrap();

    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel();

    let wrapped = IpcSender::from(raw_tx);
    let cloned = wrapped.clone();

    tx.send(wrapped).unwrap();
    let recovered_tx1 = rx.recv().unwrap().into_inner();

    tx.send(cloned).unwrap();
    let recovered_tx2 = rx.recv().unwrap().into_inner();

    recovered_tx1.send(1).unwrap();
    recovered_tx2.send(2).unwrap();
    assert_eq!(raw_rx.recv().unwrap(), 1);
    assert_eq!(raw_rx.recv().unwrap(), 2);
}

#[test]
fn ipc_sender_and_receiver_in_same_message() {
    #[derive(Serialize, Deserialize)]
    struct Endpoints {
        tx: IpcSender<String>,
        rx: IpcReceiver<String>,
    }

    let (raw_tx1, raw_rx1) = raw_ipc::channel::<String>().unwrap();
    let (raw_tx2, raw_rx2) = raw_ipc::channel::<String>().unwrap();

    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel();

    sub_tx
        .send(Endpoints {
            tx: IpcSender::from(raw_tx1),
            rx: IpcReceiver::from(raw_rx2),
        })
        .unwrap();

    let endpoints: Endpoints = sub_rx.recv().unwrap();
    let recovered_tx = endpoints.tx.into_inner();
    let recovered_rx = endpoints.rx.into_inner().unwrap();

    recovered_tx.send("hello".to_string()).unwrap();
    assert_eq!(raw_rx1.recv().unwrap(), "hello");

    raw_tx2.send("world".to_string()).unwrap();
    assert_eq!(recovered_rx.recv().unwrap(), "world");
}

#[test]
fn ipc_sender_cross_thread() {
    let (server, name) = SubOneShotServer::<IpcSender<i32>>::new().unwrap();

    thread::spawn(move || {
        let (raw_tx, raw_rx) = raw_ipc::channel::<i32>().unwrap();
        let sub_tx = SubSender::connect(name).unwrap();
        sub_tx.send(IpcSender::from(raw_tx)).unwrap();
        assert_eq!(raw_rx.recv().unwrap(), 1729);
    });

    let (sub_rx, wrapped_tx) = server.accept().unwrap();
    drop(sub_rx);
    wrapped_tx.into_inner().send(1729).unwrap();
}

#[test]
fn ipc_receiver_double_serialize_error() {
    let (_raw_tx, raw_rx) = raw_ipc::channel::<i32>().unwrap();
    let wrapped = IpcReceiver::from(raw_rx);

    // First serialize succeeds (captures the receiver into the thread-local).
    assert!(postcard::to_stdvec(&wrapped).is_ok());
    // Drain the thread-local so the OS handle is released promptly and does
    // not linger until another send clears it.
    let _ = crate::mux::ipc_channel::take_ipc_receivers_for_send();
    // Second serialize fails because the receiver was already consumed.
    assert!(postcard::to_stdvec(&wrapped).is_err());
}

// --- IpcChannelSubSender: SubSender over a raw IPC channel ---

#[test]
fn ipc_channel_sub_sender_basic() {
    // Create a subchannel.
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<u32>();

    // Convert the SubSender to a transport wrapper and send it over a raw IPC channel.
    let (raw_tx, raw_rx) = raw_ipc::channel::<IpcChannelSubSender<u32>>().unwrap();
    raw_tx.send(IpcChannelSubSender::from(tx)).unwrap();

    // Reconstruct the SubSender on the "receiving process" side.
    let transport: IpcChannelSubSender<u32> = raw_rx.recv().unwrap();
    let recovered_tx: SubSender<u32> = transport.into_sub_sender().unwrap();

    recovered_tx.send(42).unwrap();
    assert_eq!(rx.recv().unwrap(), 42);
}

#[test]
fn ipc_channel_sub_sender_original_dropped() {
    // Convert to IpcChannelSubSender and then drop the original SubSender.
    // The subchannel must still work after the transport is reconstructed.
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<u32>();

    let (raw_tx, raw_rx) = raw_ipc::channel::<IpcChannelSubSender<u32>>().unwrap();
    raw_tx.send(IpcChannelSubSender::from(tx)).unwrap();
    // The original tx is consumed above; no clone remains on the sending side.

    let transport: IpcChannelSubSender<u32> = raw_rx.recv().unwrap();
    let recovered_tx: SubSender<u32> = transport.into_sub_sender().unwrap();

    recovered_tx.send(99).unwrap();
    assert_eq!(rx.recv().unwrap(), 99);
}

#[test]
fn ipc_channel_sub_sender_from_clone() {
    // Clone the SubSender, convert the clone to IpcChannelSubSender, keep the original.
    // Both the original and the reconstructed sender must be able to send messages.
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<u32>();

    let (raw_tx, raw_rx) = raw_ipc::channel::<IpcChannelSubSender<u32>>().unwrap();
    raw_tx.send(IpcChannelSubSender::from(tx.clone())).unwrap();

    let transport: IpcChannelSubSender<u32> = raw_rx.recv().unwrap();
    let recovered_tx: SubSender<u32> = transport.into_sub_sender().unwrap();

    tx.send(1).unwrap();
    recovered_tx.send(2).unwrap();
    assert_eq!(rx.recv().unwrap(), 1);
    assert_eq!(rx.recv().unwrap(), 2);
}

#[test]
fn ipc_channel_sub_sender_dropped_after_reconstruction() {
    // Verify that dropping the SubSender reconstructed via into_sub_sender()
    // causes the SubReceiver to return Disconnected.
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<u32>();

    let (raw_tx, raw_rx) = raw_ipc::channel::<IpcChannelSubSender<u32>>().unwrap();
    raw_tx.send(IpcChannelSubSender::from(tx)).unwrap();

    let transport: IpcChannelSubSender<u32> = raw_rx.recv().unwrap();
    let recovered_tx: SubSender<u32> = transport.into_sub_sender().unwrap();

    drop(recovered_tx);

    match rx.recv().unwrap_err() {
        MuxError::Disconnected => (),
        e => panic!("expected Disconnected, got {e:?}"),
    }
}

#[test]
fn ipc_channel_sub_sender_detects_receiver_disconnection() {
    // Verify that a SubSender reconstructed via into_sub_sender() correctly
    // detects when the SubReceiver is dropped.
    let channel = mux::Channel::new().unwrap();
    let (tx, rx) = channel.sub_channel::<u32>();

    let (raw_tx, raw_rx) = raw_ipc::channel::<IpcChannelSubSender<u32>>().unwrap();
    raw_tx.send(IpcChannelSubSender::from(tx)).unwrap();

    let transport: IpcChannelSubSender<u32> = raw_rx.recv().unwrap();
    let recovered_tx: SubSender<u32> = transport.into_sub_sender().unwrap();

    // Allow the demuxer time to process the Connect message from into_sub_sender
    // so the new client is registered before we drop the receiver.
    thread::sleep(Duration::from_millis(100));

    drop(rx);

    // Allow SubReceiverDisconnected to propagate to the response channel.
    thread::sleep(Duration::from_millis(100));

    match recovered_tx.send(42) {
        Err(MuxError::Disconnected) => (),
        Ok(()) => panic!("expected Disconnected: subreceiver disconnection not detected"),
        Err(e) => panic!("expected Disconnected, got {e:?}"),
    }
}
