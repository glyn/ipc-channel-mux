// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use ipc_channel_mux::mux::subchannel_router::{ROUTER, RouterProxy};
use ipc_channel_mux::mux::{Channel, MultiplexError, SubOneShotServer, SubReceiver, SubSender};
use std::{env, process};
use test_log::test;

// The integration tests may be run on their own by issuing:
// cargo test --test '*'

/// Test non-router polling behaviour when a SubSender is sent to a
/// process which terminates before the SubSender has been
/// received.
///
/// This provides the basis for testing the router behaviour.
#[test]
fn subsender_drop_inflight() {
    let executable_path: String = env!("CARGO_BIN_EXE_crashing_receiving_process").to_string();

    type ChannelPair = (SubSender<SubSender<bool>>, SubSender<()>);

    let (server, token) =
        SubOneShotServer::<ChannelPair>::new().expect("Failed to create sub one-shot server");

    let mut command = process::Command::new(executable_path);
    let child_process = command.arg(token);

    let mut child = child_process
        .spawn()
        .expect("Failed to start child process");

    let (_rx, (data, control)): (SubReceiver<ChannelPair>, ChannelPair) =
        server.accept().expect("accept failed");

    let d = Channel::new().unwrap();
    let (transmit_tx, transmit_rx) = d.sub_channel();

    data.send(transmit_tx).expect("subsender send failed");

    // Ensure that the "sending" message is received so that polling occurs.
    let (extra_tx, extra_rx) = d.sub_channel();
    extra_tx.send(()).unwrap();
    extra_rx.recv().unwrap(); // uses the same IpcReceiver as transmit_rx

    control.send(()).expect("control send failed");
    let result = child.wait().expect("wait for child process failed");
    assert_eq!(
        result.code().unwrap(),
        1,
        "child process did not terminate with exit status code 1"
    );

    match transmit_rx.recv() {
        Err(MultiplexError::Disconnected) => {},
        result => panic!("unexpected result {:?}", result),
    }
}

/// Test router behaviour when a SubSender is sent to a
/// process which terminates before the SubSender has been
/// received.
#[test]
#[ignore = "https://github.com/glyn/ipc-channel-mux/issues/2"]
fn router_subsender_drop_inflight() {
    let executable_path: String = env!("CARGO_BIN_EXE_crashing_receiving_process").to_string();

    type ChannelPair = (SubSender<SubSender<bool>>, SubSender<()>);

    let (server, token) =
        SubOneShotServer::<ChannelPair>::new().expect("Failed to create sub one-shot server");

    let mut command = process::Command::new(executable_path);
    let child_process = command.arg(token);

    let mut child = child_process
        .spawn()
        .expect("Failed to start child process");

    let (_rx, (data, control)): (SubReceiver<ChannelPair>, ChannelPair) =
        server.accept().expect("accept failed");

    let channel = RouterProxy::new_router_channel(ROUTER.clone()).unwrap();
    let (transmit_tx, callback_fired_receiver) =
        channel.route_to_new_crossbeam_receiver::<bool>().unwrap();

    data.send(transmit_tx).expect("subsender send failed");

    control.send(()).expect("control send failed");
    let result = child.wait().expect("wait for child process failed");
    assert_eq!(
        result.code().unwrap(),
        1,
        "child process did not terminate with exit status code 1"
    );

    assert!(
        callback_fired_receiver.recv().is_err(),
        "crossbeam receiver did not disconnect"
    );

    // Now shut down the router.
    ROUTER.shutdown();
}
