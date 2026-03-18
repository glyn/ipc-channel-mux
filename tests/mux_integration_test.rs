// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use ipc_channel::ipc::{IpcOneShotServer, IpcSender as RawIpcSender};
use ipc_channel_mux::mux;
use std::{env, process};

// The integration tests may be run on their own by issuing:
// cargo test --test '*'

/// Test multiplexing channels.
#[test]
fn muxing() {
    // let (multi_sender, multi_receiver) = mux::multi_channel().unwrap();
    // let sub_sender = multi_sender.new();

    use ipc_channel_mux::mux;
    let channel = mux::Channel::new().unwrap();
    let (sub_sender, sub_receiver) = channel.sub_channel();
    sub_sender.send(45_u8).unwrap();

    let data: u8 = sub_receiver.recv().unwrap();
    assert_eq!(data, 45);

    let (sub_sender2, sub_receiver2) = channel.sub_channel();
    sub_sender2.send("bananas".to_string()).unwrap();

    let data2: String = sub_receiver2.recv().unwrap();
    assert_eq!(data2, "bananas");
}

/// Test spawning a process which then acts as a client to a
/// one-shot multi server in the parent process.
#[test]
fn spawn_sub_one_shot_server_client() {
    let executable_path: String = env!("CARGO_BIN_EXE_spawn_multi_client_test_helper").to_string();

    let (server, token) =
        mux::SubOneShotServer::<String>::new().expect("Failed to create sub one-shot server");

    let mut command = process::Command::new(executable_path);
    let child_process = command.arg(token);

    let mut child = child_process
        .spawn()
        .expect("Failed to start child process");

    let (_rx, msg) = server.accept().expect("accept failed");
    assert_eq!("test message", msg);

    let result = child.wait().expect("wait for child process failed");
    assert!(
        result.success(),
        "child process failed with exit status code {}",
        result.code().expect("exit status code not available")
    );
}

/// Test that `IpcChannelSubSender` correctly transports a `SubSender` across a
/// real process boundary.
///
/// The parent wraps its `SubSender<u32>` in an `IpcChannelSubSender` and
/// delivers it to a child process over a raw IPC channel. The child
/// reconstructs the `SubSender` and sends the value `42`. The parent asserts
/// it receives `42` on the original `SubReceiver`.
#[test]
fn ipc_channel_sub_sender_cross_process() {
    let executable_path: String =
        env!("CARGO_BIN_EXE_ipc_channel_sub_sender_cross_process_helper").to_string();

    // Create the mux channel and wrap the sender for transport.
    let channel = mux::Channel::new().unwrap();
    let (sub_tx, sub_rx) = channel.sub_channel::<u32>();
    let transport = mux::IpcChannelSubSender::from(sub_tx);

    // Create a one-shot server so the child can hand us an IpcSender endpoint.
    // The child will send us an IpcSender<IpcChannelSubSender<u32>> which we
    // then use to deliver the transport.
    let (bootstrap_server, bootstrap_token) =
        IpcOneShotServer::<RawIpcSender<mux::IpcChannelSubSender<u32>>>::new()
            .expect("Failed to create bootstrap server");

    let mut child = process::Command::new(executable_path)
        .arg(bootstrap_token)
        .spawn()
        .expect("Failed to start child process");

    // Accept the child's sender and deliver the IpcChannelSubSender to it.
    let (_, child_sender) = bootstrap_server.accept().expect("bootstrap accept failed");
    child_sender
        .send(transport)
        .expect("send of IpcChannelSubSender failed");

    // The child reconstructs the SubSender and sends 42.
    assert_eq!(sub_rx.recv().unwrap(), 42);

    let result = child.wait().expect("wait for child process failed");
    assert!(
        result.success(),
        "child process failed with exit status code {}",
        result.code().expect("exit status code not available")
    );
}

/// Test behaviour when a SubSender is sent to a process which
/// terminates before the "sending" message has been received.
#[test]
fn subsender_drop_inflight_early() {
    type ChannelPair = (mux::SubSender<mux::SubSender<bool>>, mux::SubSender<()>);

    let executable_path: String = env!("CARGO_BIN_EXE_crashing_receiving_process").to_string();

    let (server, token) =
        mux::SubOneShotServer::<ChannelPair>::new().expect("Failed to create sub one-shot server");

    let mut command = process::Command::new(executable_path);
    let child_process = command.arg(token);

    let mut child = child_process
        .spawn()
        .expect("Failed to start child process");

    let (_rx, (data, control)): (mux::SubReceiver<ChannelPair>, ChannelPair) =
        server.accept().expect("accept failed");

    let d = mux::Channel::new().unwrap();
    let (transmit_tx, transmit_rx) = d.sub_channel();

    data.send(transmit_tx).expect("subsender send failed");
    control.send(()).expect("control send failed");
    let result = child.wait().expect("wait for child process failed");
    assert_eq!(
        result.code().unwrap(),
        1,
        "child process did not terminate with exit status code 1"
    );

    match transmit_rx.recv() {
        Err(mux::MuxError::Disconnected) => {},
        result => panic!("unexpected result {result:?}"),
    }
}
