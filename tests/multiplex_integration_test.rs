// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[cfg(not(any(feature = "force-inprocess", target_os = "android", target_os = "ios")))]
use ipc_channel::multiplex;
#[cfg(not(any(feature = "force-inprocess", target_os = "android", target_os = "ios")))]
use std::{env, process};

// The integration tests may be run on their own by issuing:
// cargo test --test '*'

/// Test multiplexing channels.
#[cfg(not(any(feature = "force-inprocess", target_os = "android", target_os = "ios")))]
#[test]
fn multiplexing() {
    let (mut multi_sender, mut multi_receiver) = multiplex::multi_channel().unwrap();
    let sub_sender = multi_sender.new(&mut multi_receiver);
    sub_sender.send(45 as u8).unwrap();
    let scid = sub_sender.sub_channel_id();

    let mut sub_receiver = multi_receiver.attach(scid);
    let data: u8 = sub_receiver.recv().unwrap();
    assert_eq!(data, 45);
    
    let sub_sender2 = multi_sender.new(&mut multi_receiver);
    sub_sender2.send("bananas".to_string()).unwrap();
    let scid2 = sub_sender2.sub_channel_id();

    let mut sub_receiver2 = multi_receiver.attach(scid2);
    let data2: String = sub_receiver2.recv().unwrap();
    assert_eq!(data2, "bananas");
}

/// Test spawning a process which then acts as a client to a
/// one-shot multi server in the parent process.
#[cfg(not(any(feature = "force-inprocess", target_os = "android", target_os = "ios")))]
#[test]
fn spawn_one_shot_multi_server_client() {
    use ipc_channel::multiplex;

    let executable_path: String = env!("CARGO_BIN_EXE_spawn_multi_client_test_helper").to_string();

    let (server, token) = multiplex::OneShotMultiServer::new().expect("Failed to create one-shot multi server.");

    let mut command = process::Command::new(executable_path);
    let child_process = command.arg(token);

    let mut child = child_process
        .spawn()
        .expect("Failed to start child process");

    let mut multi_receiver = server.accept().expect("accept failed");
    multi_receiver.receive().expect("receive failed");
    let (subchannel_id, name) = multi_receiver.receive_sub_channel().expect("receive sub channel failed");
    assert_eq!(name, "test subchannel");
    let mut sub_receiver = multi_receiver.attach(subchannel_id);
    let data: String = sub_receiver.recv().unwrap();
    assert_eq!(data, "test message");

    let result = child.wait().expect("wait for child process failed");
    assert!(
        result.success(),
        "child process failed with exit status code {}",
        result.code().expect("exit status code not available")
    );
}
