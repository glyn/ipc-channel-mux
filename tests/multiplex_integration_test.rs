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
    // let (multi_sender, multi_receiver) = multiplex::multi_channel().unwrap();
    // let sub_sender = multi_sender.new();

    use ipc_channel::multiplex;
    let channel = multiplex::Channel::new().unwrap();
    let (sub_sender, sub_receiver) = channel.sub_channel().unwrap();
    sub_sender.send(45 as u8).unwrap();

    let data: u8 = sub_receiver.recv().unwrap();
    assert_eq!(data, 45);

    let (sub_sender2, sub_receiver2) = channel.sub_channel().unwrap();
    sub_sender2.send("bananas".to_string()).unwrap();

    let data2: String = sub_receiver2.recv().unwrap();
    assert_eq!(data2, "bananas");
}

/// Test spawning a process which then acts as a client to a
/// one-shot multi server in the parent process.
#[cfg(not(any(feature = "force-inprocess", target_os = "android", target_os = "ios")))]
#[test]
fn spawn_sub_one_shot_server_client() {
    let executable_path: String = env!("CARGO_BIN_EXE_spawn_multi_client_test_helper").to_string();

    let (server, token) =
        multiplex::SubOneShotServer::<String>::new().expect("Failed to create sub one-shot server");

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
