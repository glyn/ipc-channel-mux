// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use ipc_channel::multiplex::MultiSender;
use std::{env, process};

/// Test executable which connects to the one-shot server with name
/// passed in as an argument and then sends a test message to the
/// server.
fn main() {
    let args: Vec<String> = env::args().collect();
    let token = args.get(1).expect("missing argument");

    let mut multi_sender: MultiSender = MultiSender::connect(token.to_string()).expect("connect failed");
    let sub_channel_sender = multi_sender.new2();
    multi_sender.notify_sub_channel(sub_channel_sender.sub_channel_id(), "test subchannel".to_string()).expect("notify failed");
    sub_channel_sender.send("test message".to_string()).expect("send failed");

    process::exit(0);
}
