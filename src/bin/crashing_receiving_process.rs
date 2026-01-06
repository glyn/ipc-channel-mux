// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use ipc_channel_mux::mux::{Channel, SubSender};
use std::{env, process};

/// Test executable which connects to the one-shot server with name
/// passed in as an argument and then sends data and control subchannel
/// senders to the client. The executable then receives on the control
/// subchannel and on return, exits the process.
///
/// The purpose is to test sending a subsender to a process which crashes
/// before the subsender can be received.
///
/// The data and control subchannels must use distinct underlying IPC
/// channels so that receiving on the control subchannel does not cause
/// messages on the data subchannel to be received.
fn main() {
    let token = env::args()
        .collect::<Vec<String>>()
        .get(1)
        .expect("missing argument")
        .clone();
    let tx: SubSender<(SubSender<SubSender<bool>>, SubSender<()>)> =
        SubSender::connect(token.to_string()).expect("connect failed");

    let d = Channel::new().unwrap();
    let (data_tx, _data_rx) = d.sub_channel();

    let c = Channel::new().unwrap();
    let (control_tx, control_rx) = c.sub_channel();

    tx.send((data_tx, control_tx)).expect("send failed");

    control_rx.recv().expect("receive failed");

    process::exit(1);
}
