// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Child-process helper for the `ipc_channel_sub_sender_cross_process_crash`
//! integration test.
//!
//! Arguments: `<bootstrap_token>`
//!
//! Protocol:
//! 1. Connect to the parent's `IpcOneShotServer` (token passed as arg).
//! 2. Create a raw IPC channel and send the `IpcSender` end to the parent
//!    so that the parent can deliver an `IpcChannelSubSender<u32>` back.
//! 3. Receive the `IpcChannelSubSender<u32>` on our `IpcReceiver` end.
//! 4. Exit immediately WITHOUT calling `into_sub_sender`.

use ipc_channel::ipc::{self, IpcSender};
use ipc_channel_mux::mux::IpcChannelSubSender;
use std::{env, process};

fn main() {
    let args: Vec<String> = env::args().collect();
    let bootstrap_token = args.get(1).expect("missing bootstrap token argument");

    // Create the channel over which the parent will send us the transport.
    let (child_tx, child_rx) =
        ipc::channel::<IpcChannelSubSender<u32>>().expect("channel creation failed");

    // Hand our sender endpoint to the parent.
    let bootstrap_sender: IpcSender<IpcSender<IpcChannelSubSender<u32>>> =
        IpcSender::connect(bootstrap_token.clone()).expect("connect to bootstrap server failed");
    bootstrap_sender
        .send(child_tx)
        .expect("send of bootstrap sender failed");

    // Receive the IpcChannelSubSender the parent wraps around its SubSender.
    let _transport: IpcChannelSubSender<u32> = child_rx.recv().expect("recv of transport failed");

    // Crash WITHOUT calling into_sub_sender.
    process::exit(1);
}
