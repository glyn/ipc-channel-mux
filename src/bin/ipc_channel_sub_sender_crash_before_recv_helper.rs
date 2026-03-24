// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Child-process helper for the `ipc_channel_sub_sender_cross_process_crash_before_recv`
//! integration test.
//!
//! Arguments: `<bootstrap_token>`
//!
//! Protocol:
//! 1. Connect to the parent's `IpcOneShotServer` (token passed as arg).
//! 2. Create a raw IPC channel for the transport and a second channel for the
//!    exit signal, then send both senders to the parent.
//! 3. Wait for the parent's exit signal (confirming that the transport has been sent).
//! 4. Exit immediately WITHOUT reading from the transport channel.

use ipc_channel::ipc::{self, IpcSender};
use ipc_channel_mux::mux::IpcChannelSubSender;
use std::{env, process};

fn main() {
    let args: Vec<String> = env::args().collect();
    let bootstrap_token = args.get(1).expect("missing bootstrap token argument");

    // Channel over which the parent will deliver the IpcChannelSubSender (intentionally never read).
    let (transport_tx, _transport_rx) =
        ipc::channel::<IpcChannelSubSender<u32>>().expect("channel creation failed");

    // Channel over which the parent signals us to exit.
    let (exit_tx, exit_rx) = ipc::channel::<()>().expect("channel creation failed");

    // Hand both senders to the parent.
    let bootstrap_sender: IpcSender<(IpcSender<IpcChannelSubSender<u32>>, IpcSender<()>)> =
        IpcSender::connect(bootstrap_token.clone()).expect("connect to bootstrap server failed");
    bootstrap_sender
        .send((transport_tx, exit_tx))
        .expect("send of bootstrap message failed");

    // Wait for the parent to confirm it has sent the transport, then exit without reading it.
    exit_rx.recv().expect("recv of exit signal failed");
    process::exit(1);
}
