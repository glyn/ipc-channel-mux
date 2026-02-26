// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
use criterion::{Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use ipc_channel::{IpcError, ipc};
use ipc_channel_mux::mux::{Channel, MuxError};
use std::thread;

fn recv_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("recv_throughput");
    group.throughput(Throughput::Elements(50u64));
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("ipc_channel_recv", |b| {
        b.iter(|| {
            const SENDS: u32 = 50;

            let (main_tx, main_rx) = ipc::channel().unwrap();

            let join_handle = thread::spawn(move || {
                loop {
                    match main_rx.recv() {
                        Ok(()) => {},
                        Err(IpcError::Disconnected) => break 42,
                        result => panic!("unexpected result {result:?}"),
                    }
                }
            });

            for _ in 0..SENDS {
                main_tx.send(()).unwrap();
            }

            drop(main_tx);
            assert_eq!(join_handle.join().unwrap(), 42);
        });
    });
    group.bench_function("uncontended_subchannel_recv", |b| {
        b.iter(|| {
            const SENDS: u32 = 50;

            let data = Channel::new().unwrap();
            let (main_tx, main_rx) = data.sub_channel();

            let join_handle = thread::spawn(move || {
                loop {
                    match main_rx.recv() {
                        Ok(()) => {},
                        Err(MuxError::Disconnected) => break 42,
                        result => panic!("unexpected result {result:?}"),
                    }
                }
            });

            for _ in 0..SENDS {
                main_tx.send(()).unwrap();
            }

            drop(main_tx);
            assert_eq!(join_handle.join().unwrap(), 42);
        });
    });
    group.bench_function("contended_subchannel_recv", |b| {
        b.iter(|| {
            const SENDS: u32 = 50;

            let data = Channel::new().unwrap();
            let (main_tx, main_rx) = data.sub_channel();
            let (other_tx, other_rx) = data.sub_channel::<()>();

            let join_handle = thread::spawn(move || match other_rx.recv() {
                Err(MuxError::Disconnected) => 42,
                result => panic!("unexpected result {result:?}"),
            });

            let join_handle2 = thread::spawn(move || {
                loop {
                    match main_rx.recv() {
                        Ok(()) => {},
                        Err(MuxError::Disconnected) => break 42,
                        result => panic!("unexpected result {result:?}"),
                    }
                }
            });

            for _ in 0..SENDS {
                main_tx.send(()).unwrap();
            }

            drop(main_tx);
            drop(other_tx);
            assert_eq!(join_handle.join().unwrap(), 42);
            assert_eq!(join_handle2.join().unwrap(), 42);
        });
    });
    group.finish();
}

criterion_group!(benches, recv_throughput);
criterion_main!(benches);
