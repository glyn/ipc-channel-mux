// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
use criterion::{Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use ipc_channel::ipc;
use ipc_channel_mux::mux::subchannel_router::ROUTER;
use std::{thread, time::Duration};

fn router_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("router_throughput");
    group.throughput(Throughput::Elements(50u64));
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(40));
    let router_proxy = ipc_channel_mux::mux::subchannel_router::RouterProxy::new();
    let data =
        ipc_channel_mux::mux::subchannel_router::RouterProxy::new_router_channel(&router_proxy)
            .unwrap();
    group.bench_function("ipc_channel_router", |b| {
        b.iter(|| {
            const SENDS: u32 = 50;

            let (main_tx, rx) = ipc::channel().unwrap();

            let main_rx =
                ipc_channel::router::ROUTER.route_ipc_receiver_to_new_crossbeam_receiver(rx);

            let join_handle = thread::spawn(move || {
                loop {
                    match main_rx.recv() {
                        Ok(()) => {},
                        _ => break 42,
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

    group.bench_function("subchannel_router", |b| {
        b.iter(|| {
            const SENDS: u32 = 50;

            let (main_tx, main_rx) = data.route_to_new_crossbeam_receiver().unwrap();

            let join_handle = thread::spawn(move || {
                loop {
                    match main_rx.recv() {
                        Ok(()) => {},
                        _ => break 42,
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
    group.finish();
    ROUTER.shutdown();
}

criterion_group!(benches, router_throughput);
criterion_main!(benches);
