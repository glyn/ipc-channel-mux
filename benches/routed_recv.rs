// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
use criterion::{Criterion, criterion_group, criterion_main};
use ipc_channel_mux::mux::{Channel, MuxError, subchannel_router::ROUTER};
use std::{thread, time::Duration};

fn routed_recv(c: &mut Criterion) {
    let mut group = c.benchmark_group("smaller sample size");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(30));
    let router_proxy = ipc_channel_mux::mux::subchannel_router::RouterProxy::new();
    let data =
        ipc_channel_mux::mux::subchannel_router::RouterProxy::new_router_channel(router_proxy)
            .unwrap();
    group.bench_function("routed_recv", |b| {
        b.iter(|| {
            const SENDS: u32 = 50;

            let (main_tx, main_rx) = data.route_to_new_crossbeam_receiver().unwrap();

            let join_handle = thread::spawn(move || {
                loop {
                    match main_rx.recv() {
                        Ok(_) => {},
                        _ => break 42,
                    }
                }
            });

            for _ in 0..SENDS {
                main_tx.send(()).unwrap();
            }

            drop(main_tx);
            assert_eq!(join_handle.join().unwrap(), 42);
        })
    });
    group.finish();
    ROUTER.shutdown();
}

criterion_group!(benches, routed_recv);
criterion_main!(benches);
