// Copyright 2026 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
use criterion::{Criterion, criterion_group, criterion_main};
use ipc_channel_mux::mux::{Channel, MuxError};
use std::{thread, time::Duration};

fn uncontended_recv(c: &mut Criterion) {
    let mut group = c.benchmark_group("smaller sample size");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(30));
    group.bench_function("uncontended_recv", |b| {
        b.iter(|| {
            const SENDS: u32 = 50;

            let data = Channel::new().unwrap();
            let (main_tx, main_rx) = data.sub_channel();

            let join_handle = thread::spawn(move || {
                loop {
                    match main_rx.recv() {
                        Ok(_) => {},
                        Err(MuxError::Disconnected) => break 42,
                        result => panic!("unexpected result {:?}", result),
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
}

criterion_group!(benches, uncontended_recv);
criterion_main!(benches);
