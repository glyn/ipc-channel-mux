// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Stress tests for raw ipc-channel (no mux layer).
//!
//! These tests mirror the concurrency patterns used by the mux test suite
//! to determine whether the intermittent STATUS_HEAP_CORRUPTION on Windows
//! originates in ipc-channel itself rather than in the mux layer.
//!
//! Each test exercises a specific ipc-channel API pattern that the mux uses
//! internally. If the heap corruption reproduces here, it confirms the bug
//! is upstream.

use ipc_channel::ipc::{self, IpcBytesReceiver, IpcBytesSender, IpcOneShotServer};
use serde::{Deserialize, Serialize};
use std::thread;
use std::time::Duration;

/// Number of iterations for stress loops. High enough to trigger
/// timing-dependent bugs but low enough for CI.
const ITERATIONS: usize = 500;

// ---------------------------------------------------------------------------
// Pattern 1: Concurrent typed send/recv across threads
//
// Mirrors: bytes_cross_thread, receiving_many_subchannels
// The mux spawns threads that send while the main thread receives.
// ---------------------------------------------------------------------------

#[test]
fn concurrent_typed_send_recv() {
    let (tx, rx) = ipc::channel::<i32>().expect("channel creation failed");

    let handle = thread::spawn(move || {
        for i in 0..ITERATIONS as i32 {
            tx.send(i).expect("send failed");
        }
    });

    for i in 0..ITERATIONS as i32 {
        let val = rx.recv().expect("recv failed");
        assert_eq!(val, i);
    }

    handle.join().expect("sender thread panicked");
}

// ---------------------------------------------------------------------------
// Pattern 2: Concurrent bytes send/recv across threads
//
// Mirrors: bytes_cross_thread
// The mux uses IpcBytesSender/IpcBytesReceiver for raw byte channels.
// ---------------------------------------------------------------------------

#[test]
fn concurrent_bytes_send_recv() {
    let (tx, rx): (IpcBytesSender, IpcBytesReceiver) =
        ipc::bytes_channel().expect("bytes channel creation failed");

    let handle = thread::spawn(move || {
        for i in 0..ITERATIONS {
            let msg = format!("message {i}");
            tx.send(msg.as_bytes()).expect("bytes send failed");
        }
    });

    for i in 0..ITERATIONS {
        let data = rx.recv().expect("bytes recv failed");
        let expected = format!("message {i}");
        assert_eq!(data, expected.as_bytes());
    }

    handle.join().expect("bytes sender thread panicked");
}

// ---------------------------------------------------------------------------
// Pattern 3: Multiple channels with interleaved recv
//
// Mirrors: the mux demuxer receiving from one IPC channel while other
// subchannels (backed by the same or different IPC channels) are active.
// Multiple threads each own a separate IPC channel and recv concurrently.
// ---------------------------------------------------------------------------

#[test]
fn multiple_channels_concurrent_recv() {
    const NUM_CHANNELS: usize = 4;

    let mut handles = Vec::new();

    for _ in 0..NUM_CHANNELS {
        let (tx, rx) = ipc::channel::<String>().expect("channel creation failed");

        let h = thread::spawn(move || {
            for i in 0..ITERATIONS {
                let val = rx.recv().expect("recv failed");
                assert_eq!(val, format!("msg {i}"));
            }
        });

        let s = thread::spawn(move || {
            for i in 0..ITERATIONS {
                tx.send(format!("msg {i}")).expect("send failed");
            }
        });

        handles.push(h);
        handles.push(s);
    }

    for h in handles {
        h.join().expect("thread panicked");
    }
}

// ---------------------------------------------------------------------------
// Pattern 4: try_recv polling loop
//
// Mirrors: SubChannelReceiver::recv which calls try_recv() in a loop,
// falling back to try_recv_timeout() when the local channel is empty.
// This exercises the Windows overlapped I/O cancel/restart path.
// ---------------------------------------------------------------------------

#[test]
fn try_recv_polling_loop() {
    let (tx, rx) = ipc::channel::<i32>().expect("channel creation failed");

    let handle = thread::spawn(move || {
        for i in 0..ITERATIONS as i32 {
            // Small random-ish delay to create timing variation
            if i % 7 == 0 {
                thread::sleep(Duration::from_micros(50));
            }
            tx.send(i).expect("send failed");
        }
    });

    let mut received = 0;
    while received < ITERATIONS as i32 {
        match rx.try_recv() {
            Ok(val) => {
                assert_eq!(val, received);
                received += 1;
            },
            Err(ipc_channel::TryRecvError::Empty) => {
                thread::sleep(Duration::from_micros(100));
            },
            Err(e) => panic!("unexpected try_recv error: {e:?}"),
        }
    }

    handle.join().expect("sender thread panicked");
}

// ---------------------------------------------------------------------------
// Pattern 5: try_recv_timeout with concurrent sender
//
// Mirrors: SubChannelReceiver::recv calling try_recv_timeout(100ms) while
// waiting for messages. This is the most likely path to trigger the
// CancelIoEx race in ipc-channel's Windows named pipe implementation.
// ---------------------------------------------------------------------------

#[test]
fn try_recv_timeout_with_concurrent_sender() {
    let (tx, rx) = ipc::channel::<i32>().expect("channel creation failed");

    let handle = thread::spawn(move || {
        for i in 0..ITERATIONS as i32 {
            // Vary send timing to exercise timeout/cancel paths
            if i % 3 == 0 {
                thread::sleep(Duration::from_micros(200));
            }
            tx.send(i).expect("send failed");
        }
    });

    let mut received = 0i32;
    while received < ITERATIONS as i32 {
        match rx.try_recv_timeout(Duration::from_millis(100)) {
            Ok(val) => {
                assert_eq!(val, received);
                received += 1;
            },
            Err(ipc_channel::TryRecvError::Empty) => {
                // Timeout fired, loop again
            },
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    handle.join().expect("sender thread panicked");
}

// ---------------------------------------------------------------------------
// Pattern 6: Interleaved try_recv and try_recv_timeout on same receiver
//
// Mirrors: SubChannelReceiver::recv which first calls try_recv() on the
// local mpsc, then (if empty) calls try_recv_timeout on the IPC receiver.
// Alternating between the two exercises the overlapped I/O state machine.
// ---------------------------------------------------------------------------

#[test]
fn interleaved_try_recv_and_timeout() {
    let (tx, rx) = ipc::channel::<i32>().expect("channel creation failed");

    let handle = thread::spawn(move || {
        for i in 0..ITERATIONS as i32 {
            if i % 5 == 0 {
                thread::sleep(Duration::from_micros(100));
            }
            tx.send(i).expect("send failed");
        }
    });

    let mut received = 0i32;
    while received < ITERATIONS as i32 {
        // First try non-blocking (like the mux checks local mpsc first)
        match rx.try_recv() {
            Ok(val) => {
                assert_eq!(val, received);
                received += 1;
                continue;
            },
            Err(ipc_channel::TryRecvError::Empty) => {},
            Err(e) => panic!("unexpected try_recv error: {e:?}"),
        }

        // Then try with timeout (like the mux falls back to IPC recv)
        match rx.try_recv_timeout(Duration::from_millis(50)) {
            Ok(val) => {
                assert_eq!(val, received);
                received += 1;
            },
            Err(ipc_channel::TryRecvError::Empty) => {},
            Err(e) => panic!("unexpected try_recv_timeout error: {e:?}"),
        }
    }

    handle.join().expect("sender thread panicked");
}

// ---------------------------------------------------------------------------
// Pattern 7: Rapid channel creation and destruction
//
// Mirrors: creating_many_subchannels and the mux creating a new IPC channel
// pair for each SubOneShotServer connection. Tests handle/resource cleanup.
// ---------------------------------------------------------------------------

#[test]
fn rapid_channel_create_destroy() {
    for i in 0..ITERATIONS {
        let (tx, rx) = ipc::channel::<i32>().expect("channel creation failed");
        tx.send(i as i32).expect("send failed");
        let val = rx.recv().expect("recv failed");
        assert_eq!(val, i as i32);
        // tx and rx dropped here — tests Windows handle cleanup
    }
}

#[test]
fn rapid_bytes_channel_create_destroy() {
    for i in 0..ITERATIONS {
        let (tx, rx) = ipc::bytes_channel().expect("bytes channel creation failed");
        let msg = format!("iter {i}");
        tx.send(msg.as_bytes()).expect("send failed");
        let data = rx.recv().expect("recv failed");
        assert_eq!(data, msg.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Pattern 8: Sender cloning with partial drop
//
// Mirrors: SubSender::clone() — the mux clones senders frequently when
// transmitting them across subchannels. Tests that dropping clones doesn't
// corrupt the channel while others remain active.
// ---------------------------------------------------------------------------

#[test]
fn sender_clone_and_partial_drop() {
    let (tx, rx) = ipc::channel::<i32>().expect("channel creation failed");

    let mut senders = Vec::new();
    for _ in 0..10 {
        senders.push(tx.clone());
    }
    drop(tx);

    // Drop half the clones
    for _ in 0..5 {
        senders.pop();
    }

    // Remaining clones should still work
    for (i, sender) in senders.iter().enumerate() {
        sender.send(i as i32).expect("send on clone failed");
    }

    for i in 0..5 {
        let val = rx.recv().expect("recv failed");
        assert_eq!(val, i as i32);
    }

    // Drop all remaining senders
    drop(senders);

    // Receiver should detect disconnection
    assert!(rx.recv().is_err());
}

// ---------------------------------------------------------------------------
// Pattern 9: Sender transmitted over IPC (sender-of-sender)
//
// Mirrors: embedded_multiplexed_senders — the mux sends SubSenders inside
// messages, which serializes IpcSender via ipc-channel's OpaqueIpcSender
// mechanism (handle duplication on Windows).
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug)]
struct SenderCarrier {
    inner_tx: ipc::IpcSender<String>,
}

#[test]
fn sender_over_ipc() {
    let (outer_tx, outer_rx) =
        ipc::channel::<SenderCarrier>().expect("outer channel creation failed");
    let (inner_tx, inner_rx) = ipc::channel::<String>().expect("inner channel creation failed");

    outer_tx
        .send(SenderCarrier { inner_tx })
        .expect("outer send failed");

    let carrier = outer_rx.recv().expect("outer recv failed");
    carrier
        .inner_tx
        .send("hello from transmitted sender".to_string())
        .expect("inner send failed");

    let msg = inner_rx.recv().expect("inner recv failed");
    assert_eq!(msg, "hello from transmitted sender");
}

// ---------------------------------------------------------------------------
// Pattern 10: Multiple sender-over-IPC with rapid lifecycle
//
// Mirrors: receiving_many_subchannels (10,000 iterations of sending a
// SubSender clone over IPC). Stresses handle duplication and cleanup.
// ---------------------------------------------------------------------------

#[test]
fn many_senders_over_ipc() {
    let (carrier_tx, carrier_rx) =
        ipc::channel::<ipc::IpcSender<i32>>().expect("carrier channel creation failed");
    let (payload_tx, payload_rx) = ipc::channel::<i32>().expect("payload channel creation failed");

    let handle = thread::spawn(move || {
        for _ in 0..ITERATIONS {
            let received_tx = carrier_rx.recv().expect("carrier recv failed");
            received_tx
                .send(42)
                .expect("payload send via received tx failed");
        }
    });

    for _ in 0..ITERATIONS {
        carrier_tx
            .send(payload_tx.clone())
            .expect("carrier send failed");
        let val = payload_rx.recv().expect("payload recv failed");
        assert_eq!(val, 42);
    }

    handle.join().expect("receiver thread panicked");
}

// ---------------------------------------------------------------------------
// Pattern 11: IpcOneShotServer bootstrap
//
// Mirrors: SubOneShotServer which wraps IpcOneShotServer for cross-process
// channel establishment. Tests the server accept path.
// ---------------------------------------------------------------------------

#[test]
fn one_shot_server_bootstrap() {
    for _ in 0..50 {
        let (server, token) =
            IpcOneShotServer::<String>::new().expect("one-shot server creation failed");

        let handle = thread::spawn(move || {
            let tx = ipc::IpcSender::connect(token).expect("connect failed");
            tx.send("bootstrap message".to_string())
                .expect("send failed");
        });

        let (_rx, msg) = server.accept().expect("accept failed");
        assert_eq!(msg, "bootstrap message");

        handle.join().expect("client thread panicked");
    }
}

// ---------------------------------------------------------------------------
// Pattern 12: Receiver drop while sender active (disconnect detection)
//
// Mirrors: multiplex_drop_only_subreceiver_for_channel — the mux drops
// receivers and expects senders to detect disconnection.
// ---------------------------------------------------------------------------

#[test]
fn receiver_drop_sender_detects_disconnect() {
    for _ in 0..ITERATIONS {
        let (tx, rx) = ipc::channel::<i32>().expect("channel creation failed");
        drop(rx);
        // Sender should eventually detect disconnect
        // (may succeed for a few sends due to buffering)
        let mut detected = false;
        for _ in 0..100 {
            if tx.send(1).is_err() {
                detected = true;
                break;
            }
        }
        assert!(detected, "sender never detected receiver disconnect");
    }
}

// ---------------------------------------------------------------------------
// Pattern 13: Mixed typed and bytes channels active simultaneously
//
// Mirrors: bytes_alongside_typed_subchannel — the mux can have both typed
// and bytes subchannels active on the same underlying IPC channel.
// Here we just ensure both channel types work concurrently.
// ---------------------------------------------------------------------------

#[test]
fn mixed_typed_and_bytes_concurrent() {
    let (typed_tx, typed_rx) = ipc::channel::<String>().expect("typed channel failed");
    let (bytes_tx, bytes_rx) = ipc::bytes_channel().expect("bytes channel failed");

    let typed_handle = thread::spawn(move || {
        for i in 0..ITERATIONS {
            typed_tx
                .send(format!("typed {i}"))
                .expect("typed send failed");
        }
    });

    let bytes_handle = thread::spawn(move || {
        for i in 0..ITERATIONS {
            let msg = format!("bytes {i}");
            bytes_tx.send(msg.as_bytes()).expect("bytes send failed");
        }
    });

    let typed_recv_handle = thread::spawn(move || {
        for i in 0..ITERATIONS {
            let val = typed_rx.recv().expect("typed recv failed");
            assert_eq!(val, format!("typed {i}"));
        }
    });

    let bytes_recv_handle = thread::spawn(move || {
        for i in 0..ITERATIONS {
            let data = bytes_rx.recv().expect("bytes recv failed");
            assert_eq!(data, format!("bytes {i}").as_bytes());
        }
    });

    typed_handle.join().expect("typed sender panicked");
    bytes_handle.join().expect("bytes sender panicked");
    typed_recv_handle.join().expect("typed receiver panicked");
    bytes_recv_handle.join().expect("bytes receiver panicked");
}

// ---------------------------------------------------------------------------
// Pattern 14: High-throughput send/recv stress
//
// Mirrors: the mux's overall load — many messages flowing through a single
// IPC channel concurrently. Uses larger iteration count and multiple sender
// threads to maximise contention on the named pipe.
// ---------------------------------------------------------------------------

#[test]
fn high_throughput_multi_sender() {
    const NUM_SENDERS: usize = 4;
    const MSGS_PER_SENDER: usize = ITERATIONS;

    let (tx, rx) = ipc::channel::<(usize, usize)>().expect("channel creation failed");

    let mut sender_handles = Vec::new();
    for sender_id in 0..NUM_SENDERS {
        let tx_clone = tx.clone();
        sender_handles.push(thread::spawn(move || {
            for msg_id in 0..MSGS_PER_SENDER {
                tx_clone.send((sender_id, msg_id)).expect("send failed");
            }
        }));
    }
    drop(tx);

    let recv_handle = thread::spawn(move || {
        let mut counts = vec![0usize; NUM_SENDERS];
        for _ in 0..(NUM_SENDERS * MSGS_PER_SENDER) {
            let (sender_id, _msg_id) = rx.recv().expect("recv failed");
            counts[sender_id] += 1;
        }
        for (sender_id, count) in counts.iter().enumerate() {
            assert_eq!(
                *count, MSGS_PER_SENDER,
                "sender {sender_id} sent {MSGS_PER_SENDER} but received {count}"
            );
        }
    });

    for h in sender_handles {
        h.join().expect("sender thread panicked");
    }
    recv_handle.join().expect("receiver thread panicked");
}

// ---------------------------------------------------------------------------
// Pattern 15: try_recv_timeout with sender drop (disconnect during wait)
//
// Mirrors: SubChannelReceiver::recv detecting disconnection via
// try_recv_timeout returning after the sender is dropped.
// ---------------------------------------------------------------------------

#[test]
fn try_recv_timeout_disconnect_during_wait() {
    for _ in 0..50 {
        let (tx, rx) = ipc::channel::<i32>().expect("channel creation failed");

        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            drop(tx);
        });

        // Should eventually get an error (IpcError) after sender drops
        loop {
            match rx.try_recv_timeout(Duration::from_millis(100)) {
                Ok(_) => panic!("should not receive a message"),
                Err(ipc_channel::TryRecvError::Empty) => continue,
                Err(ipc_channel::TryRecvError::IpcError(_)) => break,
            }
        }

        handle.join().expect("dropper thread panicked");
    }
}
