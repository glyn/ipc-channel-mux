# Migration guide from ipc-channel to ipc-channel-mux

This guide shows how to replace each `ipc-channel` API with its `ipc-channel-mux` equivalent.

## Quick reference

| ipc-channel | ipc-channel-mux | Notes |
|---|---|---|
| `ipc::channel::<T>()` | `mux::Channel::new()?.sub_channel::<T>()` | Two steps; reuse `Channel` for multiplexing |
| `IpcSender<T>` | `SubSender<T>` | |
| `IpcReceiver<T>` | `SubReceiver<T>` | Cannot be sent over subchannels |
| `IpcSender::connect()` | `SubSender::connect()` | |
| `IpcSender::send()` | `SubSender::send()` | |
| `IpcSender::to_opaque()` | `SubSender::to_opaque()` | |
| `IpcReceiver::recv()` | `SubReceiver::recv()` | |
| `IpcReceiver::try_recv()` | `SubReceiver::try_recv()` | |
| `IpcReceiver::try_recv_timeout()` | `SubReceiver::try_recv_timeout()` | |
| `IpcReceiver::to_opaque()` | `SubReceiver::to_opaque()` | |
| `OpaqueIpcSender` | `OpaqueSubSender` | |
| `OpaqueIpcReceiver` | `OpaqueSubReceiver` | Not serializable |
| `IpcOneShotServer<T>` | `SubOneShotServer<T>` | |
| `IpcSharedMemory` | `SharedMemory` | |
| `IpcReceiverSet` | _No equivalent_ | Use `subchannel_router` instead |
| `ipc::bytes_channel()` | _No equivalent_ | Use `sub_channel::<Vec<u8>>()` instead |
| `IpcBytesSender` | _No equivalent_ | Use `SubSender<Vec<u8>>` instead |
| `IpcBytesReceiver` | _No equivalent_ | Use `SubReceiver<Vec<u8>>` instead |
| `IpcError` | `MuxError` | Different variant structure |
| `ipc::TryRecvError` | `mux::TryRecvError` | Different variant structure |
| `router::ROUTER` | `subchannel_router::ROUTER` | Different method signatures |
| `router::RouterProxy` | `subchannel_router::RouterProxy` | Different method signatures |

## Channel creation

Creating a subchannel requires a multiplexing `Channel` to be created first.
Reusing the same `Channel` for multiple subchannels is what enables multiplexing.

**Before:**

~~~Rust
use ipc_channel::ipc;

let (tx, rx) = ipc::channel::<String>()?;
~~~

**After:**

~~~Rust
use ipc_channel_mux::mux;

let channel = mux::Channel::new()?;
let (tx, rx) = channel.sub_channel::<String>();
~~~

Key differences:

- `Channel::new()` can fail (it creates the underlying IPC channel). `sub_channel()` never fails.
- Create additional subchannels from the same `Channel` to benefit from multiplexing:

~~~Rust
let (tx1, rx1) = channel.sub_channel::<String>();
let (tx2, rx2) = channel.sub_channel::<i32>();
// tx1/rx1 and tx2/rx2 share the same underlying IPC channel
~~~

## Sending and receiving

The `send`, `recv`, `try_recv`, and `try_recv_timeout` methods have the same signatures and semantics.

**Before:**

~~~Rust
tx.send("hello".to_string())?;
let msg = rx.recv()?;
~~~

**After:**

~~~Rust
tx.send("hello".to_string())?;
let msg = rx.recv()?;
~~~

## One-shot servers

The API for bootstrapping channels between processes is structurally identical.

**Before:**

~~~Rust
use ipc_channel::ipc;

let (server, name) = ipc::IpcOneShotServer::<String>::new()?;

// In another process:
let tx = ipc::IpcSender::connect(name)?;
tx.send("hello".to_string())?;

// Back in the server process:
let (rx, first_msg) = server.accept()?;
~~~

**After:**

~~~Rust
use ipc_channel_mux::mux;

let (server, name) = mux::SubOneShotServer::<String>::new()?;

// In another process:
let tx = mux::SubSender::connect(name)?;
tx.send("hello".to_string())?;

// Back in the server process:
let (rx, first_msg) = server.accept()?;
~~~

## Opaque (type-erased) senders and receivers

**Before:**

~~~Rust
let opaque_tx: ipc::OpaqueIpcSender = tx.to_opaque();
let tx: ipc::IpcSender<String> = opaque_tx.to();

let opaque_rx: ipc::OpaqueIpcReceiver = rx.to_opaque();
let rx: ipc::IpcReceiver<String> = opaque_rx.to();
~~~

**After:**

~~~Rust
let opaque_tx: mux::OpaqueSubSender = tx.to_opaque();
let tx: mux::SubSender<String> = opaque_tx.to();

let opaque_rx: mux::OpaqueSubReceiver = rx.to_opaque();
let rx: mux::SubReceiver<String> = opaque_rx.to();
~~~

Key difference: `OpaqueIpcReceiver` implements `Serialize` and `Deserialize`, but `OpaqueSubReceiver` does not (subreceivers cannot be sent over subchannels).

## Shared memory

**Before:**

~~~Rust
use ipc_channel::ipc;

let (tx, rx) = ipc::channel::<ipc::IpcSharedMemory>()?;

let shmem = ipc::IpcSharedMemory::from_bytes(b"hello");
tx.send(shmem)?;

let received = rx.recv()?;
assert_eq!(&*received, b"hello");
~~~

**After:**

~~~Rust
use ipc_channel_mux::mux;

let channel = mux::Channel::new()?;
let (tx, rx) = channel.sub_channel::<mux::SharedMemory>();

let shmem = mux::SharedMemory::from_bytes(b"hello");
tx.send(shmem)?;

let received = rx.recv()?;
assert_eq!(&*received, b"hello");
~~~

Both types support `from_bytes`, `from_byte`, `deref_mut` (unsafe), `take`, and `Deref<Target=[u8]>`.

`SharedMemory` also implements `From<IpcSharedMemory>` and `Into<IpcSharedMemory>` for conversion between the two types.

## Error handling

`ipc-channel` errors map to `ipc-channel-mux` errors as follows:

| ipc-channel | ipc-channel-mux |
|---|---|
| `IpcError::Disconnected` | `MuxError::Disconnected` |
| `IpcError::SerializationError(_)` | `MuxError::IpcError(IpcError::SerializationError(_))` |
| `IpcError::Io(_)` | `MuxError::IpcError(IpcError::Io(_))` |
| — | `MuxError::InternalError(_)` (new) |
| `TryRecvError::IpcError(_)` | `TryRecvError::MuxError(_)` |
| `TryRecvError::Empty` | `TryRecvError::Empty` |

**Before:**

~~~Rust
match rx.try_recv() {
    Ok(msg) => { /* use msg */ }
    Err(ipc::TryRecvError::Empty) => { /* no message yet */ }
    Err(ipc::TryRecvError::IpcError(ipc::IpcError::Disconnected)) => { /* disconnected */ }
    Err(e) => { /* other error */ }
}
~~~

**After:**

~~~Rust
match rx.try_recv() {
    Ok(msg) => { /* use msg */ }
    Err(mux::TryRecvError::Empty) => { /* no message yet */ }
    Err(mux::TryRecvError::MuxError(mux::MuxError::Disconnected)) => { /* disconnected */ }
    Err(e) => { /* other error */ }
}
~~~

## Sending senders over channels

Both APIs support sending senders over channels.

**Before:**

~~~Rust
let (inner_tx, inner_rx) = ipc::channel::<i32>()?;
let (outer_tx, outer_rx) = ipc::channel::<ipc::IpcSender<i32>>()?;

outer_tx.send(inner_tx)?;
let received_tx = outer_rx.recv()?;
~~~

**After:**

~~~Rust
let channel = mux::Channel::new()?;
let (inner_tx, inner_rx) = channel.sub_channel::<i32>();
let (outer_tx, outer_rx) = channel.sub_channel::<mux::SubSender<i32>>();

outer_tx.send(inner_tx)?;
let received_tx = outer_rx.recv()?;
~~~

Key difference: subreceivers cannot be sent over subchannels (this would break other subreceivers sharing the underlying IPC channel).

## Router

The router APIs have different structures. In `ipc-channel`, routes are added for existing receivers. In `ipc-channel-mux`, a `RouterChannel` creates new subchannels that are automatically routed.

**Before:**

~~~Rust
use ipc_channel::ipc;
use ipc_channel::router::ROUTER;

let (tx, rx) = ipc::channel::<i32>()?;
let crossbeam_rx = ROUTER.route_ipc_receiver_to_new_crossbeam_receiver(rx);

tx.send(42)?;
assert_eq!(crossbeam_rx.recv().unwrap(), 42);
~~~

**After:**

~~~Rust
use ipc_channel_mux::mux;
use mux::subchannel_router::{ROUTER, RouterProxy};

let router_channel = RouterProxy::new_router_channel(&ROUTER)?;
let (tx, crossbeam_rx) = router_channel.route_to_new_crossbeam_receiver::<i32>()?;

tx.send(42)?;
assert_eq!(crossbeam_rx.recv().unwrap(), 42);
~~~

### Router method mapping

| ipc-channel `ROUTER` method | ipc-channel-mux `RouterChannel` method |
|---|---|
| `route_ipc_receiver_to_new_crossbeam_receiver(rx)` | `route_to_new_crossbeam_receiver::<T>()` |
| `route_ipc_receiver_to_crossbeam_sender(rx, sender)` | `route_to_crossbeam_sender::<T>(sender)` |
| `add_typed_route(rx, callback)` | `add_typed_route::<T>(callback)` |
| `add_typed_one_shot_route(rx, callback)` | _No equivalent_ |

Key differences:

- `RouterChannel` methods create the subchannel internally and return the `SubSender<T>`. In `ipc-channel`, you pass an existing `IpcReceiver<T>`.
- Create `RouterChannel` via `RouterProxy::new_router_channel(&ROUTER)`.
- The callback type is `Box<dyn FnMut(Result<T, MuxError>) + Send>` (vs `Box<dyn Fn(Result<T, SerDeError>) + Send + 'static>` for `ipc-channel`'s multi handler).

## APIs with no equivalent

### `IpcReceiverSet`

`ipc-channel-mux` does not provide a receiver set. Use `subchannel_router` to route subreceivers to crossbeam channels, then use crossbeam's `select!` macro for multi-channel waiting.

**Before:**

~~~Rust
use ipc_channel::ipc;

let (tx1, rx1) = ipc::channel::<i32>()?;
let (tx2, rx2) = ipc::channel::<String>()?;

let mut set = ipc::IpcReceiverSet::new()?;
let id1 = set.add(rx1)?;
let id2 = set.add(rx2)?;

for result in set.select()? {
    match result {
        ipc::IpcSelectionResult::MessageReceived(id, msg) if id == id1 => {
            let val: i32 = msg.to().unwrap();
        }
        ipc::IpcSelectionResult::MessageReceived(id, msg) if id == id2 => {
            let val: String = msg.to().unwrap();
        }
        _ => {}
    }
}
~~~

**After:**

~~~Rust
use ipc_channel_mux::mux;
use mux::subchannel_router::{ROUTER, RouterProxy};

let router_channel = RouterProxy::new_router_channel(&ROUTER)?;
let (tx1, cb_rx1) = router_channel.route_to_new_crossbeam_receiver::<i32>()?;
let (tx2, cb_rx2) = router_channel.route_to_new_crossbeam_receiver::<String>()?;

crossbeam_channel::select! {
    recv(cb_rx1) -> result => {
        let val: i32 = result.unwrap();
    }
    recv(cb_rx2) -> result => {
        let val: String = result.unwrap();
    }
}
~~~

### `bytes_channel()`

There is no raw bytes channel. Use a typed subchannel with `Vec<u8>` instead. This incurs serialization overhead.

**Before:**

~~~Rust
let (tx, rx) = ipc::bytes_channel()?;
tx.send(b"hello")?;
let data: Vec<u8> = rx.recv()?;
~~~

**After:**

~~~Rust
let channel = mux::Channel::new()?;
let (tx, rx) = channel.sub_channel::<Vec<u8>>();
tx.send(b"hello".to_vec())?;
let data: Vec<u8> = rx.recv()?;
~~~

### `IpcReceiver` serialization

`IpcReceiver<T>` implements `Serialize` and `Deserialize`, allowing receivers to be sent over IPC channels. `SubReceiver<T>` does not support this because sending a subreceiver would require sending the underlying IPC receiver, which would break other subreceivers sharing that IPC channel.

### `add_typed_one_shot_route`

`ipc-channel`'s router supports one-shot routes (callbacks invoked once then removed). `ipc-channel-mux`'s router does not have a direct equivalent. Use `add_typed_route` with a callback that handles a single message and ignores subsequent ones, or use `route_to_new_crossbeam_receiver` and call `recv()` once.

### Async/futures support

`ipc-channel`'s `IpcReceiver::to_stream()` (behind the `"async"` feature flag) converts a receiver into a `futures::Stream`. `ipc-channel-mux` does not currently provide async support.
