<!-- markdownlint-disable MD041 -->
`ipc-channel-mux`[^mux] is a multiplexing, inter-process implementation of Rust channels (which were inspired by CSP[^CSP]).

A Rust channel is a unidirectional, FIFO queue of messages which can be used to send messages between threads in a single operating system process.
For an excellent introduction to Rust channels, see [Using Message Passing to Transfer Data Between Threads](https://doc.rust-lang.org/stable/book/ch16-02-message-passing.html) in the Rust reference.

`ipc-channel-mux` extends Rust channels to support inter-process communication (IPC) in a single operating system instance.
`ipc-channel-mux` multiplexes _subchannels_ over IPC primitives to reduce the consumption of such primitives.
The `serde` library is used to serialize and deserialize messages sent over `ipc-channel-mux`.

[^mux]: The term _mux_ is an abbreviation for multiplexer.

## Design goals

* **Resource efficiency**: Multiplex subchannels over shared IPC channels to reduce OS resource consumption (file descriptors, sockets, etc.). Subsenders can be cloned and sent without consuming additional OS resources. See [When is multiplexing beneficial?](#when-is-multiplexing-beneficial) for more detail.
* **Drop-in replacement for Rust channels**: The API mirrors `channel()` / `Sender<T>` / `Receiver<T>` as closely as possible. See the mapping table below and [Semantic differences from Rust channels](#semantic-differences-from-rust-channels) for the differences.
* **Sender mobility**: `SubSender` implements `Serialize` and `Deserialize`, so subsenders can be sent over subchannels to other processes, enabling dynamic communication topologies. See [Subsender serialization](#subsender-serialization) for how this is implemented efficiently.
* **Disconnection detection**: Detect when all senders or the receiver of a subchannel have been dropped, even across process boundaries and even when subsenders are in-flight (being sent over a subchannel but not yet received). See [Subsender lifecycle](#subsender-lifecycle) for the mechanism.
* **Deadlock avoidance**: Proactively drain IPC channels to prevent buffer-full blocking, which could cause deadlocks when many subchannels share an IPC channel. See [Blocking sends and deadlocks](#blocking-sends-and-deadlocks) for background.

As much as possible, `ipc-channel-mux` has been designed to be a drop-in replacement for Rust channels. The mapping from the Rust channel APIs to subchannel APIs is as follows:

* `channel()` → `mux::Channel::new().unwrap().sub_channel();`
* `Sender<T>` → `mux::SubSender<T>` (requires `T: Serialize`)
* `Receiver<T>` → `mux::SubReceiver<T>` (requires `T: Deserialize`)

Note that `SubSender<T>` implements `Serialize` and `Deserialize`, so you can send subsenders over subchannels freely, just as you can with Rust channels.
However, you cannot send or receive subreceivers - the reason is explained below.

The easiest way to make your types implement `Serialize` and `Deserialize` is to use the `serde_macros` crate from crates.io as a plugin and then annotate the types you want to send with `#[derive(Deserialize, Serialize])`. In many cases, that's all you need to do — the compiler generates all the tedious boilerplate code needed to serialize and deserialize instances of your types.

## Bootstrapping channels between processes

`ipc-channel-mux` provides a one-shot server to help establish a subchannel between two processes. When a one-shot server is created, a server name is generated and returned along with the server.

The client process calls `connect()` passing the server name and this returns the sender end of an subchannel from
the client to the server. Note that there is a restriction: `connect()` may be called at most once per one-shot server.

The server process calls `accept()` on the server to accept a connect request from a client. `accept()` blocks until a client has connected to the server and sent a message. It then returns a pair consisting of the receiver end of the subchannel from client to server and the first message received from the client.

So, in order to bootstrap a subchannel between processes, you create an instance of the `SubOneShotServer` type, pass the resultant server name into the client process (perhaps via an environment variable or command line flag), and connect to the server in the client. See `spawn_sub_one_shot_server_client()` in `multiplex_integration_test.rs` for an example of how to do this using a command to spawn the client process.

## API overview

Let's look at the two ways of creating a channel: directly constructing a channel and using a one-shot server.

### Direct channel construction

Creating a subchannel requires a multiplexing IPC channel to be created first:

~~~Rust
let channel = mux::Channel::new().unwrap();
...
let (tx, rx) = channel.sub_channel();
~~~

### One-shot servers

Multiplexing one-shot servers are used like this:

~~~Rust
let (server, server_name) = mux::SubOneShotServer::new().unwrap();
...
let tx = mux::SubSender::connect(server_name).unwrap(); // Typically in another process

let (rx, data) = server.accept().unwrap();
~~~

An advantage of creating a subchannel, rather than an IPC channel, using a one-shot server is that the subchannel can then be used to transmit subsenders.[^interop]

### Non-blocking receives

`SubReceiver` supports non-blocking receives via `try_recv` and `try_recv_timeout`, analogous to the corresponding methods on `IpcReceiver` and `std::sync::mpsc::Receiver`:

~~~Rust
use ipc_channel_mux::mux;
use std::time::Duration;

let channel = mux::Channel::new().unwrap();
let (tx, rx) = channel.sub_channel();

// try_recv returns immediately with Empty if no message is available.
match rx.try_recv() {
    Err(mux::TryRecvError::Empty) => (), // no message yet
    _ => unreachable!(),
}

tx.send(42).unwrap();
assert_eq!(rx.try_recv().unwrap(), 42);

// try_recv_timeout waits for up to the specified duration.
match rx.try_recv_timeout(Duration::from_millis(1)) {
    Err(mux::TryRecvError::Empty) => (), // timed out, no message
    _ => unreachable!(),
}
~~~

### Routing

The router routes messages from subreceivers to Crossbeam channels.
This allows receiving code to utilise Crossbeam features.

The router is in the `mux::subchannel_router` module.

### Shared memory

`mux::SharedMemory` is a shared memory region that can be sent over subchannels. It is analogous to `ipc-channel`'s `IpcSharedMemory` and is transported efficiently via OS shared memory primitives:

~~~Rust
use ipc_channel_mux::mux;

let channel = mux::Channel::new().unwrap();
let (tx, rx) = channel.sub_channel();

let shmem = mux::SharedMemory::from_bytes(b"hello shared world");
tx.send(shmem).unwrap();

let received: mux::SharedMemory = rx.recv().unwrap();
assert_eq!(&*received, b"hello shared world");
~~~

`SharedMemory` can also be included as a field in user-defined message types that derive `Serialize` and `Deserialize`.

### Opaque senders and receivers

`OpaqueSubSender` and `OpaqueSubReceiver` are type-erased versions of `SubSender<T>` and `SubReceiver<T>`. They are useful when the message type is not known statically or when handling heterogeneous channels. For example, the router uses `OpaqueSubReceiver` internally so it can manage receivers of different message types together.

To convert between typed and opaque forms, use `to_opaque()` and `to::<T>()`:

~~~Rust
let opaque_tx: OpaqueSubSender = tx.to_opaque();
let tx: SubSender<MyMessage> = opaque_tx.to();

let opaque_rx: OpaqueSubReceiver = rx.to_opaque();
let rx: SubReceiver<MyMessage> = opaque_rx.to();
~~~

## Semantic differences from Rust channels

* Rust channels can be either unbounded or bounded whereas subchannels are always unbounded and `send()` never blocks.
* Rust channels do not consume OS IPC resources whereas subchannels consume IPC resources such as sockets, file descriptors, shared memory segments, named pipes, and such like, depending on the OS.
* Rust channels transfer ownership of messages whereas subchannels serialize and deserialize messages.
* Rust channels are type safe whereas subchannels depend on client and server programs using identical message types (or at least message types with compatible serial forms).

## Semantic differences from IPC channels

IPC channels are provided by Servo's [ipc-channel](https://github.com/servo/ipc-channel) crate which the implementation of `ipc-channel-mux` uses for IPC communication.

* Subchannel creation requires the underlying IPC channel to have been created already. Reusing the underlying channel when creating multiple subchannels enables those subchannels to be multiplexed over the underlying channel.
* Subchannel receivers, or _subreceivers_, may not be sent or received. This is a consequence of the MPSC nature of the underlying IPC channel: sending a subreceiver would entail sending the underlying IPC receiver and this would break any other subreceivers using that IPC receiver.
* IPC channel creation can fail, as can multiplexing IPC channel creation, but subchannel creation never fails.[^never]
* IPC receivers can be moved into an `IpcReceiverSet` and then monitored together using a "select" operation. There is no corresponding feature in the `ipc-channel-mux` API since certain scenarios involving subreceivers sharing an underlying IPC channel, some of which are in one set, some in another, and some not in a set give rise to liveness and fairness difficulties without much practical benefit. The main practical use of `IpcReceiverSet` is in implementing routing, which is implemented in `ipc-channel-mux` without adding a subreceiver set construct to the API.

## When is multiplexing beneficial?

Readers familiar with `ipc-channel` may be experiencing some _déjà vu_ at this point since `ipc-channel-mux` is built on top of `ipc-channel` and has a similar API.
The main difference is that `ipc-channel-mux` multiplexes subchannels over the IPC channels provided by `ipc-channel`.

We'll now explore when it's worth using `ipc-channel-mux` instead of `ipc-channel`.
First, it's important to note some other differences between the two kinds of channel:

* Subchannel senders, or _subsenders_, may be sent and received without consuming scarce operating system resources, such as file descriptors on Unix variants.[^dupsender] (Servo has encountered process crashes due to IPC channels consuming all the file descriptors for a process.)
* In order to communicate subreceiver drop to all the subchannel senders, one additional IPC channel is needed per sender of the IPC channel underlying the subchannel. The additional IPC channel's consumption of scare operating system resources, such as file descriptors on Unix variants, is amortised across multiple subchannels which share the sender of the IPC channel underlying the original subchannel.
* Subchannels sharing the same underlying IPC channel could interfere with each other’s performance. For example, message latency on a subchannel sharing the same underlying IPC channel as a busy subchannel could be increased.

[^dupsender]: On Unix variants, each time an IPC sender is received from an IPC channel, a file descriptor is consumed, _even when_ the same IPC sender is received multiple times.
The file descriptor is reclaimed when the received IPC sender is dropped, so file descriptor exhaustion occurs when too many received IPC senders are retained.

To replace an IPC channel with a subchannel and get some benefit, it is necessary to either:

* multiplex other subchannels over the subchannel's underlying IPC channel, or
* send multiple subsenders over the subchannel.[^dupsender]

Using a one-shot server to create a subchannel means that only that one subchannel can be multiplexed over the underlying IPC channel.
So, to replace an IPC one-shot server with a multiplexed one-shot server and get some benefit, it is necessary to either:

* set up other subchannels between the sending process (the one which called `connect()`) and the receiving process (the one which called `accept()`), or
* send multiple subsenders over the subchannel.[^dupsender]

## Packaging

`ipc-channel-mux` is packaged in its own repository and crate, separate from `ipc-channel`.
This has the following advantages:

* The code is more easily navigated, since it's portable rather than multiplatform.
* Changes may be promoted more easily, since IPC channel committers need not be involved.
* The crate can be published to crates.io for ease of consumption by Servo[^gitdep] while avoiding "infecting" the published IPC channel crate and its public API with experimental code which might be ditched if multiplexing turns out not to be useful to Servo.
* Documentation, especially this overview, is focused on multiplexing.
* Tests run fast since the IPC channel tests are elsewhere.[^testspeed]
* The dependencies of `ipc-channel-mux` are kept separate from those of IPC channel.
* Implementing `ipc-channel-mux` using the public API of IPC channel makes the projects easier to understand than if they were combined.
* If multiplexing proves useful and is applied to some IPC channel usecases in Servo, it will be possible to release a version of `ipc-channel-mux` and keep enhancing it and experimenting with applying it to other Servo usecases without giving it the (possibly misleading) status of being part of the IPC channel API. In particular, the multiplexing API can be changed as necessary without impacting backwards compatibility of IPC channel.

[^testspeed]: `cargo test` of `ipc-channel-mux` currently takes under 4 seconds whereas it used to take over 8 seconds before the multiplexing code was split out of the `ipc-channel` repo.

One possible disadvantage is that `ipc-channel-mux` cannot use IPC channel internals, which would have been possible if they were in the same repository.

Another disadvantage is that Servo will require an additional dependency.
However, it would be feasible to merge `ipc-channel-mux` into the IPC channel repository later.

[^never]: Creating a subchannel could exhaust the memory of a process, but memory allocation is treated as infallible in Rust as [Handling memory exhaustion – State of the art?](https://users.rust-lang.org/t/handling-memory-exhaustion-state-of-the-art/87375) explores.
Essentially, if memory allocation fails, the program will panic or, more likely (at least on Linux), be killed by the Out of Memory killer.

[^interop]: `ipc-channel-mux` and `ipc-channel` do not currently interoperate: an IPC channel cannot be used to transmit a subsender and a subchannel cannot be used to transmit an IPC sender or receiver.

[^gitdep]: An alternative would be to have the relevant Servo branch use a [git dependency](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-dependencies-from-git-repositories) on `ipc-channel-mux`.

## Testing

To run the tests, issue:

~~~console
cargo test
~~~

Linux is the _reference platform_ for `ipc-channel-mux`, meaning that bugs encountered on other platforms should be reproduced on Linux so that a complete regression test is available on Linux.

## Diagnostics

`ipc-channel-mux` uses the `log` crate to produce log messages when logging is enabled for one or more processes.

You can emit these log messages from an executable by setting the environment variable RUST_LOG to `debug` or, for more detail, `trace`. For example:

~~~console
RUST_LOG=debug someexecutable
~~~

If you want to see the log messages from a test, pass the `--nocapture` flag to the test executable, e.g.

~~~console
RUST_LOG=trace cargo test mux_test::multiplex_simple -- --nocapture
~~~

Note: `RUST_LOG` is not automatically propagated between processes, so you have to ensure this is done if you want to enable logging for launched processes.

For more information, see [Configure Logging](https://rust-lang-nursery.github.io/rust-cookbook/development_tools/debugging/config_log.html) in The Rust Cookbook.

## Implementation overview

`ipc-channel-mux` multiplexes its subchannels over IPC channels provided by `ipc-channel` which is implemented in terms of native IPC primitives: file descriptor passing over Unix sockets on Unix variants, Mach ports on macOS, and named pipes on Windows.

Multiplexed one-shot servers are implemented using IPC channel one-shot servers. One-shot server names are implemented as a file system path (for Unix variants, with the file system path bound to the socket) or other kinds of generated names on macOS and Windows.

The following sections describe the principles of multiplexing subchannels over IPC channels and some of the design considerations.

### Subchannel identifiers

Each subchannel needs a separate identifier. This is used to _tag_ messages for that subchannel before they are sent to the IPC channel underlying the subchannel. On message receipt, the subchannel id. is used to route the message to the appropriate subchannel.

### Subsender serialization

When a subsender is sent over a subchannel, the underlying IPC sender must be transmitted to the receiving process. To avoid redundantly transmitting the same IPC sender multiple times, the implementation uses a UUID-based optimization:

* The first time a subsender is sent over a particular IPC channel, both the IPC sender and a UUID identifying it are transmitted.
* Subsequent sends of clones of the same subsender over the same IPC channel transmit only the UUID — the receiving process already has the IPC sender from the first transmission.

This is tracked using two complementary data structures: a `Source` (using weak references to track which endpoints have been sent from the sending side) and a `Target` (mapping UUIDs to endpoints on the receiving side). Thread-local context is used during serialization and deserialization to pass this metadata without changing serde's signatures.

### Subsender lifecycle

Subsenders have a complex lifecycle because they can be cloned, sent over subchannels to other processes, and dropped independently. A subsender that has been sent over a subchannel but not yet received by the other process is said to be _in-flight_.

It would be incorrect to report a subchannel as disconnected while a subsender is still in-flight, since the receiving process may yet receive it and use it to send messages. The `SubSenderStateMachine` manages this by tracking:

* **Sources**: the set of processes that currently hold a copy of the subsender.
* **In-flight entries**: subsenders that have been serialized and sent but not yet deserialized and received.

A subchannel is only considered disconnected when all sources have dropped their copies _and_ no copies are in-flight. Periodic probing detects process crashes that might prevent in-flight subsenders from ever being received.

### Shared memory transport

`SharedMemory` is a thin wrapper around `IpcSharedMemory` with custom serialization that works with the mux's two-stage serialization model. `ipc-channel` uses thread-local storage to transport `IpcSharedMemory` values out-of-band via OS shared memory primitives. The mux's inner serialization (using postcard) would lose these values, so `SharedMemory` uses its own thread-local mechanism:

1. **Serialization (send path)**: When a `SharedMemory` value is serialized during inner (postcard) serialization, the underlying `IpcSharedMemory` is captured into a mux-managed thread-local and only an index is written into the payload bytes. After inner serialization completes, the captured values are included in the protocol message as `Vec<IpcSharedMemory>`, so that `ipc-channel`'s outer serialization transports them efficiently via OS shared memory.

2. **Deserialization (receive path)**: The outer deserialization reconstructs the `Vec<IpcSharedMemory>` from the protocol message. Before inner (postcard) deserialization, these values are placed in a mux-managed thread-local. The `SharedMemory` deserializer reads the index from the payload and retrieves the corresponding `IpcSharedMemory` from the thread-local.

This approach avoids any modifications to `ipc-channel` while still benefiting from its efficient OS-level shared memory transport.

### When to block

Generally, sends are non-blocking (but see below) so the main blocking consideration is for receives.
A receive on a subchannel _may_ have to receive from the underlying IPC channel, unless the message has already been received (and placed on a standard Rust channel corresponding to the subchannel receiver).

On subchannel receive, we first of all issue a non-blocking receive (`try_recv`) on the corresponding standard channel. If this returns a message, we can return the message as the result of subchannel receive.

If the corresponding standard channel is empty, we can safely issue a blocking receive on the IPC channel underlying the multi-receiver. (This wouldn't be true if the code supported multi-threading.)

Once a message is received, we can re-try the non-blocking receive on the standard channel to see if a message has been received for the subreceiver.
If not, we can block again on the IPC channel.

### Polling

In the last section, we mentioned issuing a blocking receive on the IPC channel underlying a multi-receiver. It's actually a little more complicated than that because we need to poll for in-flight subsenders having been destroyed.
We do this by probing the response channel associated with the IPC channel used to transmit the subsender.

Each `MultiSender` has a dedicated response channel from the receiving side. When the receiving process exits or the response channel's sender is dropped, `try_recv` on this response channel returns `IpcError::Disconnected`. The probe caches this disconnected state so that once disconnection is detected, subsequent probes immediately return `false` without calling `try_recv` again. This caching is necessary because multiple subsender state machines may share the same `MultiSender` (due to the [subsender serialization](#subsender-serialization) UUID optimization), and `try_recv` consumes the disconnection error — without caching, only the first state machine to probe would detect disconnection, while others would see an empty channel and incorrectly conclude the remote process is still alive.

Polling is implemented by issuing a `try_recv_timeout` on the IPC channel. When the timeout occurs, probing can be initiated and we can then drop the sender half of the standard channel for a subreceiver whose "other half" (meaning the senders for all clients) has hung up. This will cause the non-blocking receive on such standard channels to return with an error and we can then return `Disconnected` from the corresponding subchannel receives.

The receive on the multi-receiver's IPC channel also serves the purpose of detecting `Disconnect` messages generated when a subsender and all its clones on a particular _client_ (approximately equivalent to an IPC sender) have been dropped. That's another way that the sending side of a subchannel can "hang up", after which a receive from the subchannel should fail with `Disconnected`.

### Blocking sends and deadlocks

It turns out that a send to an IPC channel can block when the buffer fills up.
So we have to be careful to take every opportunity to receive messages from IPC channels when we can, for example before generating `Disconnect` messages when a subsender and all its clones on a particular client have been dropped.

Failure to do this can result in deadlocks.
For example, if a process creates a large number of subchannels and then drops them, messages are sent to notify the "other side" that one side has hung up.
If these messages are not received, drop of a subsender or subreceiver can block.

This risk of deadlock was present for non-multiplexed IPC channels, but the risk was lower because fewer messages were sent on each IPC channel.
With multiplexing, a potentially large number of messages can be sent.
Fortunately, a multireceiver will tend to drain messages when receiving on behalf of a subreceiver. Providing that the application code issues receives fairly frequently, the underlying IPC channels shouldn't fill up.

### Interprocess protocol

This is described in [PROTOCOL.md](./PROTOCOL.md) which, if you are reading the documentation, is reproduced below.

## Major missing features

* Each one-shot server accepts only one client connect request. This is fine if you simply want to use this API to split your application up into a fixed number of mutually untrusting processes, but it's not suitable for implementing a system service.

## Related

* [Rust channel](https://doc.rust-lang.org/std/sync/mpsc/index.html): MPSC (multi-producer, single-consumer) channels in the Rust standard library. The implementation consists of a single consumer wrapper of a port of Crossbeam channel.
* [Crossbeam channel](https://github.com/crossbeam-rs/crossbeam/tree/master/crossbeam-channel): extends Rust channels to be more like their Go counterparts. Crossbeam channels are MPMC (multi-producer, multi-consumer).
* [IPC channel](https://github.com/servo/ipc-channel): the IPC channels which `ipc-channel-mux` is implemented on top of.
* [Channels](https://docs.rs/channels/latest/channels/): provides Sender and Receiver types for communicating with a channel-like API across generic IO streams.

[^CSP]: Tony Hoare conceived Communicating Sequential Processes (CSP) as a concurrent programming language.
Stephen Brookes and A.W. Roscoe developed a sound mathematical basis for CSP as a process algebra.
CSP can now be used to reason about concurrency and to verify concurrency properties using model checkers such as FDR4.
Go channels were also inspired by CSP.
