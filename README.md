
`ipc-channel-mux` is an inter-process implementation of Rust channels (which were inspired by CSP[^CSP]).

A Rust channel is a unidirectional, FIFO queue of messages which can be used to send messages between threads in a single operating system process.
For an excellent introduction to Rust channels, see [Using Message Passing to Transfer Data Between Threads](https://doc.rust-lang.org/stable/book/ch16-02-message-passing.html) in the Rust reference.

`ipc-channel-mux`[^mux] extends Rust channels to support inter-process communication (IPC) in a single operating system instance.
`ipc-channel-mux` multiplexes _subchannels_ over IPC primitives to reduce the consumption of such primitives.
The `serde` library is used to serialize and deserialize messages sent over `ipc-channel-mux`.

[^mux]: The term _mux_ is an abbreviation for multiplexer.

As much as possible, `ipc-channel-mux` has been designed to be a drop-in replacement for Rust channels. The mapping from the Rust channel APIs to subchannel APIs is as follows:

* `channel()` → `multiplex::Channel::new().unwrap().sub_channel();`
* `Sender<T>` → `multiplex::SubSender<T>` (requires `T: Serialize`)
* `Receiver<T>` → `multiplex::SubReceiver<T>` (requires `T: Deserialize`)

Note that `SubSender<T>` implements `Serialize` and `Deserialize`, so you can send subsenders over subchannels freely, just as you can with Rust channels.
However, you cannot send or receive subreceivers - the reason is explained below.

The easiest way to make your types implement `Serialize` and `Deserialize` is to use the `serde_macros` crate from crates.io as a plugin and then annotate the types you want to send with `#[derive(Deserialize, Serialize])`. In many cases, that's all you need to do — the compiler generates all the tedious boilerplate code needed to serialize and deserialize instances of your types.

## Bootstrapping channels between processes

`ipc-channel-mux` provides a one-shot server to help establish a subchannel between two processes. When a one-shot server is created, a server name is generated and returned along with the server.

The client process calls `connect()` passing the server name and this returns the sender end of an subchannel from
the client to the server. Note that there is a restriction in `connect()` may be called at most once per one-shot server.

The server process calls `accept()` on the server to accept connect requests from clients. `accept()` blocks until a client has connected to the server and sent a message. It then returns a pair consisting of the receiver end of the subchannel from client to server and the first message received from the client.

So, in order to bootstrap an subchannel between processes, you create an instance of the `SubOneShotServer` type, pass the resultant server name into the client process (perhaps via an environment variable or command line flag), and connect to the server in the client. See `spawn_sub_one_shot_server_client()` in `multiplex_integration_test.rs` for an example of how to do this using a command to spawn the client process.

## API overview

Let's look at the two ways of creating a channel: directly constructing a channel and using a one-shot server.

#### Direct channel construction

Creating a subchannel requires a multiplexing IPC channel to be created first:

~~~Rust
let channel = multiplex::Channel::new().unwrap();
...
let (tx, rx) = channel.sub_channel();
~~~

#### One-shot servers

Multiplexing one-shot servers are used like this:

~~~Rust
let (server, server_name) = multiplex::SubOneShotServer::new().unwrap();
...
let tx = multiplex::SubSender::connect(server_name).unwrap(); // Typically in another process

let (rx, data) = server.accept().unwrap();
~~~

The advantage of creating a subchannel, rather than an IPC channel, using a one-shot server is that the subchannel can then be used to transmit subsenders.[^interop]

## Semantic differences from Rust channels

* Rust channels can be either unbounded or bounded whereas subchannels are always unbounded and `send()` never blocks.
* Rust channels do not consume OS IPC resources whereas subchannels consume IPC resources such as sockets, file descriptors, shared memory segments, named pipes, and such like, depending on the OS.
* Rust channels transfer ownership of messages whereas subchannels serialize and deserialize messages.
* Rust channels are type safe whereas subchannels depend on client and server programs using identical message types (or at least message types with compatible serial forms).

## Semantic differences from IPC channels

* Subchannel creation requires the underlying IPC channel to have been created already.
Reusing the underlying channel when creating multiple subchannels enables those subchannels to be multiplexed over the underlying channel.
* Subchannel receivers, or _subreceivers_, may not be sent or received.[^restriction] This is a consequence of the MPSC nature of the underlying IPC channel: sending a subreceiver would entail sending the underlying IPC receiver and this would break any other subreceivers using that IPC receiver.
* IPC channel creation can fail, as can multiplexing IPC channel creation, but subchannel creation never fails.[^never]

## When is multiplexing beneficial?

Readers familiar with Servo's [`ipc-channel`](https://crates.io/crates/ipc-channel) crate may be experiencing some _déjà vu_ at this point since `ipc-channel-mux` is built on top of `ipc-channel` and has a similar API.
The main difference is that `ipc-channel-mux` multiplexes subchannels over the IPC channels provided by `ipc-channel`.

We'll now explore when it's worth using `ipc-channel-mux` instead of `ipc-channel`.
First, it's important to note some other differences between the two kinds of channel:

* Subchannel senders, or _subsenders_, may be sent and received without consuming scarce operating system resources, such as file descriptors on Unix variants.[^dupsender] (Servo has encountered process crashes due to IPC channels consuming all the file descriptors for a process.)
* In order to communicate subreceiver drop to all the subchannel senders, one additional IPC channel is needed per sender of the IPC channel underlying the subchannel.
The additional IPC channel's consumption of scare operating system resources, such as file descriptors on Unix variants, is amortised across multiple subchannels which share the sender of the IPC channel underlying the original subchannel.
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

* The repository can be hosted on codeberg.org rather than github.com.
* The code is more easily navigated, since it's portable rather than multiplatform.
* Changes may be promoted more easily, since IPC channel committers need not be involved.
* The crate can be published to crates.io[^crate] for ease of consumption by Servo[^gitdep] while avoiding "infecting" the published IPC channel crate and its public API with experimental code which might be ditched if the multiplexing turns out not to be useful to Servo.
* Documentation is focused on multiplexing.
* Tests run fast since the IPC channel tests are elsewhere.[^testspeed]
* The dependencies of `ipc-channel-mux` are kept separate from those of IPC channel.
* The clean separation of the `ipc-channel-mux` crate depending on the public API of IPC channel makes the pair easier to grok.
* If the multiplexing proves useful and is applied to some IPC channel usecases in Servo, it will be possible to release a version of `ipc-channel-mux` and keep enhancing it and experimenting with applying it to other Servo usecases without giving it the (possibly misleading) status of being part of the IPC channel API.
In particular, it will be reasonable to change the multiplexing API as necessary without impacting backwards compatibility of IPC channel.

[^testspeed]: `cargo test` ran in just over 2 seconds with the new repo. whereas it used to take over 8 seconds with the ipc-channel repo.

One possible disadvantage is that it is not be possible to reuse IPC channel internals, which would have been possible if `ipc-channel-mux` was implemented in the `ipc-channel` repository.
For example, if one of the missing features for multiplexing was essentially the same as that for vanilla IPC channel, it wouldn't be possible to just generalise the code and share it (at least not without exposing an API to the code as an IPC channel external).

Another disadvantage is that Servo will require an additional dependency.
However, it would be feasible to merge `ipc-channel-mux` into the IPC channel repository later.

[^restriction]: Since subreceivers cannot be transmitted between processes, we expect a subsender created using a `multiplex::Channel` instance to be moved or transmitted to another process.

[^never]: Creating a subchannel could exhaust the memory of a process, but memory allocation is treated as infallible in Rust as [Handling memory exhaustion – State of the art?](https://users.rust-lang.org/t/handling-memory-exhaustion-state-of-the-art/87375) explores.
Essentially, if memory allocation fails, the program will panic or, more likely (at least on Linux), be killed by the Out of Memory killer.

[^interop]: In the current implementation, an IPC channel cannot be used to transmit a subsender.
Similarly, a subchannel cannot be used to transmit an IPC sender.

[^crate]: Unfortunately, this still requires the repository to be mirrored to github.com. See [Non-Github account creation](https://github.com/rust-lang/crates.io/issues/326). However, this is easily worked around by pushing to a mirror at github.com before publishing to crates.io.

[^gitdep]: An alternative would be to have the relevant Servo branch use a [git dependency](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-dependencies-from-git-repositories) on `ipc-channel-mux`.

## Testing

To run the tests, issue:

```console
cargo test
```

## Implementation overview

`ipc-channel-mux` multiplexes its subchannels over IPC channels provided by `ipc-channel` which is implemented in terms of native IPC primitives: file descriptor passing over Unix sockets on Unix variants, Mach ports on macOS, and named pipes on Windows.

Multiplexed one-shot servers are implemented using IPC channel one-shot servers. One-shot server names are implemented as a file system path (for Unix variants, with the file system path bound to the socket) or other kinds of generated names on macOS and Windows.

## Major missing features

* ROUTER - routing messages from IPC receivers to crossbeam channels. This allows receiving code to utilise crossbeam features.
* Receiver sets - monitoring multiple receivers with a single thread.
* Non-blocking receives.
* Transmission of shared memory.
* Each one-shot server accepts only one client connect request. This is fine if you simply want to use this API to split your application up into a fixed number of mutually untrusting processes, but it's not suitable for implementing a system service.

## Related

* [Rust channel](https://doc.rust-lang.org/std/sync/mpsc/index.html): MPSC (multi-producer, single-consumer) channels in the Rust standard library. The implementation
consists of a single consumer wrapper of a port of Crossbeam channel.
* [Crossbeam channel](https://github.com/crossbeam-rs/crossbeam/tree/master/crossbeam-channel): extends Rust channels to be more like their Go counterparts. Crossbeam channels are MPMC (multi-producer, multi-consumer)
* [IPC channel](https://github.com/server/ipc-channel): the IPC channels which `ipc-channel-mux` is based on on top of which it is implemented.
* [Channels](https://docs.rs/channels/latest/channels/): provides Sender and Receiver types for communicating with a channel-like API across generic IO streams.

[^CSP]: Tony Hoare conceived Communicating Sequential Processes (CSP) as a concurrent programming language.
Stephen Brookes and A.W. Roscoe developed a sound mathematical basis for CSP as a process algebra.
CSP can now be used to reason about concurrency and to verify concurrency properties using model checkers such as FDR4.
Go channels were also inspired by CSP.
