<!-- markdownlint-disable MD029 -->
# Interprocess Protocol

The following describes the interprocess protocol used by `ipc-channel-mux` to
multiplex subchannels over a single IPC channel.

## Overview

Multiple typed subchannels are multiplexed over one underlying `ipc-channel` IPC
channel. Each subchannel is identified by a UUID (`SubChannelId`). A second IPC
channel (the _response channel_) carries reverse-direction control messages from
the receiver back to the sender.

All protocol messages are variants of two enums which are serialized by `ipc-channel`:

- **`MultiMessage`** -- sent over the _forward channel_ from sender to receiver.
- **`MultiResponse`** -- sent over the _response channel_ from receiver to sender.

User-level message payloads are serialized with `postcard` and carried inside
`MultiMessage::Data` as opaque bytes.

## Identifiers

| Name | Type | Purpose |
|------|------|---------|
| `ClientId` | UUID | Identifies a sending process / connection. |
| `SubChannelId` | UUID | Identifies a subchannel. |
| IPC sender UUID | UUID | Identifies an underlying `IpcSender<MultiMessage>` for deduplication. |

## Forward Channel Messages (`MultiMessage`)

### `Connect(IpcSender<MultiResponse>, ClientId)`

Registers a new sending client with the receiver. The included
`IpcSender<MultiResponse>` is the sender half of the response channel that
the receiver will use to send `MultiResponse` messages back to this client.

### `Data(SubChannelId, Vec<u8>, Vec<(SubChannelId, IpcSenderAndOrId)>, Vec<IpcSharedMemory>)`

Carries a user-level message. Fields:

1. **Target subchannel** -- the `SubChannelId` that the message is destined for.
2. **Payload** -- the user message serialized to bytes with `postcard`.
3. **Embedded subsenders** -- a list of `(SubChannelId, IpcSenderAndOrId)` pairs
   for any `SubSender` values that were serialized inside the payload (see
   _Subsender Transmission_ below).
4. **Shared memory regions** -- a list of `IpcSharedMemory` values extracted
   during inner serialization (see _Shared Memory_ below).

### `SubChannelId(SubChannelId, String)`

Advertises a new subchannel to the receiver. Sent during inter-process
bootstrapping (the one-shot server flow) immediately after `Connect`. The
`String` is the server name used to correlate the subchannel with the
`SubOneShotServer` that is accepting.

### `Sending { scid, via, via_chan }`

Notifies the receiver that a subsender for subchannel `scid` is in flight,
being transmitted inside a `Data` message on subchannel `via`. The
`via_chan` field carries the `IpcSenderAndOrId` of the IPC sender used by
the channel carrying the subsender. When the receiver resolves `via_chan`,
it establishes a response channel with the remote demuxer, which is used
for connectivity checking (see _Probing_ below).

### `Received { scid, via, new_source }`

Confirms that a subsender for subchannel `scid`, which was in flight via
subchannel `via`, has been successfully deserialized at a new source
identified by `new_source` (a UUID). This transitions the subsender's
lifecycle from _in flight_ to _connected from a new source_.

### `Disconnect(SubChannelId, Uuid)`

Indicates that all copies of a subsender for the given `SubChannelId` at
the source identified by the given UUID have been dropped. Once all sources
and in-flight transmissions for a subchannel have disconnected, the
subchannel's receiver is notified of disconnection.

## Response Channel Messages (`MultiResponse`)

### `SubReceiverDisconnected(SubChannelId)`

Sent from the receiver to **all** connected clients when a `SubReceiver` is
dropped. This allows senders to detect early that the receiving end of a
subchannel is gone, so that subsequent `send` calls can return
`MuxError::Disconnected` without attempting the IPC send.

## IPC Sender Deduplication (`IpcSenderAndOrId`)

Transmitting an `IpcSender` over an IPC channel consumes operating-system
resources (e.g. file descriptors). To avoid sending the same underlying IPC
sender repeatedly, the protocol uses `IpcSenderAndOrId`:

~~~Rust
IpcSenderAndOrId::IpcSender(IpcSender<MultiMessage>, String)
IpcSenderAndOrId::IpcSenderId(String)
~~~

The `String` is the UUID of the IPC sender.

- **First transmission**: The sender side checks a weak hash set (`Source`). If
  the IPC sender has not been sent before, it sends
  `IpcSender(sender, uuid)`. When the receiver resolves this, it creates a
  new `MultiSender` wrapping the IPC sender. As part of this, it creates a
  new response channel pair and sends `Connect(response_sender, client_id)`
  back over the IPC sender to register itself as a client of the remote
  demuxer. It then stores the UUID-to-`MultiSender` mapping in a `Target`
  hash map.
- **Subsequent transmissions**: The sender sends only `IpcSenderId(uuid)`.
  The receiver looks up the existing `MultiSender` by UUID in its `Target`
  map. No new response channel or `Connect` message is needed.

## Non-Error Flows

### In-Process Channel Setup

1. `Channel::new()` creates one IPC channel (forward) and one IPC channel
   (response).
2. The forward `IpcSender` and the response `IpcReceiver` are wrapped in a
   `MultiSender`. The forward `IpcReceiver` and the response `IpcSender`
   are wrapped in a `MultiReceiver` with a `Demuxer`.
3. A `ClientId` is generated and the response sender is registered in the
   demuxer so `SubReceiverDisconnected` messages can be sent back.
4. `channel.sub_channel::<T>()` creates a `SubChannelId`, constructs a
   `SubChannelSender` (which wraps the shared `MultiSender`), registers a
   state machine in the demuxer for the new subchannel, and returns a
   `SubSender<T>` / `SubReceiver<T>` pair.

### Inter-Process Bootstrapping (One-Shot Server)

**Server side:**

1. `SubOneShotServer::<T>::new()` creates an `IpcOneShotServer<MultiMessage>`
   and returns the server along with its name.
2. The name is communicated to the client out of band (e.g. environment
   variable or command-line argument).

**Client side:**

3. `SubSender::connect(name)` connects to the IPC one-shot server, creating
   an `IpcSender<MultiMessage>`.
4. A response channel (`IpcSender<MultiResponse>`, `IpcReceiver<MultiResponse>`)
   is created.
5. A `ClientId` is generated.
6. `Connect(response_sender, client_id)` is sent to the server.
7. A `SubChannelId` is generated for the new subchannel.
8. `SubChannelId(subchannel_id, name)` is sent to the server.
9. The client can now send messages via `SubSender::send()`.

**Server side (continued):**

10. `server.accept()` accepts the IPC connection, receiving the `IpcReceiver`
    and the first `MultiMessage`.
11. The first message (`Connect`) is handled: the response sender is stored
    in the demuxer.
12. `SubChannelId(subchannel_id, name)` is received and validated against the
    server name.
13. A `SubChannelReceiver` is attached to the demuxer for the subchannel.
14. The first `Data` message is received and deserialized, returning
    `(SubReceiver<T>, T)`.

### Sending a Message

1. The sender checks whether the subchannel's receiver is still connected by
   draining the response channel for any `SubReceiverDisconnected` messages and
   checking the local `SubReceiverProxy`. If disconnected, return
   `MuxError::Disconnected`.
2. The user value is serialized with `postcard`. During serialization:
   - Any embedded `SubSender` values record their IPC sender and subchannel
     ID into thread-local vectors.
   - Any `SharedMemory` values push their `IpcSharedMemory` into a
     thread-local vector and serialize only an index.
3. The thread-local shared memory regions are collected.
4. The thread-local serialized subsender information is collected.
5. For each embedded subsender, a `Sending { scid, via, via_chan }` message is
   sent to notify the receiver that the subsender is in flight.
6. A `Data(subchannel_id, payload, subsenders, shmems)` message is sent over
   the forward IPC channel.

### Receiving a Message

1. The `SubChannelReceiver` tries to read from its local `mpsc` channel
   (which receives demuxed messages).
2. If empty and the demuxer lock is available, it locks the demuxer and calls
   `try_recv_timeout` on the IPC receiver with a 100 ms polling interval.
3. If the demuxer lock is contended, it waits on the local `mpsc` channel with
   a 100 us timeout (another thread is draining the IPC receiver and may
   deliver a message to this subchannel's `mpsc` channel).
4. When a `MultiMessage` arrives from the IPC channel, the demuxer's `handle`
   method routes it by `SubChannelId` to the correct subchannel's state
   machine, which delivers it to the subchannel's `mpsc` sender.
5. When the `SubChannelReceiver` reads a `ResolvedMessage` from its `mpsc`
   channel:
   - The deserialization context is established: embedded subsender
     information and shared memory regions are placed in thread-local storage.
   - The payload is deserialized with `postcard`.
   - During deserialization, any embedded `SubSender` values are
     reconstructed by popping from the thread-local subsender list, and a
     `Received { scid, via, new_source }` message is sent back to confirm
     receipt.
   - The deserialization context is cleared.
   - The deserialized value is returned.

### Subsender Transmission Lifecycle

When a `SubSender` is sent inside a message on another subchannel, its
lifecycle is tracked to ensure proper disconnection detection:

1. **Serialization**: The `SubSender`'s IPC sender and subchannel ID are
   recorded in thread-local storage (not serialized into the payload).
2. **Sending notification**: A `Sending { scid, via, via_chan }` message is
   sent. The receiver registers the subsender as _in flight_ via subchannel
   `via` and records a probe function that checks the response channel of
   the `MultiSender` for the carrying channel.
3. **Data transmission**: The `Data` message carries the subsender information
   alongside the payload.
4. **Receipt confirmation**: When the `Data` message is received and the
   payload is deserialized, a `Received { scid, via, new_source }` message is
   sent. The receiver's state machine moves the subsender from _in flight_ to
   _connected from `new_source`_.
5. **Disconnection**: When the received subsender is dropped, a
   `Disconnect(scid, source)` message is sent. The state machine removes
   that source. Once all sources are disconnected and no transmissions are in
   flight, the subchannel is fully disconnected.

### Probing

When subsenders are in flight (between `Sending` and `Received`), the
receiver periodically checks whether the channel carrying the subsender is
still alive. This is done during idle polling (when `try_recv_timeout` times
out):

1. For each subchannel with in-flight transmissions, the registered probe
   function is called.
2. The probe performs a non-blocking `try_recv` on the response channel
   associated with the `MultiSender` for the carrying channel. If
   `try_recv` returns `IpcError`, the remote process has crashed and the
   channel is broken. If it returns `Empty`, the channel is still alive.
   Any `SubReceiverDisconnected` messages received are processed normally.
3. If a probe fails, all in-flight entries for that channel are removed and,
   if no sources remain, the subchannel is marked as disconnected.

This prevents indefinite waits when a process carrying a subsender in transit
has crashed.

### Shared Memory

`SharedMemory` values are transported using a two-stage serialization model:

1. **Inner serialization** (`postcard`): Each `SharedMemory` value pushes its
   `IpcSharedMemory` into a thread-local vector and serializes as just the
   index into that vector.
2. **Outer serialization** (`ipc-channel`): The collected `IpcSharedMemory`
   values are included in the `Data` message. The `ipc-channel` layer
   transports them efficiently using operating-system shared memory
   primitives.
3. **Deserialization**: The received `IpcSharedMemory` values are placed into
   a thread-local vector. During `postcard` deserialization, each
   `SharedMemory` reads its index and retrieves the corresponding
   `IpcSharedMemory` from the vector.

This avoids duplicating shared memory contents into the binary payload.

## Error Flows

### Receiver Disconnection (SubReceiver Dropped)

1. When a `SubChannelReceiver` is dropped, it broadcasts
   `SubReceiverDisconnected(subchannel_id)` via the response channel to all
   connected clients.
2. Before each send, the `MultiSender` drains the response channel. If it
   finds `SubReceiverDisconnected` for the target subchannel, it marks the
   local `SubReceiverProxy` as disconnected.
3. Subsequent sends on that subchannel return `MuxError::Disconnected`.
4. During drop, the `SubChannelReceiver` also drains any queued messages. For
   any messages containing embedded subsenders, it sends
   `ReceiveFailed { scid, via }` back for each subsender so their lifecycles
   are properly resolved.

### Sender Disconnection (SubSender Dropped)

1. When the last clone of a `SubSender` is dropped, the `SubSenderTracker`'s
   drop handler fires.
2. If the receiver is still connected, a `Disconnect(subchannel_id, source)`
   message is sent over the forward channel.
3. The demuxer's state machine removes that source. If all sources are gone
   and no transmissions are in flight, a disconnect signal is delivered to the
   `SubChannelReceiver`'s `mpsc` channel.
4. The `SubChannelReceiver::recv` loop sees the disconnect and returns
   `MuxError::Disconnected`.

### Subsender Reception Failure

If a `Data` message arrives at a demuxer for a subchannel that has no
registered state machine (e.g. the receiver was already dropped), the demuxer
sends `ReceiveFailed { scid, via }` for each embedded subsender via the
subsender's `MultiSender`. Each `SubChannelSender` captures the IPC sender
from its parent `MultiSender` at creation time — this is the same IPC sender
it uses to send `Data` messages on its own subchannel, so it points to the
demuxer where that subchannel's state machine is registered. When
`ReceiveFailed` is sent through it, the message arrives at that demuxer. The
demuxer handles it by finding the state machine for the subsender's
subchannel and calling `receive_failed`, removing the in-flight entry for
`via`. If no sources remain and nothing is in flight, the subchannel is
marked disconnected.

### IPC Channel Failure

If the underlying IPC channel encounters an error (e.g. the remote process
crashed), `ipc-channel` returns an `IpcError`. This is wrapped in
`MuxError::IpcError` and propagated to the caller of `send` or `recv`.

### Probe Failure

If `try_recv` on the response channel returns `IpcError` during polling,
the associated in-flight subsenders are removed from the state machine.
If this leaves the subchannel with no sources and no in-flight
transmissions, it is marked disconnected and the receiver is notified.
