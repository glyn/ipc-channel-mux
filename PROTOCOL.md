<!-- markdownlint-disable MD029 -->
# Interprocess Protocol

The following describes the interprocess protocol used by `ipc-channel-mux` to
multiplex subchannels over a single IPC channel.

## Overview

Multiple typed subchannels are multiplexed over one underlying `ipc-channel` IPC
channel (the _forward channel_). Each subchannel is identified by a UUID
(`SubChannelId`). A second IPC channel (the _response channel_) carries
reverse-direction control messages from the receiver back to the sender.

All protocol messages are variants of two enums which are serialized by
`ipc-channel`:

- **`MultiMessage`** -- sent over the forward channel from sender to receiver.
- **`MultiResponse`** -- sent over the response channel from receiver to sender.

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
`IpcSender<MultiResponse>` is the sender half of a response channel that
the receiver will use to send `MultiResponse` messages back to this client.

### `Data(SubChannelId, Vec<u8>, Vec<(SubChannelId, IpcSenderAndOrId)>, Vec<IpcSharedMemory>)`

Carries a user-level message. Fields:

1. **Target subchannel** -- the `SubChannelId` that the message is destined for.
2. **Payload** -- the user message serialized to bytes with `postcard`.
3. **Embedded subsenders** -- a list of `(SubChannelId, IpcSenderAndOrId)` pairs
   for any `SubSender` values that were serialized inside the payload (see
   _Subsender Transmission_ below).
4. **Shared memory regions** -- a list of `IpcSharedMemory` values extracted
   during serialization (see _Shared Memory_ below).

### `SubChannelId(SubChannelId, String)`

Advertises a new subchannel to the receiver. Sent during inter-process
bootstrapping (the one-shot server flow) immediately after `Connect`. The
`String` is the server name used to correlate the subchannel with the
`SubOneShotServer` that is accepting.

### `Sending { scid, via, via_chan }`

Notifies the receiver that a subsender for subchannel `scid` is in flight,
being transmitted inside a `Data` message on subchannel `via`. The
`via_chan` field carries the `IpcSenderAndOrId` of the IPC sender used by
the channel carrying the subsender.

### `Received { scid, via, new_source }`

Confirms that a subsender for subchannel `scid`, which was in flight via
subchannel `via`, has been successfully deserialized at a new source
identified by `new_source` (a UUID). This transitions the subsender's
lifecycle from _in flight_ to _connected from a new source_.

### `ReceiveFailed { scid, via }`

Indicates that a subsender for subchannel `scid`, which was in flight via
subchannel `via`, could not be received (e.g. because the target
subchannel's receiver was already dropped). This removes the in-flight
entry. If no sources remain and nothing is in flight, the subchannel is
considered disconnected.

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

- **First transmission**: The IPC sender has not been sent before, so
  `IpcSender(sender, uuid)` is sent. The receiver creates a response channel
  and sends `Connect(response_sender, client_id)` back over the IPC sender.
- **Subsequent transmissions**: Only `IpcSenderId(uuid)` is sent. The receiver
  looks up the existing IPC sender by UUID. No new response channel or
  `Connect` message is needed.

## API Operations and Their Protocol Messages

### In-Process Channel Setup (`Channel::new` / `sub_channel`)

`Channel::new()` creates a forward IPC channel and a response IPC channel. No
protocol messages are sent. Calling `sub_channel()` creates a new
`SubChannelId` and returns a `SubSender<T>` / `SubReceiver<T>` pair. No
protocol messages are sent during subchannel creation either.

### Inter-Process Bootstrapping (`SubOneShotServer` / `SubSender::connect`)

**Client side (`SubSender::connect`):**

1. Connects to the IPC one-shot server, creating an `IpcSender<MultiMessage>`.
2. Creates a response channel.
3. Generates a `ClientId`.
4. Sends `Connect(response_sender, client_id)` to the server.
5. Generates a `SubChannelId` for the new subchannel.
6. Sends `SubChannelId(subchannel_id, name)` to the server.

**Server side (`server.accept()`):**

7. Accepts the IPC connection.
8. Receives `Connect` and registers the response sender.
9. Receives `SubChannelId(subchannel_id, name)` and validates it against the
   server name.
10. Receives the first `Data` message and returns `(SubReceiver<T>, T)`.

### Sending a Message (`SubSender::send`)

1. The sender checks whether the subchannel's receiver is still connected
   by checking for any `SubReceiverDisconnected` messages on the response
   channel. If disconnected, returns `MuxError::Disconnected`.
2. The user value is serialized with `postcard`. Any embedded `SubSender`
   values and `SharedMemory` values are extracted during serialization.
3. For each embedded subsender, a `Sending { scid, via, via_chan }` message
   is sent to notify the receiver that the subsender is in flight.
4. A `Data(subchannel_id, payload, subsenders, shmems)` message is sent
   over the forward IPC channel.

### Receiving a Message (`SubReceiver::recv`)

1. When a `Data` message arrives, it is routed by `SubChannelId` to the
   correct subchannel.
2. The payload is deserialized with `postcard`. Any embedded `SubSender`
   values are reconstructed, and a `Received { scid, via, new_source }`
   message is sent back for each to confirm receipt.
3. The deserialized value is returned.

### Subsender Transmission Lifecycle

When a `SubSender` is sent inside a message on another subchannel, its
lifecycle is tracked to ensure proper disconnection detection:

1. **Serialization**: The `SubSender`'s IPC sender and subchannel ID are
   extracted (not serialized into the payload).
2. **Sending notification**: A `Sending { scid, via, via_chan }` message is
   sent. The receiver registers the subsender as _in flight_ via subchannel
   `via`.
3. **Data transmission**: The `Data` message carries the subsender
   information alongside the payload.
4. **Receipt confirmation**: When the payload is deserialized, a
   `Received { scid, via, new_source }` message is sent. The subsender
   transitions from _in flight_ to _connected from `new_source`_.
5. **Disconnection**: When the received subsender is dropped, a
   `Disconnect(scid, source)` message is sent. Once all sources are
   disconnected and no copies are in flight, the subchannel is fully
   disconnected.

### Probing

When subsenders are in flight (between `Sending` and `Received`), the
receiver periodically checks whether the channel carrying the subsender is
still alive by performing a non-blocking `try_recv` on the response channel
associated with the carrying channel's IPC sender:

- If `try_recv` returns `IpcError`, the remote process has crashed and all
  in-flight entries for that channel are removed. If no sources remain, the
  subchannel is marked as disconnected.
- If `try_recv` returns `Empty`, the channel is still alive.
- Any `SubReceiverDisconnected` messages received are processed normally.

This prevents indefinite waits when a process carrying a subsender in transit
has crashed.

### Shared Memory

`SharedMemory` values are transported using a two-stage serialization model:

1. **Serialization** (`postcard`): Each `SharedMemory` value extracts its
   `IpcSharedMemory` and serializes as just an index.
2. **Transport** (`ipc-channel`): The collected `IpcSharedMemory` values
   are included in the `Data` message. The `ipc-channel` layer transports
   them efficiently using operating-system shared memory primitives.
3. **Deserialization**: Each `SharedMemory` reads its index and retrieves the
   corresponding `IpcSharedMemory` from the `Data` message.

This avoids duplicating shared memory contents into the binary payload.

## Error Flows

### Receiver Disconnection (`SubReceiver` Dropped)

1. `SubReceiverDisconnected(subchannel_id)` is broadcast via the response
   channel to all connected clients.
2. Before each send, the sender checks the response channel. If it finds
   `SubReceiverDisconnected` for the target subchannel, subsequent sends
   return `MuxError::Disconnected`.
3. Any queued messages containing embedded subsenders are cleaned up by
   sending `ReceiveFailed { scid, via }` for each, so their lifecycles
   are properly resolved.

### Sender Disconnection (`SubSender` Dropped)

1. When the last clone of a `SubSender` is dropped,
   `Disconnect(subchannel_id, source)` is sent over the forward channel.
2. Once all sources are gone and no transmissions are in flight, the
   subchannel is fully disconnected and `SubReceiver::recv` returns
   `MuxError::Disconnected`.

### IPC Channel Failure

If the underlying IPC channel encounters an error (e.g. the remote process
crashed), `ipc-channel` returns an `IpcError`. This is wrapped in
`MuxError::IpcError` and propagated to the caller of `send` or `recv`.
