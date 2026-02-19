// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Routers allow converting IPC subchannels to crossbeam channels.
//! The [RouterProxy] provides various methods to register
//! `SubReceiver<T>`s. The router will then either call the appropriate callback or route the
//! message to a crossbeam `Sender<T>` or `Receiver<T>`. You should use the global `ROUTER` to
//! access the `RouterProxy` methods (via `ROUTER`'s `Deref` for `RouterProxy`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::{self, JoinHandle};

use crossbeam_channel::{self, Receiver, Sender};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::mux::{
    self, MuxError, OpaqueSubReceiver2, RawMessage, SubReceiver2, SubReceiverSet,
    SubSelectionResult, SubSender,
};

/// Global object wrapping a `RouterProxy`.
/// Add routes ([add_typed_route](RouterChannel::add_typed_route)), or route
/// to crossbeam receivers (e.g. [route_to_new_crossbeam_receiver](RouterChannel::route_to_new_crossbeam_receiver)).
pub static ROUTER: LazyLock<Arc<RouterProxy>> = LazyLock::new(RouterProxy::new);

/// A `RouterProxy` provides methods for establishing and talking to the router.
pub struct RouterProxy {
    comm: Mutex<RouterProxyComm>,
}

impl Drop for RouterProxy {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[allow(clippy::new_without_default)]
impl RouterProxy {
    /// Spawn a router thread which waits for events on its registered `SubReceiver<T>`s.
    /// The `RouterProxy`'s methods communicate with the running router thread to
    /// register new `SubReceiver<T>`'s.
    pub fn new() -> Arc<RouterProxy> {
        // Router acts like a receiver, running in its own thread with both
        // receiver ends.
        // Router proxy takes both sending ends.
        let (msg_sender, msg_receiver) = crossbeam_channel::unbounded();
        let chan = mux::Channel2::new().unwrap();
        let (wakeup_sender, wakeup_receiver) = chan.sub_channel();
        let handle = thread::Builder::new()
            .name("router-proxy".to_string())
            .spawn(move || Router::new(msg_receiver, wakeup_receiver).run())
            .expect("Failed to spawn router proxy thread");
        Arc::new(RouterProxy {
            comm: Mutex::new(RouterProxyComm {
                msg_sender,
                wakeup_sender,
                shutdown: false,
                handle: Some(handle),
            }),
        })
    }

    /// Create a new `RouterChannel`, which is used to construct routed subchannels.
    pub fn new_router_channel(proxy: &Arc<RouterProxy>) -> Result<RouterChannel, MuxError> {
        Ok(RouterChannel {
            chan: mux::Channel2::new()?,
            proxy: proxy.clone(),
        })
    }

    /// Add a new (receiver, callback) pair to the router, and send a wakeup message
    /// to the router.
    ///
    /// Consider using [add_typed_route](Self::add_typed_route) instead, which prevents
    /// mismatches between the receiver and callback types.
    fn add_route(
        &self,
        subreceiver: OpaqueSubReceiver2,
        callback: RouterHandler,
    ) -> Result<(), RouterError> {
        let comm = self.comm.lock().unwrap();

        if comm.shutdown {
            return Err(RouterError::ShuttingDown);
        }

        let (ack_sender, ack_receiver) = crossbeam_channel::bounded(1);
        comm.msg_sender
            .send(RouterMsg::AddRoute(subreceiver, callback, ack_sender))
            .unwrap();
        comm.wakeup_sender.send(()).unwrap();
        // Wait for the router to process the AddRoute (including switch_sender)
        // before returning. This ensures that subsequent sends on the SubSender
        // find a valid internal channel in the SubSenderStateMachine.
        drop(comm);
        ack_receiver.recv().unwrap();
        Ok(())
    }

    /// Add a new `(receiver, callback)` pair to the router, and send a wakeup message
    /// to the router. This method is strongly typed and guarantees
    /// that `subreceiver` and `callback` use the same message type.
    fn add_typed_route<T>(
        &self,
        subreceiver: SubReceiver2<T>,
        mut callback: TypedRouterHandler<T>,
    ) -> Result<(), RouterError>
    where
        T: Serialize + for<'de> Deserialize<'de> + 'static,
    {
        // Before passing the message on to the callback, turn it into the appropriate type
        let modified_callback = move |msg: RawMessage| {
            let typed_message = msg.to::<T>();
            callback(typed_message);
        };

        self.add_route(subreceiver.to_opaque(), Box::new(modified_callback))
    }

    /// A convenience function to route an `SubReceiver<T>` to an existing `Sender<T>`.
    fn route_subreceiver_to_crossbeam_sender<T>(
        &self,
        subreceiver: SubReceiver2<T>,
        crossbeam_sender: Sender<T>,
    ) -> Result<(), RouterError>
    where
        T: for<'de> Deserialize<'de> + Serialize + Send + 'static,
    {
        self.add_typed_route(
            subreceiver,
            Box::new(move |message| drop(crossbeam_sender.send(message.unwrap()))),
        )
    }

    /// A convenience function to route an `SubReceiver<T>` to a `Receiver<T>`: the most common
    /// use of a `Router`.
    fn route_subreceiver_to_new_crossbeam_receiver<T>(
        &self,
        subreceiver: SubReceiver2<T>,
    ) -> Result<Receiver<T>, RouterError>
    where
        T: for<'de> Deserialize<'de> + Serialize + Send + 'static,
    {
        let (crossbeam_sender, crossbeam_receiver) = crossbeam_channel::unbounded();
        self.route_subreceiver_to_crossbeam_sender(subreceiver, crossbeam_sender)?;
        Ok(crossbeam_receiver)
    }

    /// Send a shutdown message to the router containing an ACK sender,
    /// send a wakeup message to the router, and block on the ACK.
    /// Calling it is idempotent,
    /// which can be useful when running a multi-process system in single-process mode.
    pub fn shutdown(&self) {
        let mut comm = self.comm.lock().unwrap();

        if comm.shutdown {
            return;
        }
        comm.shutdown = true;

        let (ack_sender, ack_receiver) = crossbeam_channel::unbounded();
        comm.wakeup_sender
            .send(())
            .map(|()| {
                comm.msg_sender
                    .send(RouterMsg::Shutdown(ack_sender))
                    .unwrap();
                ack_receiver.recv().unwrap();
            })
            .unwrap();
        comm.handle
            .take()
            .expect("Should have a join handle at shutdown")
            .join()
            .expect("Failed to join on the router proxy thread");
    }
}

/// A RouterChannel is analogous to [mux::Channel], but is used to construct routed subchannels.
/// All SubChannels created using a given RouterChannel share an underlying IPC channel.
///
/// RouterChannel is the only way to construct routed subchannels and avoids routed subchannels
/// sharing an underlying IPC channel with non-routed subchannels, which would give rise to
/// liveness and fairness issues.
pub struct RouterChannel {
    chan: mux::Channel2,
    proxy: Arc<RouterProxy>,
}

/// RouterError describes errors related to routing.
#[derive(Debug, Error)]
pub enum RouterError {
    /// The router is shutting down.
    #[error("Router shutting down")]
    ShuttingDown,
}

impl RouterChannel {
    /// Add a new `(receiver, callback)` pair to the router, and send a wakeup message
    /// to the router. This method is strongly typed and guarantees
    /// that `subreceiver` and `callback` use the same message type.
    pub fn add_typed_route<T>(
        &self,
        callback: TypedRouterHandler<T>,
    ) -> Result<SubSender<T>, RouterError>
    where
        T: Serialize + for<'de> Deserialize<'de> + 'static,
    {
        let (tx, rx) = self.chan.sub_channel::<T>();
        self.proxy.add_typed_route(rx, callback)?;
        Ok(tx)
    }

    /// A convenience function to route a new `SubReceiver<T>` to an existing `Sender<T>`
    /// and return the `SubSender<T>` associated with the `SubReceiver<T>`.
    pub fn route_to_crossbeam_sender<T>(
        &self,
        crossbeam_sender: Sender<T>,
    ) -> Result<SubSender<T>, RouterError>
    where
        T: for<'de> Deserialize<'de> + Serialize + Send + 'static,
    {
        let (tx, rx) = self.chan.sub_channel::<T>();
        self.proxy
            .route_subreceiver_to_crossbeam_sender(rx, crossbeam_sender)?;
        Ok(tx)
    }

    /// A convenience function to route a new `SubReceiver<T>` to a `Receiver<T>`: the most common
    /// use of a `Router`. Returns the `SubSender<T>` associated with the `SubReceiver<T>`.
    pub fn route_to_new_crossbeam_receiver<T>(
        &self,
    ) -> Result<(SubSender<T>, Receiver<T>), RouterError>
    where
        T: for<'de> Deserialize<'de> + Serialize + Send + 'static,
    {
        let (tx, rx) = self.chan.sub_channel::<T>();
        Ok((
            tx,
            self.proxy.route_subreceiver_to_new_crossbeam_receiver(rx)?,
        ))
    }
}

struct RouterProxyComm {
    msg_sender: Sender<RouterMsg>,
    wakeup_sender: SubSender<()>,
    shutdown: bool,
    handle: Option<JoinHandle<()>>,
}

/// Router runs in its own thread listening for events. Adds events to its SubReceiverSet
/// and listens for events using select().
struct Router {
    /// Get messages from RouterProxy.
    msg_receiver: Receiver<RouterMsg>,
    /// The ID/index of the special channel we use to identify messages from msg_receiver.
    msg_wakeup_id: u64,
    /// Set of all receivers which have been registered for us to select on.
    subreceiver_set: SubReceiverSet,
    /// Maps ids to their handler functions.
    handlers: HashMap<u64, RouterHandler>,
}

impl Router {
    fn new(msg_receiver: Receiver<RouterMsg>, wakeup_receiver: SubReceiver2<()>) -> Router {
        let mut subreceiver_set = SubReceiverSet::new().unwrap();
        let msg_wakeup_id = subreceiver_set.add(wakeup_receiver).unwrap();
        Router {
            msg_receiver,
            msg_wakeup_id,
            subreceiver_set,
            handlers: HashMap::new(),
        }
    }

    /// Continuously loop waiting for wakeup signals from router proxy.
    /// Iterate over events either:
    /// 1) If a message comes in from our special `wakeup_receiver` (identified through
    ///    msg_wakeup_id. Read message from `msg_receiver` and add a new receiver
    ///    to our receiver set.
    /// 2) Call appropriate handler based on message id.
    /// 3) Remove handler once channel closes.
    fn run(&mut self) {
        loop {
            // Wait for events to come from our select() new channels are added to
            // our ReceiverSet below.
            let Ok(results) = self.subreceiver_set.select() else {
                break;
            };

            // Iterate over numerous events that were ready at this time.
            for result in results {
                match result {
                    // Message came from the RouterProxy. Listen on our `msg_receiver`
                    // channel.
                    SubSelectionResult::MessageReceived(id, _) if id == self.msg_wakeup_id => {
                        match self.msg_receiver.recv().unwrap() {
                            RouterMsg::AddRoute(receiver, handler, ack_sender) => {
                                let new_receiver_id =
                                    self.subreceiver_set.add_opaque(receiver).unwrap();
                                self.handlers.insert(new_receiver_id, handler);
                                let _ = ack_sender.send(());
                            },
                            RouterMsg::Shutdown(sender) => {
                                sender
                                    .send(())
                                    .expect("Failed to send comfirmation of shutdown.");
                                return;
                            },
                        }
                    },
                    // Event from one of our registered receivers, call callback.
                    SubSelectionResult::MessageReceived(id, message) => {
                        self.handlers.get_mut(&id).unwrap()(message);
                    },
                    SubSelectionResult::ChannelClosed(id) => {
                        let _ = self.handlers.remove(&id).unwrap();
                    },
                }
            }
        }
    }
}

enum RouterMsg {
    /// Register the receiver OpaqueSubReceiver for listening for events on.
    /// When a message comes from this receiver, call RouterHandler.
    /// The Sender<()> is used to acknowledge that the route has been added.
    AddRoute(OpaqueSubReceiver2, RouterHandler, Sender<()>),
    /// Shutdown the router, providing a sender to send an acknowledgement.
    Shutdown(Sender<()>),
}

/// Function to call when a new event is received from the corresponding receiver.
pub type RouterHandler = Box<dyn FnMut(RawMessage) + Send>;

/// Like [RouterHandler] but includes the type that will be passed to the callback
pub type TypedRouterHandler<T> = Box<dyn FnMut(Result<T, MuxError>) + Send>;
