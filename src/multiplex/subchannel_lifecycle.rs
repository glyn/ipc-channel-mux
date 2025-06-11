// Copyright 2025 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module provides lifecycle management for subchannel senders.
//! It consists of two types:
//! * Source: a sourceId and a set of IpcSenderIds at the source
//! * Target: Keeps track of IpcSenderIds at rest and in flight
//! WIP: flesh this out concretely in multiplex.rs and then generalise
//!      it here?
//!
//! IDEA: Model this on counter.rs.

// TODO: should I pull in all the lifecycle-related code?


#[cfg(test)]
mod tests {
    use super::*;
}