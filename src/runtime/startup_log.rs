// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Drain coordinator stdio before journal exists. Leaving its channels unread
//! could fill the message quota during an adoption error and prevent journal
//! startup itself. These two temporary relays retire with their producer.

use hyper_os::{Error, Result, Status, handle::*, wait::*};
use std::collections::VecDeque;

const LIMIT: usize = 65_536;

pub(super) fn capture(
    input: OwnedHandle<ByteChannelObject>,
) -> Result<OwnedHandle<ByteChannelObject>> {
    let (writer, reader) = hyper_os::channel::create_pair()?;
    std::thread::Builder::new()
        .name("startup-log".into())
        .spawn(move || {
            let _ = relay(input, writer);
        })
        .map_err(|_| Error::Status(Status::NO_MEMORY))?;
    Ok(reader)
}

fn relay(
    input: OwnedHandle<ByteChannelObject>,
    output: OwnedHandle<ByteChannelObject>,
) -> Result<()> {
    let mut pending = VecDeque::new();
    let mut scratch = vec![0; hyper_os::channel::MAX_MESSAGE_BYTES];
    let mut closed = false;
    let mut dropped = 0_u64;
    loop {
        // Bound each turn; a noisy producer must not starve the output queue.
        for _ in 0..8 {
            if closed {
                break;
            }
            match input.as_byte_channel().try_receive(&mut scratch) {
                Ok(n) if n <= LIMIT - pending.len() => pending.extend(&scratch[..n]),
                Ok(n) => dropped = dropped.saturating_add(n as u64),
                Err(Error::Status(Status::WOULD_BLOCK)) => break,
                Err(Error::Status(Status::PEER_CLOSED)) => closed = true,
                Err(error) => return Err(error),
            }
        }
        if pending.is_empty() && dropped != 0 {
            pending.extend(format!("\n[startup log: {dropped} bytes dropped]\n").bytes());
            dropped = 0;
        }
        if !pending.is_empty() {
            let bytes = pending.as_slices().0;
            let n = bytes.len().min(4096);
            match output.as_byte_channel().try_send(&bytes[..n]) {
                Ok(()) => {
                    pending.drain(..n);
                }
                Err(Error::Status(Status::WOULD_BLOCK)) => {}
                Err(error) => return Err(error),
            }
        }
        if closed && pending.is_empty() && dropped == 0 {
            return Ok(());
        }
        let mut wait = Vec::with_capacity(2);
        if !closed {
            wait.push(WaitItem::new(
                input.as_handle_ref(),
                ObjectSignals::<ByteChannelObject>::READABLE
                    .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
            ));
        }
        let mut signals = ObjectSignals::<ByteChannelObject>::PEER_CLOSED;
        if !pending.is_empty() || dropped != 0 {
            signals = signals.union(ObjectSignals::<ByteChannelObject>::WRITABLE);
        }
        wait.push(WaitItem::new(output.as_handle_ref(), signals));
        let observation = wait_many(&wait, hyper_os::DEADLINE_INFINITE)?;
        if observation.index == wait.len() - 1
            && ObjectSignals::<ByteChannelObject>::PEER_CLOSED.is_present_in(observation.observed)
        {
            return Ok(());
        }
    }
}
