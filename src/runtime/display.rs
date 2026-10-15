// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Temporary line-oriented boot presentation alongside ordinary service logs.

use super::{Manager, now};
use hyper_component::{Snapshot, State};
use hyper_component_config::Topology;
use hyper_os::{Error, Result, Status, handle::*, wait::*};

mod render;
use render::Renderer;

pub(super) struct BootDisplay {
    renderer: Renderer,
    coordinator: usize,
    until: u64,
    active: bool,
}

impl BootDisplay {
    pub(super) fn begin(coordinator: usize, topology: &Topology) -> Result<Self> {
        let began = now()?;
        let until = began.saturating_add(60_000_000_000);
        let mut display = Self {
            renderer: Renderer::new(began, topology),
            coordinator,
            until,
            active: true,
        };
        if write(display.renderer.banner().as_bytes()).is_err() {
            display.finish(false);
        }
        Ok(display)
    }

    fn update(&mut self, snapshot: &Snapshot) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        let frame = self.renderer.update(snapshot);
        if frame.is_empty() {
            Ok(())
        } else {
            write(frame.as_bytes())
        }
    }

    pub(super) fn finish(&mut self, success: bool) {
        if self.active {
            self.active = false;
            let time = now().unwrap_or_else(|_| self.until.saturating_sub(60_000_000_000));
            let _ = write(self.renderer.finish(time, success).as_bytes());
        }
    }
}

impl Manager {
    pub(super) fn update_display(&mut self) {
        let Some(display) = &mut self.display else {
            return;
        };
        if !display.active {
            return;
        }
        if let Ok(time) = now() {
            let snapshot = self.lifecycle.snapshot(time);
            let coordinator = self.lifecycle.status(display.coordinator).state;
            if display.update(&snapshot).is_err()
                || matches!(
                    coordinator,
                    State::Failed | State::Completed | State::Stopped
                )
                || time >= display.until
            {
                display.finish(false);
                startup_diagnostics(&mut self.startup_logs);
            }
        }
    }
}

/// A slow UART must not indefinitely block lifecycle processing. Writes enter
/// the same kernel FIFO as raw output; enqueue order survives the handoff.
fn write(mut bytes: &[u8]) -> Result<()> {
    let console = hyper_rt::process::console()?.as_console();
    let until = now()?.saturating_add(500_000_000);
    while !bytes.is_empty() {
        match console.try_write(bytes) {
            Ok(n) => bytes = &bytes[n..],
            Err(Error::Status(Status::WOULD_BLOCK)) => {
                wait_many(
                    &[WaitItem::new(
                        console.as_handle_ref(),
                        ObjectSignals::<ConsoleObject>::WRITABLE,
                    )],
                    until,
                )?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Preflight or journal startup failures still need a visible diagnosis. This
/// only drains already buffered bytes; it never waits for a failed producer.
pub(super) fn startup_diagnostics(
    logs: &mut Vec<(hyper_journal::Source, OwnedHandle<ByteChannelObject>)>,
) {
    let mut bytes = [0; 4096];
    for (_, reader) in logs.drain(..) {
        for _ in 0..16 {
            let Ok(n) = reader.as_byte_channel().try_receive(&mut bytes) else {
                break;
            };
            let text = String::from_utf8_lossy(&bytes[..n]).replace('\n', "\r\n");
            if write(text.as_bytes()).is_err() {
                break;
            }
        }
    }
}
