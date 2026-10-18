// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
use connection::{Rejection, RejectionKind};
use hyper_component_config::CapabilityType;
use hyper_os::capability_channel::CapabilityDisposition;

pub(super) struct Pending {
    consumer: usize,
    consumer_generation: u64,
    provider: usize,
    provider_generation: u64,
    capability: String,
    until: u64,
    session: Option<OwnedHandle<CapabilityChannelObject>>,
    reply: OwnedHandle<ByteChannelObject>,
}

impl Manager {
    pub(super) fn connection(&mut self, index: usize, generation: u64, call: connection::Call) {
        let result = (|| {
            if !self.lifecycle.is_current(index, generation) {
                return Err(Rejection::new(
                    RejectionKind::Unavailable,
                    "caller execution is no longer active",
                ));
            }
            match &call.request {
                connection::Request::Publish { name } => {
                    self.lifecycle.publish(index, generation, name)
                }
                connection::Request::Status { message } => {
                    self.lifecycle.detail(index, generation, message)
                }
                connection::Request::ReportFailure { message } => {
                    self.lifecycle.detail(index, generation, message).map(|()| {
                        self.report(index, &format!("worker failure: {message}"), true);
                    })
                }
                connection::Request::Connect { .. } => return Ok(false),
            }
            .map_err(|message| Rejection::new(RejectionKind::InvalidRequest, message))?;
            Ok(true)
        })();
        match result {
            Err(error) => connection::respond(call.reply, Some(error)),
            Ok(true) => connection::respond(call.reply, None),
            Ok(false) => self.connect(index, generation, call),
        }
    }

    fn connect(&mut self, index: usize, generation: u64, call: connection::Call) {
        let connection::Request::Connect { name, deadline_ns } = &call.request else {
            return;
        };
        let result = (|| {
            let route = self
                .lifecycle
                .topology
                .route(index, name)
                .ok_or_else(|| {
                    Rejection::new(
                        RejectionKind::NotOffered,
                        "capability is not offered to this component",
                    )
                })?
                .clone();
            if !matches!(route.kind, CapabilityType::Protocol { .. }) {
                return Err(Rejection::new(
                    RejectionKind::InvalidRequest,
                    "requested capability is not a protocol",
                ));
            }
            if self.pending.len() >= MAX_PENDING
                || self.pending.iter().filter(|p| p.consumer == index).count() >= 8
            {
                return Err(Rejection::new(
                    RejectionKind::ResourceLimit,
                    "connection queue is full",
                ));
            }
            let time = now()
                .map_err(|e| Rejection::new(RejectionKind::Unavailable, format!("clock: {e:?}")))?;
            if *deadline_ns <= time {
                return Err(Rejection::new(
                    RejectionKind::TimedOut,
                    "connection deadline exceeded",
                ));
            }
            if self.lifecycle.status(route.provider).state == State::RestartWait {
                return Err(Rejection::new(
                    RejectionKind::Restarting,
                    "restart backoff is active",
                ));
            }
            let epoch = self
                .lifecycle
                .demand(route.provider)
                .map_err(|message| Rejection::new(RejectionKind::Unavailable, message))?;
            Ok((
                route,
                epoch,
                (*deadline_ns).min(time.saturating_add(60_000_000_000)),
            ))
        })();
        match result {
            Err(error) => connection::respond(call.reply, Some(error)),
            Ok((route, epoch, until)) => self.pending.push(Pending {
                consumer: index,
                consumer_generation: generation,
                provider: route.provider,
                provider_generation: epoch,
                capability: route.capability,
                until,
                session: call.session,
                reply: call.reply,
            }),
        }
    }

    pub(super) fn dispatch(&mut self) -> Result<()> {
        let time = now()?;
        let mut index = 0;
        while index < self.pending.len() {
            let pending = &mut self.pending[index];
            let status = self.lifecycle.status(pending.provider);
            let error = if time >= pending.until {
                Some(Rejection::new(
                    RejectionKind::TimedOut,
                    "provider connection deadline exceeded",
                ))
            } else if !self
                .lifecycle
                .is_current(pending.consumer, pending.consumer_generation)
            {
                Some(Rejection::new(
                    RejectionKind::Unavailable,
                    "caller execution ended",
                ))
            } else if status.state == State::RestartWait {
                Some(Rejection::new(
                    RejectionKind::Restarting,
                    "provider is restarting",
                ))
            } else if status.generation > pending.provider_generation
                || matches!(
                    status.state,
                    State::Failed | State::Stopping | State::RestartWait | State::Completed
                )
                || (status.generation == pending.provider_generation
                    && status.state == State::Stopped)
            {
                Some(Rejection::new(
                    RejectionKind::Unavailable,
                    "provider execution unavailable",
                ))
            } else {
                None
            };
            let result = if let Some(error) = error {
                Some(Err(error))
            } else if status.state == State::Running
                && status.generation == pending.provider_generation
                && status.published.contains(&pending.capability)
            {
                if let Some(outgoing) = &self.running[pending.provider].outgoing {
                    let bytes = serde_json::to_vec(&connection::Delivery {
                        capability: pending.capability.clone(),
                    })
                    .map_err(|_| Error::InvalidResponse)?;
                    let offer = CapabilityDisposition::move_handle(
                        &mut pending.session,
                        RightsOffer::Exact(connection::SESSION_RIGHTS),
                    )?;
                    match outgoing.try_send(&bytes, &mut [offer]) {
                        Ok(()) => Some(Ok(())),
                        Err(Error::Status(hyper_os::Status::WOULD_BLOCK)) => None,
                        Err(e) => Some(Err(Rejection::new(
                            RejectionKind::Unavailable,
                            format!("provider endpoint: {e:?}"),
                        ))),
                    }
                } else {
                    Some(Err(Rejection::new(
                        RejectionKind::Unavailable,
                        "provider endpoint unavailable",
                    )))
                }
            } else {
                None
            };
            if let Some(result) = result {
                let pending = self.pending.remove(index);
                connection::respond(pending.reply, result.err());
            } else {
                index += 1;
            }
        }
        Ok(())
    }
}
