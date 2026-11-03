// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::command::AppliedCommand;
use hyper_component::{Reply, Request};

impl Manager {
    pub(super) fn management(
        &self,
    ) -> Result<(
        OwnedHandle<CapabilityChannelObject>,
        OwnedHandle<CapabilityChannelObject>,
    )> {
        let (observer, observe) = CapabilityChannel::create()?;
        let (controller, control) = CapabilityChannel::create()?;
        for (listener, readonly) in [(observer, true), (controller, false)] {
            let (send, wake, snapshot) =
                (self.send.clone(), self.wake.clone(), self.snapshot.clone());
            let routes = self.lifecycle.topology.routes.clone();
            let paths: Vec<_> = self
                .lifecycle
                .topology
                .instances
                .iter()
                .map(|i| i.path.clone())
                .collect();
            std::thread::Builder::new()
                .name("component-management".into())
                .spawn(move || {
                    loop {
                        let mut call = match hyper_service_ipc::receive::<Request>(
                            &listener,
                            hyper_os::DEADLINE_INFINITE,
                        ) {
                            Ok(hyper_service_ipc::Admission::Accepted(c)) => c,
                            Err(Error::Status(hyper_os::Status::PEER_CLOSED)) => return,
                            Ok(hyper_service_ipc::Admission::Rejected(_)) => continue,
                            Err(error) => {
                                eprintln!("component management listener stopped: {error}");
                                return;
                            }
                        };
                        let mut error = None;
                        if !matches!(
                            call.request,
                            Request::Status { .. } | Request::Routes { .. }
                        ) {
                            if readonly {
                                error = Some("read-only component endpoint".into());
                            } else {
                                match send.try_send(Event::Command(call)) {
                                    Ok(()) => {
                                        let _ = wake.as_byte_channel().try_send(&[1]);
                                        continue;
                                    }
                                    Err(mpsc::TrySendError::Full(Event::Command(returned))) => {
                                        call = returned;
                                        error = Some(
                                            "management queue full; command not accepted".into(),
                                        );
                                    }
                                    Err(_) => return,
                                }
                            }
                        }
                        let snapshot = match snapshot.lock() {
                            Ok(snapshot) => snapshot.clone(),
                            Err(_) => return,
                        };
                        let mut response = Reply {
                            error,
                            snapshot,
                            routes: Vec::new(),
                        };
                        match &call.request {
                            Request::Status { path: Some(path) } => {
                                response.snapshot.components.retain(|c| c.path == *path);
                                if response.snapshot.components.is_empty() {
                                    response.error = Some("unknown component path".into());
                                }
                            }
                            Request::Routes { path, capability } => {
                                if let Some(index) = paths.iter().position(|p| p == path) {
                                    response.routes = routes
                                        .iter()
                                        .filter(|r| {
                                            r.consumer == index
                                                && capability
                                                    .as_ref()
                                                    .is_none_or(|n| *n == r.local_name)
                                        })
                                        .cloned()
                                        .collect();
                                    if capability.is_some() && response.routes.is_empty() {
                                        response.error = Some(
                                            "capability is not offered to this component".into(),
                                        );
                                    }
                                } else {
                                    response.error = Some("unknown component path".into());
                                }
                            }
                            _ => {}
                        }
                        let _ = call.respond(&response);
                    }
                })
                .map_err(|_| Error::Status(hyper_os::Status::NO_MEMORY))?;
        }
        Ok((observe.into_handle(), control.into_handle()))
    }
}

/// Replies are bounded independently of the lifecycle reactor. Dropping an
/// undeliverable receipt never drops the manager's Process or retirement owners.
pub(super) fn reply_worker() -> Result<mpsc::SyncSender<AppliedCommand>> {
    let (send, receive) = mpsc::sync_channel::<AppliedCommand>(8);
    std::thread::Builder::new()
        .name("component-command-reply".into())
        .spawn(move || {
            for AppliedCommand { call, receipt } in receive {
                let _ = call.respond(&Reply {
                    error: receipt.result.err(),
                    snapshot: receipt.snapshot,
                    routes: Vec::new(),
                });
            }
        })
        .map_err(|_| Error::Status(hyper_os::Status::NO_MEMORY))?;
    Ok(send)
}
