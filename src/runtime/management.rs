// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
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
                        let call = match hyper_service_ipc::receive::<Request>(
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
                        let mut committed = None;
                        if !matches!(
                            call.request,
                            Request::Status { .. } | Request::Routes { .. }
                        ) {
                            if readonly {
                                error = Some("read-only component endpoint".into());
                            } else {
                                let (reply, response) = mpsc::sync_channel(1);
                                if send
                                    .try_send(Event::Command {
                                        request: call.request.clone(),
                                        reply,
                                    })
                                    .is_err()
                                {
                                    error = Some("management queue full".into());
                                } else {
                                    let _ = wake.as_byte_channel().try_send(&[1]);
                                    error = match response
                                        .recv_timeout(std::time::Duration::from_secs(4))
                                    {
                                        Ok(receipt) => {
                                            committed = Some(receipt.snapshot);
                                            receipt.result.err()
                                        }
                                        Err(_) => Some(
                                            "operation pending; inspect state before retrying"
                                                .into(),
                                        ),
                                    };
                                }
                            }
                        }
                        let snapshot = match committed {
                            Some(snapshot) => snapshot,
                            None => match snapshot.lock() {
                                Ok(snapshot) => snapshot.clone(),
                                Err(_) => return,
                            },
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
