// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hyper_component_config::Transfer;
use hyper_os::{
    startup,
    task::{ProcessBuilder, StartFailure},
};

impl Manager {
    pub(super) fn launch(&mut self, index: usize, generation: u64) -> Result<()> {
        let instance = &self.lifecycle.topology.instances[index];
        let name = instance.path.clone();
        let program = instance
            .declaration
            .program
            .clone()
            .ok_or(Error::InvalidResponse)?;
        let (prepared, group, worker_group) = if program.prepared {
            let Prepared {
                builder,
                group,
                reply,
                deadline,
            } = self.prepared[index].take().ok_or(Error::MissingHandle)?;
            self.running[index].install_reply = Some((reply, deadline));
            (Some(builder), group, None)
        } else {
            let group = hyper_os::task::create_task_group(
                self.authority.factory.as_handle_ref(),
                self.authority.domain.as_handle_ref(),
            )?;
            let worker_group = group.duplicate(Rights::TASK_GROUP_ATTACH_PROCESS)?;
            (None, group, Some(worker_group))
        };
        self.running[index].group = Some(group);
        let mut grants = Vec::new();
        #[cfg(feature = "test-service-failure")]
        let mut failure_observer = None;
        #[cfg(feature = "test-service-failure")]
        if program.image == "/bin/service-probe"
            && program.arguments.first().is_some_and(|mode| {
                matches!(
                    mode.as_str(),
                    "stop-component" | "stop-component-then-report"
                )
            })
        {
            // The probe waits for a genuine Process termination. Supervision
            // remains linear in the manager; only a wait observer is exported.
            let target = program
                .arguments
                .get(1)
                .and_then(|path| self.lifecycle.topology.index(path))
                .ok_or(Error::InvalidResponse)?;
            let process = self.running[target]
                .process
                .as_ref()
                .ok_or(Error::MissingHandle)?;
            let (sender, receiver) = CapabilityChannel::create()?;
            failure_observer = Some((sender, Some(process.as_process_supervisor().observe()?)));
            grants.push((
                0x80ff_0011,
                Rights::READ.union(Rights::WAIT),
                Object::CapabilityChannel(Some(receiver.into_handle())),
            ));
        }
        if !program.prepared {
            for route in self
                .lifecycle
                .topology
                .routes
                .iter()
                .filter(|r| r.consumer == index && r.purpose.is_some())
            {
                let hyper_component_config::CapabilityType::Object { rights, .. } = route.kind
                else {
                    return Err(Error::InvalidResponse);
                };
                let rights = Rights::from_bits(rights).ok_or(Error::InvalidResponse)?;
                let object = self
                    .exports
                    .get_mut(&(route.provider, route.capability.clone()))
                    .ok_or(Error::MissingHandle)?;
                let value = if route.transfer == Transfer::Duplicate {
                    object.duplicate(rights.union(Rights::TRANSFER))?
                } else {
                    object.take_owned()?
                };
                grants.push((route.purpose.ok_or(Error::InvalidResponse)?, rights, value));
            }
        }
        let authority = self.authority.clone();
        let log = self.log_connector.clone();
        let canceled = self.running[index].canceled.clone();
        let (send, wake) = (self.send.clone(), self.wake.clone());
        std::thread::Builder::new()
            .name("component-start".into())
            .spawn(move || {
                let mut outgoing = None;
                let mut retirement = None;
                let mut phase = "prepare";
                let process = (|| {
                    let builder = match prepared {
                        Some(builder) => builder,
                        None => {
                            let image = authority.root.open(
                                &program.image,
                                hyper_service::process::EXECUTABLE_FILE_RIGHTS,
                            )?;
                            let loader = hyper_service::process::open_loader(
                                &authority.environment,
                                hyper_service::process::LoaderKind::Dynamic,
                            )?;
                            let group = worker_group.as_ref().ok_or(Error::MissingHandle)?;
                            let builder = ProcessBuilder::create(
                                authority.factory.as_handle_ref(),
                                group.as_handle_ref(),
                                authority.domain.as_handle_ref(),
                                image.as_handle_ref(),
                                loader.as_handle_ref(),
                            )?;
                            builder.set_name(name.rsplit('/').next().unwrap_or(&name))?;
                            builder.add_argument(&program.image)?;
                            for arg in &program.arguments {
                                builder.add_argument(arg)?;
                            }
                            builder.set_memory_lock_allowed(program.resident)?;
                            builder.add_handle_duplicate(
                                authority.libraries.as_handle_ref(),
                                startup::DYNAMIC_LIBRARY_DIRECTORY.as_raw(),
                                RightsOffer::Exact(
                                    Rights::READ.union(Rights::EXECUTE).union(Rights::PROTECT),
                                ),
                            )?;
                            for (purpose, rights, object) in &mut grants {
                                object.install(&builder, *purpose, *rights, Transfer::Move)?;
                            }
                            attach_stdio(
                                &builder,
                                &name,
                                generation,
                                log.as_deref().ok_or(Error::MissingHandle)?,
                            )?;
                            builder
                        }
                    };
                    phase = "attach connector";
                    outgoing = Some(attach_connector(
                        &builder,
                        index,
                        generation,
                        send.clone(),
                        wake.clone(),
                        canceled.clone(),
                    )?);
                    phase = "retain diagnostics";
                    retirement = Some(builder.retain_on_exit(8 * 1024 * 1024, 10_000_000_000)?);
                    phase = "seal";
                    builder.seal()?;
                    if canceled.load(std::sync::atomic::Ordering::Acquire) {
                        return Err(Error::Status(hyper_os::Status::BAD_STATE));
                    }
                    phase = "start";
                    let process = builder.start().map_err(|f| match f {
                        StartFailure::Rejected { error, .. } | StartFailure::Committed(error) => {
                            error
                        }
                    })?;
                    #[cfg(feature = "test-service-failure")]
                    if let Some((sender, observer)) = &mut failure_observer {
                        use hyper_os::capability_channel::CapabilityDisposition;
                        // Process observers deliberately cannot be retained in
                        // a ProcessBuilder. Transfer after the child is running.
                        let mut offers = [CapabilityDisposition::move_handle(
                            observer,
                            RightsOffer::Exact(Rights::WAIT),
                        )?];
                        hyper_service_ipc::send(
                            sender,
                            hyper_service_ipc::deadline()?,
                            b"observer",
                            &mut offers,
                        )?;
                    }
                    Ok(process)
                })();
                if process.is_err() {
                    // A committed start failure may leave no supervisor handle;
                    // dropping the separate control preauthorizes cleanup.
                    retirement.take();
                }
                emit(
                    &send,
                    &wake,
                    Event::Started {
                        index,
                        generation,
                        process,
                        retirement,
                        outgoing,
                        phase,
                    },
                );
            })
            .map_err(|_| Error::Status(hyper_os::Status::NO_MEMORY))?;
        self.running[index].construction = true;
        Ok(())
    }

    #[cfg(feature = "test-service-failure")]
    pub(super) fn inject_failure(&self, index: usize) -> Result<()> {
        let program = self.lifecycle.topology.instances[index]
            .declaration
            .program
            .as_ref()
            .ok_or(Error::InvalidResponse)?;
        if program.image == "/bin/service-probe"
            && program.arguments.first().is_some_and(|mode| {
                matches!(
                    mode.as_str(),
                    "stop-component" | "stop-component-then-report"
                )
            })
        {
            let target = program
                .arguments
                .get(1)
                .and_then(|path| self.lifecycle.topology.index(path))
                .ok_or(Error::InvalidResponse)?;
            let process = self.running[target]
                .process
                .as_ref()
                .ok_or(Error::MissingHandle)?;
            // Inject after the probe has started, so killing journal cannot
            // prevent the probe's own stdio registration. This deliberately
            // bypasses lifecycle.stop() to model an unexpected process death.
            process.as_process_supervisor().request_stop()?;
        }
        Ok(())
    }

    pub(super) fn install(&mut self, mut call: installation::Call) {
        let mut reply = Some(call.reply);
        let mut reply_objects = Vec::new();
        let result = (|| {
            let path = match &call.request {
                installation::Request::Prepared { path } => path,
                installation::Request::Export {
                    realm,
                    name,
                    kind,
                    rights,
                } => {
                    let index = self
                        .lifecycle
                        .topology
                        .index(realm)
                        .ok_or("unknown source realm")?;
                    let declaration = &self.lifecycle.topology.instances[index].declaration;
                    if declaration.program.is_some() {
                        return Err(
                            "persistent object exports belong to non-executable realms".into()
                        );
                    }
                    let cap = declaration
                        .capabilities
                        .iter()
                        .find(|c| c.name == *name)
                        .ok_or("object export not declared")?;
                    let hyper_component_config::CapabilityType::Object {
                        object_type,
                        rights: wanted,
                    } = cap.kind
                    else {
                        return Err("not an object capability".into());
                    };
                    let rights = Rights::from_bits(*rights).ok_or("invalid rights")?;
                    let required = Rights::from_bits(wanted)
                        .ok_or("invalid declared rights")?
                        .union(Rights::TRANSFER)
                        .union(if cap.transfer == Transfer::Duplicate {
                            Rights::DUPLICATE
                        } else {
                            Rights::NONE
                        });
                    if object_type != *kind
                        || !rights.contains(required)
                        || call.objects.len() != 1
                        || self.exports.contains_key(&(index, name.clone()))
                    {
                        return Err("invalid or duplicate object export".into());
                    }
                    self.exports
                        .insert((index, name.clone()), call.objects.remove(0));
                    return Ok(false);
                }
                installation::Request::Progress { milestone } => {
                    self.lifecycle.progress(milestone)?;
                    return Ok(false);
                }
                installation::Request::FinishDisplay { success } => {
                    self.update_display();
                    if let Some(display) = &mut self.display {
                        display.finish(*success);
                    }
                    super::display::startup_diagnostics(&mut self.startup_logs);
                    return Ok(false);
                }
                installation::Request::Logging => {
                    if self.log_connector.is_some() || call.objects.len() != 1 {
                        return Err("logging already installed".into());
                    }
                    let Object::CapabilityChannel(Some(log)) = call.objects.remove(0) else {
                        return Err("invalid log connector".into());
                    };
                    for (source, reader) in self.startup_logs.drain(..) {
                        hyper_journal::register(log.as_handle_ref(), &source, reader)
                            .map_err(|e| format!("startup logging: {e:?}"))?;
                    }
                    let (writer, reader) = hyper_os::channel::create_pair()
                        .map_err(|e| format!("manager logging: {e:?}"))?;
                    hyper_journal::register(
                        log.as_handle_ref(),
                        &hyper_journal::Source {
                            component: "/component-manager".into(),
                            generation: 1,
                            stream: hyper_journal::Stream::Log,
                        },
                        reader,
                    )
                    .map_err(|e| format!("manager logging: {e:?}"))?;
                    self.diagnostic = Some(writer);
                    self.log_connector = Some(Arc::new(log));
                    return Ok(false);
                }
                installation::Request::Management => {
                    if self.management_clients.is_none() {
                        self.management_clients = Some(
                            self.management()
                                .map_err(|e| format!("management: {e:?}"))?,
                        );
                    }
                    let (observe, control) = self
                        .management_clients
                        .as_ref()
                        .ok_or("management unavailable")?;
                    for handle in [observe, control] {
                        reply_objects.push((
                            Object::CapabilityChannel(Some(
                                handle
                                    .duplicate(hyper_service_ipc::CLIENT_RIGHTS)
                                    .map_err(|e| format!("management: {e:?}"))?,
                            )),
                            hyper_service_ipc::CLIENT_RIGHTS,
                        ));
                    }
                    return Ok(false);
                }
                installation::Request::Abort { path } => {
                    let index = self
                        .lifecycle
                        .topology
                        .index(path)
                        .ok_or("unknown component")?;
                    self.lifecycle
                        .stop(index, now().map_err(|e| format!("clock: {e:?}"))?);
                    return Ok(false);
                }
            };
            let index = self
                .lifecycle
                .topology
                .index(path)
                .ok_or("instance not declared")?;
            let status = self.lifecycle.status(index);
            if status.generation != 0 || !matches!(status.state, State::Stopped) {
                return Err("instance installation is already committed".into());
            }
            let program = self.lifecycle.topology.instances[index]
                .declaration
                .program
                .as_ref()
                .ok_or("instance has no executable")?;
            match &call.request {
                installation::Request::Prepared { .. } => {
                    if !program.prepared
                        || self.prepared[index].is_some()
                        || call.objects.len() != 2
                    {
                        return Err("invalid prepared installation".into());
                    }
                    let Object::Group(Some(group)) = call.objects.remove(1) else {
                        return Err("expected group".into());
                    };
                    let Object::Builder(Some(builder)) = call.objects.remove(0) else {
                        return Err("expected builder".into());
                    };
                    self.lifecycle.demand(index)?;
                    self.prepared[index] = Some(Prepared {
                        builder: ProcessBuilder::from_handle(builder),
                        group,
                        reply: reply.take().ok_or("missing installation reply")?,
                        deadline: call.deadline,
                    });
                    return Ok(true);
                }
                installation::Request::Export { .. }
                | installation::Request::Logging
                | installation::Request::FinishDisplay { .. }
                | installation::Request::Progress { .. }
                | installation::Request::Management
                | installation::Request::Abort { .. } => {}
            }
            Ok(false)
        })();
        match result {
            Ok(true) => {}
            Ok(false) => {
                if let Some(reply) = reply {
                    self.installation_reply(reply, call.deadline, Ok(reply_objects));
                }
            }
            Err(error) => {
                if let Some(reply) = reply {
                    self.installation_reply(reply, call.deadline, Err(error));
                }
            }
        }
    }
}

fn attach_connector(
    builder: &ProcessBuilder,
    index: usize,
    generation: u64,
    send: mpsc::SyncSender<Event>,
    wake: Arc<OwnedHandle<ByteChannelObject>>,
    canceled: Arc<std::sync::atomic::AtomicBool>,
) -> Result<CapabilityChannel> {
    let (listener, client) = CapabilityChannel::create()?;
    let (outgoing, receiver) = CapabilityChannel::create()?;
    builder
        .add_handle_move(
            client.into_handle(),
            hyper_component::CONNECTOR.as_raw(),
            RightsOffer::Exact(hyper_service_ipc::CLIENT_RIGHTS),
        )
        .map_err(|f| f.error())?;
    builder
        .add_handle_move(
            receiver.into_handle(),
            hyper_component::OUTGOING.as_raw(),
            RightsOffer::Exact(hyper_service_ipc::SERVER_RIGHTS),
        )
        .map_err(|f| f.error())?;
    std::thread::Builder::new()
        .name("component-connector".into())
        .spawn(move || {
            loop {
                if canceled.load(std::sync::atomic::Ordering::Acquire) {
                    return;
                }
                let Ok(until) =
                    hyper_os::time::deadline_after(std::time::Duration::from_millis(250))
                else {
                    return;
                };
                match connection::receive(&listener, until.as_raw()) {
                    Ok(call) => {
                        // Per-instance receivers cannot grow an unbounded queue.
                        // Backpressure parks only this connector's worker.
                        emit(
                            &send,
                            &wake,
                            Event::Connection {
                                index,
                                generation,
                                call,
                            },
                        );
                    }
                    Err(Error::Status(hyper_os::Status::PEER_CLOSED)) => return,
                    Err(_) => continue,
                }
            }
        })
        .map_err(|_| Error::Status(hyper_os::Status::NO_MEMORY))?;
    Ok(outgoing)
}

fn attach_stdio(
    builder: &ProcessBuilder,
    path: &str,
    generation: u64,
    log: &OwnedHandle<CapabilityChannelObject>,
) -> Result<()> {
    let (input, eof) = hyper_os::channel::create_pair()?;
    drop(eof);
    builder
        .add_handle_move(
            input,
            hyper_service::stdio::STANDARD_INPUT.as_raw(),
            RightsOffer::Exact(
                Rights::READ
                    .union(Rights::WAIT)
                    .union(Rights::DUPLICATE)
                    .union(Rights::TRANSFER),
            ),
        )
        .map_err(|f| f.error())?;
    for (purpose, stream) in [
        (
            hyper_service::stdio::STANDARD_OUTPUT,
            hyper_journal::Stream::Stdout,
        ),
        (
            hyper_service::stdio::STANDARD_ERROR,
            hyper_journal::Stream::Stderr,
        ),
    ] {
        let (writer, reader) = hyper_os::channel::create_pair()?;
        hyper_journal::register(
            log.as_handle_ref(),
            &hyper_journal::Source {
                component: path.into(),
                generation,
                stream,
            },
            reader,
        )?;
        builder
            .add_handle_move(
                writer,
                purpose.as_raw(),
                RightsOffer::Exact(
                    Rights::WRITE
                        .union(Rights::WAIT)
                        .union(Rights::DUPLICATE)
                        .union(Rights::TRANSFER),
                ),
            )
            .map_err(|f| f.error())?;
    }
    Ok(())
}
