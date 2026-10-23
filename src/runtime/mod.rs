// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

pub mod boot;
mod display;
mod launch;
mod management;
mod routing;
mod scheduling;
mod startup_log;

use crate::lifecycle::Lifecycle;
use hyper_component::{Snapshot, State, connection, installation, objects::Object};
use hyper_os::{
    Error, Result, capability_channel::CapabilityChannel, handle::*, task::ProcessPhase,
};
use std::sync::{Arc, Mutex, mpsc};

pub(super) const MAX_EVENTS: usize = 32;
pub(super) const MAX_PENDING: usize = 64;

pub(super) enum Event {
    Connection {
        index: usize,
        generation: u64,
        call: connection::Call,
    },
    Install(installation::Call),
    Started {
        index: usize,
        generation: u64,
        process: Result<OwnedHandle<ProcessObject>>,
        retirement: Option<hyper_os::task::ProcessRetirement>,
        outgoing: Option<CapabilityChannel>,
        phase: &'static str,
    },
    Command {
        request: hyper_component::Request,
        reply: mpsc::SyncSender<std::result::Result<(), String>>,
    },
}

#[derive(Default)]
pub(super) struct Running {
    process: Option<OwnedHandle<ProcessObject>>,
    retirement: Option<hyper_os::task::ProcessRetirement>,
    group: Option<OwnedHandle<TaskGroupObject>>,
    construction: bool,
    canceled: Arc<std::sync::atomic::AtomicBool>,
    outgoing: Option<CapabilityChannel>,
    install_reply: Option<(CapabilityChannel, u64)>,
    termination_reported: bool,
}

pub struct Authority {
    pub root: hyper_os::fs::Directory,
    pub libraries: hyper_os::fs::Directory,
    pub factory: OwnedHandle<TaskFactoryObject>,
    pub domain: OwnedHandle<ResourceDomainObject>,
    pub environment: hyper_os::fs::Directory,
}

pub(super) struct Prepared {
    builder: hyper_os::task::ProcessBuilder,
    group: OwnedHandle<TaskGroupObject>,
    reply: CapabilityChannel,
    deadline: u64,
}

pub struct Manager {
    display: Option<display::BootDisplay>,
    startup_logs: Vec<(hyper_journal::Source, OwnedHandle<ByteChannelObject>)>,
    lifecycle: Lifecycle,
    authority: Arc<Authority>,
    running: Vec<Running>,
    prepared: Vec<Option<Prepared>>,
    exports: std::collections::BTreeMap<(usize, String), Object>,
    pending: Vec<routing::Pending>,
    events: mpsc::Receiver<Event>,
    send: mpsc::SyncSender<Event>,
    wake: Arc<OwnedHandle<ByteChannelObject>>,
    wake_reader: OwnedHandle<ByteChannelObject>,
    snapshot: Arc<Mutex<Snapshot>>,
    replies: mpsc::SyncSender<InstallationReply>,
    diagnostic: Option<OwnedHandle<ByteChannelObject>>,
    diagnostics: crate::diagnostics::Diagnostics,
    log_connector: Option<Arc<OwnedHandle<CapabilityChannelObject>>>,
    management_clients: Option<(
        OwnedHandle<CapabilityChannelObject>,
        OwnedHandle<CapabilityChannelObject>,
    )>,
}

impl Manager {
    pub fn new(lifecycle: Lifecycle, authority: Authority) -> Result<Self> {
        let count = lifecycle.topology.instances.len();
        let snapshot = Arc::new(Mutex::new(lifecycle.snapshot(now()?)));
        let (send, events) = mpsc::sync_channel(MAX_EVENTS);
        let (wake, wake_reader) = hyper_os::channel::create_pair()?;
        let (replies, reply_receiver) = mpsc::sync_channel::<InstallationReply>(8);
        std::thread::Builder::new()
            .name("component-install-reply".into())
            .spawn(move || {
                for reply in reply_receiver {
                    reply.send();
                }
            })
            .map_err(|_| Error::Status(hyper_os::Status::NO_MEMORY))?;
        Ok(Self {
            display: None,
            startup_logs: Vec::new(),
            replies,
            diagnostic: None,
            diagnostics: crate::diagnostics::Diagnostics::default(),
            log_connector: None,
            lifecycle,
            authority: Arc::new(authority),
            running: (0..count).map(|_| Running::default()).collect(),
            prepared: (0..count).map(|_| None).collect(),
            exports: std::collections::BTreeMap::new(),
            pending: Vec::new(),
            events,
            send,
            wake: Arc::new(wake),
            wake_reader,
            snapshot,
            management_clients: None,
        })
    }

    pub fn run(&mut self) -> Result<()> {
        let mut scratch = [0; 128];
        loop {
            while self
                .wake_reader
                .as_byte_channel()
                .try_receive(&mut scratch)
                .is_ok()
            {}
            for _ in 0..MAX_EVENTS {
                let Ok(event) = self.events.try_recv() else {
                    break;
                };
                self.event(event)?;
            }
            self.observe()?;
            let prepared = &self.prepared;
            let topology = &self.lifecycle.topology;
            let ready: Vec<_> = topology
                .instances
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    c.declaration.program.as_ref().is_some_and(|p| {
                        (p.prepared && prepared[i].is_some())
                            || (!p.prepared
                                && self.log_connector.is_some()
                                && topology
                                    .routes
                                    .iter()
                                    .filter(|r| r.consumer == i && r.purpose.is_some())
                                    .all(|r| {
                                        self.exports
                                            .contains_key(&(r.provider, r.capability.clone()))
                                    }))
                    })
                })
                .collect();
            for (index, generation) in self.lifecycle.begin_ready(now()?, |i| ready[i]) {
                if let Err(e) = self.launch(index, generation) {
                    self.report(index, &format!("launch failed: {e:?}"), true);
                    self.lifecycle
                        .fail(index, generation, &format!("launch: {e:?}"), now()?);
                    // observe() joins the construction group before releasing
                    // this generation, including failures before Process start.
                }
            }
            self.dispatch()?;
            self.flush_diagnostics();
            self.update_display();
            *self.snapshot.lock().map_err(|_| Error::InvalidResponse)? =
                self.lifecycle.snapshot(now()?);
            self.wait()?;
        }
    }

    fn event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::Connection {
                index,
                generation,
                call,
            } => self.connection(index, generation, call),
            Event::Install(call) => self.install(call),
            Event::Command { request, reply } => {
                let result = self.command(request);
                let _ = reply.try_send(result);
            }
            Event::Started {
                index,
                generation,
                process,
                retirement,
                outgoing,
                phase,
            } => {
                self.running[index].outgoing = outgoing;
                let current = self.lifecycle.status(index).generation == generation;
                if !current {
                    return Err(Error::InvalidResponse);
                }
                self.running[index].construction = false;
                self.running[index].retirement = retirement;
                match process {
                    Ok(process) => {
                        let koid = process.basic_info()?.koid.get();
                        if self.lifecycle.started(index, generation, koid).is_err() {
                            let _ = process.as_process_supervisor().request_stop();
                        }
                        if let Some((reply, deadline)) = self.running[index].install_reply.take() {
                            let observer = process.as_process_supervisor().observe()?;
                            self.installation_reply(
                                reply,
                                deadline,
                                Ok(vec![(
                                    Object::Process(Some(observer)),
                                    Rights::WAIT.union(Rights::TRANSFER),
                                )]),
                            );
                        }
                        self.running[index].process = Some(process);
                        #[cfg(feature = "test-service-failure")]
                        self.inject_failure(index)?;
                    }
                    Err(error) => {
                        self.report(
                            index,
                            &format!("process start failed during {phase}: {error:?}"),
                            true,
                        );
                        self.lifecycle.fail(
                            index,
                            generation,
                            &format!("process start: {error:?}"),
                            now()?,
                        );
                        if let Some((reply, deadline)) = self.running[index].install_reply.take() {
                            self.installation_reply(
                                reply,
                                deadline,
                                Err(format!("process start: {error:?}")),
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn observe(&mut self) -> Result<()> {
        for i in 0..self.running.len() {
            let generation = self.lifecycle.status(i).generation;
            if self.lifecycle.startup_expired(i, now()?) {
                self.report(i, "startup deadline exceeded", true);
                self.lifecycle
                    .fail(i, generation, "startup deadline exceeded", now()?);
            }
            let info = self.running[i]
                .process
                .as_ref()
                .map(|p| p.as_process_supervisor().info())
                .transpose()?;
            let terminal = info.as_ref().is_some_and(|p| p.terminal.is_some());
            // Termination has begun, but teardown may still be blocked in a
            // failed provider. Report now, while retaining every resource owner.
            if terminal && !self.running[i].termination_reported {
                let message = match info.as_ref().and_then(|p| p.terminal.as_ref()) {
                    Some(reason) => format!("terminated: {reason:?}"),
                    None => "terminated: reason unavailable".into(),
                };
                let success = info.as_ref().is_some_and(process_succeeded);
                let report = if success && !self.critical(i) {
                    "exited successfully (status 0); releasing resources"
                } else {
                    &message
                };
                self.report(i, report, !success);
                self.running[i].termination_reported = true;
                self.lifecycle.exiting(i, generation);
                self.lifecycle.exit_detail(i, generation, &message);
            }
            if terminal && let Some(retirement) = self.running[i].retirement.as_ref() {
                let observed = retirement.info().and_then(|retained| {
                    if retained.ready() || retained.released() {
                        retirement.release().map(|()| Some(retained))
                    } else {
                        Ok(None)
                    }
                });
                match observed {
                    Ok(Some(retained)) => {
                        self.running[i].retirement.take();
                        if !info.as_ref().is_some_and(process_succeeded) {
                            self.report(i, &format!(
                                "postmortem: fault={} pc=0x{:x} address=0x{:x} retained={} bytes",
                                retained.fault_class, retained.fault_pc,
                                retained.fault_address, retained.retained_bytes,
                            ), true);
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        // Losing diagnostics must not terminate the manager or
                        // hold a failed process forever. Dropping its unique
                        // control is the kernel's cleanup fallback.
                        self.running[i].retirement.take();
                        self.report(i, &format!("postmortem unavailable: {error:?}"), true);
                    }
                }
            }
            let instance = &mut self.running[i];
            let state = self.lifecycle.status(i).state;
            if state == State::Stopping || terminal {
                instance
                    .canceled
                    .store(true, std::sync::atomic::Ordering::Release);
                if let Some(group) = &instance.group {
                    let _ = group.as_task_group().request_stop();
                }
                if let Some(process) = &instance.process {
                    let _ = process.as_process_supervisor().request_stop();
                }
                instance.outgoing.take();
            }
            if instance.construction
                || info
                    .as_ref()
                    .is_some_and(|p| p.phase != ProcessPhase::Retired)
            {
                continue;
            }
            if !matches!(state, State::Starting | State::Running | State::Stopping) {
                continue;
            }
            if instance.process.is_none() && state != State::Stopping {
                continue;
            }
            if let Some(group) = &instance.group {
                match group.as_task_group().wait_terminated(now()?) {
                    Ok(()) => {}
                    Err(Error::Status(hyper_os::Status::TIMED_OUT)) => continue,
                    Err(error) => return Err(error),
                }
            }
            let success = info.as_ref().is_some_and(process_succeeded);
            self.lifecycle
                .retired(i, generation, success, now()?)
                .map_err(|_| Error::InvalidResponse)?;
            *instance = Running::default();
        }
        Ok(())
    }

    fn wait(&self) -> Result<()> {
        use hyper_os::wait::*;
        let mut items = vec![WaitItem::new(
            self.wake_reader.as_handle_ref(),
            ObjectSignals::<ByteChannelObject>::READABLE,
        )];
        for instance in &self.running {
            if items.len() == hyper_os::wait::MAX_ITEMS {
                break;
            }
            if let Some(p) = &instance.process
                && p.as_process_supervisor()
                    .info()
                    .is_ok_and(|i| matches!(i.phase, ProcessPhase::Created | ProcessPhase::Running))
            {
                items.push(WaitItem::new(
                    p.as_handle_ref(),
                    ObjectSignals::<ProcessObject>::TERMINATED,
                ));
            }
        }
        let until = hyper_os::time::deadline_after(std::time::Duration::from_millis(
            if self.pending.is_empty() && !self.diagnostics.pending() {
                100
            } else {
                10
            },
        ))?
        .as_raw();
        match wait_many(&items, until) {
            Ok(_) | Err(Error::Status(hyper_os::Status::TIMED_OUT)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn critical(&self, index: usize) -> bool {
        self.lifecycle.topology.instances[index]
            .declaration
            .program
            .as_ref()
            .is_some_and(|p| p.critical)
    }

    fn report(&mut self, index: usize, message: &str, failure: bool) {
        let status = self.lifecycle.status(index);
        let critical = self.critical(index);
        let severity = if critical {
            "CRITICAL"
        } else if failure {
            "ERROR"
        } else {
            "INFO"
        };
        self.diagnostics.push(
            format!(
                "{severity}: {} generation={} {message}; process=0x{:x}",
                status.path,
                status.generation,
                status.process.unwrap_or(0),
            ),
            critical,
        );
    }

    fn flush_diagnostics(&mut self) {
        let console = hyper_rt::process::console();
        self.diagnostics.flush(
            |bytes| {
                self.diagnostic
                    .as_ref()
                    .is_some_and(|log| log.as_byte_channel().try_send(bytes).is_ok())
            },
            |bytes| console.as_ref().ok()?.as_console().try_write(bytes).ok(),
        );
    }
}

fn now() -> Result<u64> {
    Ok(hyper_os::time::monotonic_now()?.as_nanoseconds())
}

fn emit(send: &mpsc::SyncSender<Event>, wake: &OwnedHandle<ByteChannelObject>, event: Event) {
    if send.send(event).is_ok() {
        let _ = wake.as_byte_channel().try_send(&[1]);
    }
}

struct InstallationReply {
    channel: CapabilityChannel,
    deadline: u64,
    result: std::result::Result<Vec<(Object, Rights)>, String>,
}
impl InstallationReply {
    fn send(self) {
        let (mut objects, error) = match self.result {
            Ok(p) => (p, None),
            Err(e) => (Vec::new(), Some(e)),
        };
        let Ok(bytes) = serde_json::to_vec(&installation::Reply { error }) else {
            return;
        };
        let mut offers = Vec::new();
        for (object, rights) in &mut objects {
            let Ok(offer) = object.move_offer(*rights) else {
                return;
            };
            offers.push(offer);
        }
        let _ = hyper_service_ipc::send(&self.channel, self.deadline, &bytes, &mut offers);
    }
}
impl Manager {
    fn installation_reply(
        &self,
        channel: CapabilityChannel,
        deadline: u64,
        result: std::result::Result<Vec<(Object, Rights)>, String>,
    ) {
        // Overflow closes this reply endpoint; the caller observes failure. The
        // Process/resource owner remains in the manager after a committed start.
        let _ = self.replies.try_send(InstallationReply {
            channel,
            deadline,
            result,
        });
    }
}

fn process_succeeded(info: &hyper_os::task::ProcessInfo) -> bool {
    use hyper_os::task::ProcessTermination;
    matches!(
        info.terminal,
        Some(
            ProcessTermination::ProcessExited { status: 0 }
                | ProcessTermination::LastThreadExited { status: 0 }
                | ProcessTermination::ThreadExited { status: 0 }
        )
    )
}
