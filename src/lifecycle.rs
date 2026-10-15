// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use hyper_component::{ComponentStatus, Snapshot, State};
use hyper_component_config::{CapabilityType, Restart, Startup, Topology, Transfer};

struct Execution {
    status: ComponentStatus,
    deadline: u64,
    desired: bool,
    restart_requested: bool,
    consumed: bool,
    failed: bool,
}

/// One owner arbitrates start/stop/exit. A run remains Stopping until its process,
/// construction worker and descendants have all retired. Every asynchronous event
/// must carry the generation returned by `begin()`, including export publication.
pub struct Lifecycle {
    pub topology: Topology,
    executions: Vec<Execution>,
    revision: u64,
    progress: Vec<String>,
}

impl Lifecycle {
    pub fn new(topology: Topology) -> Self {
        let executions = topology
            .instances
            .iter()
            .map(|i| Execution {
                status: ComponentStatus {
                    path: i.path.clone(),
                    state: State::Stopped,
                    generation: 0,
                    process: None,
                    restarts: 0,
                    detail: String::new(),
                    published: Vec::new(),
                },
                deadline: 0,
                desired: i
                    .declaration
                    .program
                    .as_ref()
                    .is_some_and(|p| p.startup == Startup::Eager),
                restart_requested: false,
                consumed: false,
                failed: false,
            })
            .collect();
        Self {
            topology,
            executions,
            revision: 0,
            progress: Vec::new(),
        }
    }

    pub fn status(&self, index: usize) -> &ComponentStatus {
        &self.executions[index].status
    }
    pub fn snapshot(&self, now: u64) -> Snapshot {
        Snapshot {
            revision: self.revision,
            captured_at_ns: now,
            components: self.executions.iter().map(|e| e.status.clone()).collect(),
            progress: self.progress.clone(),
        }
    }
    pub fn is_current(&self, index: usize, generation: u64) -> bool {
        self.executions.get(index).is_some_and(|e| {
            e.status.generation == generation
                && matches!(e.status.state, State::Starting | State::Running)
        })
    }
    pub fn renewable(&self, index: usize) -> bool {
        !self.executions[index].consumed
            && self.topology.instances[index]
                .declaration
                .program
                .as_ref()
                .is_some_and(|p| !p.prepared)
    }

    /// The bootstrap receiver cannot recreate its original transferred inventory.
    pub fn consume_startup(&mut self, index: usize) {
        self.executions[index].consumed = true;
    }

    pub fn demand(&mut self, index: usize) -> Result<u64, String> {
        if self
            .topology
            .instances
            .get(index)
            .and_then(|i| i.declaration.program.as_ref())
            .is_none()
        {
            return Err("instance has no executable".into());
        }
        let e = &mut self.executions[index];
        match e.status.state {
            State::Starting | State::Running => return Ok(e.status.generation),
            State::Stopping => return Err("previous execution is retiring".into()),
            State::RestartWait => return Err("restart backoff is active".into()),
            _ => {}
        }
        if e.consumed {
            return Err("startup resources were consumed; fresh ownership is required".into());
        }
        if e.status.state == State::Failed {
            return Err(format!("component failed: {}", e.status.detail));
        }
        e.desired = true;
        e.status
            .generation
            .checked_add(1)
            .ok_or_else(|| "execution generation exhausted".into())
    }

    /// Select each launch at most once, even when many connections request it.
    /// The adapter owns resources for returned generations before starting work.
    pub fn begin(&mut self, now: u64) -> Vec<(usize, u64)> {
        self.begin_ready(now, |_| true)
    }

    pub fn begin_ready(&mut self, now: u64, ready: impl Fn(usize) -> bool) -> Vec<(usize, u64)> {
        let mut launches = Vec::new();
        for i in 0..self.executions.len() {
            let Some(program) = &self.topology.instances[i].declaration.program else {
                continue;
            };
            let e = &mut self.executions[i];
            if e.status.state == State::RestartWait && now >= e.deadline {
                e.status.state = State::Stopped;
            }
            if !e.desired
                || !matches!(e.status.state, State::Stopped | State::Completed)
                || e.consumed
                || !ready(i)
            {
                continue;
            }
            let Some(generation) = e.status.generation.checked_add(1) else {
                e.status.state = State::Failed;
                e.status.detail = "execution generation exhausted".into();
                continue;
            };
            e.status.generation = generation;
            e.status.state = State::Starting;
            e.status.published.clear();
            e.status.detail = "starting".into();
            e.deadline = now.saturating_add(program.timeout_ms * 1_000_000);
            e.failed = false;
            e.consumed = program.prepared
                || self
                    .topology
                    .routes
                    .iter()
                    .any(|r| r.consumer == i && r.transfer == Transfer::Move);
            self.revision = self.revision.saturating_add(1);
            launches.push((i, generation));
        }
        launches
    }

    pub fn started(&mut self, index: usize, generation: u64, process: u64) -> Result<(), String> {
        if !self.is_current(index, generation) {
            return Err("stale start completion".into());
        }
        let e = &mut self.executions[index];
        e.status.process = Some(process);
        e.status.state = State::Running;
        e.status.detail = "running".into();
        self.revision = self.revision.saturating_add(1);
        Ok(())
    }

    pub fn publish(&mut self, index: usize, generation: u64, name: &str) -> Result<(), String> {
        if !self.is_current(index, generation) {
            return Err("stale provider publication".into());
        }
        if !self.topology.instances[index]
            .declaration
            .capabilities
            .iter()
            .any(|c| c.name == name && matches!(c.kind, CapabilityType::Protocol { .. }))
        {
            return Err("protocol is not declared by this component".into());
        }
        let e = &mut self.executions[index];
        if !e.status.published.iter().any(|p| p == name) {
            e.status.published.push(name.into());
            self.revision = self.revision.saturating_add(1);
        }
        Ok(())
    }

    pub fn stop(&mut self, index: usize, now: u64) {
        let timeout = self.topology.instances[index]
            .declaration
            .program
            .as_ref()
            .map_or(60_000, |p| p.timeout_ms);
        let e = &mut self.executions[index];
        e.desired = false;
        e.restart_requested = false;
        e.status.published.clear();
        e.status.state = if matches!(
            e.status.state,
            State::Starting | State::Running | State::Stopping
        ) {
            State::Stopping
        } else {
            State::Stopped
        };
        e.deadline = now.saturating_add(timeout * 1_000_000);
        e.status.detail = "stop requested".into();
        self.revision = self.revision.saturating_add(1);
    }

    pub fn restart(&mut self, index: usize, now: u64) -> Result<(), String> {
        if !self.renewable(index) {
            return Err("component needs a fresh ownership handoff".into());
        }
        self.stop(index, now);
        let e = &mut self.executions[index];
        if e.status.state == State::Stopping {
            e.restart_requested = true;
        } else {
            e.desired = true;
            e.status.restarts = 0;
        }
        Ok(())
    }

    pub fn fail(&mut self, index: usize, generation: u64, message: &str, now: u64) {
        if !self.is_current(index, generation) {
            return;
        }
        self.stop(index, now);
        let e = &mut self.executions[index];
        e.failed = true;
        e.desired = true;
        e.status.detail = message.chars().take(256).collect();
    }

    /// Adapter calls only after construction has finished and group teardown
    /// has joined. Neither a timeout nor TERMINATED alone authorizes replacement.
    pub fn retired(
        &mut self,
        index: usize,
        generation: u64,
        success: bool,
        now: u64,
    ) -> Result<(), String> {
        let e = &mut self.executions[index];
        if e.status.generation != generation
            || !matches!(
                e.status.state,
                State::Starting | State::Running | State::Stopping
            )
        {
            return Err("stale retirement".into());
        }
        let program = self.topology.instances[index]
            .declaration
            .program
            .as_ref()
            .ok_or("no program")?;
        e.status.process = None;
        e.status.published.clear();
        if e.restart_requested {
            e.restart_requested = false;
            e.status.restarts = 0;
            e.desired = true;
            e.status.state = State::Stopped;
        } else if e.desired
            && (!success || e.failed)
            && !e.consumed
            && program.restart == Restart::OnFailure
            && e.status.restarts < program.restart_limit
        {
            e.status.restarts += 1;
            e.status.state = State::RestartWait;
            e.deadline = now.saturating_add(program.restart_delay_ms * 1_000_000);
        } else {
            e.status.state = if !e.desired {
                State::Stopped
            } else if success && !e.failed {
                State::Completed
            } else {
                State::Failed
            };
            // A successful oneshot or ordinary exit must not become an eager
            // infinite respawn loop. A later authorized request may activate it.
            e.desired = false;
        }
        self.revision = self.revision.saturating_add(1);
        Ok(())
    }

    pub fn startup_expired(&self, index: usize, now: u64) -> bool {
        let e = &self.executions[index];
        e.status.state == State::Starting && now >= e.deadline
    }

    /// Revoke this generation's routing as soon as Process exit is observed.
    /// Keep the desired state and retry budget until the adapter joins every
    /// descendant; a natural exit is not an administrative stop request.
    pub fn exiting(&mut self, index: usize, generation: u64) {
        if !self.is_current(index, generation) {
            return;
        }
        let e = &mut self.executions[index];
        e.status.state = State::Stopping;
        e.status.published.clear();
        e.status.detail = "process exited; waiting for descendants to retire".into();
        self.revision = self.revision.saturating_add(1);
    }

    pub fn detail(&mut self, index: usize, generation: u64, message: &str) -> Result<(), String> {
        if !self.is_current(index, generation) {
            return Err("stale status update".into());
        }
        self.executions[index].status.detail = message.chars().take(256).collect();
        self.revision = self.revision.saturating_add(1);
        Ok(())
    }

    pub fn exit_detail(&mut self, index: usize, generation: u64, message: &str) {
        let e = &mut self.executions[index];
        if e.status.generation == generation && !e.failed {
            e.status.detail = message.chars().take(256).collect();
        }
    }

    pub fn progress(&mut self, milestone: &str) -> Result<(), String> {
        if !hyper_component_config::valid_name(milestone) || self.progress.len() >= 16 {
            return Err("invalid progress fact".into());
        }
        if !self.progress.iter().any(|p| p == milestone) {
            self.progress.push(milestone.into());
            self.revision = self.revision.saturating_add(1);
        }
        Ok(())
    }
}
