// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Translate declarative policy at the process-construction boundary.

use hyper_component_config::{FairBackend, Scheduling, SchedulingPolicy, SchedulingRole};
use hyper_os::{Result, scheduling, task::ProcessBuilder};

pub(super) fn apply(builder: &ProcessBuilder, configuration: Option<Scheduling>) -> Result<()> {
    let Some(configuration) = configuration else {
        return Ok(());
    };
    let parameters = scheduling::Parameters {
        band: configuration.band,
        role: match configuration.role {
            SchedulingRole::Ordinary => scheduling::Role::Ordinary,
            SchedulingRole::Priority => scheduling::Role::Priority,
            SchedulingRole::Protected => scheduling::Role::Protected,
        },
        policy: match configuration.policy {
            SchedulingPolicy::Fair {
                weight,
                quantum_ns,
                backend,
            } => scheduling::Policy::Fair {
                weight,
                quantum_ns,
                backend: match backend {
                    FairBackend::Stride => scheduling::FairBackend::Stride,
                    FairBackend::RoundRobin => scheduling::FairBackend::RoundRobin,
                    FairBackend::Default => scheduling::FairBackend::Default,
                },
            },
            SchedulingPolicy::Budget {
                budget_ns,
                period_ns,
                burst_ns,
            } => scheduling::Policy::Budget {
                budget_ns,
                period_ns,
                burst_ns,
            },
            SchedulingPolicy::Fifo { priority } => scheduling::Policy::Fifo { priority },
        },
    };
    // Selection is the supervisor's authority; children may only demote their
    // own treatment unless another explicit construction grant says otherwise.
    builder.set_scheduling(parameters, hyper_os::Rights::SCHED_LOWER)
}
