// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded, complete status lines alongside service logs. Console history is
//! never cleared, rewritten or addressed with cursor movement sequences.

use hyper_component::{Snapshot, State};
use hyper_component_config::Topology;
use std::collections::BTreeMap;
use std::fmt::Write;

const CYAN: &str = "\x1b[36m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";

struct Observation {
    generation: u64,
    state: State,
    since: u64,
    elapsed: u64,
    detail: String,
}

pub(super) struct Renderer {
    began: u64,
    descriptions: BTreeMap<String, String>,
    seen: BTreeMap<String, Observation>,
    milestones: Vec<String>,
}

impl Renderer {
    pub(super) fn new(began: u64, topology: &Topology) -> Self {
        Self {
            began,
            descriptions: topology
                .instances
                .iter()
                .map(|instance| {
                    (
                        instance.path.clone(),
                        clean(&instance.declaration.description, 30),
                    )
                })
                .collect(),
            seen: BTreeMap::new(),
            milestones: Vec::new(),
        }
    }

    pub(super) fn banner(&self) -> String {
        format!(
            "\r\nHypeR component manager / {} / boot +{:.3}s\r\n",
            std::env::consts::ARCH,
            seconds(self.began),
        )
    }

    pub(super) fn update(&mut self, snapshot: &Snapshot) -> String {
        let now = snapshot.captured_at_ns;
        let mut output = String::new();
        for status in &snapshot.components {
            if status.generation == 0 {
                continue;
            }
            let changed = self.seen.get(&status.path).is_none_or(|seen| {
                seen.generation != status.generation
                    || seen.state != status.state
                    || seen.detail != status.detail
            });
            if !changed {
                continue;
            }
            let seen = self.seen.entry(status.path.clone()).or_insert(Observation {
                generation: status.generation,
                state: status.state,
                since: now,
                elapsed: 0,
                detail: String::new(),
            });
            if seen.generation != status.generation {
                seen.generation = status.generation;
                seen.since = now;
                seen.elapsed = 0;
            }
            if matches!(
                status.state,
                State::Starting | State::Completed | State::Failed
            ) || seen.state == State::Starting
            {
                seen.elapsed = now.saturating_sub(seen.since);
            }
            seen.state = status.state;
            seen.detail.clone_from(&status.detail);
            let (color, label) = match status.state {
                State::Starting => ("33", "START  "),
                State::Running => ("32", "RUNNING"),
                State::Completed => ("32", "DONE   "),
                State::Failed => ("31", "FAILED "),
                State::Stopping => ("33", "STOP   "),
                State::RestartWait => ("33", "RETRY  "),
                State::Stopped => ("2", "STOPPED"),
            };
            let role = self
                .descriptions
                .get(&status.path)
                .map_or("", String::as_str);
            let _ = write!(
                output,
                "  \x1b[{color}m[{label}]{RESET} {:<23} {role:<30} {DIM}{:>8.2} ms{RESET}\r\n",
                clean(&status.path, 23),
                seen.elapsed as f64 / 1_000_000.0,
            );
            if matches!(status.state, State::Failed | State::RestartWait) {
                let _ = write!(
                    output,
                    "      \x1b[31m! {}{RESET}\r\n",
                    clean(&status.detail, 66)
                );
            }
        }
        for milestone in &snapshot.progress {
            if self.milestones.contains(milestone) {
                continue;
            }
            self.milestones.push(milestone.clone());
            let _ = write!(
                output,
                "  {CYAN}>>{RESET} {:<46} {DIM}+{:.3}s{RESET}\r\n",
                clean(&milestone.replace('-', " "), 46),
                seconds(now.saturating_sub(self.began))
            );
        }
        output
    }

    pub(super) fn finish(&mut self, now: u64, success: bool) -> String {
        let mut output = String::new();
        let count = |state| {
            self.seen
                .values()
                .filter(|seen| seen.state == state)
                .count()
        };
        let _ = write!(
            output,
            "  {} running, {} finished, {} failed / elapsed {:.3}s\r\n",
            count(State::Running),
            count(State::Completed),
            count(State::Failed),
            seconds(now.saturating_sub(self.began)),
        );
        output.push_str(if success {
            "HypeR: startup complete\r\n\r\n"
        } else {
            "HypeR: startup incomplete; continuing with available services\r\n\r\n"
        });
        output
    }
}

fn seconds(ns: u64) -> f64 {
    ns as f64 / 1_000_000_000.0
}

/// Keep metadata inside one ASCII terminal row, including untrusted status text.
fn clean(text: &str, limit: usize) -> String {
    text.chars()
        .take(limit)
        .map(|c| {
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '?'
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "../../../tests/display/render.rs"]
mod tests;
