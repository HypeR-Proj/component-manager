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
    started: Option<u64>,
    elapsed: Option<u64>,
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
                        clean(&instance.declaration.description, 26),
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
                seen.generation != status.generation || seen.state != status.state
            });
            let diagnostic_changed = self.seen.get(&status.path).is_none_or(|seen| {
                seen.generation != status.generation || seen.detail != status.detail
            });
            let seen = self.seen.entry(status.path.clone()).or_insert(Observation {
                generation: status.generation,
                state: status.state,
                started: None,
                elapsed: None,
                detail: String::new(),
            });
            if seen.generation != status.generation {
                seen.generation = status.generation;
                seen.started = None;
                seen.elapsed = None;
            }
            if changed {
                if status.state == State::Starting {
                    seen.started = Some(now);
                    seen.elapsed = None;
                } else {
                    seen.elapsed = seen.started.map(|start| now.saturating_sub(start));
                }
            }
            seen.state = status.state;
            seen.detail.clone_from(&status.detail);
            // Exit reasons are reported immediately by lifecycle diagnostics.
            // Resource teardown is not another startup result: show DONE,
            // FAILED or STOPPED only after retirement, while retaining the real
            // Stopping state for the summary and componentctl.
            if changed {
                Self::status_line(&mut output, status, seen, &self.descriptions);
            }
            if matches!(status.state, State::Failed | State::RestartWait)
                && (changed || diagnostic_changed)
                && !status.detail.is_empty()
            {
                let _ = write!(
                    output,
                    "      \x1b[31m! {}: {}{RESET}\r\n",
                    clean(&status.path, 24),
                    clean(&status.detail, 44),
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

    fn status_line(
        output: &mut String,
        status: &hyper_component::ComponentStatus,
        seen: &Observation,
        descriptions: &BTreeMap<String, String>,
    ) {
        let (color, label) = match status.state {
            State::Starting => ("33", "START  "),
            State::Running => ("32", "RUNNING"),
            State::Completed => ("32", "DONE   "),
            State::Failed => ("31", "FAILED "),
            State::Stopping => return,
            State::RestartWait => ("33", "RETRY  "),
            State::Stopped => ("2", "STOPPED"),
        };
        let role = descriptions.get(&status.path).map_or("", String::as_str);
        let duration = if status.state == State::Starting {
            String::new()
        } else if let Some(elapsed) = seen.elapsed {
            let kind = if status.state == State::Running {
                "start"
            } else {
                "total"
            };
            format!("{kind} {:.2} ms", elapsed as f64 / 1_000_000.0)
        } else {
            "time unknown".into()
        };
        // Identity must remain distinguishable even for nested components with
        // a common prefix. Long paths get their own wrapped row, not ellipses.
        let path = clean(&status.path, status.path.len());
        if path.len() > 20 {
            let _ = write!(output, "  \x1b[{color}m[{label}]{RESET} ");
            for (index, chunk) in path.as_bytes().chunks(66).enumerate() {
                if index != 0 {
                    output.push_str("            ");
                }
                let _ = write!(output, "{}\r\n", String::from_utf8_lossy(chunk));
            }
            if !role.is_empty() || !duration.is_empty() {
                let _ = write!(
                    output,
                    "                                 {role:<26} {DIM}{duration:>18}{RESET}\r\n"
                );
            }
        } else {
            let _ = write!(
                output,
                "  \x1b[{color}m[{label}]{RESET} {path:<20} {role:<26} {DIM}{duration:>18}{RESET}\r\n",
            );
        }
        if status.generation > 1 {
            let _ = write!(
                output,
                "      {DIM}generation {} / restarts {}{RESET}\r\n",
                status.generation, status.restarts,
            );
        }
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
            "  {} running, {} completed, {} failed / startup {:.3}s\r\n",
            count(State::Running),
            count(State::Completed),
            count(State::Failed),
            seconds(now.saturating_sub(self.began)),
        );
        let mut pending = false;
        for (state, label) in [
            (State::Starting, "starting"),
            (State::Stopping, "cleaning up"),
            (State::RestartWait, "waiting to retry"),
            (State::Stopped, "stopped"),
        ] {
            let n = count(state);
            if n != 0 {
                pending = true;
                let _ = write!(output, "  {n} {label}\r\n");
            }
        }
        output.push_str(if success {
            "HypeR: startup complete\r\n\r\n"
        } else {
            "HypeR: startup incomplete\r\n\r\n"
        });
        if !success || pending || count(State::Failed) != 0 {
            output.push_str(
                "  Inspect: componentctl list; componentctl status <path>; journalctl\r\n\r\n",
            );
        }
        output
    }
}

fn seconds(ns: u64) -> f64 {
    ns as f64 / 1_000_000_000.0
}

/// Keep metadata inside one ASCII terminal row, including untrusted status text.
fn clean(text: &str, limit: usize) -> String {
    let mut cleaned: String = text
        .chars()
        .take(limit)
        .map(|c| {
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '?'
            }
        })
        .collect();
    if text.chars().count() > limit && limit >= 3 {
        cleaned.truncate(limit - 3);
        cleaned.push_str("...");
    }
    cleaned
}

#[cfg(test)]
#[path = "../../../tests/display/render.rs"]
mod tests;
