// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded lifecycle reports, independent of journal and raw-console services.
//! The manager retries Console backpressure without blocking exit observation.

use std::collections::VecDeque;

const MAX_REPORTS: usize = 128;

struct Report {
    journal: String,
    console: String,
    offset: usize,
    direct: bool,
    retained: bool,
}

#[derive(Default)]
pub(crate) struct Diagnostics {
    pending: VecDeque<Report>,
    lost: u64,
}

impl Diagnostics {
    pub(crate) fn push(&mut self, text: String, direct: bool) {
        if self.pending.len() == MAX_REPORTS {
            self.pending.pop_front();
            self.lost = self.lost.saturating_add(1);
        }
        // A component's status text is untrusted terminal input, not formatting.
        let mut sanitized = String::new();
        for c in text.chars().take(768) {
            if c.is_control() {
                sanitized.extend(c.escape_default());
            } else {
                sanitized.push(c);
            }
        }
        self.pending.push_back(Report {
            journal: format!("{sanitized}\n"),
            console: format!("\r\nHypeR component-manager: {sanitized}\r\n"),
            offset: 0,
            direct,
            retained: false,
        });
    }

    pub(crate) fn pending(&self) -> bool {
        !self.pending.is_empty() || self.lost != 0
    }

    pub(crate) fn flush(
        &mut self,
        mut journal: impl FnMut(&[u8]) -> bool,
        mut console: impl FnMut(&[u8]) -> Option<usize>,
    ) {
        if self.lost != 0 && self.pending.len() < MAX_REPORTS {
            let lost = std::mem::take(&mut self.lost);
            self.push(format!("WARNING: {lost} lifecycle reports lost to output backpressure; inspect componentctl status"), true);
        }
        // Preserve physical record order, including partial Console writes.
        // Journal admission can still progress while Console is backpressured.
        let mut console_blocked = false;
        self.pending.retain_mut(|report| {
            if !report.retained {
                report.retained = journal(report.journal.as_bytes());
                if !report.retained {
                    report.direct = true;
                }
            }
            if report.direct && !console_blocked {
                let bytes = &report.console.as_bytes()[report.offset..];
                if let Some(n) = console(bytes) {
                    report.offset += n.min(bytes.len());
                }
                console_blocked = report.offset != report.console.len();
            }
            if report.direct {
                report.offset != report.console.len()
            } else {
                !report.retained
            }
        });
    }
}

#[cfg(test)]
#[path = "../tests/diagnostics/queue.rs"]
mod tests;
