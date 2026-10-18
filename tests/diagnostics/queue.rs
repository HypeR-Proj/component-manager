// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn critical_reports_bypass_journal_and_survive_partial_console_writes() {
    let mut queue = Diagnostics::default();
    queue.push("CRITICAL: /console-raw terminated".into(), true);
    queue.push("CRITICAL: /journal terminated".into(), true);
    queue.flush(|_| false, |_| None);
    assert!(queue.pending());
    let mut output = Vec::new();
    while queue.pending() {
        queue.flush(
            |_| false,
            |bytes| {
                let n = bytes.len().min(7);
                output.extend_from_slice(&bytes[..n]);
                Some(n)
            },
        );
    }
    assert_eq!(
        String::from_utf8_lossy(&output),
        "\r\nHypeR component-manager: CRITICAL: /console-raw terminated\r\n\r\nHypeR component-manager: CRITICAL: /journal terminated\r\n"
    );
}

#[test]
fn ordinary_reports_use_journal_but_fall_back_on_rejection() {
    let mut queue = Diagnostics::default();
    queue.push("ERROR: /worker terminated".into(), false);
    let mut logs = Vec::new();
    queue.flush(
        |bytes| {
            logs.extend_from_slice(bytes);
            true
        },
        |_| panic!("unneeded Console write"),
    );
    assert!(!queue.pending());
    assert_eq!(logs, b"ERROR: /worker terminated\n");
    queue.push("ERROR: /worker terminated".into(), false);
    let mut console = Vec::new();
    queue.flush(
        |_| false,
        |bytes| {
            console.extend_from_slice(bytes);
            Some(bytes.len())
        },
    );
    assert!(!queue.pending());
    assert!(String::from_utf8_lossy(&console).contains("ERROR: /worker terminated"));
}

#[test]
fn backpressure_does_not_repeat_journal_records_and_overflow_is_visible() {
    let mut queue = Diagnostics::default();
    for _ in 0..MAX_REPORTS + 2 {
        queue.push("CRITICAL: failed".into(), true);
    }
    let mut count = 0;
    queue.flush(
        |_| {
            count += 1;
            true
        },
        |_| None,
    );
    queue.flush(
        |_| {
            count += 1;
            true
        },
        |_| None,
    );
    assert_eq!(count, MAX_REPORTS);
    queue.flush(|_| true, |bytes| Some(bytes.len()));
    let mut output = Vec::new();
    queue.flush(
        |_| true,
        |bytes| {
            output.extend_from_slice(bytes);
            Some(bytes.len())
        },
    );
    assert!(String::from_utf8_lossy(&output).contains("2 lifecycle reports lost"));
    assert!(!queue.pending());
}

#[test]
fn delegated_messages_cannot_inject_terminal_controls_or_new_records() {
    let mut queue = Diagnostics::default();
    queue.push("bad\n\x1b[2J".into(), false);
    let mut log = Vec::new();
    queue.flush(
        |bytes| {
            log.extend_from_slice(bytes);
            true
        },
        |_| None,
    );
    assert_eq!(log, b"bad\\n\\u{1b}[2J\n");
}
