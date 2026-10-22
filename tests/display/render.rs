// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hyper_component::ComponentStatus;
use serde_json::json;

fn renderer() -> Result<Renderer, String> {
    let root = json!({"name":"root","children":[{"name":"console","component":"console"}]});
    let console = json!({"name":"console", "description":"Physical console transport",
        "program":{"image":"/svc/console-raw"}});
    let topology = Topology::parse(
        "root",
        [
            ("root.json".into(), root.to_string().into_bytes()),
            ("console.json".into(), console.to_string().into_bytes()),
        ],
    )?;
    Ok(Renderer::new(1_000_000_000, &topology))
}

fn snapshot(state: State, now: u64) -> Snapshot {
    Snapshot {
        captured_at_ns: now,
        components: vec![ComponentStatus {
            path: "/console".into(),
            state,
            generation: 1,
            process: None,
            restarts: 0,
            detail: String::new(),
            published: Vec::new(),
        }],
        ..Snapshot::default()
    }
}

#[test]
fn launch_emits_complete_lines_and_silent_polls_write_nothing() -> Result<(), String> {
    let mut ui = renderer()?;
    let mut state = snapshot(State::Starting, 1_000_000_000);
    let first = ui.update(&state);
    assert!(first.contains("Physical console transport"));
    assert!(!first.contains("0.00 ms"));
    assert!(first.ends_with("\r\n"));
    state.captured_at_ns += 100_000_000;
    assert!(ui.update(&state).is_empty());
    state.captured_at_ns += 200_000_000;
    assert!(ui.update(&state).is_empty());
    state.components[0].state = State::Running;
    let running = ui.update(&state);
    assert!(running.ends_with("\r\n"));
    assert!(!running.contains("\x1b[2K"));
    assert!(running.contains("start 300.00 ms"));
    assert!(running.contains("RUNNING"));
    assert!(!running.contains("READY"));
    state.captured_at_ns += 1_000_000_000;
    assert!(ui.update(&state).is_empty());
    for detail in [
        "ready",
        "1 filesystem worker(s) running",
        "2 filesystem worker(s) running",
    ] {
        state.components[0].detail = detail.into();
        state.components[0].published.push(detail.into());
        assert!(
            ui.update(&state).is_empty(),
            "detail changes are not launches"
        );
    }
    Ok(())
}

#[test]
fn milestones_and_failure_text_cannot_inject_terminal_commands() -> Result<(), String> {
    let mut ui = renderer()?;
    let mut state = snapshot(State::Starting, 1_000_000_000);
    ui.update(&state);
    state.progress.push("ownership-transferred".into());
    let phase = ui.update(&state);
    assert!(phase.ends_with("\r\n"));
    assert!(phase.contains("ownership transferred"));
    assert!(ui.update(&state).is_empty());
    state.components[0].state = State::Failed;
    state.components[0].detail = "failed\x1b[2J\r\nforged".into();
    let failure = ui.update(&state);
    assert!(!failure.contains("\x1b[2J"));
    assert!(failure.contains("failed?[2J??forged"));
    state.components[0].detail = "worker stopped".into();
    let detail = ui.update(&state);
    assert!(detail.contains("! /console: worker stopped"));
    assert!(!detail.contains("[FAILED"));
    let summary = ui.finish(2_000_000_000, false);
    assert!(summary.contains("1 failed"));
    assert!(summary.contains("startup incomplete"));
    Ok(())
}

#[test]
fn retirement_remains_visible_without_erasing_shared_lines() -> Result<(), String> {
    let mut ui = renderer()?;
    ui.update(&snapshot(State::Running, 1_000_000_000));
    let mut retire = snapshot(State::Starting, 2_000_000_000);
    retire.components[0].path = "/bootstrap-retire".into();
    let start = ui.update(&retire);
    assert!(start.contains("[START  ]"));
    assert!(start.ends_with("\r\n"));
    retire.captured_at_ns += 100_000_000;
    retire.components[0].state = State::Running;
    let running = ui.update(&retire);
    assert!(running.contains("[RUNNING]"));
    assert!(running.ends_with("\r\n"));
    assert!(!running.contains("\x1b[2K"));
    retire.captured_at_ns += 100_000_000;
    retire.components[0].state = State::Stopping;
    retire.components[0].detail = "terminated: ProcessExited { status: 0 }".into();
    assert!(ui.update(&retire).is_empty());
    let pending = ui.finish(retire.captured_at_ns, false);
    assert!(pending.contains("1 cleaning up"));
    assert!(pending.contains("0 completed"));
    retire.captured_at_ns += 800_000_000;
    retire.components[0].state = State::Completed;
    let done = ui.update(&retire);
    assert!(done.contains("[DONE   ]"));
    assert!(done.contains("total 1000.00 ms"));
    assert!(done.ends_with("\r\n"));
    assert!(!done.contains("\x1b[2K"));
    Ok(())
}

#[test]
fn adopted_processes_have_no_invented_time_and_restarts_reset_it() -> Result<(), String> {
    let mut ui = renderer()?;
    let mut state = snapshot(State::Running, 1_000_000_000);
    let adopted = ui.update(&state);
    assert!(adopted.contains("time unknown"));
    assert!(!adopted.contains("0.00 ms"));
    state.captured_at_ns += 500_000_000;
    state.components[0].state = State::Completed;
    assert!(ui.update(&state).contains("time unknown"));
    state.components[0].generation = 2;
    state.components[0].state = State::Starting;
    let start = ui.update(&state);
    assert!(start.contains("generation 2"));
    state.captured_at_ns += 42_125_000;
    state.components[0].state = State::Running;
    assert!(ui.update(&state).contains("start 42.12 ms"));
    state.captured_at_ns += 1_000_000_000;
    state.components[0].generation = 3;
    let missed_start = ui.update(&state);
    assert!(missed_start.contains("generation 3"));
    assert!(missed_start.contains("time unknown"));
    Ok(())
}

#[test]
fn summary_accounts_for_unfinished_components_and_offers_diagnostics() -> Result<(), String> {
    let mut ui = renderer()?;
    let mut state = Snapshot::default();
    for (i, phase) in [
        State::Running,
        State::Completed,
        State::Failed,
        State::Starting,
        State::Stopping,
        State::RestartWait,
        State::Stopped,
    ]
    .into_iter()
    .enumerate()
    {
        let mut entry = snapshot(phase, 1_000_000_000).components.remove(0);
        entry.path = format!("/component-{i}");
        state.components.push(entry);
    }
    ui.update(&state);
    let summary = ui.finish(2_000_000_000, false);
    for text in [
        "1 running",
        "1 completed",
        "1 failed",
        "1 starting",
        "1 cleaning up",
        "1 waiting to retry",
        "1 stopped",
        "componentctl status",
        "journalctl",
    ] {
        assert!(summary.contains(text), "missing summary category: {text}");
    }
    assert!(!summary.contains("startup complete"));
    Ok(())
}

#[test]
fn serial_rows_fit_eighty_columns_and_truncation_is_visible() -> Result<(), String> {
    let mut ui = renderer()?;
    let mut state = snapshot(State::Starting, 1_000_000_000);
    state.components[0].path = "/realm/long-component-name".into();
    ui.descriptions.insert(
        state.components[0].path.clone(),
        clean("a description exceeding the column width", 26),
    );
    let mut output = ui.update(&state);
    state.captured_at_ns += 60_000_000_000;
    state.components[0].state = State::Failed;
    state.components[0].detail =
        "failure with a very long diagnostic including controls\x1b[2J\r\n".into();
    output.push_str(&ui.update(&state));
    let mut plain = String::new();
    let mut chars = output.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            assert_eq!(chars.next(), Some('['));
            for code in chars.by_ref() {
                if code == 'm' {
                    break;
                }
                assert!(code.is_ascii_digit() || code == ';');
            }
        } else {
            plain.push(c);
        }
    }
    assert!(plain.contains("..."));
    assert!(plain.contains("/realm/long-component-name"));
    for line in plain.split("\r\n") {
        assert!(line.len() < 80, "serial row wraps: {line}");
    }
    Ok(())
}
