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
    assert!(first.ends_with("\r\n"));
    state.captured_at_ns += 100_000_000;
    assert!(ui.update(&state).is_empty());
    state.captured_at_ns += 200_000_000;
    assert!(ui.update(&state).is_empty());
    state.components[0].state = State::Running;
    let running = ui.update(&state);
    assert!(running.ends_with("\r\n"));
    assert!(!running.contains("\x1b[2K"));
    assert!(running.contains("300.00 ms"));
    assert!(running.contains("RUNNING"));
    assert!(!running.contains("READY"));
    state.captured_at_ns += 1_000_000_000;
    assert!(ui.update(&state).is_empty());
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
    state.components[0].state = State::Failed;
    state.components[0].detail = "failed\x1b[2J\r\nforged".into();
    let failure = ui.update(&state);
    assert!(!failure.contains("\x1b[2J"));
    assert!(failure.contains("failed?[2J??forged"));
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
    retire.captured_at_ns += 900_000_000;
    retire.components[0].state = State::Completed;
    let done = ui.update(&retire);
    assert!(done.contains("[DONE   ]"));
    assert!(done.contains("1000.00 ms"));
    assert!(done.ends_with("\r\n"));
    assert!(!done.contains("\x1b[2K"));
    Ok(())
}
