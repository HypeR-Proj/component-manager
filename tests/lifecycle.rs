// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use hyper_component::{Request, State};
use hyper_component_config::Topology;
use hyper_component_manager::lifecycle::Lifecycle;
use serde_json::json;

fn engine(prepared: bool) -> Result<Lifecycle, String> {
    let root = json!({"name":"root","children":[{"name":"server","component":"server"}]});
    let server = json!({"name":"server", "program":{"image":"/svc/server","prepared":prepared},
        "capabilities":[{"name":"echo","kind":{"type":"protocol","protocol":"test.Echo"}}]});
    let graph = Topology::parse(
        "root",
        [
            ("root.json".into(), root.to_string().into_bytes()),
            ("server.json".into(), server.to_string().into_bytes()),
        ],
    )?;
    Ok(Lifecycle::new(graph))
}

#[test]
fn concurrent_demands_share_one_launch_generation() -> Result<(), String> {
    let mut e = engine(false)?;
    for _ in 0..128 {
        assert_eq!(e.demand(1)?, 1);
    }
    assert_eq!(e.begin(0), [(1, 1)]);
    for _ in 0..128 {
        assert_eq!(e.demand(1)?, 1);
        assert!(e.begin(1).is_empty());
    }
    e.started(1, 1, 42)?;
    assert!(e.status(1).published.is_empty());
    e.publish(1, 1, "echo")?;
    assert_eq!(e.status(1).published, ["echo"]);
    Ok(())
}

#[test]
fn cancellation_during_launch_cannot_resurrect_a_stopped_instance() -> Result<(), String> {
    let mut e = engine(false)?;
    e.demand(1)?;
    e.begin(0);
    e.stop(1, 1);
    assert!(e.force_required(1));
    assert!(!e.accepts_connections(1, 1));
    assert!(e.started(1, 1, 42).is_err());
    assert!(e.publish(1, 1, "echo").is_err());
    assert!(e.demand(1).is_err());
    e.retired(1, 1, true, 2)?;
    assert!(e.begin(3).is_empty());
    assert_eq!(e.demand(1)?, 2);
    Ok(())
}

#[test]
fn graceful_stop_keeps_cleanup_routes_until_the_original_deadline() -> Result<(), String> {
    let mut e = engine(false)?;
    e.topology.instances[1]
        .declaration
        .program
        .as_mut()
        .ok_or("program")?
        .timeout_ms = 10;
    e.demand(1)?;
    e.begin(0);
    e.started(1, 1, 42)?;
    e.publish(1, 1, "echo")?;
    e.stop(1, 5);
    assert_eq!(e.status(1).state, State::Stopping);
    assert!(e.status(1).published.is_empty());
    assert!(e.accepts_connections(1, 1));
    assert!(!e.accepts_connections(1, 2));
    e.detail(1, 1, "cleanup in progress")?;
    assert!(e.detail(1, 2, "stale cleanup").is_err());
    assert!(e.publish(1, 1, "echo").is_err());
    assert!(!e.force_required(1));
    assert!(!e.stop_expired(1, 10_000_004));
    e.stop(1, 9_000_000);
    assert!(e.stop_expired(1, 10_000_005));
    // A delayed action for another generation cannot revoke cleanup routes.
    e.force_stop(1, 2);
    assert!(e.accepts_connections(1, 1));
    e.force_stop(1, 1);
    assert!(e.force_required(1));
    assert!(!e.accepts_connections(1, 1));
    assert!(e.detail(1, 1, "cleanup after forced stop").is_err());
    assert!(e.begin(20_000_000).is_empty());
    e.retired(1, 1, true, 20_000_001)?;
    assert_eq!(e.status(1).state, State::Stopped);
    Ok(())
}

#[test]
fn natural_exit_during_grace_revokes_cleanup_routes_before_retirement() -> Result<(), String> {
    let mut e = engine(false)?;
    e.demand(1)?;
    e.begin(0);
    e.started(1, 1, 42)?;
    e.stop(1, 1);
    assert!(e.accepts_connections(1, 1));
    e.exiting(1, 1);
    assert!(e.force_required(1));
    assert!(!e.accepts_connections(1, 1));
    e.stop(1, 2);
    assert!(e.force_required(1));
    e.retired(1, 1, true, 3)?;
    assert_eq!(e.status(1).state, State::Stopped);
    Ok(())
}

#[test]
fn old_connector_cannot_publish_into_replacement() -> Result<(), String> {
    let mut e = engine(false)?;
    e.demand(1)?;
    e.begin(0);
    e.started(1, 1, 42)?;
    e.restart(1, 1)?;
    assert!(e.begin(2).is_empty());
    e.retired(1, 1, true, 3)?;
    assert_eq!(e.begin(4), [(1, 2)]);
    assert!(e.publish(1, 1, "echo").is_err());
    assert!(e.retired(1, 1, true, 5).is_err());
    assert!(e.publish(1, 2, "admin").is_err());
    Ok(())
}

#[test]
fn a_later_stop_cancels_pending_restart() -> Result<(), String> {
    let mut e = engine(false)?;
    e.demand(1)?;
    e.begin(0);
    e.started(1, 1, 42)?;
    e.restart(1, 1)?;
    e.stop(1, 2);
    e.retired(1, 1, true, 3)?;
    assert!(e.begin(4).is_empty());
    Ok(())
}

#[test]
fn prepared_authority_is_never_recreated_after_failure_or_stop() -> Result<(), String> {
    let mut e = engine(true)?;
    e.demand(1)?;
    e.begin(0);
    e.fail(1, 1, "loader failed", 1);
    assert!(e.force_required(1));
    assert!(!e.accepts_connections(1, 1));
    e.retired(1, 1, false, 2)?;
    assert!(e.demand(1).is_err());
    assert!(e.restart(1, 3).is_err());
    assert!(e.begin(4).is_empty());
    Ok(())
}

#[test]
fn successful_exit_does_not_trigger_eager_respawn() -> Result<(), String> {
    let mut e = engine(false)?;
    e.demand(1)?;
    e.begin(0);
    e.started(1, 1, 42)?;
    e.retired(1, 1, true, 2)?;
    assert_eq!(e.status(1).state, State::Completed);
    assert!(e.begin(3).is_empty());
    Ok(())
}

#[test]
fn framework_startup_purposes_do_not_alias_journal_or_retirement() {
    let purposes = [
        hyper_component::CONNECTOR.as_raw(),
        hyper_component::OUTGOING.as_raw(),
        hyper_component::OBSERVE.as_raw(),
        hyper_component::CONTROL.as_raw(),
        hyper_component::NOTIFY.as_raw(),
        hyper_component::installation::INSTALLER.as_raw(),
        hyper_journal::QUERY.as_raw(),
        hyper_journal::MIRROR.as_raw(),
        hyper_bootstrap_handoff::retirement::ENDPOINT.as_raw(),
        hyper_service::process::control::STARTUP.as_raw(),
    ];
    let distinct: std::collections::BTreeSet<_> = purposes.into_iter().collect();
    assert_eq!(distinct.len(), purposes.len());
}

#[test]
fn process_exit_revokes_routes_before_descendants_finish() -> Result<(), String> {
    let mut e = engine(false)?;
    let program = e.topology.instances[1]
        .declaration
        .program
        .as_mut()
        .ok_or("program")?;
    program.restart = hyper_component_config::Restart::OnFailure;
    e.demand(1)?;
    e.begin(0);
    e.started(1, 1, 42)?;
    e.publish(1, 1, "echo")?;
    e.exiting(1, 1);
    assert_eq!(e.status(1).state, State::Stopping);
    assert!(e.status(1).published.is_empty());
    assert!(e.publish(1, 1, "echo").is_err());
    assert!(e.demand(1).is_err());
    assert!(e.begin(10).is_empty());
    e.retired(1, 1, false, 20)?;
    assert_eq!(e.status(1).state, State::RestartWait);
    assert_eq!(e.begin(1_000_000_020), [(1, 2)]);
    // A delayed exit observation from the previous execution is harmless.
    e.exiting(1, 1);
    assert_eq!(e.status(1).state, State::Starting);
    Ok(())
}

#[test]
fn administrative_stop_during_exit_cancels_automatic_restart() -> Result<(), String> {
    let mut e = engine(false)?;
    let program = e.topology.instances[1]
        .declaration
        .program
        .as_mut()
        .ok_or("program")?;
    program.restart = hyper_component_config::Restart::OnFailure;
    e.demand(1)?;
    e.begin(0);
    e.started(1, 1, 42)?;
    e.exiting(1, 1);
    e.stop(1, 2);
    e.retired(1, 1, false, 3)?;
    assert_eq!(e.status(1).state, State::Stopped);
    assert!(e.begin(10_000_000_000).is_empty());
    Ok(())
}

#[test]
fn repeated_progress_remains_idempotent_at_capacity() -> Result<(), String> {
    let mut lifecycle = engine(false)?;
    for index in 0..16 {
        lifecycle.progress(&format!("fact-{index}"))?;
    }
    let full = lifecycle.snapshot(1);
    lifecycle.progress("fact-0")?;
    lifecycle.progress("fact-15")?;
    assert_eq!(lifecycle.snapshot(2).revision, full.revision);
    assert_eq!(lifecycle.snapshot(2).progress, full.progress);
    assert!(lifecycle.progress("fact-16").is_err());
    assert!(lifecycle.progress("invalid/fact").is_err());
    assert_eq!(lifecycle.snapshot(3).revision, full.revision);
    Ok(())
}

#[test]
fn command_receipts_keep_their_own_state_across_later_events() -> Result<(), String> {
    let mut lifecycle = engine(false)?;
    let before = lifecycle.snapshot(0);
    let started = lifecycle.command(
        Request::Start {
            path: "/server".into(),
        },
        1,
    );
    started.result?;
    assert!(started.snapshot.revision > before.revision);
    // Start acknowledges intent; the launch belongs to a later reactor pass.
    assert_eq!(started.snapshot.components[1].state, State::Stopped);
    assert_eq!(lifecycle.begin(2), [(1, 1)]);
    lifecycle.started(1, 1, 42)?;
    let running = lifecycle.snapshot(3);
    let stopped = lifecycle.command(
        Request::Stop {
            path: "/server".into(),
        },
        4,
    );
    stopped.result?;
    assert!(stopped.snapshot.revision > running.revision);
    assert_eq!(stopped.snapshot.captured_at_ns, 4);
    assert_eq!(stopped.snapshot.components[1].state, State::Stopping);
    assert_eq!(stopped.snapshot.components[1].generation, 1);
    assert_eq!(stopped.snapshot.components[1].process, Some(42));
    let restarted = lifecycle.command(
        Request::Restart {
            path: "/server".into(),
        },
        5,
    );
    restarted.result?;
    assert!(restarted.snapshot.revision > stopped.snapshot.revision);
    lifecycle.retired(1, 1, true, 6)?;
    assert_eq!(lifecycle.begin(7), [(1, 2)]);
    lifecycle.started(1, 2, 43)?;
    assert_eq!(stopped.snapshot.components[1].state, State::Stopping);
    assert_eq!(stopped.snapshot.components[1].process, Some(42));
    assert_eq!(restarted.snapshot.components[1].generation, 1);
    assert_eq!(started.snapshot.components[1].generation, 0);
    Ok(())
}

#[test]
fn rejected_command_receipt_has_the_unchanged_owner_state() -> Result<(), String> {
    let mut lifecycle = engine(true)?;
    let before = lifecycle.snapshot(0);
    for request in [
        Request::Stop {
            path: "/missing".into(),
        },
        Request::Start { path: "/".into() },
        Request::Stop {
            path: "/server".into(),
        },
        Request::Restart {
            path: "/server".into(),
        },
        Request::Status { path: None },
    ] {
        let receipt = lifecycle.command(request, 10);
        assert!(receipt.result.is_err());
        assert_eq!(receipt.snapshot.revision, before.revision);
        assert_eq!(receipt.snapshot.captured_at_ns, 10);
        assert_eq!(receipt.snapshot.components[1].state, State::Stopped);
        assert_eq!(receipt.snapshot.components[1].generation, 0);
    }
    Ok(())
}
