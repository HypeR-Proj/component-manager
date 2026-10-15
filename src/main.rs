// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

#[cfg(target_os = "hyper")]
fn main() -> std::process::ExitCode {
    match hyper_component_manager::runtime::boot::run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("HypeR component-manager: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(not(target_os = "hyper"))]
fn main() {}
