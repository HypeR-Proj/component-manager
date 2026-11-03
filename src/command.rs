// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Transfer a pending RPC into one lifecycle owner's applied intent.

use crate::lifecycle::{CommandReceipt, Lifecycle};
use hyper_component::Request;
use hyper_service_ipc::Call;

pub struct AppliedCommand {
    pub call: Call<Request>,
    pub receipt: CommandReceipt,
}

/// The last liveness observation is the cancellation cutoff. Once accepted,
/// retain intent and resource owners even if the caller loses the reply.
pub fn apply_pending(
    lifecycle: &mut Lifecycle,
    call: Call<Request>,
) -> hyper_os::Result<Option<AppliedCommand>> {
    if !call.is_pending()? {
        return Ok(None);
    }
    let now = hyper_os::time::monotonic_now()?.as_nanoseconds();
    let receipt = lifecycle.command(call.request.clone(), now);
    Ok(Some(AppliedCommand { call, receipt }))
}
