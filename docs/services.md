<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# Service and journal commands


`componentctl` lists component state and supports status, route inspection,
start, stop and restart. `journalctl` reads bounded volatile logs, with
`-c COMPONENT_PATH`, `-n COUNT` and `-f`. For automated inspection,
`journalctl --json [-c COMPONENT_PATH] [--after N] [--through N] [--limit N]`
returns one JSON query page, including sequence cursors, source generations and
retention/rejection/mirror-loss counters. The limit is 1–128 records (also bounded
by the journal's page byte budget); continue from `next`, fixing `through` to the
first page's `newest` for a stable snapshot. JSON paging is separate from the
human-oriented `--lines`/`--follow` mode. The trusted shell delegates separate
control and query capabilities to these commands. See
[component management](components.md) for routes, activation and ownership limits.
