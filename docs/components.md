<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# Components and service supervision

The formal root is `component-manager`. There is no intermediate formal init and
no service dependency or target engine. Components form a static instance tree;
capability routes determine which services can connect. Domain-specific startup and
logging remain separate processes; the root manager renders early boot status. Bootstrap does not link any
of these formal libraries.

| Program | Responsibility |
| --- | --- |
| `component-manager` | Validate the instance tree and routes, accept bootstrap ownership, own component Process supervisors, activate providers, report lifecycle state and display early startup progress. |
| `system-startup` | Validate the offered resources, adopt existing console/I/O/filesystem providers, mount formal volumes, provision the initial VM fleet and arrange bootstrap retirement. It exits after successful migration. |
| `journal` | Collect attributed byte streams, retain bounded records in RAM, serve queries and mirror logs to raw console. |
| `bootstrap-retire` | Stop transferred predecessor processes, drain their console, join teardown and normally unmount the old archive. |
| `session-manager` | Own per-console terminal services and interactive clients. |

These programs install in `/svc`; foreground tools install in `/bin`. This is a
deployment convention, not an executable-path restriction. VM and filesystem
managers retain their domain workers. The component manager does not link device,
filesystem, terminal, journal retention or VM provisioning implementations.

## Composition

The root manager reads `/etc/hyper/components.json`:

```json
{
  "configuration_directory": "/etc/components",
  "root": "root",
  "startup": "/startup"
}
```

The directory is configurable. Each `NAME.json` contains a reusable declaration
whose `name` equals `NAME`. A parent's `children` instantiate declarations under
local names. `/` identifies the root realm; `/tools/indexer` identifies one
instance, even when another instance uses the same declaration. Realms may have
no executable. Files are loaded once; unknown fields, recursive trees, invalid
routes and size limits reject preparation before bootstrap transfers ownership.

A parent explicitly offers a child's exposed protocol to another child:

```json
{
  "name": "tools",
  "children": [
    {"name": "indexer", "component": "indexer"},
    {"name": "client", "component": "client"}
  ],
  "offer": [{
    "name": "index", "to": "client", "from": "#indexer", "source": "query",
    "kind": {"type": "protocol", "protocol": "example.Index"}
  }]
}
```

The provider declares `capabilities` and `expose`:

```json
{
  "name": "indexer",
  "program": {"image": "/svc/indexer", "startup": "lazy"},
  "capabilities": [{"name": "query", "kind": {"type": "protocol", "protocol": "example.Index"}}],
  "expose": [{"name": "query", "from": "self", "source": "query", "kind": {"type": "protocol", "protocol": "example.Index"}}]
}
```

The consumer declares `use` with its local name `index` and matching protocol.
Sources are only `self`, `parent` or `#immediate-child`. Every intervening realm
must explicitly expose/offer the capability. An absolute instance path is never
connection authority. Routes also support Native objects with `object_type`,
Native `rights` bits and a consumer startup `purpose`; each boundary can narrow
rights. `transfer: "move"` permits only one consuming use and prevents restarting
that component with the consumed handle.

`system-startup` may install realm-owned objects through explicit `exports` in
its policy, each naming `realm`, `name` and `source`. The matching realm
capability supplies the rights ceiling. Supported sources are `component.observe`,
`component.control`, `bootstrap.root-directory`, and the task, object, memory and
CPU inspectors. Exported handles are distinct duplicates; routes determine
whether a consumer receives another duplicate or consumes the exported handle.
The root manager does not infer authority from a filesystem path or component name.
Persistent object capabilities belong to non-executable realms. Prepared instances
receive their object grants in the coordinator's construction transaction; their
declarations may use routed protocols but cannot silently override those grants.

Component-manager reports startup progress alongside ordinary service logs. A
single opening line identifies the manager, architecture and boot time. Complete
status lines show component descriptions and coordinator milestones, followed by
a compact lifecycle summary. A row is emitted only when the state or launch
generation changes; worker counts and other ordinary detail updates remain
queryable with `componentctl status`. `START`, `RUNNING`, `DONE` and `FAILED`
describe process state, not protocol readiness. `START` has no duration yet;
`RUNNING` shows `start` latency, and terminal states show `total` time from the
observed start through retirement. Both use milliseconds with two decimal places.
Already-running processes whose launch was not observed show `time unknown`.
Milestone `+` times and the final duration are relative to the manager's opening
banner, whose `boot +` timestamp is relative to kernel boot.

Exit reasons are reported immediately through lifecycle diagnostics. The display
does not add a redundant `STOP` row while resources retire; it reports the final
`DONE`, `FAILED` or `STOPPED` result, including bootstrap retirement. New failure
details remain visible even if the state does not change. Restart generations are
identified, and unfinished cleanup and retries are counted in the final summary.
Incomplete startup includes commands for inspecting component state and logs.
Rows fit an 80-column serial console; long component paths wrap without losing
identity, while clipped descriptions and diagnostic previews have an ellipsis.
There is no animation, row rewriting or console-wide log suppression. Timings are
observations, not hardware or guest boot benchmarks. Coordinator exit or a
60-second deadline ends the temporary report without changing service readiness,
logging or resource ownership.

Formal io-runtime, fs-backend and vm-manager receive separate stdout/stderr
channels registered with journal before launch. Their children inherit those
streams: I/O VM output belongs to io-runtime, filesystem worker output to
fs-backend, and VM runtime output to vm-manager. A normal formal service cannot
silently fall back to raw-console registration if journal has not started. The
journal's own startup diagnostics use explicit raw-console channels to avoid a
circular dependency. Coordinator stdout/stderr use bounded temporary relays until
journal registration, so early errors cannot block the process that starts it.
Bootstrap services retain their independent raw log channels until retirement.
The manager supervises components; session-manager owns terminal and shell
lifetimes. See [Native terminals](https://hyper-proj.github.io/terminal-service/docs/terminal.html) for the initial session handoff.

## Activation and lifetime

- `startup` defaults to `lazy`; `eager` requests activation after launch resources
  and logging are available. Reading status or inspecting routes never starts a
  component.
- A client uses its private connector and local capability name. The first
  connection starts a stopped provider. Concurrent connections share one launch
  generation. The provider publishes its declared protocol and accepts sessions
  on its outgoing endpoint. Publication is separate from Process start.
- Once connected, client and provider communicate directly. The manager is not
  in the request/reply data path. It queues at most 64 pending connections,
  at most eight per consumer, with a maximum 60-second connection deadline.
  Missing publication, provider failure and queue exhaustion return errors.
  Connection rejection carries a structured reason: `not-offered`,
  `invalid-request`, `unavailable`, `restarting`, `timed-out`, or `resource-limit`.
  Transport errors remain separate. A dead provider is unavailable, not a
  permission denial; only an absent capability route is `not-offered`.
- Endpoints are tied to instance and generation. A stale connector cannot publish
  or connect for a replacement. Existing sessions are not replayed or repaired
  across provider death; callers must reconnect and handle application recovery.
- `restart` is `never` (default) or `on-failure`, with `restart_limit` (default 3,
  maximum 10) and `restart_delay_ms` (default 1000). An explicit restart resets
  the retry budget. Exit zero reaches `completed`; it does not create an eager
  respawn loop. A later authorized start or connection can activate it again.
- Start/stop/exit decisions have one owner. Replacement waits for Process retirement,
  the construction worker and the owned TaskGroup to finish. A timeout cannot
  substitute for teardown. An observed Process exit immediately withdraws its
  published routes and enters `stopping` while descendants retire. A subsequent
  stop cancels an outstanding restart.
- `resident` permits the application to use memory locking; it does not pin the
  application automatically. Critical migration participants explicitly lock
  their current mappings before the relevant ownership rendezvous.

Limits are 64 instantiated components, depth 12, 16 KiB per declaration and
128 KiB total configuration. There are at most 32 uses per declaration and 16
arguments per program. The tree is static: dynamic collections, live reload,
idle auto-stop and automatic manager recovery are not implemented.

## Bootstrap adoption

Stage1 still launches the configured successor through its existing handoff ABI.
The shipped successor is `/svc/component-manager`. It prepares the configured
startup coordinator, waits for its resource-policy validation and locked mappings,
then accepts the original atomic inventory and commits the root transition.
It forwards domain resources to the coordinator through a separate private
committed-startup rendezvous. This leaves the kernel and bootstrap contract intact.

`/etc/hyper/startup.json` identifies provider roles, journal, mount/retirement jobs
and initial applications. Composition translates board deployment grants into this
finite migration policy. `prepared: true` declarations require the coordinator to
submit an unsealed ProcessBuilder and its TaskGroup. The manager adds private
component endpoints, starts the process and retains its supervisor; the coordinator
receives only a WAIT observer. Ordinary components cannot access this installer.
Every prepared instance needs exactly one entry in this policy; every routed
object needs a matching startup export. These checks run before ownership commit.
Prepared construction honors the declared arguments and `resident` permission;
adoption providers must permit residency because their protocol requires it.
The retirement role prepends its process count before configured arguments.
An initial application's `interactive: true` marks the single terminal readiness
participant, independent of its image path. The coordinator waits for its READY
notification before publishing `interactive-ready`.

The current console, I/O, filesystem, journal, session and VM-manager instances
use prepared construction. They cannot be stopped or restarted through generic
control: their transferred endpoints or live domain resources need an explicit
ownership protocol. This restriction is checked even after a failed first start.
The startup coordinator itself cannot be recreated from its consumed inventory.
After adoption, the I/O and filesystem providers remain ordinary managed instances
for status, exit observation and attributed stdout/stderr. The stop/restart
restriction does not exempt them from supervision or journal queries.

Startup publishes diagnostic facts such as `core-providers-adopted`,
`filesystems-mounted`, `interactive-ready` and `bootstrap-retired`. These are not
named dependencies and cannot start other components. The coordinator performs
the finite sequence; successful retirement alone publishes `bootstrap-retired`.
If ownership becomes uncertain, it retains known owners and reports failure.
Neither it nor the manager pretends that a committed MOVE rolled back. Stage1
recovery remains available only before the original handoff commit.

## Failure reports

Formal component construction arms bounded process retention before execution:
up to 8 MiB for at most 10 seconds. The manager first reports the immutable
termination reason, then observes the retained fault/memory summary and explicitly
releases its retirement control. Filesystem, terminal and VM-runtime children use
the same mechanism under their existing domain supervisor. Ordinary applications
do not receive retention authority. Losing the supervisor or reaching the kernel
deadline releases the retained memory; a crashed service cannot retain itself.
Diagnostics do not preserve a working IPC endpoint or make old sessions reusable.
`TERMINATED` reports stopped execution; diagnostic readiness and final retirement
are separate transitions. A replacement still waits for final resource retirement.

This is diagnostic retention, not state recovery. Prepared services with consumed
ownership remain non-restartable. Files, VM devices and in-flight writes are not
silently rebound or replayed against a new service generation.

The manager reports Process termination as soon as it observes it, before waiting
for descendant or resource retirement. Reports include the instance path,
generation, Process KOID and kernel termination reason (exit status, requested
stop, or fault). A successful non-critical exit is an informational
`exited successfully (status 0)` record, not a failure notice; its original kernel
reason remains in `componentctl status`. Critical exits still report directly to
Console even for status zero. Launch failures
and startup deadlines are also reported even when no Process was created.

`program.critical` selects direct Console reporting in addition to journal;
it does not stop the manager, grant restart authority or change ownership rules.
The default composition marks console, I/O, filesystem, journal and session
providers critical. Ordinary components report through journal; rejected log
writes, including a full queue or dead journal, fall back to Console. This path
uses the manager's kernel Console capability and does not depend on console-raw,
terminal-service, the shell or loading a diagnostic executable. It remains active
after the boot display finishes.

Reports use a bounded 128-record queue. Console backpressure is retried without
blocking lifecycle observation; partial writes preserve report order. Overflow
is counted and reported when output resumes. Neither an indefinitely stalled
physical console nor a failed component manager can guarantee visible output.
These records are diagnostics, not a durable audit log or a hang detector.

Domain supervisors keep ownership of their children. Filesystem, terminal/session
and VM supervisors can report child failures over their private component
connector. The manager supplies the parent identity and its configured critical
policy; a child cannot choose another component's identity. These reports do not
mark the parent Process as exited or stop healthy siblings. Filesystem reports
include worker identity, mount point and termination reason. Normal unmount and
successful shell exits are not child failures.

## Administration and logs

```sh
componentctl
componentctl status /io-runtime
componentctl routes /session-manager journal
componentctl start /tools/indexer
componentctl stop /tools/indexer
componentctl restart /tools/indexer
journalctl -n 100
journalctl -c /vm-manager -f
```

Control acknowledges acceptance; query status for completion. Read-only observation
and administrative control use distinct capabilities. The trusted shell delegates
these only to resolved `/bin/componentctl`; `/bin/journalctl` receives its direct
journal query session. Component status includes path, generation, Process KOID,
restart count, published protocols and bounded detail text. Route inspection shows
each realm boundary.

Ordinary processes receive stdin at EOF and independent stdout/stderr byte writers.
Programs using std I/O need no framework adaptation. Trusted supervisors register
sources; printed text cannot forge attribution. Records identify component path,
execution generation, stream, monotonic time and sequence. Legacy internal domain
log registrations retain their supplied labels and generation zero.

Retention is volatile: 1 MiB/4096 records globally and 128 KiB/512 records per
component. Lines split at 4096 bytes; partial lines flush after their source has
been idle for at least 100 ms, or immediately on EOF. New fragments restart that
source's idle interval, so a timer tick cannot split a line just received.
Retention uses a sequence index and per-component byte/count accounting; appending
a record does not scan the retained history. Component quotas span streams and
execution generations, and the accounting entry disappears with its last record.
Each ingestion pass handles at most 16 admissions before servicing active sources;
connection churn cannot monopolize the pass. A source yields after eight messages
or at least 64 KiB (the last message is consumed whole). Line batches contain at most
64 records and 16 KiB. Parsing and nonblocking mirror writes run outside the
retention lock. Queries select at most 128 records/24 KiB under that lock, then
materialize and serialize the reply outside it. At most eight query sessions can
hold such pages; an evicted record lives only until those bounded readers release
it. The byte quotas estimate retained record cost, not total process RSS; indices,
input buffers and in-flight replies also consume bounded memory.
Queries use bounded framed byte replies;
clients cannot retain journal-sponsored reply VMOs. The CLI escapes terminal control
characters, reports retention loss and uses a fixed upper cursor for initial lists.
`journalctl --json` exposes one bounded page with records, source attribution,
cursors and loss counters. `--after`, `--through` and `--limit` select the page;
these options require JSON mode. See [service administration](services.md).

Logs mirror to raw serial during startup and after terminal activation, alongside
ordinary kernel logs. The mirror uses each source's line framing, rather than
mixing individual stdout/stderr write fragments. Partial records also terminate a
physical line, while retained records preserve their continuation flag. Logs may
interrupt the interactive display. If the mirror queue cannot accept a complete
batch, that batch is dropped from the mirror and its bytes are counted; retained
records are unaffected. Producer queues provide bounded backpressure.
This is not a durable audit log. Panic output remains an independent kernel path.

## Verification

`make app-test APP_TEST_PACKAGE=hyper-component-config` checks tree/route policy;
`hyper-component-manager` checks lifecycle arbitration; `hyper-system-startup`
checks adoption/provisioning policy; `hyper-journald` checks retention, indexed
eviction, query lifetime and batched line framing.
`make test-services` exercises nested routes, eight concurrent lazy connections,
separate observation/control authority, stdio, connection deadlines, restart bounds
and retirement using actual Native processes. See [bootstrap handoff](https://hyper-proj.github.io/lib-bootstrap-handoff/docs/handoff.html)
and [boot layout](https://hyper-proj.github.io/bootstrap/docs/boot-layout.html) for the filesystem and ownership contracts.

`make test-service-failure` builds an isolated fault-injection variant and
terminates live console, journal, session and VM-manager processes. It also checks
fault/exit/launch failures, child reporting and Console fallback after journal
termination. Production builds do not contain the test fault-injection
hook. `make test-service-provider-failure ARCH=aarch64` repeats the direct-Console
checks for I/O and filesystem providers on disposable, disk-backed QEMU roots.
The storage boot suite separately checks mounted filesystem worker death and
isolation of healthy mounts, including the parent's direct Console report.

For a manual host retention microbenchmark, build the optimized test executable:

```sh
make app-test APP_TEST_PACKAGE=hyper-journald APP_TEST_ARGS='--release --lib --no-run'
```

Run the executable printed by Cargo with `retention_throughput --ignored --nocapture`.
It measures appends with one or sixteen producers and interleaved queries, excluding
IPC, scheduling and serial I/O. This is not an end-to-end logging throughput claim
or a CI performance threshold.

## Scheduling authority

A component may select initial scheduling through `program.scheduling`:

```json
{
  "policy": {"kind": "fair", "weight": 16, "quantum_ns": 5000000},
  "band": 64,
  "role": "priority"
}
```

The generated service deployment uses this bounded Priority/Fair profile. A
formal source service can override it with its `scheduling` field; composition
copies that field into the component declaration. `backend` is optional for
Fair: omission selects the kernel's configured implementation, so switching
between stride and round-robin does not require editing service manifests.
Budget and FIFO are alternate `policy.kind` values. Budget fields are
`budget_ns`, `period_ns`, and `burst_ns`; its period must match kernel policy.
FIFO has `priority` (0 is highest). `role` accepts `ordinary`, `priority`, and
`protected`; the role controls arbitration independently of the algorithm.

The manager validates the declaration before activation and configures the
ProcessBuilder before sealing it. A declaration requests policy; it grants no
kernel authority. Changes require distinct `sched-priority`, `sched-class`, or
`sched-budget` rights on the builder. Children receive only `sched-lower` on
their ExecutionContext by default. Ordinary WRITE never authorizes elevation.
Ordinary new processes enter the App scheduling boundary; new threads inside a
process inherit their creator's current parameters. The filesystem manager
explicitly applies its own parameters to its provider workers, using its
separately delegated construction rights. This does not give workers the right
to elevate themselves.

`hyper_os::scheduling` exposes current-thread, Thread-handle, and vCPU-handle
queries and changes. INSPECT authorizes queries; lowering treatment does not
imply the right to restore it later. Configuration changes validate every
requested field and the object's immutable ceiling before committing. Failed
changes leave the previous configuration intact.

Weights and Budget accounting are per Thread (per vCPU for virtual machines).
They do not impose an aggregate process or VM CPU quota: creating additional
threads creates additional runnable entities. Deployment must bound thread and
vCPU counts separately when it needs an aggregate resource limit.
