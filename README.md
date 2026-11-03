<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# component-manager

Native component lifecycle owner. Admission workers decode management, connection
and private installation requests before forwarding them to the lifecycle reactor.

## Workspace tests

Use the [HypeR workspace](https://github.com/HypeR-Proj/HypeR) to resolve dependencies:

```sh
python3 project.py test component-manager
```

Paths in the documentation are relative to the repo-managed workspace root.
Edit this repository and prepare the workspace again before building.

## Listener failure policy

A malformed request or matched transfer rejection does not stop a management,
connection or installation worker. Only accepted calls enter lifecycle policy.
Receiver-local errors are diagnosed and stop the affected listener. The finite
connector wait may time out to check component cancellation; endpoint closure
ends the worker. Neither case is confused with a malformed client request.

## Management results

The lifecycle owner applies each start, stop or restart command and captures its
result, revision and snapshot together. The management reply carries that owned
snapshot even if another command or process event runs before the reply is sent.
That snapshot is also published for observers before the acknowledgment.

Success means lifecycle intent was applied. Start can still show `Stopped`
until the reactor selects its launch. Stop and restart of an active execution
acknowledge `Stopping` with the retained generation and process identity;
retirement and replacement happen later. Inspect current status to observe those
transitions. An acknowledgment timeout remains an uncertain outcome: inspect
state before retrying, especially for restart.

Startup progress is a set of at most sixteen named facts. Repeating an existing
fact succeeds without changing the revision, including when the set is full;
only a new fact consumes capacity.
