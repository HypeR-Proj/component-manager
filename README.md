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
