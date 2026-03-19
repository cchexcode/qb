# qb — EMACS for Kubernetes

A powerful, extensible terminal UI for managing Kubernetes clusters.

`qb` communicates directly with the Kubernetes API server using your kubeconfig.
It does **not** shell out to `kubectl` — all cluster communication is native via the `kube` crate.

```
┌ minikube > All Namespaces > Deployments ──────────────────── ⟳ just now ──┐
├──────────────────────────┬────────────────────────────────────────────────┤
│ CLUSTER                  │ NAME              NAMESPACE   READY  UP  AGE   │
│   Cluster                │▶ nginx-ingress    ingress     3/3    3   12d   │
│     Overview             │  coredns          kube-system 2/2    2   30d   │
│     Nodes                │  metrics-server   monitoring  1/1    1    5d   │
│     Namespaces           │                                                │
│     Events               │                                                │
│   Workloads              │                                                │
│     Deployments (3)      │                                                │
│     StatefulSets (1)     │                                                │
│     Pods (8)             │                                                │
│   Network                │                                                │
│     Services (4)         │                                                │
│   Config                 │                                                │
│     ConfigMaps           │                                                │
│     Secrets              │                                                │
│ GLOBAL                   │                                                │
│   Port Forwards          │                                                │
├──────────────────────────┴────────────────────────────────────────────────┤
│ r Resources  c minikube  n All Namespaces  ^p Palette  ? Help  q Quit     │
└───────────────────────────────────────────────────────────────────────────┘
```

## Features

### Browse & Navigate
- **All resource types** — Deployments, StatefulSets, DaemonSets, ReplicaSets, Pods, CronJobs, Jobs, HPAs, Services, Ingresses, Endpoints, NetworkPolicies, ConfigMaps, Secrets, PVCs, PVs, StorageClasses, ServiceAccounts, Roles, RoleBindings, ClusterRoles, ClusterRoleBindings, Nodes, Namespaces, Events
- **Resource count badges** — Sidebar shows cached counts next to each type
- **Multi-cluster** — Switch between kubeconfig contexts (`c`) or load a different kubeconfig (`O`)
- **All namespaces by default** — Namespace column appears automatically; filter with `n`
- **Smart detail view** — Structured rendering per resource type with describe-style events
- **YAML view** — Syntax-highlighted YAML, toggle with `v`
- **Auto-refresh** — Lists and detail views refresh every 2s, preserving scroll and selection
- **Watch mode** — Detail view auto-refreshes (on by default, toggle with `w`)

### Act
- **Edit resources** (`e`) — Opens in `$EDITOR`, shows diff preview, apply with Enter
- **Create resources** (`C`) — Opens `$EDITOR` with YAML template, applies on save
- **Delete resources** (`D`) — Confirmation popup
- **Scale workloads** (`S`) — Deployments, StatefulSets, ReplicaSets
- **Restart workloads** (`R`) — Deployments, StatefulSets, DaemonSets
- **Diff resources** (`d`) — Mark one resource, select another, see side-by-side diff

### Logs
- **Live streaming** — Follow logs from all pods/containers of a workload
- **Regex filter** — `/` to filter, matches highlighted
- **Line selection** — `j`/`k` or mouse click to select, `Enter` to open full line
- **Wrap toggle** (`w`) — Wrap long log lines
- **Pod/container selector** — `p`/`c` to narrow down

### Port Forwarding
- **Create** (`F`) — Forward any service, deployment, or pod port to localhost
- **Auto-restart** — Forwards survive pod restarts and rolling updates
- **Manage** — View all forwards under GLOBAL > Port Forwards; pause/resume/cancel

### Exec (Experimental)
- **Quick exec** (`x`) — Opens `/bin/sh` in a new terminal window
- **Custom exec** (`X`) — Choose container, command, and terminal app
- **Requires** `qb -e` flag and `$TERMINAL` or `$TERM_PROGRAM` env var

### Command Palette & Search
- **Command palette** (`Ctrl+P`) — Fuzzy search resources, `>` prefix for commands
- **Global search** (`Tab` in palette) — Search across ALL resource types
- **Resources searchable as** `type/name` (e.g. `deployment/myapp`, `service/frontend`)
- **Help** (`?`) — Searchable keybinding reference

### Secret Management
- **Decode secrets** — `Space` to decode individual values in-place
- **Copy to clipboard** — `y` to copy decoded plaintext

### Mouse Support
- Click to select resources, scroll wheel navigation, click popup items
- Click log lines to select (when not wrapping)

## Installation

### From crates.io

Requires [Rust](https://rustup.rs/) 1.75+.

```sh
cargo install qb
```

### From source

```sh
git clone git@github.com:cchexcode/qb.git
cd qb
cargo build --release -p qb
# Binary is at target/release/qb
```

## Usage

```sh
qb              # browse using default kubeconfig
qb -e           # enable experimental features (exec)
```

Switch kubeconfig at runtime with `O`. Switch context with `c`. Switch namespace with `n`.

## How It Works

`qb` uses the [kube](https://github.com/kube-rs/kube) crate to communicate directly with the Kubernetes API server. Authentication and cluster connection details are read from your kubeconfig file (`~/.kube/config` or `$KUBECONFIG`).

The TUI is built with [ratatui](https://ratatui.rs/) and [crossterm](https://github.com/crossterm-rs/crossterm). API calls are deferred and executed between render frames to keep the UI responsive. Log streaming and port forwarding use async tasks with channels.

## License

MIT
