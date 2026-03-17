# qb

A fast, keyboard-driven terminal UI for browsing Kubernetes clusters.

`qb` communicates directly with the Kubernetes API server using your kubeconfig.
It does **not** shell out to `kubectl` — all cluster communication is native.

```
┌ minikube > All Namespaces > Deployments ────────────────────────────────────┐
├──────────────────────────┬──────────────────────────────────────────────────┤
│ Cluster                  │ NAME              NAMESPACE   READY  UP  AGE     │
│ ▶ Overview               │▶ nginx-ingress    ingress     3/3    3   12d     │
│   Nodes                  │  coredns          kube-system 2/2    2   30d     │
│   Namespaces             │  metrics-server   monitoring  1/1    1    5d     │
│   Events                 │                                                  │
│ Workloads                │                                                  │
│   Deployments            │                                                  │
│   StatefulSets           │                                                  │
│   DaemonSets             │                                                  │
│   Pods                   │                                                  │
│ Network                  │                                                  │
│   Services               │                                                  │
│   Ingresses              │                                                  │
│ Config                   │                                                  │
│   ConfigMaps             │                                                  │
│   Secrets                │                                                  │
├──────────────────────────┴──────────────────────────────────────────────────┤
│ r  Resources  c  minikube  n  All Namespaces  /  Filter  l  Logs    q  Quit │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Features

- **Browse resources** — Deployments, StatefulSets, ReplicaSets, Pods, CronJobs, Jobs, ConfigMaps, Secrets, Services
- **Multi-cluster** — Switch between kubeconfig contexts on the fly
- **All namespaces by default** — Namespace column appears automatically; filter down with `n`
- **Smart detail view** — Structured, typed rendering for every resource (replicas, containers, conditions, ports, etc.)
- **YAML view** — Full YAML with syntax highlighting, toggle with `y`/`s`
- **Secret management** — Decode individual secret values in-place, copy plaintext to clipboard
- **Live logs** — Stream logs from all pods/containers of a workload; regex filter and follow mode
- **Auto-refresh** — Resource list updates every 2 seconds without losing your selection
- **Mouse support** — Click to select resources, scroll wheel navigation, click popup items
- **Breadcrumb navigation** — Top bar always shows `cluster > namespace > type > resource > logs`

## Installation

### From crates.io (recommended)

Requires [Rust](https://rustup.rs/) 1.75+.

```sh
cargo install qb
```

### From git

```sh
cargo install --git git@github.com:cchexcode/qb.git -p qb
```

### From source

```sh
git clone git@github.com:cchexcode/qb.git
cd qb
cargo build --release -p qb
# Binary is at target/release/qb
```

## How It Works

`qb` uses the [kube](https://github.com/kube-rs/kube) crate to communicate directly with the Kubernetes API server. Authentication and cluster connection details are read from your kubeconfig file (`~/.kube/config` or `$KUBECONFIG`).

The TUI is built with [ratatui](https://ratatui.rs/) and [crossterm](https://github.com/crossterm-rs/crossterm). The interface follows patterns inspired by [gitui](https://github.com/extrawurst/gitui) — colored hotkey bar at the bottom, modal popups for selection, and keyboard-first navigation.

API calls are deferred and executed between render frames to keep the UI responsive. Log streaming uses async tasks with channels to push new lines into the view without blocking.
