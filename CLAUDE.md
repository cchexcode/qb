# CLAUDE.md

> **Self-updating**: When you learn something new about this project's patterns, conventions,
> architecture, or coding standards during a task, update this file immediately. Keep it concise
> and authoritative — this is the single source of truth for how to work in this codebase.

## Project

qb is EMACS for Kubernetes — a powerful, extensible ratatui-based terminal UI for managing
Kubernetes clusters. It talks directly to the K8s API server via the `kube` crate using
kubeconfig — no kubectl dependency. Single binary, no external frontend, no remote services.

**Workspace layout**: `crates/qb/` is the sole crate. Workspace root at `Cargo.toml`.

```
crates/qb/src/
  main.rs          Entry point, CLI routing
  args.rs          CLI argument parsing (clap derive)
  config.rs        Persistent config: profiles, favorites, saved port forwards, version checking.
                   Loads/saves ~/.config/qb/config.yaml.
  k8s/mod.rs       K8s API client, resource types, pod resolution, log fetching, cluster stats,
                   resource editing (replace via DynamicObject)
  portforward.rs   Port forward manager: background tasks, auto-restart, pod resolution,
                   port extraction helpers, pause/resume/cancel lifecycle
  tui/mod.rs       Terminal setup, event loop (render → load → poll logs → poll pf → poll input),
                   external editor invocation (suspend/resume terminal), config save on quit
  tui/app.rs       App struct (all state), key handlers, deferred loading, filter state,
                   dict entry selection/expansion, edit flow (diff, apply, re-edit),
                   port forward dialog + creation flow, favorites toggle, profile management
  tui/command.rs   Command registry — single source of truth for all keybindings.
                   Drives hotkey bars, help screen, and command palette.
  tui/ui.rs        Rendering: main view, detail view, log view, events log, cluster stats,
                   favorites view, port forwards view, edit diff view (inline + side-by-side),
                   popups, breadcrumb with refresh indicator, hotkey bars, node grid cards
  tui/smart.rs     Per-resource-type structured renderers, SecretDetailState, DictState,
                   selectable labels/annotations, structured resource requests/limits
  tui/logs.rs      LogViewState, follow-mode streaming, regex filter
```

## Design Principles

1. **Native K8s only** — All cluster communication via the `kube` crate. Never shell out to kubectl.
2. **All-namespaces by default** — Every list query defaults to all namespaces. Namespace column
   appears automatically. Switching clusters resets to all-namespaces.
3. **All-pods/all-containers by default** — Log views aggregate all pods and containers for a
   workload. Users narrow down with popup selectors (`p`/`c`).
4. **Non-blocking UI** — API calls are deferred via `PendingLoad` and executed between frames.
   Log streaming uses async tasks with `mpsc` channels polled each tick. The UI never blocks on I/O.
5. **Auto-refresh** — Resource list and cluster stats refresh every 2 seconds. Scroll position and
   selection are always preserved across refreshes — never reset viewport on auto-refresh.
   Breadcrumb shows refresh timestamp right-aligned. `p` pauses/resumes globally.
6. **Smart then YAML** — Detail views open in Smart mode (typed, structured rendering per resource
   type). `v` cycles between Smart and YAML views.
7. **Keyboard-only** — Gitui-inspired hotkey bar at the bottom. All navigation uses vim-style
   bindings (j/k, Ctrl+d/u, g/G). No mouse support.
8. **Breadcrumb always visible** — Top bar shows `cluster > namespace > type > resource > logs` in
   every view, plus a right-aligned refresh indicator (`⟳ just now` / `⟳ Ns ago` / `⏸ paused`).
9. **Cluster stats as default view** — App starts on the Overview panel showing cluster-wide
   statistics, node grid cards, and pod health gauges.
10. **Instant nav** — Sidebar selection loads immediately on j/k movement, no Enter required.
    Enter moves focus to the right panel.
11. **Space = expand everywhere** — `Space` is the universal expand/decode key across all views
    (secret values, label/annotation values).
12. **Edit via $EDITOR** — `e` opens the resource YAML in `$EDITOR` (falls back to vim → vi).
    On save, shows a diff preview (inline or side-by-side) with apply/re-edit/cancel options.
13. **All text editing uses $EDITOR** — Labels, annotations, resource YAML, and new resources
    are all edited by opening `$EDITOR` with YAML content. No inline character-by-character
    editing for structured data. The TUI suspends, editor runs, TUI resumes on close.

## Key Bindings Summary

Keyboard-only. All navigation uses vim-style bindings.

### Global

| Key | Action |
|-----|--------|
| `Ctrl+C` | Force quit |
| `q` | Quit / back |
| `Esc` | Back / dismiss popup or selection |
| `?` | Help (context-only by default, `Tab` to show all) |
| `Ctrl+P` | Command palette |
| `Ctrl+S` | Save current profile |
| `P` | Load/switch profile |
| `p` | Pause/resume auto-refresh (except log view, port forwards view) |

### Main View

| Key | Action |
|-----|--------|
| `j` / `Down` | Move down in sidebar or resource table |
| `k` / `Up` | Move up in sidebar or resource table |
| `Ctrl+d` / `PgDn` | Page down in table |
| `Ctrl+u` / `PgUp` | Page up in table |
| `g` / `Home` | Jump to top of list |
| `G` / `End` | Jump to bottom of list |
| `Enter` | Open detail / move focus to right panel |
| `Tab` | Toggle focus between sidebar and table |
| `r` | Focus resource table |
| `c` | Switch cluster context (popup) |
| `n` | Switch namespace (popup) |
| `O` | Open kubeconfig |
| `/` | Filter resources by regex |
| `x` | Clear filter |
| `e` | Edit selected resource ($EDITOR) |
| `l` | Open logs (workload resources) |
| `F` | Create port forward (popup, for workload/service resources) |
| `D` | Delete selected resource (confirmation popup) |
| `R` | Restart workload |
| `S` | Scale workload (Deployment/StatefulSet/ReplicaSet) |
| `y` | Copy resource name |
| `d` | Mark / diff resources |
| `C` | Create new resource |
| `X` | Exec into pod (experimental) |
| `*` | Toggle favorite for selected resource |

### Detail View

| Key | Action |
|-----|--------|
| `j` / `Down` | Scroll down or navigate entries |
| `k` / `Up` | Scroll up or navigate entries |
| `Ctrl+d` / `PgDn` | Page down |
| `Ctrl+u` / `PgUp` | Page up |
| `Home` | Jump to top |
| `v` | Cycle view: Smart → YAML → Smart |
| `s` | Enter/leave label/annotation selection |
| `Enter` | Edit selected label/annotation |
| `Space` | Expand/collapse selected entry or decode secret |
| `y` | Copy: selected entry value, full YAML, or secret |
| `e` | Edit resource ($EDITOR) |
| `l` | Open logs (workload resources) |
| `F` | Create port forward (popup) |
| `D` | Delete resource (confirmation popup) |
| `R` | Restart workload |
| `S` | Scale workload |
| `w` | Toggle watch mode (auto-refresh detail) |
| `Tab` | Toggle related resources selection |
| `X` | Exec into pod (experimental) |
| `*` | Toggle favorite for current resource |

### Log View

| Key | Action |
|-----|--------|
| `j` / `Down` | Move cursor down |
| `k` / `Up` | Move cursor up |
| `Ctrl+d` / `PgDn` | Page down |
| `Ctrl+u` / `PgUp` | Page up |
| `g` / `Home` | Jump to top |
| `G` / `End` | Jump to bottom |
| `f` | Toggle follow (live streaming) |
| `/` | Filter by regex |
| `x` | Clear filter |
| `p` | Select pod (popup) |
| `c` | Select container (popup) |
| `w` | Toggle line wrapping |
| `t` | Set time filter |
| `Y` | Copy all logs to clipboard |
| `Enter` | Open selected line detail |

### Edit Diff View

| Key | Action |
|-----|--------|
| `j` / `Down` | Scroll down |
| `k` / `Up` | Scroll up |
| `Ctrl+d` / `PgDn` | Page down |
| `Ctrl+u` / `PgUp` | Page up |
| `v` | Cycle: inline diff ↔ side-by-side diff |
| `Enter` | Apply changes to cluster |
| `e` | Re-edit (reopen $EDITOR with current edits) |
| `Esc` | Cancel edit |

### Port Forwards View

| Key | Action |
|-----|--------|
| `j` / `Down` | Navigate list |
| `k` / `Up` | Navigate list |
| `p` | Pause/resume selected forward |
| `d` | Cancel (delete) selected forward |

## Sidebar Structure

The sidebar is a flat list with category headers (non-selectable) and selectable items.
Overview is always the first item and the default landing screen.

1. **Overview** — Cluster stats, health, node grid (default on startup)
2. **Favorites** — Starred resources across all types (`*` to toggle, shows count badge)
3. **Port Forwards** — View, pause, resume, cancel; persisted to config
4. **WORKLOADS** — Deployments, StatefulSets, DaemonSets, ReplicaSets, Pods, CronJobs, Jobs, HPAs
5. **NETWORK** — Services, Ingresses, Endpoints, NetworkPolicies
6. **CONFIG** — ConfigMaps, Secrets
7. **STORAGE** — PVCs, PVs, StorageClasses
8. **RBAC** — ServiceAccounts, Roles, RoleBindings, ClusterRoles, ClusterRoleBindings
9. **CLUSTER** — Nodes, Namespaces, Events

Categories are non-selectable headers (`NavItemKind::Category`). Navigation with j/k
skips over them.

Cluster-scoped resources (Node, Namespace, PV, StorageClass, ClusterRole, ClusterRoleBinding)
use `list_cluster`/`get_value_cluster` helpers instead of the namespaced variants.

## Special Views

### Cluster Stats (Overview)

Shown by default on startup. Displays stat cards (K8s version, nodes, namespaces, deployments,
services), pod health gauge bar, and a responsive grid of node cards. Each node card shows
status, role, version, cpu/memory/pods capacity, os/arch, and age. Grid auto-tiles based on
terminal width.

### Events Log

Events render as a scrollable log stream (not a table). Each event line has a type icon
(● green / ⚠ yellow), color-coded reason, repeat count badge, and message. Events sort
oldest-first (newest at bottom) with auto-follow when cursor is at the bottom.

### Resource Filter

Press `/` in the main view to filter resources by regex. Matches against name, namespace, and
all column values. Filter bar shows match count. `x` clears. Works on both the table view and
events log. Filter persists across auto-refreshes, clears on resource type change.

### Selectable Labels/Annotations

In the detail smart view, press `s` to enter selection mode on labels/annotations. Navigate
with `j`/`k`, press `Space` to expand/collapse long values (word-wrapped at 100 chars), press
`y` to copy `key: value` to clipboard. Press `s` again to leave selection mode. Values longer
than 70 chars are truncated with `...` until expanded.

### Resource Editing

Press `e` on any resource (from list or detail view) to edit it. The flow:
1. YAML is opened in `$EDITOR` (falls back to `vim` then `vi`)
2. TUI suspends, editor runs, TUI resumes on editor exit
3. Diff preview shows changes (inline or side-by-side, toggle with `v`)
4. `Enter` applies via `Api::replace` (kube dynamic API with `DynamicObject`)
5. On error, shows message and offers `e` to re-edit or `Esc` to cancel
6. On success, returns to detail view with refreshed data

### Port Forwarding

Press `F` on any workload or service resource to create a port forward. The flow:
1. Popup shows available ports (from `spec.ports` for Services, container ports for workloads)
2. Navigate ports with `j`/`k`, edit local port number (defaults to same as remote)
3. `Enter` creates the forward, `Esc` cancels
4. Port forwards run as background tokio tasks with auto-restart on failure
5. View all forwards in the Port Forwards sidebar item
6. Port forwards are automatically persisted to the active profile in config

Port forward architecture:
- `PortForwardManager` in `portforward.rs` owns all entries and communicates with tasks via
  `mpsc` channels (same pattern as log streaming)
- Each forward binds a local `TcpListener` and for each connection resolves the current pod
  (via label selector for services/deployments, direct name for pods)
- Pod resolution per-connection means forwards survive pod restarts and rolling updates
- Reconnection with exponential backoff (up to 10 retries per connection)
- `cancel_tx`/`pause_tx` watch channels control lifecycle from the UI
- Status updates flow from background tasks → `PfUpdate` channel → `poll_updates()`
- `PfTarget::DirectPod` for pod forwards, `PfTarget::LabelSelector` for service/deployment

### Delete Resource

Press `D` on any resource (from list or detail view). Confirmation popup with Enter/y to
confirm, Esc/n to cancel. Uses `Api::delete` via the dynamic API. After deletion, refreshes
the resource list. If in detail view, returns to main view.

### Scale Workloads

Press `S` on Deployment, StatefulSet, or ReplicaSet. Popup shows current replica count and
an editable field for the new count. Uses `Api::patch` with `Patch::Merge` to update
`spec.replicas`. `ResourceType::supports_scale()` gates which types support this.

### Container Exec/Shell

**Experimental** — requires `qb -e` to enable.

Press `x` to open the exec dialog: choose container (Tab to switch fields, Up/Down for
containers), edit command (split on whitespace for argv), and override the terminal application.

Exec opens a **new terminal window** running `kubectl exec -it`. The terminal is resolved as:
1. `$TERMINAL` env var (explicit override, freedesktop convention)
2. `$TERM_PROGRAM` env var (set automatically by most terminals: Ghostty, iTerm2, Alacritty,
   kitty, WezTerm, etc.)
3. Error if neither is set — user must set one of the above

On macOS, Apple Terminal.app uses osascript `do script`. All other terminals are invoked
directly as `<terminal> -e <script>`, which spawns a new window. The exec dialog allows
overriding the terminal per-session.

The kubectl command includes `--context` and `--kubeconfig` (if a custom kubeconfig was loaded
via `O`) to ensure the exec targets the correct cluster. The spawned terminal auto-closes
after the session ends (`; exit` appended).

### Describe-Style Events

The detail smart view automatically shows related events at the bottom, fetched via
`KubeClient::fetch_related_events` using a `involvedObject.name` field selector. Events
display type icon (green dot / yellow warning), age, repeat count, reason, and message.
Fetched alongside the detail value in `load_resource_detail`.

### Resource Count Badges

Sidebar nav items show cached resource counts as `(N)` badges. Counts are updated in
`load_resources()` and stored in `App::resource_counts: HashMap<ResourceType, usize>`.
Only types that have been loaded at least once show counts.

### Pod/Container Selection

In the log view, `p` and `c` open popup list selectors (not cycling). Lists include "All" as
the first entry. Navigate with j/k, select with Enter.

### Pause

`p` toggles auto-refresh globally (main view, detail view). When paused, the breadcrumb shows
`⏸ paused` and log stream polling is suppressed. In the log view, `p` is the pod selector
instead — use `f` (follow) to control live streaming there. In the port forwards view, `p`
pauses/resumes the selected port forward instead.

### Persistent Config

State is persisted to `~/.config/qb/config.yaml` via `config.rs`. The config file stores:
- **version** — Must be compatible with CLI version (secenv-style: 0.0.0 allows any, pre-1.0
  minor must match, post-1.0 major must match).
- **active_profile** — Name of the currently active profile.
- **profiles** — Map of named profiles, each containing favorites, saved port forwards, and
  an optional kubeconfig path.

Config auto-saves on: favorite toggle, port forward create/cancel, profile save/switch, quit.
Created automatically on first run with a "default" profile.

### Favorites

Press `*` on any resource (list or detail view) to toggle it as a favorite. Favorites are
stored per-profile in the config file. The sidebar shows "Favorites" between Overview and
Port Forwards, with a count badge.

The favorites view acts like a normal resource list — all standard commands (Enter, `e`, `l`,
`D`, `R`, `S`, `F`, `y`) work on the selected favorite, resolving to the favorite's resource
type. Press `*` to de-favorite. Resources show `★` prefix when favorited. Missing resources
(context no longer available) show `⚠` with red styling.

### Profiles

Profiles group favorites and saved port forwards under a name. The default profile is always
created on first run. `Ctrl+S` opens a save dialog, `P` opens the profile switcher. Profiles
are stored in config and auto-saved on changes.

## Coding Standards

### Object-oriented style

All behavior lives in `impl` blocks on the struct that owns the relevant state. No loose
functions except pure value helpers (e.g., `format_age`, `meta_name`, JSON navigation helpers
like `jget`/`js`/`ji`).

```rust
// YES — method on the struct
impl App {
    fn load_resources(&mut self) { ... }
}

// YES — pure helper, no state
fn format_age(timestamp: Option<&Time>) -> String { ... }

// NO — do not create free functions that take &mut App or &KubeClient
```

### Module structure

Follow Rust conventions: one `mod.rs` per directory, re-export public items. Each module has a
single clear responsibility:

- `k8s/` — data access only. No TUI imports. Returns domain types (`ResourceEntry`, `PodInfo`,
  `ClusterStatsData`, `Value`). Also handles `replace_resource_yaml` via dynamic API.
- `tui/mod.rs` — terminal setup, event loop, and external editor invocation (suspend/resume).
- `tui/app.rs` — state + event handling. No rendering code. Includes edit flow state
  (`PendingEdit`, `EditContext`, `DiffMode`).
- `tui/ui.rs` — rendering only. Reads from App, writes to Frame. No mutations except
  clamping scroll/cursor positions and syncing `DictState`.
- `tui/smart.rs` — per-resource-type renderers. Returns `Vec<Line<'static>>`. Accepts
  `ds: &mut DictState` for label/annotation selection/expansion. May mutate `SecretDetailState`.
- `tui/logs.rs` — log view state encapsulated in `LogViewState`. Owns its own streaming handles,
  filter state, and scroll position.

### Enum-driven dispatch

Resource types are a `ResourceType` enum. Adding a new type means:
1. Add variant to `ResourceType`
2. Add to `all_by_category()`
3. Add `column_headers()` match arm
4. Add `map_*` static method + `list_typed`/`get_value` match arms in `KubeClient`
   (or `list_cluster`/`get_value_cluster` for cluster-scoped resources)
5. Add `render_*` function in `smart.rs` + dispatch match arm (accepts `ds: &mut DictState`)
6. If it supports logs, add to `supports_logs()` match
7. Add to `api_resource()` and `is_cluster_scoped()` match arms
8. Add `sort_key: None` (or `Some(...)` for sortable types) to the `ResourceEntry` constructor

Views (`View::Main | Detail | Logs | EditDiff`), detail modes (`DetailMode::Smart | Yaml`),
diff modes (`DiffMode::Inline | SideBySide`), and pending loads (`PendingLoad`) all use enums
for exhaustive matching. No `unreachable!()` in dispatch.

### Patterns to follow

- **Command registry** (`tui/command.rs`): All commands are defined once in a static `COMMANDS`
  table. Each `Cmd` has key label, display label, description, contexts, hotkey visibility,
  palette eligibility, and an optional availability predicate. This single table drives:
  - The hotkey bar (`build_hotkey_bar` in ui.rs)
  - The help screen (`filtered_help_entries` in app.rs)
  - The command palette (`palette_commands` in command.rs)
  To add a new command: add a `Cmd` entry to `COMMANDS`, add the key handler in the appropriate
  `handle_*_key` method, and it automatically appears in the hotkey bar, help, and palette.
  **MUST**: Every command MUST have correct `contexts` — the help screen defaults to showing
  only commands available in the current context. `help_entries_for_context()` filters by
  context + Global. Users press `Tab` to toggle to the full list. If a command's context is
  wrong, it will be invisible in the help screen when the user needs it most.
- **Deferred loading**: Queue a `PendingLoad` variant → `process_pending_load()` runs it after
  the next render. Never call `block_on` inside a key handler directly.
- **Selection preservation**: When refreshing a list, save the selected item's `(name, namespace)`
  pair, reload, then find the same pair in the new list. Never reset `TableState` to `default()`
  on refresh — only update the selected index to preserve viewport offset.
- **Scroll preservation**: Auto-refresh must never reset scroll positions. Only user-initiated
  navigation (switching resource type, opening a new detail) should reset scroll.
- **Popup = `Option<Popup>`**: One popup at a time. `None` means no popup. Popup keys are handled
  before view-specific keys. Pod/container selectors are popups, not cycling.
- **Log prefixes**: Every log line from multi-pod/container sources is prefixed with
  `[pod/container]` so the user can identify the source and filter with regex.
- **Filtered views**: Resource filter computes `visible_resource_indices()` — a mapping from
  display position to real `resources` index. Table renders only visible rows. Navigation and
  Enter translate through this mapping. Events cursor indexes into the filtered view.
- **DictState**: `smart::DictState` bundles mutable state for label/annotation selection. Built
  fresh each render in `render_smart_lines`, synced back to `App` fields (`dict_entries`,
  `dict_line_offsets`, `dict_cursor`, `expanded_keys`). `dict_section` registers entries and
  renders the cursor highlight.
- **Dynamic column widths**: Table column widths are computed from actual data content (header +
  values + 2 padding), capped at 50. NAME column uses `Constraint::Min`, others `Constraint::Length`.
  Detail view `field()` pads labels to `max(label.len(), 18)`. Conditions and resource tables also
  compute widths from data.
- **Edit flow**: `PendingEdit` is set by key handler, consumed by the event loop in `tui/mod.rs`
  to suspend terminal → run `$EDITOR` → resume terminal. Result feeds into `handle_edit_result`
  which computes the diff and enters `View::EditDiff`. Apply uses `KubeClient::replace_resource_yaml`
  with `DynamicObject` + `ApiResource` for generic resource replacement.
- **External editor**: Resolved as `$EDITOR` → `vim` → `vi`. Terminal is fully suspended
  (leave alternate screen, disable raw mode) before editor runs, and restored after.
- **OSC 52 vs arboard**: Clipboard currently uses `arboard` (native macOS/Linux clipboard). If
  terminal-only clipboard is needed in the future, OSC 52 escape sequences are the alternative.

### Style

- Match arms use leading `|` pipes (configured in `.rustfmt.toml`).
- Max line width: 120 chars.
- Prefer `&str` / `&'static str` return types for display methods on enums.
- Use `saturating_sub`/`saturating_add` for all scroll/index arithmetic.
- Avoid `unwrap()` — use `unwrap_or`, `unwrap_or_default`, or propagate with `?`.

## Dependencies

- **kube** + **k8s-openapi** — K8s API client. `ws` feature enables port forwarding. Timestamps use `jiff` (not chrono).
- **ratatui** + **crossterm** — TUI framework.
- **rustls** with `ring` feature — TLS crypto provider (must be explicit since rustls 0.23+).
- **jiff** — Time arithmetic (duration formatting, age display). Replaced chrono.
- **arboard** — System clipboard access for secret copying.
- **similar** — Text diff computation (unified diff for edit preview).
- **tempfile** — Temp file creation for editor invocation.
- **semver** — Semantic version parsing and comparison for config compatibility checking.
- **dirs** — Cross-platform config directory resolution (`~/.config`).

## Build

```sh
cargo build -p qb              # debug
cargo build --release -p qb    # release
cargo run -p qb                # run
cargo run -p qb -- --context my-cluster --namespace kube-system
```

## Formatting

**Always run after every edit session:**

```sh
cargo +nightly fmt
```

This formats the entire workspace. Never skip this step — all code must be formatted before
committing or reviewing.
