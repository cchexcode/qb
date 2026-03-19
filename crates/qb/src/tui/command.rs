use crate::k8s::ResourceType;

// ---------------------------------------------------------------------------
// Command context — identifies where a command is available
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Ctx {
    Global,
    Nav,
    Resources,
    ClusterStats,
    Events,
    Detail,
    Logs,
    EditDiff,
    PortForwards,
}

impl Ctx {
    pub fn label(self) -> &'static str {
        match self {
            | Self::Global => "Global",
            | Self::Nav => "Sidebar",
            | Self::Resources => "Resources",
            | Self::ClusterStats => "Overview",
            | Self::Events => "Events",
            | Self::Detail => "Detail",
            | Self::Logs => "Logs",
            | Self::EditDiff => "Edit Diff",
            | Self::PortForwards => "Port Forwards",
        }
    }
}

// ---------------------------------------------------------------------------
// Command definition — single source of truth for each action
// ---------------------------------------------------------------------------

pub struct Cmd {
    /// Short key label for the hotkey bar, e.g. "j/k", "Ctrl+d", "F".
    pub key: &'static str,
    /// Short action label for the hotkey bar, e.g. "Scroll", "Logs".
    pub label: &'static str,
    /// Longer description for the help screen.
    pub description: &'static str,
    /// Which contexts this command appears in.
    pub contexts: &'static [Ctx],
    /// Show in the hotkey bar? Navigation keys may be hidden.
    pub hotkey: bool,
    /// Show in the command palette (> mode)?
    pub palette: bool,
    /// Predicate: is this command available given the current state?
    /// `None` means always available. The function receives:
    ///   (selected_resource_type, experimental, has-specific-state)
    /// We use a simple flags struct to avoid coupling to App.
    pub available: Option<fn(&CmdFlags) -> bool>,
}

/// Lightweight snapshot of app state for command availability checks.
/// Avoids coupling command.rs to the full App struct.
pub struct CmdFlags {
    pub resource_type: Option<ResourceType>,
    pub experimental: bool,
    pub has_filter: bool,
    pub has_pods_gt1: bool,
    pub has_containers_gt1: bool,
    pub following: bool,
    pub wrapping: bool,
    pub has_since: bool,
    pub has_dict_entries: bool,
    pub dict_cursor_active: bool,
    pub has_related: bool,
    pub paused: bool,
    pub detail_auto_refresh: bool,
    pub pf_count: usize,
    pub diff_mark_set: bool,
    pub node_cordoned: bool,
}

// ---------------------------------------------------------------------------
// The registry — all commands defined here
// ---------------------------------------------------------------------------

/// Returns commands that should appear in the hotkey bar for a context.
pub fn hotkey_bar(ctx: Ctx, flags: &CmdFlags) -> Vec<&'static Cmd> {
    COMMANDS
        .iter()
        .filter(|cmd| cmd.hotkey)
        .filter(|cmd| cmd.contexts.contains(&ctx))
        .filter(|cmd| cmd.available.map(|f| f(flags)).unwrap_or(true))
        .collect()
}

/// Returns commands eligible for the command palette.
pub fn palette_commands(flags: &CmdFlags) -> Vec<&'static Cmd> {
    COMMANDS
        .iter()
        .filter(|cmd| cmd.palette)
        .filter(|cmd| cmd.available.map(|f| f(flags)).unwrap_or(true))
        .collect()
}

/// Returns all commands for the help screen (all contexts, all commands).
pub fn help_entries(flags: &CmdFlags) -> Vec<&'static Cmd> {
    COMMANDS
        .iter()
        .filter(|cmd| cmd.available.map(|f| f(flags)).unwrap_or(true))
        .collect()
}

// ---------------------------------------------------------------------------
// Availability predicates
// ---------------------------------------------------------------------------

fn supports_logs(f: &CmdFlags) -> bool {
    f.resource_type.map(|rt| rt.supports_logs()).unwrap_or(false)
}

fn supports_scale(f: &CmdFlags) -> bool {
    f.resource_type.map(|rt| rt.supports_scale()).unwrap_or(false)
}

fn supports_exec(f: &CmdFlags) -> bool {
    f.experimental && f.resource_type.map(|rt| rt.supports_exec()).unwrap_or(false)
}

fn supports_pf(f: &CmdFlags) -> bool {
    f.resource_type
        .map(|rt| {
            matches!(
                rt,
                ResourceType::Service
                    | ResourceType::Deployment
                    | ResourceType::StatefulSet
                    | ResourceType::DaemonSet
                    | ResourceType::ReplicaSet
                    | ResourceType::Pod
                    | ResourceType::Job
                    | ResourceType::CronJob
            )
        })
        .unwrap_or(false)
}

fn supports_restart(f: &CmdFlags) -> bool {
    f.resource_type
        .map(|rt| {
            matches!(
                rt,
                ResourceType::Deployment | ResourceType::StatefulSet | ResourceType::DaemonSet
            )
        })
        .unwrap_or(false)
}

fn is_cronjob(f: &CmdFlags) -> bool {
    f.resource_type == Some(ResourceType::CronJob)
}

fn has_filter(f: &CmdFlags) -> bool {
    f.has_filter
}

fn is_node(f: &CmdFlags) -> bool {
    f.resource_type == Some(ResourceType::Node)
}

fn has_dict(f: &CmdFlags) -> bool {
    f.has_dict_entries
}

fn has_dict_cursor(f: &CmdFlags) -> bool {
    f.dict_cursor_active
}

fn has_related(f: &CmdFlags) -> bool {
    f.has_related
}

fn has_multi_pods(f: &CmdFlags) -> bool {
    f.has_pods_gt1
}

fn has_multi_containers(f: &CmdFlags) -> bool {
    f.has_containers_gt1
}

fn has_pf_entries(f: &CmdFlags) -> bool {
    f.pf_count > 0
}

// ---------------------------------------------------------------------------
// Static command table
// ---------------------------------------------------------------------------

static COMMANDS: &[Cmd] = &[
    // ── Global ──────────────────────────────────────────────────────────
    Cmd {
        key: "Ctrl+C",
        label: "Quit",
        description: "Force quit immediately",
        contexts: &[Ctx::Global],
        hotkey: false,
        palette: false,
        available: None,
    },
    Cmd {
        key: "q",
        label: "Quit",
        description: "Quit current view or go back",
        contexts: &[Ctx::Global],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "Esc",
        label: "Back",
        description: "Go back, dismiss popup or selection",
        contexts: &[Ctx::Global],
        hotkey: false,
        palette: false,
        available: None,
    },
    Cmd {
        key: "?",
        label: "Help",
        description: "Show keybindings help",
        contexts: &[Ctx::Global],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "Ctrl+P",
        label: "Palette",
        description: "Open command palette (prefix > for commands)",
        contexts: &[Ctx::Global],
        hotkey: true,
        palette: false,
        available: None,
    },
    // ── Navigation (shared) ─────────────────────────────────────────────
    Cmd {
        key: "j / Down",
        label: "Down",
        description: "Move down / scroll down",
        contexts: &[
            Ctx::Nav,
            Ctx::Resources,
            Ctx::Events,
            Ctx::Detail,
            Ctx::Logs,
            Ctx::EditDiff,
            Ctx::ClusterStats,
            Ctx::PortForwards,
        ],
        hotkey: false,
        palette: false,
        available: None,
    },
    Cmd {
        key: "k / Up",
        label: "Up",
        description: "Move up / scroll up",
        contexts: &[
            Ctx::Nav,
            Ctx::Resources,
            Ctx::Events,
            Ctx::Detail,
            Ctx::Logs,
            Ctx::EditDiff,
            Ctx::ClusterStats,
            Ctx::PortForwards,
        ],
        hotkey: false,
        palette: false,
        available: None,
    },
    Cmd {
        key: "Ctrl+d / PgDn",
        label: "Page Down",
        description: "Page down (jump 10–20 items)",
        contexts: &[
            Ctx::Nav,
            Ctx::Resources,
            Ctx::Events,
            Ctx::Detail,
            Ctx::Logs,
            Ctx::EditDiff,
            Ctx::ClusterStats,
            Ctx::PortForwards,
        ],
        hotkey: false,
        palette: false,
        available: None,
    },
    Cmd {
        key: "Ctrl+u / PgUp",
        label: "Page Up",
        description: "Page up (jump 10–20 items)",
        contexts: &[
            Ctx::Nav,
            Ctx::Resources,
            Ctx::Events,
            Ctx::Detail,
            Ctx::Logs,
            Ctx::EditDiff,
            Ctx::ClusterStats,
            Ctx::PortForwards,
        ],
        hotkey: false,
        palette: false,
        available: None,
    },
    Cmd {
        key: "g / Home",
        label: "Top",
        description: "Jump to top of list",
        contexts: &[Ctx::Resources, Ctx::Events, Ctx::Detail, Ctx::Logs],
        hotkey: false,
        palette: false,
        available: None,
    },
    Cmd {
        key: "G / End",
        label: "Bottom",
        description: "Jump to bottom of list",
        contexts: &[Ctx::Resources, Ctx::Events, Ctx::Logs],
        hotkey: false,
        palette: false,
        available: None,
    },
    // ── Main view: sidebar ──────────────────────────────────────────────
    Cmd {
        key: "r",
        label: "Resources",
        description: "Focus the resource table",
        contexts: &[Ctx::Nav],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "Tab",
        label: "Focus",
        description: "Toggle focus between sidebar and table",
        contexts: &[Ctx::Nav, Ctx::Resources, Ctx::Events, Ctx::ClusterStats],
        hotkey: false,
        palette: false,
        available: None,
    },
    Cmd {
        key: "Enter",
        label: "Open",
        description: "Open detail view or move focus to table",
        contexts: &[Ctx::Nav, Ctx::Resources, Ctx::Events],
        hotkey: false,
        palette: false,
        available: None,
    },
    // ── Main view: actions ──────────────────────────────────────────────
    Cmd {
        key: "c",
        label: "Cluster",
        description: "Switch cluster context",
        contexts: &[Ctx::Nav, Ctx::Resources, Ctx::ClusterStats],
        hotkey: true,
        palette: true,
        available: None,
    },
    Cmd {
        key: "n",
        label: "Namespace",
        description: "Switch namespace",
        contexts: &[Ctx::Nav, Ctx::Resources, Ctx::ClusterStats],
        hotkey: true,
        palette: true,
        available: None,
    },
    Cmd {
        key: "O",
        label: "Kubeconfig",
        description: "Open a kubeconfig file",
        contexts: &[Ctx::Nav, Ctx::Resources, Ctx::ClusterStats],
        hotkey: true,
        palette: true,
        available: None,
    },
    Cmd {
        key: "p",
        label: "Pause",
        description: "Pause/resume auto-refresh",
        contexts: &[Ctx::Nav, Ctx::Resources, Ctx::ClusterStats, Ctx::Detail],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "/",
        label: "Filter",
        description: "Filter resources by regex",
        contexts: &[Ctx::Resources, Ctx::Events, Ctx::Logs],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "x",
        label: "Clear",
        description: "Clear active filter",
        contexts: &[Ctx::Resources, Ctx::Logs],
        hotkey: true,
        palette: false,
        available: Some(has_filter),
    },
    Cmd {
        key: "e",
        label: "Edit",
        description: "Edit resource in $EDITOR",
        contexts: &[Ctx::Resources, Ctx::Detail],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "l",
        label: "Logs",
        description: "Open logs for workload pods",
        contexts: &[Ctx::Resources, Ctx::Detail],
        hotkey: true,
        palette: false,
        available: Some(supports_logs),
    },
    Cmd {
        key: "F",
        label: "PortFwd",
        description: "Create a port forward",
        contexts: &[Ctx::Resources, Ctx::Detail],
        hotkey: true,
        palette: true,
        available: Some(supports_pf),
    },
    Cmd {
        key: "D",
        label: "Delete",
        description: "Delete the selected resource",
        contexts: &[Ctx::Resources, Ctx::Detail],
        hotkey: true,
        palette: true,
        available: None,
    },
    Cmd {
        key: "R",
        label: "Restart",
        description: "Rollout restart workload",
        contexts: &[Ctx::Resources, Ctx::Detail],
        hotkey: true,
        palette: true,
        available: Some(supports_restart),
    },
    Cmd {
        key: "S",
        label: "Scale",
        description: "Scale replicas up or down",
        contexts: &[Ctx::Resources, Ctx::Detail],
        hotkey: true,
        palette: true,
        available: Some(supports_scale),
    },
    Cmd {
        key: "y",
        label: "Copy",
        description: "Copy resource name to clipboard",
        contexts: &[Ctx::Resources],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "d",
        label: "Diff",
        description: "Mark resource for diff comparison",
        contexts: &[Ctx::Resources],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "C",
        label: "Create",
        description: "Create a new resource from YAML",
        contexts: &[Ctx::Resources],
        hotkey: true,
        palette: true,
        available: None,
    },
    Cmd {
        key: "X",
        label: "Exec",
        description: "Exec shell into pod",
        contexts: &[Ctx::Resources, Ctx::Detail],
        hotkey: true,
        palette: true,
        available: Some(supports_exec),
    },
    Cmd {
        key: "K",
        label: "Cordon",
        description: "Cordon/uncordon node (toggle scheduling)",
        contexts: &[Ctx::Resources, Ctx::Detail],
        hotkey: true,
        palette: true,
        available: Some(is_node),
    },
    Cmd {
        key: "T",
        label: "Drain",
        description: "Drain node (cordon + evict pods)",
        contexts: &[Ctx::Resources, Ctx::Detail],
        hotkey: true,
        palette: true,
        available: Some(is_node),
    },
    Cmd {
        key: "T",
        label: "Trigger",
        description: "Create a Job from this CronJob",
        contexts: &[Ctx::Resources, Ctx::Detail],
        hotkey: true,
        palette: true,
        available: Some(is_cronjob),
    },
    // ── Events view ─────────────────────────────────────────────────────
    Cmd {
        key: "G",
        label: "Bottom",
        description: "Jump to latest event",
        contexts: &[Ctx::Events],
        hotkey: true,
        palette: false,
        available: None,
    },
    // ── Detail view ─────────────────────────────────────────────────────
    Cmd {
        key: "v",
        label: "View",
        description: "Cycle between Smart and YAML view",
        contexts: &[Ctx::Detail],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "w",
        label: "Watch",
        description: "Toggle auto-refresh for this resource",
        contexts: &[Ctx::Detail],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "s",
        label: "Select",
        description: "Enter/leave label/annotation selection",
        contexts: &[Ctx::Detail],
        hotkey: true,
        palette: false,
        available: Some(has_dict),
    },
    Cmd {
        key: "Space",
        label: "Expand",
        description: "Expand/collapse value or decode secret",
        contexts: &[Ctx::Detail],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "y",
        label: "Copy",
        description: "Copy selected value or full YAML",
        contexts: &[Ctx::Detail],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "Enter",
        label: "Edit",
        description: "Edit selected label/annotation",
        contexts: &[Ctx::Detail],
        hotkey: false,
        palette: false,
        available: Some(has_dict_cursor),
    },
    Cmd {
        key: "Tab",
        label: "Related",
        description: "Toggle related resources selection",
        contexts: &[Ctx::Detail],
        hotkey: true,
        palette: false,
        available: Some(has_related),
    },
    // ── Log view ────────────────────────────────────────────────────────
    Cmd {
        key: "f",
        label: "Follow",
        description: "Toggle live log streaming",
        contexts: &[Ctx::Logs],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "w",
        label: "Wrap",
        description: "Toggle line wrapping",
        contexts: &[Ctx::Logs],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "t",
        label: "Time",
        description: "Set time range filter for logs",
        contexts: &[Ctx::Logs],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "p",
        label: "Pod",
        description: "Select which pod to show logs from",
        contexts: &[Ctx::Logs],
        hotkey: true,
        palette: false,
        available: Some(has_multi_pods),
    },
    Cmd {
        key: "c",
        label: "Container",
        description: "Select which container to show logs from",
        contexts: &[Ctx::Logs],
        hotkey: true,
        palette: false,
        available: Some(has_multi_containers),
    },
    Cmd {
        key: "Y",
        label: "Copy All",
        description: "Copy all log lines to clipboard",
        contexts: &[Ctx::Logs],
        hotkey: true,
        palette: false,
        available: None,
    },
    // ── Edit diff ───────────────────────────────────────────────────────
    Cmd {
        key: "Enter",
        label: "Apply",
        description: "Apply changes to cluster",
        contexts: &[Ctx::EditDiff],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "v",
        label: "View",
        description: "Cycle inline / side-by-side diff",
        contexts: &[Ctx::EditDiff],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "e",
        label: "Re-edit",
        description: "Reopen in $EDITOR with current changes",
        contexts: &[Ctx::EditDiff],
        hotkey: true,
        palette: false,
        available: None,
    },
    Cmd {
        key: "Esc",
        label: "Cancel",
        description: "Discard changes and go back",
        contexts: &[Ctx::EditDiff],
        hotkey: true,
        palette: false,
        available: None,
    },
    // ── Port forwards ───────────────────────────────────────────────────
    Cmd {
        key: "p",
        label: "Pause",
        description: "Pause/resume the selected port forward",
        contexts: &[Ctx::PortForwards],
        hotkey: true,
        palette: false,
        available: Some(has_pf_entries),
    },
    Cmd {
        key: "d",
        label: "Cancel",
        description: "Cancel (delete) the selected port forward",
        contexts: &[Ctx::PortForwards],
        hotkey: true,
        palette: false,
        available: Some(has_pf_entries),
    },
];
