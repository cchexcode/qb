use {
    super::{
        logs::LogViewState,
        smart::SecretDetailState,
    },
    crate::{
        config::{
            QbConfig,
            SavedPortForward,
        },
        k8s::{
            ClusterStatsData,
            KubeClient,
            RelatedEvent,
            ResourceEntry,
            ResourceType,
        },
        portforward::{
            self,
            PfTarget,
            PortForwardManager,
            PortInfo,
        },
    },
    crossterm::event::{
        KeyCode,
        KeyEvent,
        KeyModifiers,
    },
    ratatui::widgets::{
        ListState,
        TableState,
    },
    regex::Regex,
    serde_json::Value,
};

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
pub enum Focus {
    Nav,
    Resources,
}

#[derive(Clone, Copy, PartialEq)]
pub enum View {
    Main,
    Detail,
    Logs,
    EditDiff,
}

/// What the right panel displays in the Main view.
#[derive(Clone, PartialEq)]
pub enum Panel {
    Overview,
    Favorites,
    PortForwards,
    Profiles,
    ResourceList(ResourceType),
}

// ---------------------------------------------------------------------------
// Edit state
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
pub enum DiffKind {
    Context,
    Added,
    Removed,
}

#[derive(Clone, Copy, PartialEq)]
pub enum DiffMode {
    Inline,
    SideBySide,
}

pub struct EditContext {
    pub resource_type: ResourceType,
    pub name: String,
    pub namespace: String,
    #[allow(dead_code)]
    pub original_yaml: String,
    pub edited_yaml: String,
    pub diff_lines: Vec<(DiffKind, String)>,
    pub diff_mode: DiffMode,
    pub scroll: u16,
    pub error: Option<String>,
}

/// Set by key handler, consumed by the event loop to suspend TUI and run
/// $EDITOR.
pub struct PendingEdit {
    pub resource_type: ResourceType,
    pub name: String,
    pub namespace: String,
    pub yaml: String,
    /// Set during re-edit so we can restore the diff view if user makes no
    /// changes.
    pub original_yaml: Option<String>,
}

/// Set by key handler, consumed by the event loop to edit metadata in $EDITOR.
pub struct PendingMetadataEdit {
    pub kind: MetadataEditKind,
    pub resource_type: ResourceType,
    pub name: String,
    pub namespace: String,
    pub yaml: String,
}

pub struct PendingExec {
    pub pod_name: String,
    pub namespace: String,
    pub container: String,
    pub command: Vec<String>,
}

pub struct PendingCreate {
    pub yaml: String,
}

pub struct PaletteEntry {
    pub label: String,
    pub description: String,
    pub kind: PaletteEntryKind,
}

pub enum PaletteEntryKind {
    Resource {
        name: String,
        namespace: String,
        resource_type: Option<ResourceType>,
    },
    PaletteCommand {
        /// The key label from the command registry (e.g. "R", "S", "D").
        key: &'static str,
    },
}

#[derive(Clone, Copy, PartialEq)]
pub enum DetailMode {
    Smart,
    Yaml,
}

#[derive(Clone, Copy, PartialEq)]
pub enum MetadataEditKind {
    Labels,
    Annotations,
}

#[allow(clippy::enum_variant_names)]
pub enum Popup {
    ContextSelect {
        items: Vec<String>,
        state: ListState,
    },
    NamespaceSelect {
        items: Vec<String>,
        state: ListState,
    },
    PodSelect {
        items: Vec<String>,
        state: ListState,
    },
    ContainerSelect {
        items: Vec<String>,
        state: ListState,
    },
    PortForwardCreate(PfCreateDialog),
    ConfirmDelete {
        name: String,
        namespace: String,
        resource_type: ResourceType,
    },
    ConfirmDrain {
        node_name: String,
    },
    ConfirmQuit {
        pf_count: usize,
    },
    TriggerCronJob {
        cronjob_name: String,
        namespace: String,
        buf: String,
    },
    ScaleInput {
        name: String,
        namespace: String,
        resource_type: ResourceType,
        current: u32,
        buf: String,
    },
    ExecShell {
        pod_name: String,
        namespace: String,
        containers: Vec<String>,
        container_cursor: usize,
        command_buf: String,
        terminal_buf: String,
        editing_terminal: bool,
    },
    KubeconfigInput {
        buf: String,
    },
    TimeFilter {
        buf: String,
    },
    ProfileSave {
        buf: String,
    },
    ProfileLoad {
        items: Vec<String>,
        state: ListState,
    },
    PortForwardEditPort {
        pf_id: usize,
        old_port: u16,
        buf: String,
    },
    ProfileClone {
        source_name: String,
        buf: String,
    },
    ConfirmDeleteProfile {
        profile_name: String,
    },
}

pub struct PfCreateDialog {
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub namespace: String,
    pub target: PfTarget,
    pub ports: Vec<PortInfo>,
    pub port_cursor: usize,
    pub local_port_buf: String,
}

pub enum PendingLoad {
    Resources,
    Namespaces,
    SwitchContext(String),
    ResourceDetail { name: String, namespace: String },
    Logs { name: String, namespace: String },
    ReloadLogs,
    ClusterStats,
}

// ---------------------------------------------------------------------------
// Navigation items
// ---------------------------------------------------------------------------

pub struct NavItem {
    pub label: String,
    pub kind: NavItemKind,
}

pub enum NavItemKind {
    Category,
    Resource(ResourceType),
    ClusterStats,
    PortForwards,
    Favorites,
    Profiles,
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Sentinel label used as the first entry in the namespace popup.
pub const ALL_NAMESPACES_LABEL: &str = "All Namespaces";

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

pub struct App {
    pub kube: KubeClient,

    // Navigation sidebar
    pub nav_items: Vec<NavItem>,
    pub nav_state: ListState,

    // Resource table
    pub resources: Vec<ResourceEntry>,
    pub resource_state: TableState,
    pub resource_table_state: TableState,
    pub panel: Panel,
    pub return_panel: Option<Panel>,
    pub resource_counts: std::collections::HashMap<ResourceType, usize>,

    // Cluster stats (shown when "Overview" is selected)
    pub cluster_stats: Option<ClusterStatsData>,
    pub cluster_stats_scroll: u16,

    // Detail view
    pub detail_value: Value,
    pub detail_yaml: String,
    pub detail_scroll: u16,
    pub detail_mode: DetailMode,
    pub secret_state: Option<SecretDetailState>,
    pub detail_name: String,
    pub detail_namespace: String,
    pub related_events: Vec<RelatedEvent>,
    pub related_resources: Vec<crate::k8s::RelatedResource>,
    pub related_cursor: Option<usize>,
    pub related_tab: usize,
    /// Line offset where related resource items start (after tab bar). Set by
    /// render.
    pub related_line_start: usize,
    /// Inner height of the detail view content area. Set by render.
    pub detail_area_height: usize,

    // Edit / Exec / Create
    pub pending_edit: Option<PendingEdit>,
    pub pending_exec: Option<PendingExec>,
    pub pending_create: Option<PendingCreate>,
    pub pending_metadata_edit: Option<PendingMetadataEdit>,
    pub exec_terminal_override: Option<String>,
    pub edit_ctx: Option<EditContext>,
    /// Tracks which label/annotation keys are expanded in smart view.
    pub expanded_keys: std::collections::HashSet<String>,
    /// Ordered list of all label/annotation dict entries: ("section:key",
    /// "key", "value"). Populated each render by smart.rs. Enables j/k
    /// navigation and y copy.
    pub dict_entries: Vec<(String, String, String)>,
    /// Currently selected dict entry index (into dict_entries).
    pub dict_cursor: Option<usize>,
    /// Line offsets for each dict entry (for scroll-to-cursor). Populated
    /// each render.
    pub dict_line_offsets: Vec<usize>,

    // UI state
    pub focus: Focus,
    pub view: View,
    pub popup: Option<Popup>,
    pub should_quit: bool,
    pub experimental: bool,
    pub status: String,
    pub status_history: Vec<(std::time::Instant, String)>,
    pub error: Option<String>,
    pub pending_load: Option<PendingLoad>,

    // Log view
    pub log_state: Option<LogViewState>,
    pub log_detail_line: Option<String>,

    // Resource filter (regex on name, namespace, columns)
    pub resource_filter_text: String,
    pub resource_filter_regex: Option<Regex>,
    pub resource_filter_editing: bool,
    pub resource_filter_buf: String,

    // Events log-style view state
    pub events_scroll: usize,
    pub events_cursor: usize,
    pub events_auto_scroll: bool,

    // Auto-refresh
    pub paused: bool,
    pub last_refresh: std::time::Instant,

    // Watch mode for detail view
    pub detail_auto_refresh: bool,

    // Diff between resources
    pub diff_mark: Option<(String, String, String)>, // (name, namespace, yaml)

    // Command palette
    pub palette_open: bool,
    pub palette_global: bool,
    pub palette_buf: String,
    pub palette_results: Vec<PaletteEntry>,
    pub palette_cursor: usize,
    pub palette_all_resources: Vec<(ResourceType, Vec<crate::k8s::ResourceEntry>)>,

    // Help palette
    pub help_open: bool,
    pub help_buf: String,
    pub help_cursor: usize,
    pub help_scroll: usize,
    pub help_context_only: bool,

    // Port forwards
    pub pf_manager: PortForwardManager,
    pub pf_cursor: usize,
    pub pf_table_state: TableState,

    // Config & profiles
    pub config: QbConfig,
    pub favorites_cursor: usize,
    pub favorites_table_state: TableState,
    pub profiles_cursor: usize,
    pub profiles_table_state: TableState,
}

impl App {
    pub fn new(kube: KubeClient, experimental: bool, config: QbConfig) -> Self {
        let nav_items = Self::build_nav_items();
        let mut nav_state = ListState::default();
        // Select first selectable item (skip Category headers)
        let first_selectable = nav_items
            .iter()
            .position(|item| Self::is_selectable_nav(&item.kind))
            .unwrap_or(0);
        nav_state.select(Some(first_selectable));

        let mut app = Self {
            kube,
            nav_items,
            nav_state,
            resources: Vec::new(),
            resource_state: TableState::default(),
            resource_table_state: TableState::default(),
            panel: Panel::Overview,
            return_panel: None,
            resource_counts: std::collections::HashMap::new(),
            cluster_stats: None,
            cluster_stats_scroll: 0,
            detail_value: Value::Null,
            detail_yaml: String::new(),
            detail_scroll: 0,
            detail_mode: DetailMode::Smart,
            secret_state: None,
            detail_name: String::new(),
            detail_namespace: String::new(),
            related_events: Vec::new(),
            related_resources: Vec::new(),
            related_cursor: None,
            related_tab: 0,
            related_line_start: 0,
            detail_area_height: 0,
            pending_edit: None,
            pending_exec: None,
            pending_create: None,
            pending_metadata_edit: None,
            exec_terminal_override: None,
            edit_ctx: None,
            expanded_keys: std::collections::HashSet::new(),
            dict_entries: Vec::new(),
            dict_cursor: None,
            dict_line_offsets: Vec::new(),
            log_state: None,
            log_detail_line: None,
            resource_filter_text: String::new(),
            resource_filter_regex: None,
            resource_filter_editing: false,
            resource_filter_buf: String::new(),
            events_scroll: 0,
            events_cursor: 0,
            events_auto_scroll: true,
            paused: false,
            focus: Focus::Nav,
            view: View::Main,
            popup: None,
            should_quit: false,
            experimental,
            status: String::new(),
            status_history: Vec::new(),
            error: None,
            pending_load: Some(PendingLoad::ClusterStats),
            last_refresh: std::time::Instant::now(),
            detail_auto_refresh: true,
            diff_mark: None,
            palette_open: false,
            palette_global: false,
            palette_buf: String::new(),
            palette_results: Vec::new(),
            palette_cursor: 0,
            palette_all_resources: Vec::new(),
            help_open: false,
            help_buf: String::new(),
            help_cursor: 0,
            help_scroll: 0,
            help_context_only: true,
            pf_manager: PortForwardManager::new(),
            pf_cursor: 0,
            pf_table_state: TableState::default(),
            config,
            favorites_cursor: 0,
            favorites_table_state: TableState::default(),
            profiles_cursor: 0,
            profiles_table_state: TableState::default(),
        };
        app.config.active_profile_mut().kubeconfig = app.kube.kubeconfig_path().map(|s| s.to_string());
        let _ = app.config.save();
        app.restore_saved_port_forwards();
        app.update_status();
        app
    }

    /// Restore all saved port forwards from the active profile on startup.
    fn restore_saved_port_forwards(&mut self) {
        let saved: Vec<_> = self.config.active_profile().port_forwards.clone();
        for spf in &saved {
            let target = match spf.target_type.as_str() {
                | "direct_pod" => {
                    PfTarget::DirectPod {
                        pod_name: spf.selector.clone(),
                    }
                },
                | _ => {
                    PfTarget::LabelSelector {
                        selector: spf.selector.clone(),
                    }
                },
            };
            let pod_name = match &target {
                | PfTarget::DirectPod { pod_name } => pod_name.clone(),
                | PfTarget::LabelSelector { .. } => "(resolving)".to_string(),
            };
            let rt = crate::k8s::ResourceType::from_singular_name(&spf.resource_type);
            let resource_label = format!(
                "{}/{}",
                rt.map(|r| r.display_name()).unwrap_or(&spf.resource_type),
                spf.resource_name
            );

            if spf.paused {
                // Create entry in Paused state without spawning a task
                self.pf_manager.create_paused(
                    spf.namespace.clone(),
                    pod_name,
                    spf.context.clone(),
                    resource_label,
                    spf.local_port,
                    spf.remote_port,
                    target,
                );
            } else {
                // Create and start immediately
                let client = self.kube.client().clone();
                self.pf_manager.create(
                    client,
                    spf.namespace.clone(),
                    pod_name,
                    spf.context.clone(),
                    resource_label,
                    spf.local_port,
                    spf.remote_port,
                    target,
                );
            }
        }
    }

    /// Which command context the app is currently in.
    pub fn current_context(&self) -> super::command::Ctx {
        use super::command::Ctx;
        match self.view {
            | View::Detail => Ctx::Detail,
            | View::Logs => Ctx::Logs,
            | View::EditDiff => Ctx::EditDiff,
            | View::Main if self.focus == Focus::Nav => Ctx::Nav,
            | View::Main => {
                match &self.panel {
                    | Panel::Overview => Ctx::ClusterStats,
                    | Panel::Favorites => Ctx::Resources,
                    | Panel::PortForwards => Ctx::PortForwards,
                    | Panel::Profiles => Ctx::Profiles,
                    | Panel::ResourceList(rt) if *rt == ResourceType::Event => Ctx::Events,
                    | Panel::ResourceList(_) => Ctx::Resources,
                }
            },
        }
    }

    /// Snapshot of app state for command availability checks.
    pub fn cmd_flags(&self) -> super::command::CmdFlags {
        let (has_pods_gt1, has_containers_gt1, following, wrapping, has_since) = if let Some(s) = &self.log_state {
            (
                s.pods.len() > 1,
                s.active_containers().len() > 1,
                s.following,
                s.wrap,
                s.since_seconds.is_some(),
            )
        } else {
            (false, false, false, false, false)
        };
        let effective_resource_type = self.effective_resource_type();

        super::command::CmdFlags {
            resource_type: effective_resource_type,
            experimental: self.experimental,
            has_filter: !self.resource_filter_text.is_empty(),
            has_pods_gt1,
            has_containers_gt1,
            following,
            wrapping,
            has_since,
            has_labels: self.dict_entries.iter().any(|(q, ..)| q.starts_with("Labels:")),
            has_annotations: self.dict_entries.iter().any(|(q, ..)| q.starts_with("Annotations:")),
            dict_cursor_active: self.dict_cursor.is_some(),
            has_related: !self.related_resources.is_empty(),
            paused: self.paused,
            detail_auto_refresh: self.detail_auto_refresh,
            pf_count: self.pf_manager.entries().len(),
            diff_mark_set: self.diff_mark.is_some(),
            node_cordoned: if effective_resource_type == Some(ResourceType::Node) {
                if self.view == View::Detail {
                    self.detail_value
                        .get("spec")
                        .and_then(|s| s.get("unschedulable"))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                } else {
                    self.resource_state
                        .selected()
                        .and_then(|idx| self.resources.get(idx))
                        .and_then(|e| e.columns.first())
                        .map(|s| s.contains("SchedulingDisabled"))
                        .unwrap_or(false)
                }
            } else {
                false
            },
        }
    }

    fn build_nav_items() -> Vec<NavItem> {
        let mut items = Vec::new();

        // Global items at the top
        items.push(NavItem {
            label: " Overview".to_string(),
            kind: NavItemKind::ClusterStats,
        });
        items.push(NavItem {
            label: " Favorites".to_string(),
            kind: NavItemKind::Favorites,
        });
        items.push(NavItem {
            label: " Port Forwards".to_string(),
            kind: NavItemKind::PortForwards,
        });
        items.push(NavItem {
            label: " Profiles".to_string(),
            kind: NavItemKind::Profiles,
        });

        // Resource categories
        for (cat, types) in ResourceType::all_by_category() {
            items.push(NavItem {
                label: cat.display_name().to_uppercase(),
                kind: NavItemKind::Category,
            });
            for rt in types {
                items.push(NavItem {
                    label: format!(" {}", rt.display_name()),
                    kind: NavItemKind::Resource(rt),
                });
            }
        }

        items
    }

    fn update_status(&mut self) {
        let ctx = self.kube.current_context();
        let ns = self.kube.namespace_display();
        let rt_name = self
            .selected_resource_type()
            .map(|r| r.display_name())
            .unwrap_or("None");
        let count = self.resources.len();
        self.status = format!("ctx: {} | ns: {} | {}: {}", ctx, ns, rt_name, count);
    }

    /// Clear all cached resource data. Called on context switch, profile
    /// switch, and namespace change.
    fn clear_cached_state(&mut self) {
        self.resource_counts.clear();
        self.resources.clear();
        self.cluster_stats = None;
        self.detail_value = serde_json::Value::Null;
        self.detail_yaml.clear();
        self.detail_name.clear();
        self.detail_namespace.clear();
        self.related_events.clear();
        self.related_resources.clear();
        self.related_cursor = None;
    }

    pub fn push_status(&mut self, msg: impl Into<String>) {
        let s = msg.into();
        self.status = s.clone();
        self.status_history.push((std::time::Instant::now(), s));
        // Keep only last 10 entries
        if self.status_history.len() > 10 {
            self.status_history.remove(0);
        }
    }

    pub fn selected_resource_type(&self) -> Option<ResourceType> {
        match &self.panel {
            | Panel::ResourceList(rt) => Some(*rt),
            | _ => None,
        }
    }

    pub fn effective_resource_type(&self) -> Option<ResourceType> {
        match &self.panel {
            | Panel::ResourceList(rt) => Some(*rt),
            | Panel::Favorites => {
                self.config
                    .active_profile()
                    .favorites
                    .get(self.favorites_cursor)
                    .and_then(|fav| ResourceType::from_singular_name(&fav.resource_type))
            },
            | _ => None,
        }
    }

    fn return_to_main(&mut self) {
        self.view = View::Main;
        self.focus = Focus::Resources;
        if let Some(panel) = self.return_panel.take() {
            self.panel = panel;
        }
    }

    fn is_secret_smart_view(&self) -> bool {
        self.detail_mode == DetailMode::Smart
            && self.selected_resource_type() == Some(ResourceType::Secret)
            && self.secret_state.is_some()
    }

    // -----------------------------------------------------------------------
    // Resource filter
    // -----------------------------------------------------------------------

    /// Returns indices into `self.resources` that match the current filter.
    pub fn visible_resource_indices(&self) -> Vec<usize> {
        if self.resource_filter_text.is_empty() {
            (0..self.resources.len()).collect()
        } else {
            self.resources
                .iter()
                .enumerate()
                .filter(|(_, e)| self.resource_matches(e))
                .map(|(i, _)| i)
                .collect()
        }
    }

    fn resource_matches(&self, entry: &ResourceEntry) -> bool {
        if let Some(re) = &self.resource_filter_regex {
            re.is_match(&entry.name) || re.is_match(&entry.namespace) || entry.columns.iter().any(|c| re.is_match(c))
        } else {
            let needle = &self.resource_filter_text;
            entry.name.contains(needle)
                || entry.namespace.contains(needle)
                || entry.columns.iter().any(|c| c.contains(needle))
        }
    }

    fn begin_resource_filter(&mut self) {
        self.resource_filter_editing = true;
        self.resource_filter_buf = self.resource_filter_text.clone();
    }

    fn apply_resource_filter(&mut self) {
        self.resource_filter_text = self.resource_filter_buf.clone();
        self.resource_filter_regex = if self.resource_filter_text.is_empty() {
            None
        } else {
            Regex::new(&self.resource_filter_text).ok()
        };
        self.resource_filter_editing = false;
        // Reset selection to first visible entry
        let visible = self.visible_resource_indices();
        if let Some(&first) = visible.first() {
            self.resource_state.select(Some(first));
            self.events_cursor = 0;
        }
    }

    fn cancel_resource_filter(&mut self) {
        self.resource_filter_editing = false;
        self.resource_filter_buf = self.resource_filter_text.clone();
    }

    fn clear_resource_filter(&mut self) {
        self.resource_filter_text.clear();
        self.resource_filter_regex = None;
        self.resource_filter_buf.clear();
    }

    // -----------------------------------------------------------------------
    // Deferred loading
    // -----------------------------------------------------------------------

    pub async fn process_pending_load(&mut self) {
        if let Some(load) = self.pending_load.take() {
            match load {
                | PendingLoad::Resources => self.load_resources().await,
                | PendingLoad::Namespaces => self.load_namespaces().await,
                | PendingLoad::SwitchContext(ctx) => self.do_switch_context(&ctx).await,
                | PendingLoad::ResourceDetail { name, namespace } => self.load_resource_detail(&namespace, &name).await,
                | PendingLoad::Logs { name, namespace } => self.load_logs(&namespace, &name).await,
                | PendingLoad::ReloadLogs => self.reload_logs().await,
                | PendingLoad::ClusterStats => self.load_cluster_stats().await,
            }
        }
    }

    /// Poll the log stream channel for new lines (called every event loop
    /// tick). Paused state suppresses polling.
    pub fn poll_log_stream(&mut self) {
        if self.paused {
            return;
        }
        if let Some(state) = &mut self.log_state {
            state.poll_stream();
        }
    }

    async fn load_resources(&mut self) {
        if let Some(rt) = self.selected_resource_type() {
            let prev_selected = self
                .resource_state
                .selected()
                .and_then(|idx| self.resources.get(idx))
                .map(|e| (e.name.clone(), e.namespace.clone()));

            match self.kube.list_resources(rt).await {
                | Ok(mut entries) => {
                    // Sort events chronologically (oldest first, newest at bottom)
                    if rt == ResourceType::Event {
                        entries.sort_by(|a, b| {
                            let ts_a = a.sort_key.as_deref().unwrap_or("");
                            let ts_b = b.sort_key.as_deref().unwrap_or("");
                            ts_a.cmp(ts_b)
                        });
                    }
                    self.resources = entries;
                    self.resource_counts.insert(rt, self.resources.len());
                    let new_idx = prev_selected
                        .and_then(|(name, ns)| self.resources.iter().position(|e| e.name == name && e.namespace == ns))
                        .or_else(|| {
                            if self.resources.is_empty() {
                                None
                            } else {
                                let prev_idx = self.resource_state.selected().unwrap_or(0);
                                Some(prev_idx.min(self.resources.len() - 1))
                            }
                        });
                    // Preserve viewport offset — only update the selected index, not the
                    // entire TableState, so auto-refresh doesn't jump the scroll position.
                    self.resource_state.select(new_idx);
                    // Clamp events cursor to new list size
                    if rt == ResourceType::Event && !self.resources.is_empty() {
                        self.events_cursor = self.events_cursor.min(self.resources.len() - 1);
                    }
                    self.error = None;
                },
                | Err(e) => {
                    self.resources.clear();
                    self.resource_state.select(None);
                    self.error = Some(format!("Failed to load {}: {}", rt.display_name(), e));
                },
            }
            self.last_refresh = std::time::Instant::now();
            self.update_status();
        }
    }

    pub fn maybe_auto_refresh(&mut self) {
        if self.paused {
            return;
        }
        if self.view == View::Main
            && self.popup.is_none()
            && self.pending_load.is_none()
            && self.last_refresh.elapsed() >= std::time::Duration::from_secs(2)
        {
            match &self.panel {
                | Panel::Overview => {
                    self.pending_load = Some(PendingLoad::ClusterStats);
                },
                | Panel::ResourceList(_) => {
                    self.pending_load = Some(PendingLoad::Resources);
                },
                | _ => {},
            }
        }
        // Detail view watch mode
        if self.detail_auto_refresh
            && self.view == View::Detail
            && self.popup.is_none()
            && self.pending_load.is_none()
            && self.last_refresh.elapsed() >= std::time::Duration::from_secs(2)
        {
            if !self.detail_name.is_empty() {
                self.pending_load = Some(PendingLoad::ResourceDetail {
                    name: self.detail_name.clone(),
                    namespace: self.detail_namespace.clone(),
                });
                self.last_refresh = std::time::Instant::now();
            }
        }
    }

    async fn load_namespaces(&mut self) {
        match self.kube.list_namespaces().await {
            | Ok(namespaces) => {
                let mut items = vec![ALL_NAMESPACES_LABEL.to_string()];
                items.extend(namespaces);
                let mut state = ListState::default();
                match self.kube.current_namespace() {
                    | None => state.select(Some(0)),
                    | Some(current) => {
                        if let Some(idx) = items.iter().position(|n| n == current) {
                            state.select(Some(idx));
                        } else {
                            state.select(Some(0));
                        }
                    },
                }
                self.popup = Some(Popup::NamespaceSelect { items, state });
                self.error = None;
            },
            | Err(e) => {
                self.error = Some(format!("Failed to load namespaces: {}", e));
            },
        }
    }

    async fn do_switch_context(&mut self, ctx: &str) {
        match self.kube.switch_context(ctx).await {
            | Ok(()) => {
                self.clear_cached_state();
                self.pending_load = Some(PendingLoad::Resources);
                self.error = None;
                // Persist selected context
                self.config.active_profile_mut().context = Some(ctx.to_string());
                if let Err(e) = self.config.save() {
                    self.error = Some(format!("Failed to save config: {}", e));
                }
            },
            | Err(e) => {
                self.error = Some(format!("Failed to switch context: {}", e));
            },
        }
    }

    async fn load_resource_detail(&mut self, ns: &str, name: &str) {
        if let Some(rt) = self.selected_resource_type() {
            match self.kube.get_resource(rt, ns, name).await {
                | Ok(value) => {
                    self.detail_yaml = serde_yaml::to_string(&value).unwrap_or_default();
                    let is_same_resource = self.detail_name == name && self.detail_namespace == ns;
                    if rt == ResourceType::Secret {
                        if is_same_resource && self.detail_auto_refresh && self.secret_state.is_some() {
                            // Preserve selection and decoded state on watch refresh
                            if let Some(state) = &mut self.secret_state {
                                state.update_values(&value);
                            }
                        } else {
                            self.secret_state = Some(SecretDetailState::from_value(&value));
                        }
                    } else {
                        self.secret_state = None;
                    }
                    self.detail_value = value;
                    self.detail_name = name.to_string();
                    self.detail_namespace = ns.to_string();
                    if !self.detail_auto_refresh || !is_same_resource {
                        self.detail_scroll = 0;
                        self.detail_mode = DetailMode::Smart;
                        self.expanded_keys.clear();
                        self.dict_entries.clear();
                        self.dict_cursor = None;
                        self.dict_line_offsets.clear();
                    }
                    self.view = View::Detail;
                    self.error = None;
                    self.related_events = self.kube.fetch_related_events(ns, name).await.unwrap_or_default();
                    // Always re-fetch related resources (they change as pods scale, etc.)
                    let new_related = self
                        .kube
                        .fetch_related_resources(rt, ns, name, &self.detail_value)
                        .await;
                    if is_same_resource && self.detail_auto_refresh {
                        // Preserve cursor position by matching the selected resource
                        if let Some(cursor) = self.related_cursor {
                            if let Some(old) = self.related_resources.get(cursor) {
                                let old_name = &old.name;
                                let old_ns = &old.namespace;
                                let old_rt = old.resource_type;
                                // Find the same resource in the new list
                                self.related_cursor = new_related.iter().position(|r| {
                                    r.resource_type == old_rt && r.name == *old_name && r.namespace == *old_ns
                                });
                            }
                        }
                    } else {
                        self.related_cursor = None;
                        self.related_tab = 0;
                    }
                    self.related_resources = new_related;
                },
                | Err(e) => {
                    self.error = Some(format!("Failed to load resource: {}", e));
                },
            }
        }
    }

    async fn load_logs(&mut self, ns: &str, name: &str) {
        let rt = match self.selected_resource_type() {
            | Some(rt) if rt.supports_logs() => rt,
            | _ => return,
        };

        match self.kube.find_pods(rt, ns, name).await {
            | Ok(pods) => {
                if pods.is_empty() {
                    self.error = Some("No pods found for this resource".into());
                    return;
                }
                // Build all pod/container pairs for the default "all" view
                let pairs: Vec<(String, String)> = pods
                    .iter()
                    .flat_map(|p| p.containers.iter().map(move |c| (p.name.clone(), c.clone())))
                    .collect();
                let default_since = Some(3600); // 1h default
                match self.kube.fetch_logs_multi(ns, &pairs, 500, default_since).await {
                    | Ok(lines) => {
                        let mut state = LogViewState::new(pods, ns.to_string(), lines);
                        state.since_seconds = default_since;
                        self.log_state = Some(state);
                        self.view = View::Logs;
                        self.error = None;
                    },
                    | Err(e) => {
                        self.error = Some(format!("Failed to fetch logs: {}", e));
                    },
                }
            },
            | Err(e) => {
                self.error = Some(format!("Failed to find pods: {}", e));
            },
        }
    }

    async fn reload_logs(&mut self) {
        let log_state = match &mut self.log_state {
            | Some(s) => s,
            | None => return,
        };

        log_state.stop_following();

        let pairs = log_state.active_streams();
        let ns = log_state.namespace.clone();
        let since = log_state.since_seconds;

        match self.kube.fetch_logs_multi(&ns, &pairs, 500, since).await {
            | Ok(lines) => {
                log_state.lines = lines;
                log_state.scroll = 0;
                log_state.auto_scroll = true;
                self.error = None;
            },
            | Err(e) => {
                self.error = Some(format!("Failed to reload logs: {}", e));
            },
        }
    }

    async fn load_cluster_stats(&mut self) {
        match self.kube.fetch_cluster_stats().await {
            | Ok(stats) => {
                self.cluster_stats = Some(stats);
                // Preserve scroll position on auto-refresh
                self.error = None;
            },
            | Err(e) => {
                self.error = Some(format!("Failed to load cluster stats: {}", e));
            },
        }
        self.last_refresh = std::time::Instant::now();
    }

    // -----------------------------------------------------------------------
    // Event handling
    // -----------------------------------------------------------------------

    pub async fn handle_key(&mut self, key: KeyEvent) {
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.open_confirm_quit();
            return;
        }

        if key.code == KeyCode::Char('p') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.toggle_palette(false).await;
            return;
        }

        if key.code == KeyCode::Char('s') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.open_profile_save();
            return;
        }

        if self.help_open {
            self.handle_help_key(key);
            return;
        }

        if self.palette_open {
            self.handle_palette_key(key).await;
            return;
        }

        // [p] Toggle pause — global, works in any view (except popups/filter
        // editing/port forwards/logs)
        if key.code == KeyCode::Char('p') && self.popup.is_none() && !self.resource_filter_editing {
            // Don't consume 'p' in: log view (pod selector), edit diff, port forwards view
            // (pause/resume)
            if self.view != View::Logs && self.view != View::EditDiff && !matches!(self.panel, Panel::PortForwards) {
                self.paused = !self.paused;
                self.push_status(if self.paused {
                    "Paused — auto-refresh disabled"
                } else {
                    "Resumed — auto-refresh enabled"
                });
                return;
            }
        }

        if key.code == KeyCode::Char('?') && self.popup.is_none() && !self.resource_filter_editing {
            self.help_open = true;
            self.help_buf.clear();
            self.help_cursor = 0;
            self.help_context_only = true;
            return;
        }

        if self.popup.is_some() {
            self.handle_popup_key(key).await;
            return;
        }

        if self.view == View::Detail {
            self.handle_detail_key(key).await;
            return;
        }

        if self.view == View::Logs {
            self.handle_log_key(key);
            return;
        }

        if self.view == View::EditDiff {
            self.handle_edit_diff_key(key).await;
            return;
        }

        // Resource filter editing captures all input in main view
        if self.resource_filter_editing {
            self.handle_resource_filter_key(key);
            return;
        }

        match key.code {
            | KeyCode::Char('q') => {
                self.open_confirm_quit();
            },
            | KeyCode::Char('r') => {
                self.focus = Focus::Resources;
            },
            | KeyCode::Char('c') if !matches!(self.panel, Panel::Profiles) => {
                self.open_context_selector();
            },
            | KeyCode::Char('n') => {
                self.pending_load = Some(PendingLoad::Namespaces);
            },
            | KeyCode::Char('O') => {
                let default = self
                    .kube
                    .kubeconfig_path()
                    .or_else(|| std::env::var("KUBECONFIG").ok().as_deref().map(|_| ""))
                    .unwrap_or("~/.kube/config")
                    .to_string();
                let default = if default.is_empty() {
                    std::env::var("KUBECONFIG").unwrap_or_else(|_| "~/.kube/config".to_string())
                } else {
                    default
                };
                self.popup = Some(Popup::KubeconfigInput { buf: default });
            },
            | KeyCode::Char('P') => {
                self.open_profile_load();
            },
            | KeyCode::Char('/') => {
                self.begin_resource_filter();
            },
            | KeyCode::Char('x') if !self.resource_filter_text.is_empty() => {
                self.clear_resource_filter();
            },
            | KeyCode::Tab | KeyCode::BackTab => {
                self.focus = match self.focus {
                    | Focus::Nav => Focus::Resources,
                    | Focus::Resources => Focus::Nav,
                };
            },
            | _ => {
                match self.focus {
                    | Focus::Nav => self.handle_nav_key(key),
                    | Focus::Resources => self.handle_resource_key(key).await,
                }
            },
        }
    }

    fn handle_resource_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Enter => self.apply_resource_filter(),
            | KeyCode::Esc => self.cancel_resource_filter(),
            | KeyCode::Backspace => {
                self.resource_filter_buf.pop();
            },
            | KeyCode::Char(c) => {
                self.resource_filter_buf.push(c);
            },
            | _ => {},
        }
    }

    fn handle_nav_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Up | KeyCode::Char('k') => {
                self.nav_up();
                self.load_nav_selection();
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                self.nav_down();
                self.load_nav_selection();
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                for _ in 0..10 {
                    self.nav_up();
                }
                self.load_nav_selection();
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                for _ in 0..10 {
                    self.nav_down();
                }
                self.load_nav_selection();
            },
            | KeyCode::Enter => {
                self.load_nav_selection();
                self.focus = Focus::Resources;
            },
            | _ => {},
        }
    }

    /// Load whichever resource type or cluster stats is currently highlighted
    /// in the nav.
    fn load_nav_selection(&mut self) {
        self.return_panel = None;
        let idx = match self.nav_state.selected() {
            | Some(i) => i,
            | None => return,
        };
        let kind = &self.nav_items[idx].kind;
        match kind {
            | NavItemKind::Favorites => {
                self.panel = Panel::Favorites;
                self.clear_resource_filter();
                self.view = View::Main;
            },
            | NavItemKind::PortForwards => {
                self.panel = Panel::PortForwards;
                self.clear_resource_filter();
                self.view = View::Main;
            },
            | NavItemKind::Profiles => {
                self.panel = Panel::Profiles;
                self.clear_resource_filter();
                self.view = View::Main;
            },
            | NavItemKind::ClusterStats => {
                if !matches!(self.panel, Panel::Overview) {
                    self.panel = Panel::Overview;
                    self.clear_resource_filter();
                    self.cluster_stats_scroll = 0;
                    self.pending_load = Some(PendingLoad::ClusterStats);
                }
            },
            | NavItemKind::Resource(rt) => {
                let rt = *rt;
                if self.selected_resource_type() != Some(rt) {
                    self.panel = Panel::ResourceList(rt);
                    self.resource_table_state = TableState::default();
                    self.events_scroll = 0;
                    self.events_cursor = 0;
                    self.events_auto_scroll = true;
                    self.clear_resource_filter();
                    self.pending_load = Some(PendingLoad::Resources);
                }
            },
            | NavItemKind::Category => {},
        }
    }

    fn is_selectable_nav(kind: &NavItemKind) -> bool {
        matches!(
            kind,
            NavItemKind::Resource(_)
                | NavItemKind::ClusterStats
                | NavItemKind::PortForwards
                | NavItemKind::Favorites
                | NavItemKind::Profiles
        )
    }

    fn nav_up(&mut self) {
        let current = self.nav_state.selected().unwrap_or(0);
        if current == 0 {
            return;
        }
        let mut next = current - 1;
        while next > 0 {
            if Self::is_selectable_nav(&self.nav_items[next].kind) {
                break;
            }
            next -= 1;
        }
        if Self::is_selectable_nav(&self.nav_items[next].kind) {
            self.nav_state.select(Some(next));
        }
    }

    fn nav_down(&mut self) {
        let current = self.nav_state.selected().unwrap_or(0);
        let max = self.nav_items.len() - 1;
        if current >= max {
            return;
        }
        let mut next = current + 1;
        while next < max {
            if Self::is_selectable_nav(&self.nav_items[next].kind) {
                break;
            }
            next += 1;
        }
        if Self::is_selectable_nav(&self.nav_items[next].kind) {
            self.nav_state.select(Some(next));
        }
    }

    async fn handle_resource_key(&mut self, key: KeyEvent) {
        match self.panel.clone() {
            | Panel::Favorites => {
                self.handle_favorites_key(key).await;
                return;
            },
            | Panel::Profiles => {
                self.handle_profiles_key(key).await;
                return;
            },
            | Panel::PortForwards => {
                self.handle_port_forwards_key(key);
                return;
            },
            | Panel::Overview => {
                // Cluster stats: scroll only
                match key.code {
                    | KeyCode::Up | KeyCode::Char('k') => {
                        self.cluster_stats_scroll = self.cluster_stats_scroll.saturating_sub(1);
                    },
                    | KeyCode::Down | KeyCode::Char('j') => {
                        self.cluster_stats_scroll = self.cluster_stats_scroll.saturating_add(1);
                    },
                    | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        self.cluster_stats_scroll = self.cluster_stats_scroll.saturating_sub(20);
                    },
                    | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        self.cluster_stats_scroll = self.cluster_stats_scroll.saturating_add(20);
                    },
                    | KeyCode::Home => {
                        self.cluster_stats_scroll = 0;
                    },
                    | _ => {},
                }
                return;
            },
            | Panel::ResourceList(rt) if rt == ResourceType::Event => {
                self.handle_events_key(key);
                return;
            },
            | Panel::ResourceList(_) => {
                // fall through to normal resource handling below
            },
        }
        let visible = self.visible_resource_indices();
        let vis_len = visible.len();
        // Map current TableState selection (real index) to filtered position
        let vis_pos = self
            .resource_state
            .selected()
            .and_then(|sel| visible.iter().position(|&i| i == sel))
            .unwrap_or(0);

        match key.code {
            | KeyCode::Up | KeyCode::Char('k') => {
                if vis_pos > 0 {
                    self.resource_state.select(Some(visible[vis_pos - 1]));
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if vis_pos + 1 < vis_len {
                    self.resource_state.select(Some(visible[vis_pos + 1]));
                }
            },
            | KeyCode::Enter => {
                if let Some(&real_idx) = visible.get(vis_pos) {
                    if let Some(entry) = self.resources.get(real_idx) {
                        self.resource_state.select(Some(real_idx));
                        self.pending_load = Some(PendingLoad::ResourceDetail {
                            name: entry.name.clone(),
                            namespace: entry.namespace.clone(),
                        });
                    }
                }
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                let new_pos = vis_pos.saturating_sub(20);
                if let Some(&idx) = visible.get(new_pos) {
                    self.resource_state.select(Some(idx));
                }
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                let new_pos = (vis_pos + 20).min(vis_len.saturating_sub(1));
                if let Some(&idx) = visible.get(new_pos) {
                    self.resource_state.select(Some(idx));
                }
            },
            | KeyCode::Home => {
                if let Some(&first) = visible.first() {
                    self.resource_state.select(Some(first));
                }
            },
            | KeyCode::End => {
                if let Some(&last) = visible.last() {
                    self.resource_state.select(Some(last));
                }
            },
            | KeyCode::Char('l') => {
                self.open_logs_for_selected();
            },
            | KeyCode::Char('e') => {
                self.start_edit_from_list().await;
            },
            | KeyCode::Char('y') => {
                self.copy_resource_name();
            },
            | KeyCode::Char('F') => {
                self.open_port_forward_dialog().await;
            },
            | KeyCode::Char('D') => {
                self.open_delete_confirm();
            },
            | KeyCode::Char('R') => {
                self.restart_selected_workload().await;
            },
            | KeyCode::Char('d') => {
                self.handle_diff_mark().await;
            },
            | KeyCode::Char('C') => {
                self.start_create_resource();
            },
            | KeyCode::Char('S') => {
                self.open_scale_input().await;
            },
            | KeyCode::Char('K') => {
                self.toggle_cordon_node().await;
            },
            | KeyCode::Char('T') => {
                if self.selected_resource_type() == Some(ResourceType::Node) {
                    let (name, _) = self.selected_resource_name_ns();
                    if !name.is_empty() {
                        self.popup = Some(Popup::ConfirmDrain { node_name: name });
                    }
                } else if self.selected_resource_type() == Some(ResourceType::CronJob) {
                    self.open_trigger_cronjob();
                }
            },
            | KeyCode::Char('X') if self.experimental => {
                self.open_exec_shell().await;
            },
            | KeyCode::Char('*') => {
                self.toggle_favorite_selected();
            },
            | _ => {},
        }
    }

    fn handle_events_key(&mut self, key: KeyEvent) {
        let visible = self.visible_resource_indices();
        let max = visible.len().saturating_sub(1);
        match key.code {
            | KeyCode::Up | KeyCode::Char('k') => {
                self.events_cursor = self.events_cursor.saturating_sub(1);
                self.events_auto_scroll = false;
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if self.events_cursor < max {
                    self.events_cursor += 1;
                }
                self.events_auto_scroll = self.events_cursor == max;
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.events_cursor = self.events_cursor.saturating_sub(30);
                self.events_auto_scroll = false;
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.events_cursor = (self.events_cursor + 30).min(max);
                self.events_auto_scroll = self.events_cursor == max;
            },
            | KeyCode::Home | KeyCode::Char('g') => {
                self.events_cursor = 0;
                self.events_auto_scroll = false;
            },
            | KeyCode::End | KeyCode::Char('G') => {
                self.events_cursor = max;
                self.events_auto_scroll = true;
            },
            | KeyCode::Enter => {
                // Translate cursor position in filtered view → real resource index
                if let Some(&real_idx) = visible.get(self.events_cursor) {
                    if let Some(entry) = self.resources.get(real_idx) {
                        self.resource_state.select(Some(real_idx));
                        self.pending_load = Some(PendingLoad::ResourceDetail {
                            name: entry.name.clone(),
                            namespace: entry.namespace.clone(),
                        });
                    }
                }
            },
            | _ => {},
        }
    }

    async fn handle_detail_key(&mut self, key: KeyEvent) {
        let secret_smart = self.is_secret_smart_view();

        match key.code {
            | KeyCode::Esc | KeyCode::Char('q') => {
                // Cascading dismiss: selection → related → view
                if self.dict_cursor.is_some() {
                    self.dict_cursor = None;
                } else if self.related_cursor.is_some() {
                    self.related_cursor = None;
                } else {
                    self.return_to_main();
                }
            },
            // [v] Cycle view: Smart → YAML → Smart
            | KeyCode::Char('v') => {
                self.detail_mode = match self.detail_mode {
                    | DetailMode::Smart => DetailMode::Yaml,
                    | DetailMode::Yaml => DetailMode::Smart,
                };
                self.detail_scroll = 0;
            },
            // [y] Copy to clipboard
            | KeyCode::Char('y') => {
                if secret_smart {
                    self.copy_secret_to_clipboard();
                } else if self.detail_mode == DetailMode::Yaml {
                    self.copy_yaml_to_clipboard();
                } else if self.dict_cursor.is_some() {
                    self.copy_dict_entry_to_clipboard();
                }
            },
            // [Space] Expand/decode selected item
            | KeyCode::Char(' ') => {
                if secret_smart {
                    if let Some(state) = &mut self.secret_state {
                        state.toggle_decode();
                    }
                } else if let Some(cursor) = self.dict_cursor {
                    if let Some((qualified_key, ..)) = self.dict_entries.get(cursor) {
                        let key = qualified_key.clone();
                        if self.expanded_keys.contains(&key) {
                            self.expanded_keys.remove(&key);
                        } else {
                            self.expanded_keys.insert(key);
                        }
                    }
                }
            },
            // j/k: navigate dict entries (when selected) or secret keys, scroll otherwise
            | KeyCode::Left => {
                if self.related_cursor.is_some() {
                    let cat_count = self.related_categories().len();
                    if cat_count > 0 && self.related_tab > 0 {
                        self.related_tab -= 1;
                        self.related_cursor = Some(self.related_tab_indices().first().copied().unwrap_or(0));
                        self.scroll_to_related_cursor(0);
                    }
                }
            },
            | KeyCode::Right => {
                if self.related_cursor.is_some() {
                    let cat_count = self.related_categories().len();
                    if cat_count > 0 && self.related_tab + 1 < cat_count {
                        self.related_tab += 1;
                        self.related_cursor = Some(self.related_tab_indices().first().copied().unwrap_or(0));
                        self.scroll_to_related_cursor(0);
                    }
                }
            },
            | KeyCode::Up | KeyCode::Char('k') => {
                if let Some(c) = self.related_cursor {
                    let indices = self.related_tab_indices();
                    if let Some(pos) = indices.iter().position(|&i| i == c) {
                        if pos > 0 {
                            self.related_cursor = Some(indices[pos - 1]);
                            self.scroll_to_related_cursor(pos - 1);
                        }
                    }
                } else if secret_smart {
                    if let Some(state) = &mut self.secret_state {
                        state.nav_up();
                    }
                } else if self.dict_cursor.is_some() {
                    self.dict_nav_up();
                } else {
                    self.detail_scroll = self.detail_scroll.saturating_sub(1);
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if let Some(c) = self.related_cursor {
                    let indices = self.related_tab_indices();
                    if let Some(pos) = indices.iter().position(|&i| i == c) {
                        if pos + 1 < indices.len() {
                            self.related_cursor = Some(indices[pos + 1]);
                            self.scroll_to_related_cursor(pos + 1);
                        }
                    }
                } else if secret_smart {
                    if let Some(state) = &mut self.secret_state {
                        state.nav_down();
                    }
                } else if self.dict_cursor.is_some() {
                    self.dict_nav_down();
                } else {
                    self.detail_scroll = self.detail_scroll.saturating_add(1);
                }
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.detail_scroll = self.detail_scroll.saturating_sub(20);
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.detail_scroll = self.detail_scroll.saturating_add(20);
            },
            | KeyCode::Home => {
                self.detail_scroll = 0;
            },
            // [l] Enter/leave label selection mode
            | KeyCode::Char('l') => {
                if secret_smart {
                    // no-op for secrets
                } else if self.dict_cursor.is_some()
                    && self
                        .dict_entries
                        .get(self.dict_cursor.unwrap_or(0))
                        .map_or(false, |(q, ..)| q.starts_with("Labels:"))
                {
                    self.dict_cursor = None;
                } else {
                    let first_label = self.dict_entries.iter().position(|(q, ..)| q.starts_with("Labels:"));
                    if let Some(idx) = first_label {
                        self.related_cursor = None;
                        self.dict_cursor = Some(idx);
                        self.scroll_to_dict_cursor();
                    }
                }
            },
            // [a] Enter/leave annotation selection mode
            | KeyCode::Char('a') => {
                if secret_smart {
                    // no-op for secrets
                } else if self.dict_cursor.is_some()
                    && self
                        .dict_entries
                        .get(self.dict_cursor.unwrap_or(0))
                        .map_or(false, |(q, ..)| q.starts_with("Annotations:"))
                {
                    self.dict_cursor = None;
                } else {
                    let first_annot = self
                        .dict_entries
                        .iter()
                        .position(|(q, ..)| q.starts_with("Annotations:"));
                    if let Some(idx) = first_annot {
                        self.related_cursor = None;
                        self.dict_cursor = Some(idx);
                        self.scroll_to_dict_cursor();
                    }
                }
            },
            // [e] Edit resource
            | KeyCode::Char('e') => {
                self.start_edit_from_detail();
            },
            // [L] Open logs (for workload resources)
            | KeyCode::Char('L') => {
                self.open_logs_for_selected();
            },
            // [F] Port forward
            | KeyCode::Char('F') => {
                self.open_port_forward_dialog().await;
            },
            | KeyCode::Char('D') => {
                self.open_delete_confirm_detail();
            },
            | KeyCode::Char('R') => {
                self.restart_selected_workload().await;
            },
            | KeyCode::Char('w') => {
                self.detail_auto_refresh = !self.detail_auto_refresh;
                self.push_status(if self.detail_auto_refresh {
                    "Watch mode enabled"
                } else {
                    "Watch mode disabled"
                });
            },
            | KeyCode::Char('S') => {
                self.open_scale_input_detail();
            },
            | KeyCode::Char('X') if self.experimental => {
                self.open_exec_shell().await;
            },
            | KeyCode::Char('K') => {
                self.toggle_cordon_node().await;
            },
            | KeyCode::Char('T') => {
                if self.selected_resource_type() == Some(ResourceType::Node) {
                    let name = self.detail_name.clone();
                    if !name.is_empty() {
                        self.popup = Some(Popup::ConfirmDrain { node_name: name });
                    }
                } else if self.selected_resource_type() == Some(ResourceType::CronJob) {
                    self.open_trigger_cronjob();
                }
            },
            // [r] Toggle related resource selection
            | KeyCode::Char('r') => {
                if self.related_resources.is_empty() {
                    // no-op
                } else if self.related_cursor.is_some() {
                    self.related_cursor = None;
                } else {
                    self.dict_cursor = None; // exit dict selection
                    let cats = self.related_categories();
                    if !cats.is_empty() {
                        self.related_tab = self.related_tab.min(cats.len().saturating_sub(1));
                        let indices = self.related_tab_indices();
                        self.related_cursor = indices.first().copied();
                        self.scroll_to_related_cursor(0);
                    }
                }
            },
            // [Enter] Edit selected label/annotation, or navigate to related resource
            | KeyCode::Enter => {
                if let Some(cursor) = self.dict_cursor {
                    if let Some((qualified_key, ..)) = self.dict_entries.get(cursor) {
                        let kind = if qualified_key.starts_with("Labels:") {
                            MetadataEditKind::Labels
                        } else {
                            MetadataEditKind::Annotations
                        };
                        self.open_metadata_edit(kind);
                    }
                } else if let Some(cursor) = self.related_cursor {
                    if let Some(rel) = self.related_resources.get(cursor) {
                        self.navigate_to_related(rel.resource_type, rel.name.clone(), rel.namespace.clone())
                            .await;
                    }
                }
            },
            | KeyCode::Char('*') => {
                self.toggle_favorite_detail();
            },
            | _ => {},
        }
    }

    /// Returns the unique category names from related resources, in order.
    pub fn related_categories(&self) -> Vec<&'static str> {
        let mut cats = Vec::new();
        for r in &self.related_resources {
            if !cats.contains(&r.category) {
                cats.push(r.category);
            }
        }
        cats
    }

    /// Returns indices into related_resources for the currently selected tab.
    pub fn related_tab_indices(&self) -> Vec<usize> {
        let cats = self.related_categories();
        let current_cat = cats.get(self.related_tab).copied().unwrap_or("");
        self.related_resources
            .iter()
            .enumerate()
            .filter(|(_, r)| r.category == current_cat)
            .map(|(i, _)| i)
            .collect()
    }

    /// Scroll detail view to keep the related resource at `pos` visible.
    fn scroll_to_related_cursor(&mut self, pos: usize) {
        let target_line = self.related_line_start + pos;
        let scroll = self.detail_scroll as usize;
        let visible = self.detail_area_height;
        if visible == 0 {
            return;
        }
        if target_line >= scroll + visible {
            self.detail_scroll = (target_line - visible + 1) as u16;
        } else if target_line < scroll {
            self.detail_scroll = target_line as u16;
        }
    }

    async fn navigate_to_related(&mut self, rt: ResourceType, name: String, namespace: String) {
        // Switch to the related resource's type and load its detail
        self.panel = Panel::ResourceList(rt);
        self.related_cursor = None;
        self.related_tab = 0;
        self.resource_table_state = TableState::default();
        // Update nav selection
        if let Some(nav_idx) = self
            .nav_items
            .iter()
            .position(|item| matches!(&item.kind, NavItemKind::Resource(r) if *r == rt))
        {
            self.nav_state.select(Some(nav_idx));
        }
        // Load the resource list for this type
        if let Ok(entries) = self.kube.list_resources(rt).await {
            if let Some(idx) = entries.iter().position(|e| e.name == name && e.namespace == namespace) {
                self.resource_state.select(Some(idx));
            }
            self.resource_counts.insert(rt, entries.len());
            self.resources = entries;
        }
        self.pending_load = Some(PendingLoad::ResourceDetail { name, namespace });
    }

    fn dict_nav_up(&mut self) {
        match self.dict_cursor {
            | Some(0) => {
                // Stay at first entry — don't exit selection mode
            },
            | Some(i) => {
                self.dict_cursor = Some(i - 1);
                self.scroll_to_dict_cursor();
            },
            | None => {},
        }
    }

    fn dict_nav_down(&mut self) {
        let max = self.dict_entries.len().saturating_sub(1);
        match self.dict_cursor {
            | Some(i) if i >= max => {
                // Stay at last entry — don't exit selection mode
            },
            | Some(i) => {
                self.dict_cursor = Some(i + 1);
                self.scroll_to_dict_cursor();
            },
            | None => {},
        }
    }

    /// Scroll the detail view to keep the selected dict entry visible.
    fn scroll_to_dict_cursor(&mut self) {
        if let Some(cursor) = self.dict_cursor {
            if let Some(&line_offset) = self.dict_line_offsets.get(cursor) {
                let scroll = self.detail_scroll as usize;
                // Rough visible height estimate (will be exact next frame)
                let visible_height = 30usize;
                if line_offset < scroll {
                    self.detail_scroll = line_offset as u16;
                } else if line_offset >= scroll + visible_height {
                    self.detail_scroll = line_offset.saturating_sub(visible_height / 2) as u16;
                }
            }
        }
    }

    fn copy_yaml_to_clipboard(&mut self) {
        match arboard::Clipboard::new().and_then(|mut cb| cb.set_text(&self.detail_yaml)) {
            | Ok(()) => {
                self.error = None;
                self.push_status(format!("Copied YAML ({} bytes)", self.detail_yaml.len()));
            },
            | Err(e) => {
                self.error = Some(format!("Clipboard error: {}", e));
            },
        }
    }

    fn copy_dict_entry_to_clipboard(&mut self) {
        if let Some(cursor) = self.dict_cursor {
            if let Some((_, key, value)) = self.dict_entries.get(cursor) {
                let key = key.clone();
                let value = value.clone();
                let text = format!("{}: {}", key, value);
                match arboard::Clipboard::new().and_then(|mut cb| cb.set_text(&text)) {
                    | Ok(()) => {
                        self.error = None;
                        self.push_status(format!("Copied '{}' ({} bytes)", key, value.len()));
                    },
                    | Err(e) => {
                        self.error = Some(format!("Clipboard error: {}", e));
                    },
                }
            }
        }
    }

    fn handle_log_key(&mut self, key: KeyEvent) {
        // Filter editing mode captures all input
        if let Some(state) = &self.log_state {
            if state.filter_editing {
                self.handle_log_filter_key(key);
                return;
            }
        }

        let visible_count = self.log_state.as_ref().map(|s| s.visible_lines().len()).unwrap_or(0);

        match key.code {
            | KeyCode::Esc | KeyCode::Char('q') => {
                // Dismiss detail popup first, then deselect, then exit
                if self.log_detail_line.is_some() {
                    self.log_detail_line = None;
                } else if self
                    .log_state
                    .as_ref()
                    .map(|s| s.selected_line.is_some())
                    .unwrap_or(false)
                {
                    if let Some(state) = &mut self.log_state {
                        state.selected_line = None;
                        state.selection_anchor = None;
                    }
                } else {
                    if let Some(state) = &mut self.log_state {
                        state.stop_following();
                    }
                    self.log_state = None;
                    self.log_detail_line = None;
                    self.return_to_main();
                }
            },
            // [/] Start filter edit
            | KeyCode::Char('/') => {
                if let Some(state) = &mut self.log_state {
                    state.begin_filter_edit();
                }
            },
            // [f] Toggle follow
            | KeyCode::Char('f') => {
                if let Some(state) = &mut self.log_state {
                    if state.following {
                        state.stop_following();
                    } else {
                        state.start_following(self.kube.client().clone());
                    }
                }
            },
            // [c] Select container
            | KeyCode::Char('c') => {
                self.open_container_selector();
            },
            // [p] Select pod
            | KeyCode::Char('p') => {
                self.open_pod_selector();
            },
            // [Y] Copy all logs to clipboard
            | KeyCode::Char('Y') => {
                self.copy_logs_to_clipboard();
            },
            // [x] Clear filter
            | KeyCode::Char('x') => {
                if let Some(state) = &mut self.log_state {
                    state.clear_filter();
                }
            },
            // [w] Toggle wrap
            | KeyCode::Char('w') => {
                if let Some(state) = &mut self.log_state {
                    state.wrap = !state.wrap;
                }
            },
            // [t] Time filter
            | KeyCode::Char('t') => {
                self.popup = Some(Popup::TimeFilter { buf: "30m".to_string() });
            },
            // [Enter] Open selected line in detail popup
            | KeyCode::Enter => {
                if let Some(state) = &self.log_state {
                    if let Some(sel) = state.selected_line {
                        let visible = state.visible_lines();
                        if let Some(line) = visible.get(sel) {
                            self.log_detail_line = Some(line.to_string());
                        }
                    }
                }
            },
            // Navigation — moves selected_line cursor
            // Shift+Up/K extends selection from anchor
            | KeyCode::Up | KeyCode::Char('k') | KeyCode::Char('K') => {
                let extending = key.modifiers.contains(KeyModifiers::SHIFT) || key.code == KeyCode::Char('K');
                if let Some(state) = &mut self.log_state {
                    match state.selected_line {
                        | Some(sel) if sel > 0 => {
                            if extending && state.selection_anchor.is_none() {
                                state.selection_anchor = Some(sel);
                            } else if !extending {
                                state.selection_anchor = None;
                            }
                            state.selected_line = Some(sel - 1);
                            if sel - 1 < state.scroll {
                                state.scroll = sel - 1;
                            }
                            state.auto_scroll = false;
                        },
                        | None => {
                            let vis_count = state.visible_lines().len();
                            if vis_count > 0 {
                                let pos = state.scroll.min(vis_count.saturating_sub(1));
                                state.selected_line = Some(pos);
                                state.auto_scroll = false;
                            }
                        },
                        | _ => {},
                    }
                }
            },
            | KeyCode::Down | KeyCode::Char('j') | KeyCode::Char('J') => {
                let extending = key.modifiers.contains(KeyModifiers::SHIFT) || key.code == KeyCode::Char('J');
                if let Some(state) = &mut self.log_state {
                    let vis_count = state.visible_lines().len();
                    match state.selected_line {
                        | Some(sel) if sel + 1 < vis_count => {
                            if extending && state.selection_anchor.is_none() {
                                state.selection_anchor = Some(sel);
                            } else if !extending {
                                state.selection_anchor = None;
                            }
                            state.selected_line = Some(sel + 1);
                            state.auto_scroll = sel + 1 == vis_count.saturating_sub(1);
                        },
                        | None if vis_count > 0 => {
                            let pos = state.scroll.min(vis_count.saturating_sub(1));
                            state.selected_line = Some(pos);
                        },
                        | _ => {},
                    }
                }
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(state) = &mut self.log_state {
                    state.scroll_up(30);
                }
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(state) = &mut self.log_state {
                    state.scroll_down(30, visible_count);
                }
            },
            | KeyCode::Home | KeyCode::Char('g') => {
                if let Some(state) = &mut self.log_state {
                    let vis_count = state.visible_lines().len();
                    if vis_count > 0 {
                        state.selected_line = Some(0);
                        state.selection_anchor = None;
                        state.scroll = 0;
                        state.auto_scroll = false;
                    }
                }
            },
            | KeyCode::End | KeyCode::Char('G') => {
                if let Some(state) = &mut self.log_state {
                    let vis_count = state.visible_lines().len();
                    if vis_count > 0 {
                        state.selected_line = Some(vis_count.saturating_sub(1));
                        state.selection_anchor = None;
                        state.scroll = vis_count.saturating_sub(1);
                        state.auto_scroll = true;
                    }
                }
            },
            | _ => {},
        }
    }

    fn handle_log_filter_key(&mut self, key: KeyEvent) {
        let state = match &mut self.log_state {
            | Some(s) => s,
            | None => return,
        };

        match key.code {
            | KeyCode::Enter => state.apply_filter(),
            | KeyCode::Esc => state.cancel_filter_edit(),
            | KeyCode::Backspace => {
                state.filter_buf.pop();
            },
            | KeyCode::Char(c) => {
                state.filter_buf.push(c);
            },
            | _ => {},
        }
    }

    fn copy_secret_to_clipboard(&mut self) {
        if let Some(state) = &self.secret_state {
            let key_name = state.selected_key().unwrap_or("").to_string();

            let decoded = match state.selected_plaintext_value() {
                | Some(v) => v,
                | None => {
                    self.error = Some(format!("Cannot decode '{}' (not valid UTF-8)", key_name));
                    return;
                },
            };

            // Use arboard for system clipboard access.
            // The decoded value is the actual plaintext secret (already base64-decoded).
            match arboard::Clipboard::new().and_then(|mut cb| cb.set_text(&decoded)) {
                | Ok(()) => {
                    self.error = None;
                    self.push_status(format!("Copied '{}' to clipboard ({} bytes)", key_name, decoded.len()));
                },
                | Err(e) => {
                    self.error = Some(format!("Clipboard error: {}", e));
                },
            }
        }
    }

    async fn handle_popup_key(&mut self, key: KeyEvent) {
        // Port forward create popup has its own handler
        if matches!(self.popup, Some(Popup::PortForwardCreate(_))) {
            self.handle_pf_create_popup_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::ConfirmDelete { .. })) {
            self.handle_confirm_delete_key(key).await;
            return;
        }
        if matches!(self.popup, Some(Popup::ConfirmDrain { .. })) {
            self.handle_confirm_drain_key(key).await;
            return;
        }
        if matches!(self.popup, Some(Popup::ConfirmQuit { .. })) {
            self.handle_confirm_quit_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::TriggerCronJob { .. })) {
            self.handle_trigger_cronjob_key(key).await;
            return;
        }
        if matches!(self.popup, Some(Popup::ScaleInput { .. })) {
            self.handle_scale_input_key(key).await;
            return;
        }
        if matches!(self.popup, Some(Popup::ExecShell { .. })) {
            self.handle_exec_shell_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::KubeconfigInput { .. })) {
            self.handle_kubeconfig_input_key(key).await;
            return;
        }
        if matches!(self.popup, Some(Popup::TimeFilter { .. })) {
            self.handle_time_filter_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::ProfileSave { .. })) {
            self.handle_profile_save_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::ProfileLoad { .. })) {
            self.handle_profile_load_key(key).await;
            return;
        }
        if matches!(self.popup, Some(Popup::PortForwardEditPort { .. })) {
            self.handle_pf_edit_port_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::ProfileClone { .. })) {
            self.handle_profile_clone_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::ConfirmDeleteProfile { .. })) {
            self.handle_confirm_delete_profile_key(key);
            return;
        }

        match key.code {
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Up | KeyCode::Char('k') => {
                if let Some(popup) = &mut self.popup {
                    let state = match popup {
                        | Popup::ContextSelect { state, .. }
                        | Popup::NamespaceSelect { state, .. }
                        | Popup::PodSelect { state, .. }
                        | Popup::ContainerSelect { state, .. } => state,
                        | Popup::PortForwardCreate(_)
                        | Popup::ConfirmDelete { .. }
                        | Popup::ConfirmDrain { .. }
                        | Popup::ScaleInput { .. }
                        | Popup::ExecShell { .. }
                        | Popup::KubeconfigInput { .. }
                        | Popup::TriggerCronJob { .. }
                        | Popup::ConfirmQuit { .. }
                        | Popup::TimeFilter { .. }
                        | Popup::ProfileSave { .. }
                        | Popup::ProfileLoad { .. }
                        | Popup::PortForwardEditPort { .. }
                        | Popup::ProfileClone { .. }
                        | Popup::ConfirmDeleteProfile { .. } => unreachable!(),
                    };
                    let current = state.selected().unwrap_or(0);
                    if current > 0 {
                        state.select(Some(current - 1));
                    }
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if let Some(popup) = &mut self.popup {
                    let (items_len, state) = match popup {
                        | Popup::ContextSelect { items, state }
                        | Popup::NamespaceSelect { items, state }
                        | Popup::PodSelect { items, state }
                        | Popup::ContainerSelect { items, state } => (items.len(), state),
                        | Popup::PortForwardCreate(_)
                        | Popup::ConfirmDelete { .. }
                        | Popup::ConfirmDrain { .. }
                        | Popup::ScaleInput { .. }
                        | Popup::ExecShell { .. }
                        | Popup::KubeconfigInput { .. }
                        | Popup::TriggerCronJob { .. }
                        | Popup::ConfirmQuit { .. }
                        | Popup::TimeFilter { .. }
                        | Popup::ProfileSave { .. }
                        | Popup::ProfileLoad { .. }
                        | Popup::PortForwardEditPort { .. }
                        | Popup::ProfileClone { .. }
                        | Popup::ConfirmDeleteProfile { .. } => unreachable!(),
                    };
                    let current = state.selected().unwrap_or(0);
                    if current + 1 < items_len {
                        state.select(Some(current + 1));
                    }
                }
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(popup) = &mut self.popup {
                    let state = match popup {
                        | Popup::ContextSelect { state, .. }
                        | Popup::NamespaceSelect { state, .. }
                        | Popup::PodSelect { state, .. }
                        | Popup::ContainerSelect { state, .. } => state,
                        | _ => unreachable!(),
                    };
                    let current = state.selected().unwrap_or(0);
                    state.select(Some(current.saturating_sub(10)));
                }
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(popup) = &mut self.popup {
                    let (items_len, state) = match popup {
                        | Popup::ContextSelect { items, state }
                        | Popup::NamespaceSelect { items, state }
                        | Popup::PodSelect { items, state }
                        | Popup::ContainerSelect { items, state } => (items.len(), state),
                        | _ => unreachable!(),
                    };
                    let current = state.selected().unwrap_or(0);
                    state.select(Some((current + 10).min(items_len.saturating_sub(1))));
                }
            },
            | KeyCode::Enter => {
                let action = match &self.popup {
                    | Some(Popup::ContextSelect { items, state }) => {
                        state
                            .selected()
                            .and_then(|idx| items.get(idx).cloned())
                            .map(PendingLoad::SwitchContext)
                    },
                    | Some(Popup::NamespaceSelect { items, state }) => {
                        let ns = state.selected().and_then(|idx| items.get(idx).cloned());
                        if let Some(ns) = ns {
                            if ns == ALL_NAMESPACES_LABEL {
                                self.kube.set_namespace(None);
                            } else {
                                self.kube.set_namespace(Some(ns));
                            }
                            self.clear_cached_state();
                            Some(PendingLoad::Resources)
                        } else {
                            None
                        }
                    },
                    | Some(Popup::PodSelect { state, .. }) => {
                        if let Some(idx) = state.selected() {
                            if let Some(log_state) = &mut self.log_state {
                                log_state.selected_pod = if idx == 0 { None } else { Some(idx - 1) };
                                log_state.selected_container = None;
                            }
                            Some(PendingLoad::ReloadLogs)
                        } else {
                            None
                        }
                    },
                    | Some(Popup::ContainerSelect { state, .. }) => {
                        if let Some(idx) = state.selected() {
                            if let Some(log_state) = &mut self.log_state {
                                log_state.selected_container = if idx == 0 { None } else { Some(idx - 1) };
                            }
                            Some(PendingLoad::ReloadLogs)
                        } else {
                            None
                        }
                    },
                    | Some(Popup::PortForwardCreate(_))
                    | Some(Popup::ConfirmDelete { .. })
                    | Some(Popup::ConfirmDrain { .. })
                    | Some(Popup::ScaleInput { .. })
                    | Some(Popup::ExecShell { .. })
                    | Some(Popup::KubeconfigInput { .. })
                    | Some(Popup::TriggerCronJob { .. })
                    | Some(Popup::ConfirmQuit { .. })
                    | Some(Popup::TimeFilter { .. })
                    | Some(Popup::ProfileSave { .. })
                    | Some(Popup::ProfileLoad { .. })
                    | Some(Popup::PortForwardEditPort { .. })
                    | Some(Popup::ProfileClone { .. })
                    | Some(Popup::ConfirmDeleteProfile { .. })
                    | None => None,
                };
                self.popup = None;
                if let Some(load) = action {
                    self.pending_load = Some(load);
                }
            },
            | _ => {},
        }
    }

    fn open_logs_for_selected(&mut self) {
        if let Some(rt) = self.selected_resource_type() {
            if rt.supports_logs() {
                if let Some(idx) = self.resource_state.selected() {
                    if let Some(entry) = self.resources.get(idx) {
                        self.pending_load = Some(PendingLoad::Logs {
                            name: entry.name.clone(),
                            namespace: entry.namespace.clone(),
                        });
                    }
                }
            }
        }
    }

    fn open_context_selector(&mut self) {
        let contexts = self.kube.contexts();
        let current = self.kube.current_context().to_string();
        let mut state = ListState::default();
        if let Some(idx) = contexts.iter().position(|c| c == &current) {
            state.select(Some(idx));
        } else if !contexts.is_empty() {
            state.select(Some(0));
        }
        self.popup = Some(Popup::ContextSelect { items: contexts, state });
    }

    fn open_pod_selector(&mut self) {
        let log_state = match &self.log_state {
            | Some(s) => s,
            | None => return,
        };
        if log_state.pods.len() <= 1 {
            return;
        }
        let mut items = vec!["All".to_string()];
        items.extend(log_state.pods.iter().map(|p| p.name.clone()));
        let mut state = ListState::default();
        // Current selection: None → "All" (0), Some(i) → i+1
        let sel = log_state.selected_pod.map(|i| i + 1).unwrap_or(0);
        state.select(Some(sel));
        self.popup = Some(Popup::PodSelect { items, state });
    }

    fn open_container_selector(&mut self) {
        let log_state = match &self.log_state {
            | Some(s) => s,
            | None => return,
        };
        let containers = log_state.active_containers();
        if containers.len() <= 1 {
            return;
        }
        let mut items = vec!["All".to_string()];
        items.extend(containers);
        let mut state = ListState::default();
        let sel = log_state.selected_container.map(|i| i + 1).unwrap_or(0);
        state.select(Some(sel));
        self.popup = Some(Popup::ContainerSelect { items, state });
    }

    // -----------------------------------------------------------------------
    // Port forwards
    // -----------------------------------------------------------------------

    fn handle_port_forwards_key(&mut self, key: KeyEvent) {
        let count = self.pf_manager.entries().len();
        match key.code {
            | KeyCode::Up | KeyCode::Char('k') => {
                if self.pf_cursor > 0 {
                    self.pf_cursor -= 1;
                    self.pf_table_state.select(Some(self.pf_cursor));
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if count > 0 && self.pf_cursor < count.saturating_sub(1) {
                    self.pf_cursor += 1;
                    self.pf_table_state.select(Some(self.pf_cursor));
                }
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.pf_cursor = self.pf_cursor.saturating_sub(10);
                self.pf_table_state.select(Some(self.pf_cursor));
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if count > 0 {
                    self.pf_cursor = (self.pf_cursor + 10).min(count.saturating_sub(1));
                    self.pf_table_state.select(Some(self.pf_cursor));
                }
            },
            // [p] Toggle: running→pause, paused→activate, error→pause
            | KeyCode::Char('p') => {
                if let Some(entry) = self.pf_manager.entries().get(self.pf_cursor) {
                    let id = entry.id;
                    let local_port = entry.local_port;
                    let resource_name = entry.resource_label.split('/').nth(1).unwrap_or("").to_string();
                    let namespace = entry.namespace.clone();
                    let context = entry.context.clone();
                    if matches!(entry.status, portforward::PortForwardStatus::Paused) {
                        // Paused → activate: spawn a new task
                        let client = self.kube.client().clone();
                        if !self.pf_manager.resume_spawn(id, client) {
                            self.pf_manager.resume(id);
                        }
                        self.update_saved_pf_paused(&resource_name, &namespace, &context, local_port, false);
                    } else if matches!(entry.status, portforward::PortForwardStatus::Error(_)) {
                        // Error → pause (stop retrying, user can activate later)
                        self.pf_manager.pause(id);
                        self.update_saved_pf_paused(&resource_name, &namespace, &context, local_port, true);
                    } else if entry.status.is_running() {
                        // Running → pause
                        self.pf_manager.pause(id);
                        self.update_saved_pf_paused(&resource_name, &namespace, &context, local_port, true);
                    }
                }
            },
            // [d] Cancel (delete) selected forward
            | KeyCode::Char('d') => {
                if let Some(entry) = self.pf_manager.entries().get(self.pf_cursor) {
                    let id = entry.id;
                    let resource_name = entry.resource_label.split('/').nth(1).unwrap_or("").to_string();
                    let namespace = entry.namespace.clone();
                    let context = entry.context.clone();
                    let local_port = entry.local_port;
                    self.pf_manager.cancel(id);
                    self.pf_manager.remove_cancelled();
                    self.config.active_profile_mut().remove_port_forward(
                        &resource_name,
                        &namespace,
                        &context,
                        local_port,
                    );
                    if let Err(e) = self.config.save() {
                        self.error = Some(format!("Failed to save config: {}", e));
                    }
                    let new_count = self.pf_manager.entries().len();
                    if new_count == 0 {
                        self.pf_cursor = 0;
                    } else {
                        self.pf_cursor = self.pf_cursor.min(new_count.saturating_sub(1));
                    }
                }
            },
            // [e] Edit local port
            | KeyCode::Char('e') => {
                if let Some(entry) = self.pf_manager.entries().get(self.pf_cursor) {
                    let id = entry.id;
                    let old_port = entry.local_port;
                    self.popup = Some(Popup::PortForwardEditPort {
                        pf_id: id,
                        old_port,
                        buf: old_port.to_string(),
                    });
                }
            },
            | _ => {},
        }
    }

    fn handle_pf_edit_port_key(&mut self, key: KeyEvent) {
        let (pf_id, old_port, buf) = match &mut self.popup {
            | Some(Popup::PortForwardEditPort { pf_id, old_port, buf }) => (*pf_id, *old_port, buf),
            | _ => return,
        };
        match key.code {
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Backspace => {
                buf.pop();
            },
            | KeyCode::Char(c) if c.is_ascii_digit() => {
                buf.push(c);
            },
            | KeyCode::Enter => {
                let new_port: u16 = match buf.parse() {
                    | Ok(p) if p > 0 => p,
                    | _ => {
                        self.error = Some("Invalid port number".into());
                        return;
                    },
                };
                let pf_id = pf_id;
                let old_port = old_port;
                if new_port == old_port {
                    self.popup = None;
                    return;
                }
                self.apply_pf_port_change(pf_id, old_port, new_port);
                self.popup = None;
            },
            | _ => {},
        }
    }

    /// Change the local port of a port forward: cancel old, create new with
    /// updated port, update config.
    fn apply_pf_port_change(&mut self, pf_id: usize, old_port: u16, new_port: u16) {
        let entry = match self.pf_manager.entries().iter().find(|e| e.id == pf_id) {
            | Some(e) => e,
            | None => return,
        };

        let remote_port = entry.remote_port;
        let namespace = entry.namespace.clone();
        let context = entry.context.clone();
        let resource_label = entry.resource_label.clone();
        let pod_name = entry.pod_name.clone();
        let target = entry.target.clone();
        let was_paused = matches!(entry.status, portforward::PortForwardStatus::Paused);
        let resource_name = entry.resource_label.split('/').nth(1).unwrap_or("").to_string();

        // Cancel old
        self.pf_manager.cancel(pf_id);
        self.pf_manager.remove_cancelled();

        // Update config: remove old, add new
        self.config
            .active_profile_mut()
            .remove_port_forward(&resource_name, &namespace, &context, old_port);

        let target_for_save = target.clone().unwrap_or_else(|| {
            PfTarget::DirectPod {
                pod_name: pod_name.clone(),
            }
        });
        let (target_type, selector) = match &target_for_save {
            | PfTarget::DirectPod { pod_name } => ("direct_pod".to_string(), pod_name.clone()),
            | PfTarget::LabelSelector { selector } => ("label_selector".to_string(), selector.clone()),
        };

        let rt_name = resource_label
            .split('/')
            .next()
            .and_then(|display| {
                crate::k8s::ResourceType::all_by_category()
                    .into_iter()
                    .flat_map(|(_, types)| types)
                    .find(|rt| rt.display_name() == display)
                    .map(|rt| rt.singular_name().to_string())
            })
            .unwrap_or_default();

        let saved = crate::config::SavedPortForward {
            resource_type: rt_name,
            resource_name: resource_name.clone(),
            namespace: namespace.clone(),
            context: context.clone(),
            local_port: new_port,
            remote_port,
            target_type,
            selector,
            paused: was_paused,
        };
        self.config.active_profile_mut().add_port_forward(saved);
        if let Err(e) = self.config.save() {
            self.error = Some(format!("Failed to save config: {}", e));
        }

        // Create new forward
        if was_paused {
            if let Some(t) = target {
                self.pf_manager
                    .create_paused(namespace, pod_name, context, resource_label, new_port, remote_port, t);
            }
        } else if let Some(t) = target {
            let client = self.kube.client().clone();
            self.pf_manager.create(
                client,
                namespace,
                pod_name,
                context,
                resource_label,
                new_port,
                remote_port,
                t,
            );
        }

        self.push_status(format!("Port changed :{} → :{}", old_port, new_port));
    }

    /// Update the `paused` field of a saved port forward in config.
    fn update_saved_pf_paused(
        &mut self,
        resource_name: &str,
        namespace: &str,
        context: &str,
        local_port: u16,
        paused: bool,
    ) {
        if let Some(spf) = self.config.active_profile_mut().port_forwards.iter_mut().find(|pf| {
            pf.resource_name == resource_name
                && pf.namespace == namespace
                && pf.context == context
                && pf.local_port == local_port
        }) {
            spf.paused = paused;
        }
        if let Err(e) = self.config.save() {
            self.error = Some(format!("Failed to save config: {}", e));
        }
    }

    async fn open_port_forward_dialog(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) => rt,
            | None => return,
        };

        // Get the selected resource entry
        let visible = self.visible_resource_indices();
        let vis_pos = self
            .resource_state
            .selected()
            .and_then(|sel| visible.iter().position(|&i| i == sel))
            .unwrap_or(0);
        let entry = match visible.get(vis_pos).and_then(|&i| self.resources.get(i)) {
            | Some(e) => e,
            | None => return,
        };
        let name = entry.name.clone();
        let namespace = entry.namespace.clone();

        // Fetch the resource value to extract ports and selector
        let value = match self.kube.get_resource(rt, &namespace, &name).await {
            | Ok(v) => v,
            | Err(e) => {
                self.error = Some(format!("Failed to fetch resource: {}", e));
                return;
            },
        };

        let ports = portforward::extract_ports(rt, &value);
        if ports.is_empty() {
            self.error = Some("No ports found on this resource".into());
            return;
        }

        // Determine target: selector-based or direct pod
        let target = if rt == ResourceType::Pod {
            PfTarget::DirectPod { pod_name: name.clone() }
        } else {
            match portforward::extract_selector(rt, &value) {
                | Some(selector) => PfTarget::LabelSelector { selector },
                | None => {
                    self.error = Some("Cannot resolve pod selector for this resource".into());
                    return;
                },
            }
        };

        let default_port = ports[0].container_port.to_string();

        self.popup = Some(Popup::PortForwardCreate(PfCreateDialog {
            resource_type: rt,
            resource_name: name,
            namespace,
            target,
            ports,
            port_cursor: 0,
            local_port_buf: default_port,
        }));
    }

    fn handle_pf_create_popup_key(&mut self, key: KeyEvent) {
        let dialog = match &mut self.popup {
            | Some(Popup::PortForwardCreate(d)) => d,
            | _ => return,
        };

        match key.code {
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Up | KeyCode::Char('k') => {
                if dialog.port_cursor > 0 {
                    dialog.port_cursor -= 1;
                    dialog.local_port_buf = dialog.ports[dialog.port_cursor].container_port.to_string();
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if dialog.port_cursor + 1 < dialog.ports.len() {
                    dialog.port_cursor += 1;
                    dialog.local_port_buf = dialog.ports[dialog.port_cursor].container_port.to_string();
                }
            },
            | KeyCode::Backspace => {
                dialog.local_port_buf.pop();
            },
            | KeyCode::Char(c) if c.is_ascii_digit() => {
                dialog.local_port_buf.push(c);
            },
            | KeyCode::Enter => {
                self.confirm_port_forward_create();
            },
            | _ => {},
        }
    }

    fn confirm_port_forward_create(&mut self) {
        let dialog = match &self.popup {
            | Some(Popup::PortForwardCreate(d)) => d,
            | _ => return,
        };

        let local_port: u16 = match dialog.local_port_buf.parse() {
            | Ok(p) if p > 0 => p,
            | _ => {
                self.error = Some("Invalid local port".into());
                return;
            },
        };

        let remote_port = dialog.ports[dialog.port_cursor].container_port;
        let namespace = dialog.namespace.clone();
        let resource_label = format!("{}/{}", dialog.resource_type.display_name(), dialog.resource_name);
        let target = dialog.target.clone();

        // Resolve pod name for display (best effort)
        let pod_name = match &target {
            | PfTarget::DirectPod { pod_name } => pod_name.clone(),
            | PfTarget::LabelSelector { .. } => "(resolving)".to_string(),
        };

        let context = self.kube.current_context().to_string();
        let client = self.kube.client().clone();

        self.pf_manager.create(
            client,
            namespace,
            pod_name,
            context,
            resource_label.clone(),
            local_port,
            remote_port,
            target,
        );

        // Persist to config
        let (target_type, selector) = match &dialog.target {
            | PfTarget::DirectPod { pod_name } => ("direct_pod".to_string(), pod_name.clone()),
            | PfTarget::LabelSelector { selector } => ("label_selector".to_string(), selector.clone()),
        };
        let saved = SavedPortForward {
            resource_type: dialog.resource_type.singular_name().to_string(),
            resource_name: dialog.resource_name.clone(),
            namespace: dialog.namespace.clone(),
            context: self.kube.current_context().to_string(),
            local_port,
            remote_port,
            target_type,
            selector,
            paused: false,
        };
        self.config.active_profile_mut().add_port_forward(saved);
        if let Err(e) = self.config.save() {
            self.error = Some(format!("Failed to save config: {}", e));
        }

        self.popup = None;
        self.push_status(format!("Port forward created: :{} -> :{}", local_port, remote_port));
        self.error = None;
    }

    /// Poll port forward status updates (called every event loop tick).
    pub fn poll_port_forwards(&mut self) {
        self.pf_manager.poll_updates();
    }

    // -----------------------------------------------------------------------
    // Exec — spawn new terminal window
    // -----------------------------------------------------------------------

    /// Spawn a new terminal window running kubectl exec.
    pub fn spawn_exec_terminal(&mut self) {
        let exec = match self.pending_exec.take() {
            | Some(e) => e,
            | None => return,
        };

        let terminal_app = match self.exec_terminal_override.take().or_else(resolve_terminal_app) {
            | Some(t) => t,
            | None => {
                self.error = Some("Set $TERMINAL or $TERM_PROGRAM to specify your terminal emulator".into());
                return;
            },
        };

        let context = self.kube.current_context().to_string();
        let kubeconfig_flag = self
            .kube
            .kubeconfig_path()
            .map(|p| format!(" --kubeconfig {}", p))
            .unwrap_or_default();
        let cmd_str = exec.command.join(" ");

        let kubectl_cmd = format!(
            "kubectl exec -it {} -n {} -c {} --context {}{} -- {} ; exit",
            exec.pod_name, exec.namespace, exec.container, context, kubeconfig_flag, cmd_str,
        );

        let result = spawn_terminal_window(&terminal_app, &kubectl_cmd);

        match result {
            | Ok(_) => {
                self.push_status(format!(
                    "Opened shell in {}: {}/{}",
                    terminal_app, exec.pod_name, exec.container
                ));
                self.error = None;
            },
            | Err(e) => {
                self.error = Some(format!("Failed to open '{}': {}", terminal_app, e));
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Terminal resolution helpers
// ---------------------------------------------------------------------------

/// Resolve the terminal application to use. Checks $TERMINAL, then
/// platform-specific defaults.
fn resolve_terminal_app() -> Option<String> {
    for var in ["TERMINAL", "TERM_PROGRAM"] {
        if let Ok(val) = std::env::var(var) {
            if !val.is_empty() {
                return Some(val);
            }
        }
    }
    None
}

/// Spawn a new terminal window running a command.
fn spawn_terminal_window(terminal_app: &str, command: &str) -> Result<std::process::Child, std::io::Error> {
    let tmp = tempfile::Builder::new()
        .prefix("qb-exec-")
        .suffix(".sh")
        .tempfile()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    std::fs::write(tmp.path(), format!("#!/bin/sh\n{}\n", command))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o755))?;
    }

    let path = tmp.into_temp_path();
    let path_str = path.to_string_lossy().to_string();
    std::mem::forget(path); // keep alive for the spawned terminal

    // Apple Terminal.app has no CLI binary — must use osascript
    if matches!(terminal_app, "Apple_Terminal" | "Terminal" | "Terminal.app") {
        return std::process::Command::new("osascript")
            .arg("-e")
            .arg(format!(
                "tell application \"Terminal\"\nactivate\ndo script \"{}\"\nend tell",
                path_str
            ))
            .spawn();
    }

    // All other terminals: run binary directly with -e to get a NEW window
    std::process::Command::new(terminal_app)
        .arg("-e")
        .arg(&path_str)
        .spawn()
}

impl App {
    // -----------------------------------------------------------------------
    // Restart workload
    // -----------------------------------------------------------------------

    async fn restart_selected_workload(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) if rt.supports_scale() || matches!(rt, ResourceType::DaemonSet) => rt,
            | _ => return,
        };
        let (name, namespace) = if self.view == View::Detail {
            (self.detail_name.clone(), self.detail_namespace.clone())
        } else {
            let visible = self.visible_resource_indices();
            let vis_pos = self
                .resource_state
                .selected()
                .and_then(|sel| visible.iter().position(|&i| i == sel))
                .unwrap_or(0);
            match visible.get(vis_pos).and_then(|&i| self.resources.get(i)) {
                | Some(e) => (e.name.clone(), e.namespace.clone()),
                | None => return,
            }
        };
        match self.kube.restart_workload(rt, &namespace, &name).await {
            | Ok(()) => {
                self.push_status(format!("Restarted {}/{}", rt.display_name(), name));
                self.error = None;
            },
            | Err(e) => {
                self.error = Some(format!("Restart failed: {}", e));
            },
        }
    }

    // -----------------------------------------------------------------------
    // Trigger CronJob (create manual Job)
    // -----------------------------------------------------------------------

    fn open_trigger_cronjob(&mut self) {
        if self.selected_resource_type() != Some(ResourceType::CronJob) {
            return;
        }
        let (name, namespace) = if self.view == View::Detail {
            (self.detail_name.clone(), self.detail_namespace.clone())
        } else {
            self.selected_resource_name_ns()
        };
        if name.is_empty() {
            return;
        }
        let default_name = KubeClient::default_trigger_job_name(&name);
        self.popup = Some(Popup::TriggerCronJob {
            cronjob_name: name,
            namespace,
            buf: default_name,
        });
    }

    async fn handle_trigger_cronjob_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Enter => {
                let (cronjob_name, namespace, job_name) = match &self.popup {
                    | Some(Popup::TriggerCronJob {
                        cronjob_name,
                        namespace,
                        buf,
                    }) => {
                        let job_name = buf.trim().to_string();
                        if job_name.is_empty() {
                            self.error = Some("Job name cannot be empty".into());
                            self.popup = None;
                            return;
                        }
                        (cronjob_name.clone(), namespace.clone(), job_name)
                    },
                    | _ => return,
                };
                self.popup = None;
                match self.kube.trigger_cronjob(&namespace, &cronjob_name, &job_name).await {
                    | Ok(created_name) => {
                        self.push_status(format!("Created job {}", created_name));
                        self.error = None;
                        self.navigate_to_related(ResourceType::Job, created_name, namespace)
                            .await;
                    },
                    | Err(e) => {
                        self.error = Some(format!("Trigger failed: {}", e));
                    },
                }
            },
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Backspace => {
                if let Some(Popup::TriggerCronJob { buf, .. }) = &mut self.popup {
                    buf.pop();
                }
            },
            | KeyCode::Char(c) => {
                if let Some(Popup::TriggerCronJob { buf, .. }) = &mut self.popup {
                    buf.push(c);
                }
            },
            | _ => {},
        }
    }

    // -----------------------------------------------------------------------
    // Node cordon / drain
    // -----------------------------------------------------------------------

    async fn toggle_cordon_node(&mut self) {
        if self.selected_resource_type() != Some(ResourceType::Node) {
            return;
        }
        let (name, _) = self.selected_resource_name_ns();
        if name.is_empty() {
            return;
        }

        // Determine current schedulable state
        let is_schedulable = if self.view == View::Detail {
            // Read from the detail JSON value
            !self
                .detail_value
                .get("spec")
                .and_then(|s| s.get("unschedulable"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        } else {
            // Read from the table columns (STATUS column)
            self.resource_state
                .selected()
                .and_then(|idx| self.resources.get(idx))
                .and_then(|e| e.columns.first())
                .map(|status| !status.contains("SchedulingDisabled"))
                .unwrap_or(true)
        };

        let result = if is_schedulable {
            self.kube.cordon_node(&name).await
        } else {
            self.kube.uncordon_node(&name).await
        };

        match result {
            | Ok(()) => {
                let action = if is_schedulable { "Cordoned" } else { "Uncordoned" };
                self.push_status(format!("{} node {}", action, name));
                self.error = None;
                if self.view == View::Detail {
                    // Reload detail to reflect the change
                    self.pending_load = Some(PendingLoad::ResourceDetail {
                        name: name.clone(),
                        namespace: String::new(),
                    });
                } else {
                    self.pending_load = Some(PendingLoad::Resources);
                }
            },
            | Err(e) => {
                self.error = Some(format!("Cordon failed: {}", e));
            },
        }
    }

    /// Returns (name, namespace) of the currently selected resource.
    fn selected_resource_name_ns(&self) -> (String, String) {
        if self.view == View::Detail {
            return (self.detail_name.clone(), self.detail_namespace.clone());
        }
        let visible = self.visible_resource_indices();
        let vis_pos = self
            .resource_state
            .selected()
            .and_then(|sel| visible.iter().position(|&i| i == sel))
            .unwrap_or(0);
        match visible.get(vis_pos).and_then(|&i| self.resources.get(i)) {
            | Some(e) => (e.name.clone(), e.namespace.clone()),
            | None => (String::new(), String::new()),
        }
    }

    // -----------------------------------------------------------------------
    // Copy resource name
    // -----------------------------------------------------------------------

    fn copy_resource_name(&mut self) {
        let visible = self.visible_resource_indices();
        let vis_pos = self
            .resource_state
            .selected()
            .and_then(|sel| visible.iter().position(|&i| i == sel))
            .unwrap_or(0);
        let entry = match visible.get(vis_pos).and_then(|&i| self.resources.get(i)) {
            | Some(e) => e,
            | None => return,
        };
        let name = entry.name.clone();
        match arboard::Clipboard::new().and_then(|mut cb| cb.set_text(&name)) {
            | Ok(()) => {
                self.push_status(format!("Copied '{}'", name));
                self.error = None;
            },
            | Err(e) => {
                self.error = Some(format!("Clipboard error: {}", e));
            },
        }
    }

    // -----------------------------------------------------------------------
    // Favorites
    // -----------------------------------------------------------------------

    /// Check if a resource is favorited in the active profile.
    pub fn is_favorite(&self, resource_type: ResourceType, name: &str, namespace: &str) -> bool {
        let context = self.kube.current_context();
        self.config
            .active_profile()
            .is_favorite(resource_type.singular_name(), name, namespace, context)
    }

    /// Toggle favorite for the currently selected resource.
    fn toggle_favorite_selected(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) => rt,
            | None => return,
        };
        let (name, namespace) = self.selected_resource_name_ns();
        if name.is_empty() {
            return;
        }
        let context = self.kube.current_context().to_string();
        let added = self.config.active_profile_mut().toggle_favorite(
            rt.singular_name().to_string(),
            name.clone(),
            namespace,
            context,
        );

        if added {
            self.push_status(format!("★ Favorited '{}'", name));
        } else {
            self.push_status(format!("☆ Unfavorited '{}'", name));
        }
        // Auto-save
        if let Err(e) = self.config.save() {
            self.error = Some(format!("Failed to save config: {}", e));
        }
    }

    /// Toggle favorite for the resource in detail view.
    fn toggle_favorite_detail(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) => rt,
            | None => return,
        };
        let name = self.detail_name.clone();
        let namespace = self.detail_namespace.clone();
        if name.is_empty() {
            return;
        }
        let context = self.kube.current_context().to_string();
        let added = self.config.active_profile_mut().toggle_favorite(
            rt.singular_name().to_string(),
            name.clone(),
            namespace,
            context,
        );

        if added {
            self.push_status(format!("★ Favorited '{}'", name));
        } else {
            self.push_status(format!("☆ Unfavorited '{}'", name));
        }
        // Auto-save
        if let Err(e) = self.config.save() {
            self.error = Some(format!("Failed to save config: {}", e));
        }
    }

    /// Navigate to a favorite from the favorites view.
    fn open_favorite_at_cursor(&mut self) {
        let favorites = self.config.active_profile().favorites.clone();
        if let Some(fav) = favorites.get(self.favorites_cursor) {
            if let Some(rt) = ResourceType::from_singular_name(&fav.resource_type) {
                self.return_panel = Some(self.panel.clone());
                self.panel = Panel::ResourceList(rt);
                self.pending_load = Some(PendingLoad::ResourceDetail {
                    name: fav.name.clone(),
                    namespace: fav.namespace.clone(),
                });
            }
        }
    }

    /// Remove a favorite from the favorites view.
    fn remove_favorite_at_cursor(&mut self) {
        let favorites = self.config.active_profile().favorites.clone();
        if let Some(fav) = favorites.get(self.favorites_cursor) {
            let fav = fav.clone();
            self.config.active_profile_mut().favorites.retain(|f| f != &fav);
            let count = self.config.active_profile().favorites.len();
            if self.favorites_cursor >= count && count > 0 {
                self.favorites_cursor = count - 1;
            }
            self.push_status(format!("Removed '{}' from favorites", fav.name));
            if let Err(e) = self.config.save() {
                self.error = Some(format!("Failed to save config: {}", e));
            }
        }
    }

    /// Resolve the favorite at the cursor, setting the panel to ResourceList
    /// for downstream commands. Returns (rt, name, namespace) if valid.
    fn resolve_favorite_at_cursor(&self) -> Option<(ResourceType, String, String)> {
        let fav = self.config.active_profile().favorites.get(self.favorites_cursor)?;
        let rt = ResourceType::from_singular_name(&fav.resource_type)?;
        Some((rt, fav.name.clone(), fav.namespace.clone()))
    }

    /// Handle keys in the favorites view.
    /// Supports all standard resource list commands plus `*` to de-favorite.
    async fn handle_favorites_key(&mut self, key: KeyEvent) {
        let count = self.config.active_profile().favorites.len();
        match key.code {
            | KeyCode::Up | KeyCode::Char('k') => {
                if self.favorites_cursor > 0 {
                    self.favorites_cursor -= 1;
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if count > 0 && self.favorites_cursor + 1 < count {
                    self.favorites_cursor += 1;
                }
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.favorites_cursor = self.favorites_cursor.saturating_sub(20);
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.favorites_cursor = (self.favorites_cursor + 20).min(count.saturating_sub(1));
            },
            | KeyCode::Home | KeyCode::Char('g') => {
                self.favorites_cursor = 0;
            },
            | KeyCode::End | KeyCode::Char('G') => {
                if count > 0 {
                    self.favorites_cursor = count - 1;
                }
            },
            // [Enter] Open detail view for this favorite
            | KeyCode::Enter => {
                self.open_favorite_at_cursor();
            },
            // [*] De-favorite (remove from favorites)
            | KeyCode::Char('*') => {
                self.remove_favorite_at_cursor();
            },
            // [l] Open logs (workload resources)
            | KeyCode::Char('l') => {
                self.open_favorite_logs();
            },
            // [e] Edit resource ($EDITOR)
            | KeyCode::Char('e') => {
                self.edit_favorite_at_cursor().await;
            },
            // [y] Copy resource name
            | KeyCode::Char('y') => {
                if let Some(fav) = self.config.active_profile().favorites.get(self.favorites_cursor) {
                    let name = fav.name.clone();
                    match arboard::Clipboard::new().and_then(|mut cb| cb.set_text(&name)) {
                        | Ok(()) => {
                            self.push_status(format!("Copied '{}'", name));
                            self.error = None;
                        },
                        | Err(e) => {
                            self.error = Some(format!("Clipboard error: {}", e));
                        },
                    }
                }
            },
            // [F] Port forward — open dialog directly with favorite data
            | KeyCode::Char('F') => {
                if let Some((rt, name, namespace)) = self.resolve_favorite_at_cursor() {
                    match self.kube.get_resource(rt, &namespace, &name).await {
                        | Ok(value) => {
                            let ports = portforward::extract_ports(rt, &value);
                            if ports.is_empty() {
                                self.error = Some("No ports found on this resource".into());
                            } else {
                                let target = if rt == ResourceType::Pod {
                                    PfTarget::DirectPod { pod_name: name.clone() }
                                } else {
                                    match portforward::extract_selector(rt, &value) {
                                        | Some(selector) => PfTarget::LabelSelector { selector },
                                        | None => {
                                            self.error = Some("Cannot resolve pod selector for this resource".into());
                                            return;
                                        },
                                    }
                                };
                                let default_port = ports[0].container_port.to_string();
                                self.popup = Some(Popup::PortForwardCreate(PfCreateDialog {
                                    resource_type: rt,
                                    resource_name: name,
                                    namespace,
                                    target,
                                    ports,
                                    port_cursor: 0,
                                    local_port_buf: default_port,
                                }));
                            }
                        },
                        | Err(e) => {
                            self.error = Some(format!("Failed to fetch resource: {}", e));
                        },
                    }
                }
            },
            // [D] Delete resource
            | KeyCode::Char('D') => {
                if let Some((rt, name, namespace)) = self.resolve_favorite_at_cursor() {
                    self.popup = Some(Popup::ConfirmDelete {
                        name,
                        namespace,
                        resource_type: rt,
                    });
                }
            },
            // [R] Restart workload
            | KeyCode::Char('R') => {
                if let Some((rt, name, namespace)) = self.resolve_favorite_at_cursor() {
                    if matches!(
                        rt,
                        ResourceType::Deployment | ResourceType::StatefulSet | ResourceType::DaemonSet
                    ) {
                        match self.kube.restart_workload(rt, &namespace, &name).await {
                            | Ok(()) => {
                                self.push_status(format!("Restarted {}/{}", rt.display_name(), name));
                                self.error = None;
                            },
                            | Err(e) => {
                                self.error = Some(format!("Restart failed: {}", e));
                            },
                        }
                    }
                }
            },
            // [S] Scale workload
            | KeyCode::Char('S') => {
                if let Some((rt, name, namespace)) = self.resolve_favorite_at_cursor() {
                    if rt.supports_scale() {
                        match self.kube.get_resource(rt, &namespace, &name).await {
                            | Ok(value) => {
                                let current = value
                                    .get("spec")
                                    .and_then(|s| s.get("replicas"))
                                    .and_then(|r| r.as_u64())
                                    .unwrap_or(0) as u32;
                                self.popup = Some(Popup::ScaleInput {
                                    name,
                                    namespace,
                                    resource_type: rt,
                                    current,
                                    buf: current.to_string(),
                                });
                            },
                            | Err(e) => {
                                self.error = Some(format!("Failed to fetch resource: {}", e));
                            },
                        }
                    }
                }
            },
            // [d] Mark / diff favorites
            | KeyCode::Char('d') => {
                if let Some((rt, name, namespace)) = self.resolve_favorite_at_cursor() {
                    if let Some((mark_name, _mark_ns, mark_yaml)) = self.diff_mark.take() {
                        // Second resource — fetch and show diff
                        match self.kube.get_resource(rt, &namespace, &name).await {
                            | Ok(value) => {
                                let yaml = serde_yaml::to_string(&value).unwrap_or_default();
                                let diff_lines = compute_diff(&mark_yaml, &yaml);
                                self.edit_ctx = Some(EditContext {
                                    resource_type: rt,
                                    name: format!("{} vs {}", mark_name, name),
                                    namespace: namespace.clone(),
                                    original_yaml: mark_yaml,
                                    edited_yaml: yaml,
                                    diff_lines,
                                    diff_mode: DiffMode::Inline,
                                    scroll: 0,
                                    error: None,
                                });
                                self.return_panel = Some(Panel::Favorites);
                                self.view = View::EditDiff;
                                self.push_status(format!("Diff: {} vs {}", mark_name, name));
                            },
                            | Err(e) => {
                                self.error = Some(format!("Failed to fetch resource: {}", e));
                            },
                        }
                    } else {
                        // First resource — mark it
                        match self.kube.get_resource(rt, &namespace, &name).await {
                            | Ok(value) => {
                                let yaml = serde_yaml::to_string(&value).unwrap_or_default();
                                self.diff_mark = Some((name.clone(), namespace, yaml));
                                self.push_status(format!(
                                    "Marked '{}' for diff \u{2014} select another and press [d]",
                                    name
                                ));
                            },
                            | Err(e) => {
                                self.error = Some(format!("Failed to fetch resource: {}", e));
                            },
                        }
                    }
                }
            },
            | _ => {},
        }
    }

    /// Open logs for the favorite at cursor.
    fn open_favorite_logs(&mut self) {
        let favorites = self.config.active_profile().favorites.clone();
        if let Some(fav) = favorites.get(self.favorites_cursor) {
            if let Some(rt) = ResourceType::from_singular_name(&fav.resource_type) {
                if rt.supports_logs() {
                    self.return_panel = Some(self.panel.clone());
                    self.panel = Panel::ResourceList(rt);
                    self.pending_load = Some(PendingLoad::Logs {
                        name: fav.name.clone(),
                        namespace: fav.namespace.clone(),
                    });
                }
            }
        }
    }

    /// Edit the resource at the favorites cursor.
    async fn edit_favorite_at_cursor(&mut self) {
        let favorites = self.config.active_profile().favorites.clone();
        if let Some(fav) = favorites.get(self.favorites_cursor) {
            if let Some(rt) = ResourceType::from_singular_name(&fav.resource_type) {
                // Fetch the resource YAML
                match self.kube.get_resource(rt, &fav.namespace, &fav.name).await {
                    | Ok(value) => {
                        let yaml = serde_yaml::to_string(&value).unwrap_or_default();
                        self.return_panel = Some(self.panel.clone());
                        self.panel = Panel::ResourceList(rt);
                        self.pending_edit = Some(PendingEdit {
                            resource_type: rt,
                            name: fav.name.clone(),
                            namespace: fav.namespace.clone(),
                            yaml,
                            original_yaml: None,
                        });
                    },
                    | Err(e) => {
                        self.error = Some(format!("Failed to fetch resource: {}", e));
                    },
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Profiles view
    // -----------------------------------------------------------------------

    fn sorted_profile_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.config.profiles.keys().cloned().collect();
        names.sort();
        names
    }

    async fn handle_profiles_key(&mut self, key: KeyEvent) {
        let names = self.sorted_profile_names();
        let count = names.len();
        match key.code {
            | KeyCode::Up | KeyCode::Char('k') => {
                if self.profiles_cursor > 0 {
                    self.profiles_cursor -= 1;
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if count > 0 && self.profiles_cursor + 1 < count {
                    self.profiles_cursor += 1;
                }
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.profiles_cursor = self.profiles_cursor.saturating_sub(20);
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.profiles_cursor = (self.profiles_cursor + 20).min(count.saturating_sub(1));
            },
            | KeyCode::Home | KeyCode::Char('g') => {
                self.profiles_cursor = 0;
            },
            | KeyCode::End | KeyCode::Char('G') => {
                if count > 0 {
                    self.profiles_cursor = count - 1;
                }
            },
            // [Enter] Switch to selected profile
            | KeyCode::Enter => {
                if let Some(name) = names.get(self.profiles_cursor) {
                    let name = name.clone();
                    self.switch_profile(&name).await;
                }
            },
            // [c] Clone profile
            | KeyCode::Char('c') => {
                if let Some(source) = names.get(self.profiles_cursor) {
                    let source = source.clone();
                    self.popup = Some(Popup::ProfileClone {
                        source_name: source.clone(),
                        buf: format!("{}-copy", source),
                    });
                }
            },
            // [D] Delete profile
            | KeyCode::Char('D') => {
                if let Some(name) = names.get(self.profiles_cursor) {
                    let name = name.clone();
                    if name == "default" {
                        self.error = Some("Cannot delete the default profile".into());
                    } else if name == self.config.active_profile {
                        self.error = Some("Cannot delete the active profile".into());
                    } else {
                        self.popup = Some(Popup::ConfirmDeleteProfile { profile_name: name });
                    }
                }
            },
            // [C] Create new empty profile
            | KeyCode::Char('C') => {
                self.popup = Some(Popup::ProfileSave { buf: String::new() });
            },
            | _ => {},
        }
    }

    fn handle_profile_clone_key(&mut self, key: KeyEvent) {
        let (source_name, buf) = match &mut self.popup {
            | Some(Popup::ProfileClone { source_name, buf }) => (source_name.clone(), buf),
            | _ => return,
        };
        match key.code {
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Backspace => {
                buf.pop();
            },
            | KeyCode::Char(c) => {
                buf.push(c);
            },
            | KeyCode::Enter => {
                let new_name = buf.clone();
                if new_name.is_empty() {
                    return;
                }
                if self.config.profiles.contains_key(&new_name) {
                    self.error = Some(format!("Profile '{}' already exists", new_name));
                    return;
                }
                let profile = self.config.profiles.get(&source_name).cloned().unwrap_or_default();
                self.config.profiles.insert(new_name.clone(), profile);
                self.popup = None;
                self.push_status(format!("Cloned '{}' → '{}'", source_name, new_name));
                if let Err(e) = self.config.save() {
                    self.error = Some(format!("Failed to save config: {}", e));
                }
            },
            | _ => {},
        }
    }

    fn handle_confirm_delete_profile_key(&mut self, key: KeyEvent) {
        let profile_name = match &self.popup {
            | Some(Popup::ConfirmDeleteProfile { profile_name }) => profile_name.clone(),
            | _ => return,
        };
        match key.code {
            | KeyCode::Enter | KeyCode::Char('y') => {
                self.config.profiles.remove(&profile_name);
                self.popup = None;
                self.push_status(format!("Deleted profile '{}'", profile_name));
                let count = self.config.profiles.len();
                if self.profiles_cursor >= count && count > 0 {
                    self.profiles_cursor = count - 1;
                }
                if let Err(e) = self.config.save() {
                    self.error = Some(format!("Failed to save config: {}", e));
                }
            },
            | KeyCode::Esc | KeyCode::Char('n') => {
                self.popup = None;
            },
            | _ => {},
        }
    }

    // -----------------------------------------------------------------------
    // Profile save/load
    // -----------------------------------------------------------------------

    /// Switch to a different profile: cancel all current port forwards,
    /// update the active profile, restore the new profile's port forwards
    /// and context.
    async fn switch_profile(&mut self, name: &str) {
        if name == self.config.active_profile {
            return;
        }
        // Save current PF paused states before switching
        // (already persisted on pause/resume, so just cancel runtime entries)
        self.pf_manager.cancel_all();
        self.clear_cached_state();

        self.config.active_profile = name.to_string();

        // Reload KubeClient if the profile has a different kubeconfig
        let profile_kubeconfig = self.config.active_profile().kubeconfig.clone();
        let current_kubeconfig = self.kube.kubeconfig_path().map(|s| s.to_string());
        if profile_kubeconfig != current_kubeconfig {
            let ctx = self.config.active_profile().context.clone();
            match KubeClient::new(profile_kubeconfig, ctx, None).await {
                | Ok(new_client) => {
                    self.kube = new_client;
                    self.config.active_profile_mut().kubeconfig = self.kube.kubeconfig_path().map(|s| s.to_string());
                },
                | Err(e) => {
                    self.error = Some(format!("Failed to load kubeconfig: {}", e));
                },
            }
        } else if let Some(ctx) = self.config.active_profile().context.clone() {
            // Same kubeconfig, just switch context
            if let Err(e) = self.kube.switch_context(&ctx).await {
                self.error = Some(format!("Failed to switch context: {}", e));
            }
        }

        // Restore port forwards from the new profile
        self.restore_saved_port_forwards();

        self.push_status(format!("Switched to profile '{}'", name));
        if let Err(e) = self.config.save() {
            self.error = Some(format!("Failed to save config: {}", e));
        }
    }

    fn open_profile_save(&mut self) {
        let current = self.config.active_profile.clone();
        self.popup = Some(Popup::ProfileSave { buf: current });
    }

    fn open_profile_load(&mut self) {
        let mut names: Vec<String> = self.config.profile_names().into_iter().map(|s| s.to_string()).collect();
        names.sort();
        let mut state = ListState::default();
        if !names.is_empty() {
            let current_pos = names.iter().position(|n| n == &self.config.active_profile).unwrap_or(0);
            state.select(Some(current_pos));
        }
        self.popup = Some(Popup::ProfileLoad { items: names, state });
    }

    fn handle_profile_save_key(&mut self, key: KeyEvent) {
        let buf = match &mut self.popup {
            | Some(Popup::ProfileSave { buf }) => buf,
            | _ => return,
        };
        match key.code {
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Backspace => {
                buf.pop();
            },
            | KeyCode::Char(c) => {
                buf.push(c);
            },
            | KeyCode::Enter => {
                let name = buf.clone();
                if name.is_empty() {
                    return;
                }
                // Clone current active profile into the new name
                let profile = self.config.active_profile().clone();
                self.config.profiles.insert(name.clone(), profile);
                self.config.active_profile = name.clone();
                self.popup = None;
                self.push_status(format!("Profile saved as '{}'", name));
                if let Err(e) = self.config.save() {
                    self.error = Some(format!("Failed to save config: {}", e));
                }
            },
            | _ => {},
        }
    }

    async fn handle_profile_load_key(&mut self, key: KeyEvent) {
        let (items, state) = match &mut self.popup {
            | Some(Popup::ProfileLoad { items, state }) => (items, state),
            | _ => return,
        };
        match key.code {
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Up | KeyCode::Char('k') => {
                let i = state.selected().unwrap_or(0);
                if i > 0 {
                    state.select(Some(i - 1));
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                let i = state.selected().unwrap_or(0);
                if i + 1 < items.len() {
                    state.select(Some(i + 1));
                }
            },
            | KeyCode::Enter => {
                if let Some(idx) = state.selected() {
                    if let Some(name) = items.get(idx) {
                        let name = name.clone();
                        self.popup = None;
                        self.switch_profile(&name).await;
                    }
                }
            },
            | _ => {},
        }
    }

    fn copy_logs_to_clipboard(&mut self) {
        if let Some(state) = &self.log_state {
            let visible = state.visible_lines();
            let (text, count) = if let Some((start, end)) = state.selection_range() {
                let selected: Vec<&str> = visible
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i >= start && *i <= end)
                    .map(|(_, l)| *l)
                    .collect();
                let n = selected.len();
                (selected.join("\n"), n)
            } else {
                let n = visible.len();
                (visible.join("\n"), n)
            };
            match arboard::Clipboard::new().and_then(|mut cb| cb.set_text(&text)) {
                | Ok(()) => {
                    self.push_status(format!("Copied {} log lines to clipboard", count));
                },
                | Err(e) => {
                    self.error = Some(format!("Clipboard error: {}", e));
                },
            }
        }
    }

    // -----------------------------------------------------------------------
    // Diff between resources
    // -----------------------------------------------------------------------

    async fn handle_diff_mark(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) => rt,
            | None => return,
        };
        let visible = self.visible_resource_indices();
        let vis_pos = self
            .resource_state
            .selected()
            .and_then(|sel| visible.iter().position(|&i| i == sel))
            .unwrap_or(0);
        let entry = match visible.get(vis_pos).and_then(|&i| self.resources.get(i)) {
            | Some(e) => e,
            | None => return,
        };
        let name = entry.name.clone();
        let namespace = entry.namespace.clone();

        if let Some((mark_name, _mark_ns, mark_yaml)) = self.diff_mark.take() {
            // Second resource selected - fetch its YAML and show diff
            match self.kube.get_resource(rt, &namespace, &name).await {
                | Ok(value) => {
                    let yaml = serde_yaml::to_string(&value).unwrap_or_default();
                    let diff_lines = compute_diff(&mark_yaml, &yaml);
                    self.edit_ctx = Some(EditContext {
                        resource_type: rt,
                        name: format!("{} vs {}", mark_name, name),
                        namespace: namespace.clone(),
                        original_yaml: mark_yaml,
                        edited_yaml: yaml,
                        diff_lines,
                        diff_mode: DiffMode::Inline,
                        scroll: 0,
                        error: None,
                    });
                    self.view = View::EditDiff;
                    self.push_status(format!("Diff: {} vs {}", mark_name, name));
                },
                | Err(e) => {
                    self.error = Some(format!("Failed to fetch resource: {}", e));
                },
            }
        } else {
            // First resource - mark it
            match self.kube.get_resource(rt, &namespace, &name).await {
                | Ok(value) => {
                    let yaml = serde_yaml::to_string(&value).unwrap_or_default();
                    self.diff_mark = Some((name.clone(), namespace, yaml));
                    self.push_status(format!(
                        "Marked '{}' for diff \u{2014} select another and press [d]",
                        name
                    ));
                    self.error = None;
                },
                | Err(e) => {
                    self.error = Some(format!("Failed to fetch resource: {}", e));
                },
            }
        }
    }

    // -----------------------------------------------------------------------
    // Create resource
    // -----------------------------------------------------------------------

    fn start_create_resource(&mut self) {
        let ns = self.kube.current_namespace().unwrap_or("default").to_string();
        let template = format!(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: new-resource\n  namespace: {}\ndata: {{}}\n",
            ns
        );
        self.pending_create = Some(PendingCreate { yaml: template });
    }

    pub async fn handle_create_result(&mut self, yaml: String) {
        if yaml.trim().is_empty() {
            self.push_status("Empty YAML, nothing created");
            return;
        }
        match self.kube.create_resource_yaml(&yaml).await {
            | Ok(_) => {
                self.push_status("Resource created");
                self.error = None;
                self.pending_load = Some(PendingLoad::Resources);
            },
            | Err(e) => {
                self.error = Some(format!("Create failed: {}", e));
            },
        }
    }

    // -----------------------------------------------------------------------
    // Command palette
    // -----------------------------------------------------------------------

    async fn toggle_palette(&mut self, global: bool) {
        if self.palette_open {
            self.palette_open = false;
            self.palette_all_resources.clear();
        } else {
            self.palette_open = true;
            self.palette_global = global;
            self.palette_buf.clear();
            self.palette_cursor = 0;

            if global {
                self.palette_all_resources.clear();
                for (_, types) in ResourceType::all_by_category() {
                    for rt in types {
                        if rt == ResourceType::Event {
                            continue;
                        }
                        if let Ok(entries) = self.kube.list_resources(rt).await {
                            if !entries.is_empty() {
                                self.palette_all_resources.push((rt, entries));
                            }
                        }
                    }
                }
            }

            self.update_palette_results();
        }
    }

    fn update_palette_results(&mut self) {
        self.palette_results.clear();
        let query = self.palette_buf.to_lowercase();
        let is_command = query.starts_with('>');

        if is_command {
            let cmd_query = query.trim_start_matches('>');
            let flags = self.cmd_flags();
            for cmd in super::command::palette_commands(&flags) {
                let haystack = format!("{} {} {}", cmd.label, cmd.description, cmd.key).to_lowercase();
                if cmd_query.is_empty() || haystack.contains(cmd_query) {
                    self.palette_results.push(PaletteEntry {
                        label: cmd.label.to_string(),
                        description: cmd.description.to_string(),
                        kind: PaletteEntryKind::PaletteCommand { key: cmd.key },
                    });
                }
            }
        } else if self.palette_global {
            // Fuzzy search over ALL resource types — searchable as "type/name"
            for (rt, entries) in &self.palette_all_resources {
                let singular = rt.singular_name();
                for entry in entries {
                    // Include "type/name" so users can search e.g. "deployment/myapp"
                    let haystack = format!(
                        "{} {}/{} {} {}",
                        singular,
                        singular,
                        entry.name,
                        entry.namespace,
                        entry.columns.join(" ")
                    )
                    .to_lowercase();
                    if query.is_empty() || query.split_whitespace().all(|word| haystack.contains(word)) {
                        let label = if entry.namespace.is_empty() {
                            format!("{}/{}", singular, entry.name)
                        } else {
                            format!("{}/{}  ({})", singular, entry.name, entry.namespace)
                        };
                        self.palette_results.push(PaletteEntry {
                            label,
                            description: String::new(),
                            kind: PaletteEntryKind::Resource {
                                name: entry.name.clone(),
                                namespace: entry.namespace.clone(),
                                resource_type: Some(*rt),
                            },
                        });
                    }
                    if self.palette_results.len() >= 100 {
                        break;
                    }
                }
            }
        } else {
            // Fuzzy search over current resource list
            let singular = self.selected_resource_type().map(|rt| rt.singular_name()).unwrap_or("");
            for entry in self.resources.iter() {
                let haystack = format!(
                    "{} {}/{} {} {}",
                    singular,
                    singular,
                    entry.name,
                    entry.namespace,
                    entry.columns.join(" ")
                )
                .to_lowercase();
                if query.is_empty() || query.split_whitespace().all(|word| haystack.contains(word)) {
                    let label = if entry.namespace.is_empty() {
                        format!("{}/{}", singular, entry.name)
                    } else {
                        format!("{}/{}  ({})", singular, entry.name, entry.namespace)
                    };
                    self.palette_results.push(PaletteEntry {
                        label,
                        description: String::new(),
                        kind: PaletteEntryKind::Resource {
                            name: entry.name.clone(),
                            namespace: entry.namespace.clone(),
                            resource_type: None,
                        },
                    });
                }
            }
        }
        // Clamp cursor
        if !self.palette_results.is_empty() {
            self.palette_cursor = self.palette_cursor.min(self.palette_results.len() - 1);
        } else {
            self.palette_cursor = 0;
        }
    }

    async fn handle_palette_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Esc => {
                self.palette_open = false;
            },
            | KeyCode::Enter => {
                if let Some(entry) = self.palette_results.get(self.palette_cursor) {
                    match &entry.kind {
                        | PaletteEntryKind::Resource {
                            name,
                            namespace,
                            resource_type,
                        } => {
                            let name = name.clone();
                            let namespace = namespace.clone();
                            let rt = *resource_type;
                            self.palette_open = false;
                            self.palette_all_resources.clear();
                            // If global result, switch to that resource type first
                            if let Some(rt) = rt {
                                self.panel = Panel::ResourceList(rt);
                                self.view = View::Main;
                                // Load the resource list for this type so detail can work
                                if let Ok(entries) = self.kube.list_resources(rt).await {
                                    if let Some(idx) =
                                        entries.iter().position(|e| e.name == name && e.namespace == namespace)
                                    {
                                        self.resource_state.select(Some(idx));
                                    }
                                    self.resource_counts.insert(rt, entries.len());
                                    self.resources = entries;
                                }
                                // Select the matching nav item
                                if let Some(nav_idx) = self
                                    .nav_items
                                    .iter()
                                    .position(|item| matches!(&item.kind, NavItemKind::Resource(r) if *r == rt))
                                {
                                    self.nav_state.select(Some(nav_idx));
                                }
                            }
                            self.pending_load = Some(PendingLoad::ResourceDetail { name, namespace });
                        },
                        | PaletteEntryKind::PaletteCommand { key } => {
                            let key = *key;
                            self.palette_open = false;
                            self.execute_palette_command(key).await;
                        },
                    }
                }
            },
            | KeyCode::Tab => {
                // Toggle between local and global search
                let was_global = self.palette_global;
                self.palette_global = !was_global;
                if self.palette_global && self.palette_all_resources.is_empty() {
                    for (_, types) in ResourceType::all_by_category() {
                        for rt in types {
                            if rt == ResourceType::Event {
                                continue;
                            }
                            if let Ok(entries) = self.kube.list_resources(rt).await {
                                if !entries.is_empty() {
                                    self.palette_all_resources.push((rt, entries));
                                }
                            }
                        }
                    }
                }
                self.palette_cursor = 0;
                self.update_palette_results();
            },
            | KeyCode::Up => {
                if self.palette_cursor > 0 {
                    self.palette_cursor -= 1;
                }
            },
            | KeyCode::Down => {
                if !self.palette_results.is_empty() && self.palette_cursor < self.palette_results.len() - 1 {
                    self.palette_cursor += 1;
                }
            },
            | KeyCode::PageUp => {
                self.palette_cursor = self.palette_cursor.saturating_sub(10);
            },
            | KeyCode::PageDown => {
                if !self.palette_results.is_empty() {
                    self.palette_cursor = (self.palette_cursor + 10).min(self.palette_results.len().saturating_sub(1));
                }
            },
            | KeyCode::Backspace => {
                self.palette_buf.pop();
                self.palette_cursor = 0;
                self.update_palette_results();
            },
            | KeyCode::Char(c) => {
                self.palette_buf.push(c);
                self.palette_cursor = 0;
                self.update_palette_results();
            },
            | _ => {},
        }
    }

    async fn execute_palette_command(&mut self, key: &str) {
        match key {
            | "R" => self.restart_selected_workload().await,
            | "S" => self.open_scale_input().await,
            | "D" => self.open_delete_confirm(),
            | "F" => self.open_port_forward_dialog().await,
            | "X" => self.open_exec_shell().await,
            | "C" => self.start_create_resource(),
            | "c" => self.open_context_selector(),
            | "n" => {
                self.pending_load = Some(PendingLoad::Namespaces);
            },
            | "O" => {
                let default = self
                    .kube
                    .kubeconfig_path()
                    .map(|s| s.to_string())
                    .or_else(|| std::env::var("KUBECONFIG").ok())
                    .unwrap_or_else(|| "~/.kube/config".to_string());
                self.popup = Some(Popup::KubeconfigInput { buf: default });
            },
            | _ => {},
        }
    }

    // -----------------------------------------------------------------------
    // Help palette
    // -----------------------------------------------------------------------

    pub fn filtered_help_entries(&self) -> Vec<&'static super::command::Cmd> {
        let flags = self.cmd_flags();
        let entries = if self.help_context_only {
            super::command::help_entries_for_context(self.current_context(), &flags)
        } else {
            super::command::help_entries(&flags)
        };
        let query = self.help_buf.to_lowercase();
        if query.is_empty() {
            return entries;
        }
        entries
            .into_iter()
            .filter(|cmd| {
                let haystack = format!(
                    "{} {} {} {}",
                    cmd.key,
                    cmd.label,
                    cmd.description,
                    cmd.contexts.iter().map(|c| c.label()).collect::<Vec<_>>().join(" ")
                )
                .to_lowercase();
                query.split_whitespace().all(|word| haystack.contains(word))
            })
            .collect()
    }

    fn handle_help_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Esc => {
                self.help_open = false;
            },
            | KeyCode::Up | KeyCode::Char('k') => {
                if self.help_cursor > 0 {
                    self.help_cursor -= 1;
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                let count = self.filtered_help_entries().len();
                if count > 0 && self.help_cursor < count.saturating_sub(1) {
                    self.help_cursor += 1;
                }
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.help_cursor = self.help_cursor.saturating_sub(10);
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                let count = self.filtered_help_entries().len();
                if count > 0 {
                    self.help_cursor = (self.help_cursor + 10).min(count.saturating_sub(1));
                }
            },
            | KeyCode::Tab => {
                self.help_context_only = !self.help_context_only;
                self.help_cursor = 0;
                self.help_scroll = 0;
            },
            | KeyCode::Backspace => {
                self.help_buf.pop();
                self.help_cursor = 0;
                self.help_scroll = 0;
            },
            | KeyCode::Char(c) => {
                self.help_buf.push(c);
                self.help_cursor = 0;
                self.help_scroll = 0;
            },
            | _ => {},
        }
    }

    // -----------------------------------------------------------------------
    // Quit confirmation
    // -----------------------------------------------------------------------

    fn open_confirm_quit(&mut self) {
        let pf_count = self
            .pf_manager
            .entries()
            .iter()
            .filter(|e| e.status.is_running())
            .count();
        self.popup = Some(Popup::ConfirmQuit { pf_count });
    }

    fn handle_confirm_quit_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Enter | KeyCode::Char('y') => {
                self.popup = None;
                self.should_quit = true;
            },
            | KeyCode::Esc | KeyCode::Char('n') => {
                self.popup = None;
            },
            | _ => {},
        }
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    fn open_delete_confirm(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) => rt,
            | None => return,
        };
        let visible = self.visible_resource_indices();
        let vis_pos = self
            .resource_state
            .selected()
            .and_then(|sel| visible.iter().position(|&i| i == sel))
            .unwrap_or(0);
        let entry = match visible.get(vis_pos).and_then(|&i| self.resources.get(i)) {
            | Some(e) => e,
            | None => return,
        };
        self.popup = Some(Popup::ConfirmDelete {
            name: entry.name.clone(),
            namespace: entry.namespace.clone(),
            resource_type: rt,
        });
    }

    fn open_delete_confirm_detail(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) => rt,
            | None => return,
        };
        if self.detail_name.is_empty() {
            return;
        }
        self.popup = Some(Popup::ConfirmDelete {
            name: self.detail_name.clone(),
            namespace: self.detail_namespace.clone(),
            resource_type: rt,
        });
    }

    async fn handle_confirm_delete_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Enter | KeyCode::Char('y') => {
                let (name, namespace, rt) = match &self.popup {
                    | Some(Popup::ConfirmDelete {
                        name,
                        namespace,
                        resource_type,
                    }) => (name.clone(), namespace.clone(), *resource_type),
                    | _ => return,
                };
                self.popup = None;
                match self.kube.delete_resource(rt, &namespace, &name).await {
                    | Ok(()) => {
                        self.push_status(format!("Deleted {}/{}", rt.display_name(), name));
                        self.error = None;
                        if self.view == View::Detail {
                            self.return_to_main();
                        }
                        if !matches!(self.panel, Panel::Favorites) {
                            self.pending_load = Some(PendingLoad::Resources);
                        }
                    },
                    | Err(e) => {
                        self.error = Some(format!("Delete failed: {}", e));
                    },
                }
            },
            | KeyCode::Esc | KeyCode::Char('n') => {
                self.popup = None;
            },
            | _ => {},
        }
    }

    async fn handle_confirm_drain_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Enter | KeyCode::Char('y') => {
                let node_name = match &self.popup {
                    | Some(Popup::ConfirmDrain { node_name }) => node_name.clone(),
                    | _ => return,
                };
                self.popup = None;
                self.drain_node_by_name(&node_name).await;
            },
            | KeyCode::Esc | KeyCode::Char('n') => {
                self.popup = None;
            },
            | _ => {},
        }
    }

    async fn drain_node_by_name(&mut self, name: &str) {
        match self.kube.drain_node(name).await {
            | Ok(evicted) => {
                self.push_status(format!("Drained node {} ({} pods evicted)", name, evicted));
                self.error = None;
                self.pending_load = Some(PendingLoad::Resources);
            },
            | Err(e) => {
                self.error = Some(format!("Drain failed: {}", e));
            },
        }
    }

    // -----------------------------------------------------------------------
    // Scale
    // -----------------------------------------------------------------------

    async fn open_scale_input(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) if rt.supports_scale() => rt,
            | _ => return,
        };
        let visible = self.visible_resource_indices();
        let vis_pos = self
            .resource_state
            .selected()
            .and_then(|sel| visible.iter().position(|&i| i == sel))
            .unwrap_or(0);
        let entry = match visible.get(vis_pos).and_then(|&i| self.resources.get(i)) {
            | Some(e) => e,
            | None => return,
        };
        let name = entry.name.clone();
        let namespace = entry.namespace.clone();
        // Try to read current replicas from the resource value
        let current = self
            .kube
            .get_resource(rt, &namespace, &name)
            .await
            .ok()
            .and_then(|v| v.get("spec").and_then(|s| s.get("replicas")).and_then(|r| r.as_u64()))
            .unwrap_or(1) as u32;
        self.popup = Some(Popup::ScaleInput {
            name,
            namespace,
            resource_type: rt,
            current,
            buf: current.to_string(),
        });
    }

    fn open_scale_input_detail(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) if rt.supports_scale() => rt,
            | _ => return,
        };
        if self.detail_name.is_empty() {
            return;
        }
        let current = self
            .detail_value
            .get("spec")
            .and_then(|s| s.get("replicas"))
            .and_then(|r| r.as_u64())
            .unwrap_or(1) as u32;
        self.popup = Some(Popup::ScaleInput {
            name: self.detail_name.clone(),
            namespace: self.detail_namespace.clone(),
            resource_type: rt,
            current,
            buf: current.to_string(),
        });
    }

    async fn handle_scale_input_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Enter => {
                let (name, namespace, rt, replicas) = match &self.popup {
                    | Some(Popup::ScaleInput {
                        name,
                        namespace,
                        resource_type,
                        buf,
                        ..
                    }) => {
                        match buf.parse::<u32>() {
                            | Ok(r) => (name.clone(), namespace.clone(), *resource_type, r),
                            | Err(_) => {
                                self.error = Some("Invalid replica count".into());
                                self.popup = None;
                                return;
                            },
                        }
                    },
                    | _ => return,
                };
                self.popup = None;
                match self.kube.scale_resource(rt, &namespace, &name, replicas).await {
                    | Ok(()) => {
                        self.push_status(format!(
                            "Scaled {}/{} to {} replicas",
                            rt.display_name(),
                            name,
                            replicas
                        ));
                        self.error = None;
                        self.pending_load = Some(PendingLoad::Resources);
                    },
                    | Err(e) => {
                        self.error = Some(format!("Scale failed: {}", e));
                    },
                }
            },
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Backspace => {
                if let Some(Popup::ScaleInput { buf, .. }) = &mut self.popup {
                    buf.pop();
                }
            },
            | KeyCode::Char(c) if c.is_ascii_digit() => {
                if let Some(Popup::ScaleInput { buf, .. }) = &mut self.popup {
                    buf.push(c);
                }
            },
            | _ => {},
        }
    }

    // -----------------------------------------------------------------------
    // Exec
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Kubeconfig
    // -----------------------------------------------------------------------

    async fn handle_kubeconfig_input_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Enter => {
                let path = match &self.popup {
                    | Some(Popup::KubeconfigInput { buf }) => buf.clone(),
                    | _ => return,
                };
                self.popup = None;
                // Expand ~ to home dir
                let expanded = if path.starts_with('~') {
                    if let Some(home) = std::env::var("HOME").ok() {
                        path.replacen('~', &home, 1)
                    } else {
                        path
                    }
                } else {
                    path
                };
                match KubeClient::new(Some(expanded), None, None).await {
                    | Ok(new_client) => {
                        self.pf_manager.cancel_all();
                        self.kube = new_client;
                        self.config.active_profile_mut().kubeconfig =
                            self.kube.kubeconfig_path().map(|s| s.to_string());
                        if let Err(e) = self.config.save() {
                            self.error = Some(format!("Failed to save config: {}", e));
                        }
                        self.panel = Panel::Overview;
                        self.resource_counts.clear();
                        self.cluster_stats_scroll = 0;
                        self.pending_load = Some(PendingLoad::ClusterStats);
                        self.error = None;
                        self.push_status("Kubeconfig loaded");
                        // Re-select overview
                        let first = self
                            .nav_items
                            .iter()
                            .position(|item| Self::is_selectable_nav(&item.kind))
                            .unwrap_or(0);
                        self.nav_state.select(Some(first));
                    },
                    | Err(e) => {
                        self.error = Some(format!("Failed to load kubeconfig: {}", e));
                    },
                }
            },
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Backspace => {
                if let Some(Popup::KubeconfigInput { buf }) = &mut self.popup {
                    buf.pop();
                }
            },
            | KeyCode::Char(c) => {
                if let Some(Popup::KubeconfigInput { buf }) = &mut self.popup {
                    buf.push(c);
                }
            },
            | _ => {},
        }
    }

    // -----------------------------------------------------------------------
    // Exec
    // -----------------------------------------------------------------------

    async fn open_exec_shell(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) if rt.supports_exec() => rt,
            | _ => return,
        };
        // Resolve pod and containers
        let (name, namespace) = if self.view == View::Detail {
            (self.detail_name.clone(), self.detail_namespace.clone())
        } else {
            let visible = self.visible_resource_indices();
            let vis_pos = self
                .resource_state
                .selected()
                .and_then(|sel| visible.iter().position(|&i| i == sel))
                .unwrap_or(0);
            match visible.get(vis_pos).and_then(|&i| self.resources.get(i)) {
                | Some(e) => (e.name.clone(), e.namespace.clone()),
                | None => return,
            }
        };
        match self.kube.find_pods(rt, &namespace, &name).await {
            | Ok(pods) if !pods.is_empty() => {
                let pod = &pods[0];
                let pod_name = pod.name.clone();
                let containers = pod.containers.clone();
                self.popup = Some(Popup::ExecShell {
                    pod_name,
                    namespace,
                    containers,
                    container_cursor: 0,
                    command_buf: "/bin/sh".to_string(),
                    terminal_buf: resolve_terminal_app().unwrap_or_default(),
                    editing_terminal: false,
                });
            },
            | Ok(_) => {
                self.error = Some("No pods found for this resource".into());
            },
            | Err(e) => {
                self.error = Some(format!("Failed to find pods: {}", e));
            },
        }
    }

    fn handle_exec_shell_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Enter => {
                let (pod_name, namespace, container, command, terminal) = match &self.popup {
                    | Some(Popup::ExecShell {
                        pod_name,
                        namespace,
                        containers,
                        container_cursor,
                        command_buf,
                        terminal_buf,
                        ..
                    }) => {
                        let container = containers.get(*container_cursor).cloned().unwrap_or_default();
                        let cmd = if command_buf.is_empty() {
                            "/bin/sh".to_string()
                        } else {
                            command_buf.clone()
                        };
                        (
                            pod_name.clone(),
                            namespace.clone(),
                            container,
                            cmd,
                            terminal_buf.clone(),
                        )
                    },
                    | _ => return,
                };
                self.popup = None;
                self.exec_terminal_override = Some(terminal);
                self.pending_exec = Some(PendingExec {
                    pod_name,
                    namespace,
                    container,
                    command: command.split_whitespace().map(String::from).collect(),
                });
            },
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Up => {
                if let Some(Popup::ExecShell {
                    container_cursor,
                    editing_terminal,
                    ..
                }) = &mut self.popup
                {
                    if !*editing_terminal && *container_cursor > 0 {
                        *container_cursor -= 1;
                    }
                }
            },
            | KeyCode::Down => {
                if let Some(Popup::ExecShell {
                    containers,
                    container_cursor,
                    editing_terminal,
                    ..
                }) = &mut self.popup
                {
                    if !*editing_terminal && *container_cursor + 1 < containers.len() {
                        *container_cursor += 1;
                    }
                }
            },
            | KeyCode::Tab => {
                // Tab toggles between command/terminal editing and container selection
                if let Some(Popup::ExecShell { editing_terminal, .. }) = &mut self.popup {
                    *editing_terminal = !*editing_terminal;
                }
            },
            | KeyCode::Backspace => {
                if let Some(Popup::ExecShell {
                    command_buf,
                    terminal_buf,
                    editing_terminal,
                    ..
                }) = &mut self.popup
                {
                    if *editing_terminal {
                        terminal_buf.pop();
                    } else {
                        command_buf.pop();
                    }
                }
            },
            | KeyCode::Char(c) => {
                if let Some(Popup::ExecShell {
                    command_buf,
                    terminal_buf,
                    editing_terminal,
                    ..
                }) = &mut self.popup
                {
                    if *editing_terminal {
                        terminal_buf.push(c);
                    } else {
                        command_buf.push(c);
                    }
                }
            },
            | _ => {},
        }
    }

    // -----------------------------------------------------------------------
    // Edit flow
    // -----------------------------------------------------------------------

    /// Start edit from the resource list view: fetch YAML for the selected
    /// resource.
    async fn start_edit_from_list(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) => rt,
            | None => return,
        };
        // Resolve the selected resource
        let visible = self.visible_resource_indices();
        let vis_pos = self
            .resource_state
            .selected()
            .and_then(|sel| visible.iter().position(|&i| i == sel))
            .unwrap_or(0);
        let entry = match visible.get(vis_pos).and_then(|&i| self.resources.get(i)) {
            | Some(e) => e,
            | None => return,
        };
        let name = entry.name.clone();
        let namespace = entry.namespace.clone();

        // Fetch the resource YAML
        match self.kube.get_resource(rt, &namespace, &name).await {
            | Ok(value) => {
                let yaml = serde_yaml::to_string(&value).unwrap_or_default();
                self.pending_edit = Some(PendingEdit {
                    resource_type: rt,
                    name,
                    namespace,
                    yaml,
                    original_yaml: None,
                });
            },
            | Err(e) => {
                self.error = Some(format!("Failed to fetch resource: {}", e));
            },
        }
    }

    /// Start edit from the detail view: use the already-loaded YAML.
    fn start_edit_from_detail(&mut self) {
        let rt = match self.selected_resource_type() {
            | Some(rt) => rt,
            | None => return,
        };
        self.pending_edit = Some(PendingEdit {
            resource_type: rt,
            name: self.detail_name.clone(),
            namespace: self.detail_namespace.clone(),
            yaml: self.detail_yaml.clone(),
            original_yaml: None,
        });
    }

    /// Called by the event loop after the editor exits. Computes the diff.
    pub fn handle_edit_result(&mut self, edit: PendingEdit, edited_yaml: String) {
        if edited_yaml.trim() == edit.yaml.trim() {
            // If this was a re-edit, restore the previous diff view
            if let Some(original_yaml) = edit.original_yaml {
                let diff_lines = compute_diff(&original_yaml, &edited_yaml);
                self.edit_ctx = Some(EditContext {
                    resource_type: edit.resource_type,
                    name: edit.name,
                    namespace: edit.namespace,
                    original_yaml,
                    edited_yaml,
                    diff_lines,
                    diff_mode: DiffMode::Inline,
                    scroll: 0,
                    error: None,
                });
                self.push_status("No changes from re-edit");
            } else {
                self.push_status("No changes");
            }
            return;
        }

        let original_yaml = edit.original_yaml.unwrap_or(edit.yaml);
        let diff_lines = compute_diff(&original_yaml, &edited_yaml);

        self.edit_ctx = Some(EditContext {
            resource_type: edit.resource_type,
            name: edit.name,
            namespace: edit.namespace,
            original_yaml,
            edited_yaml,
            diff_lines,
            diff_mode: DiffMode::Inline,
            scroll: 0,
            error: None,
        });
        self.view = View::EditDiff;
    }

    /// Key handler for the diff preview view.
    pub async fn handle_edit_diff_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Esc | KeyCode::Char('q') => {
                self.edit_ctx = None;
                // Return to previous view
                if self.detail_name.is_empty() {
                    self.return_to_main();
                } else {
                    self.view = View::Detail;
                }
            },
            | KeyCode::Char('v') => {
                if let Some(ctx) = &mut self.edit_ctx {
                    ctx.diff_mode = match ctx.diff_mode {
                        | DiffMode::Inline => DiffMode::SideBySide,
                        | DiffMode::SideBySide => DiffMode::Inline,
                    };
                    ctx.scroll = 0;
                }
            },
            | KeyCode::Enter => {
                self.apply_edit().await;
            },
            | KeyCode::Char('e') => {
                // Re-edit: reopen editor with the edited YAML
                if let Some(ctx) = self.edit_ctx.take() {
                    self.pending_edit = Some(PendingEdit {
                        resource_type: ctx.resource_type,
                        name: ctx.name,
                        namespace: ctx.namespace,
                        yaml: ctx.edited_yaml,
                        original_yaml: Some(ctx.original_yaml),
                    });
                }
            },
            | KeyCode::Up | KeyCode::Char('k') => {
                if let Some(ctx) = &mut self.edit_ctx {
                    ctx.scroll = ctx.scroll.saturating_sub(1);
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if let Some(ctx) = &mut self.edit_ctx {
                    ctx.scroll = ctx.scroll.saturating_add(1);
                }
            },
            | KeyCode::PageUp | KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(ctx) = &mut self.edit_ctx {
                    ctx.scroll = ctx.scroll.saturating_sub(20);
                }
            },
            | KeyCode::PageDown | KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(ctx) = &mut self.edit_ctx {
                    ctx.scroll = ctx.scroll.saturating_add(20);
                }
            },
            | _ => {},
        }
    }

    async fn apply_edit(&mut self) {
        let ctx = match &self.edit_ctx {
            | Some(c) => c,
            | None => return,
        };
        let rt = ctx.resource_type;
        let ns = ctx.namespace.clone();
        let name = ctx.name.clone();
        let yaml = ctx.edited_yaml.clone();

        match self.kube.replace_resource_yaml(rt, &ns, &name, &yaml).await {
            | Ok(value) => {
                self.push_status(format!("Applied changes to {}/{}", rt.display_name(), name));
                self.error = None;
                // Refresh the detail view with updated data
                self.detail_yaml = serde_yaml::to_string(&value).unwrap_or_default();
                self.detail_value = value;
                self.detail_name = name;
                self.detail_namespace = ns;
                self.edit_ctx = None;
                self.view = View::Detail;
            },
            | Err(e) => {
                // Stay on diff view, show error, allow re-edit
                if let Some(ctx) = &mut self.edit_ctx {
                    ctx.error = Some(format!("{}", e));
                }
            },
        }
    }

    // -----------------------------------------------------------------------
    // Time filter popup
    // -----------------------------------------------------------------------

    fn handle_time_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Enter => {
                let buf = match &self.popup {
                    | Some(Popup::TimeFilter { buf }) => buf.clone(),
                    | _ => return,
                };
                self.popup = None;
                let seconds = parse_duration_to_seconds(&buf);
                if seconds == 0 {
                    self.error = Some("Invalid duration. Use e.g. 30m, 2h, 1h30m".into());
                    return;
                }
                if let Some(state) = &mut self.log_state {
                    state.since_seconds = Some(seconds);
                }
                self.pending_load = Some(PendingLoad::ReloadLogs);
                self.push_status(format!("Log time filter: last {}", buf));
            },
            | KeyCode::Esc => {
                self.popup = None;
            },
            | KeyCode::Backspace => {
                if let Some(Popup::TimeFilter { buf, .. }) = &mut self.popup {
                    buf.pop();
                }
            },
            | KeyCode::Char(c) => {
                if let Some(Popup::TimeFilter { buf, .. }) = &mut self.popup {
                    buf.push(c);
                }
            },
            | _ => {},
        }
    }

    // -----------------------------------------------------------------------
    // Metadata edit popup (annotations / labels)
    // -----------------------------------------------------------------------

    fn open_metadata_edit(&mut self, kind: MetadataEditKind) {
        let rt = match self.selected_resource_type() {
            | Some(rt) => rt,
            | None => return,
        };
        let field = match kind {
            | MetadataEditKind::Labels => "labels",
            | MetadataEditKind::Annotations => "annotations",
        };
        // Serialize the labels/annotations map as YAML for editing in $EDITOR
        let map = self
            .detail_value
            .get("metadata")
            .and_then(|m| m.get(field))
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));
        let yaml = serde_yaml::to_string(&map).unwrap_or_else(|_| "{}\n".to_string());
        let header = format!(
            "# Edit {} for {}/{}\n# Save and close to apply. Empty keys are removed.\n#\n",
            field,
            rt.display_name(),
            self.detail_name
        );
        self.pending_metadata_edit = Some(PendingMetadataEdit {
            kind,
            resource_type: rt,
            name: self.detail_name.clone(),
            namespace: self.detail_namespace.clone(),
            yaml: format!("{}{}", header, yaml),
        });
    }

    pub async fn handle_metadata_edit_result(&mut self, edit: PendingMetadataEdit, edited_yaml: String) {
        // Strip comment lines
        let cleaned: String = edited_yaml
            .lines()
            .filter(|l| !l.starts_with('#'))
            .collect::<Vec<_>>()
            .join("\n");
        let new_map: serde_json::Map<String, Value> = match serde_yaml::from_str(&cleaned) {
            | Ok(m) => m,
            | Err(e) => {
                self.error = Some(format!("Invalid YAML: {}", e));
                return;
            },
        };

        // Build the original map for diffing
        let field = match edit.kind {
            | MetadataEditKind::Labels => "labels",
            | MetadataEditKind::Annotations => "annotations",
        };
        let original: serde_json::Map<String, Value> = self
            .detail_value
            .get("metadata")
            .and_then(|m| m.get(field))
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();

        // Compute patch: new keys, changed values, and removed keys (null)
        let mut patch = serde_json::Map::new();
        for (k, v) in &new_map {
            if original.get(k) != Some(v) {
                patch.insert(k.clone(), v.clone());
            }
        }
        for k in original.keys() {
            if !new_map.contains_key(k) {
                patch.insert(k.clone(), Value::Null);
            }
        }

        if patch.is_empty() {
            self.push_status("No changes");
            return;
        }

        let result = match edit.kind {
            | MetadataEditKind::Labels => {
                self.kube
                    .patch_metadata(edit.resource_type, &edit.namespace, &edit.name, Some(&patch), None)
                    .await
            },
            | MetadataEditKind::Annotations => {
                self.kube
                    .patch_metadata(edit.resource_type, &edit.namespace, &edit.name, None, Some(&patch))
                    .await
            },
        };
        match result {
            | Ok(_) => {
                self.push_status(format!("Updated {} on {}", field, edit.name));
                self.error = None;
                self.pending_load = Some(PendingLoad::ResourceDetail {
                    name: edit.name,
                    namespace: edit.namespace,
                });
            },
            | Err(e) => {
                self.error = Some(format!("Patch failed: {}", e));
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Diff computation
// ---------------------------------------------------------------------------

fn compute_diff(original: &str, edited: &str) -> Vec<(DiffKind, String)> {
    use similar::{
        ChangeTag,
        TextDiff,
    };
    let diff = TextDiff::from_lines(original, edited);
    let mut lines = Vec::new();
    for change in diff.iter_all_changes() {
        let kind = match change.tag() {
            | ChangeTag::Equal => DiffKind::Context,
            | ChangeTag::Insert => DiffKind::Added,
            | ChangeTag::Delete => DiffKind::Removed,
        };
        let prefix = match kind {
            | DiffKind::Context => " ",
            | DiffKind::Added => "+",
            | DiffKind::Removed => "-",
        };
        lines.push((kind, format!("{} {}", prefix, change.value().trim_end())));
    }
    lines
}

// ---------------------------------------------------------------------------
// Duration parsing (e.g. "30m", "2h", "1h30m", "1d")
// ---------------------------------------------------------------------------

fn parse_duration_to_seconds(s: &str) -> i64 {
    let mut total: i64 = 0;
    let mut num_buf = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            num_buf.push(c);
        } else {
            let n: i64 = num_buf.parse().unwrap_or(0);
            num_buf.clear();
            match c {
                | 'h' | 'H' => total += n * 3600,
                | 'm' | 'M' => total += n * 60,
                | 's' | 'S' => total += n,
                | 'd' | 'D' => total += n * 86400,
                | _ => {},
            }
        }
    }
    // If only digits remain, treat as minutes when no unit was parsed yet
    if !num_buf.is_empty() {
        let n: i64 = num_buf.parse().unwrap_or(0);
        if total == 0 {
            total = n * 60;
        } else {
            total += n;
        }
    }
    total
}
