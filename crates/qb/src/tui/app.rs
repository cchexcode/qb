use {
    super::{
        logs::LogViewState,
        smart::SecretDetailState,
    },
    crate::{
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
    pub kind: PaletteEntryKind,
}

pub enum PaletteEntryKind {
    Resource {
        name: String,
        namespace: String,
        resource_type: Option<ResourceType>,
    },
    Command(PaletteCommand),
}

#[derive(Clone)]
pub enum PaletteCommand {
    Restart,
    Scale,
    Delete,
    PortForward,
    Exec,
    Create,
    SwitchContext,
    SwitchNamespace,
    OpenKubeconfig,
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

#[derive(Clone, Copy, PartialEq)]
pub enum MetadataEditMode {
    Browse,
    AddKey,
    AddValue,
    EditValue,
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
    MetadataEdit {
        kind: MetadataEditKind,
        resource_type: ResourceType,
        name: String,
        namespace: String,
        entries: Vec<(String, String)>,
        cursor: usize,
        mode: MetadataEditMode,
        key_buf: String,
        value_buf: String,
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
    SuperCategory,
    Category,
    Resource(ResourceType),
    ClusterStats,
    PortForwards,
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
    pub rt: tokio::runtime::Handle,

    // Navigation sidebar
    pub nav_items: Vec<NavItem>,
    pub nav_state: ListState,

    // Resource table
    pub resources: Vec<ResourceEntry>,
    pub resource_state: TableState,
    pub resource_table_state: TableState,
    pub selected_resource_type: Option<ResourceType>,
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

    // Port forwards
    pub pf_manager: PortForwardManager,
    pub showing_port_forwards: bool,
    pub pf_cursor: usize,
    pub pf_table_state: TableState,
}

impl App {
    pub fn new(kube: KubeClient, rt: tokio::runtime::Handle, experimental: bool) -> Self {
        let nav_items = Self::build_nav_items();
        let mut nav_state = ListState::default();
        // Select first selectable item (skip SuperCategory + Category headers)
        let first_selectable = nav_items
            .iter()
            .position(|item| Self::is_selectable_nav(&item.kind))
            .unwrap_or(0);
        nav_state.select(Some(first_selectable));

        let mut app = Self {
            kube,
            rt,
            nav_items,
            nav_state,
            resources: Vec::new(),
            resource_state: TableState::default(),
            resource_table_state: TableState::default(),
            selected_resource_type: None,
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
            pf_manager: PortForwardManager::new(),
            showing_port_forwards: false,
            pf_cursor: 0,
            pf_table_state: TableState::default(),
        };
        app.update_status();
        app
    }

    fn build_nav_items() -> Vec<NavItem> {
        let mut items = Vec::new();

        // ── CLUSTER super-category ──
        items.push(NavItem {
            label: "CLUSTER".to_string(),
            kind: NavItemKind::SuperCategory,
        });

        for (cat, types) in ResourceType::all_by_category() {
            items.push(NavItem {
                label: format!("  {}", cat.display_name()),
                kind: NavItemKind::Category,
            });
            // Add "Overview" as the first item under Cluster
            if cat == crate::k8s::Category::Cluster {
                items.push(NavItem {
                    label: "    Overview".to_string(),
                    kind: NavItemKind::ClusterStats,
                });
            }
            for rt in types {
                items.push(NavItem {
                    label: format!("    {}", rt.display_name()),
                    kind: NavItemKind::Resource(rt),
                });
            }
        }

        // ── GLOBAL super-category ──
        items.push(NavItem {
            label: "GLOBAL".to_string(),
            kind: NavItemKind::SuperCategory,
        });
        items.push(NavItem {
            label: "  Port Forwards".to_string(),
            kind: NavItemKind::PortForwards,
        });

        items
    }

    fn update_status(&mut self) {
        let ctx = self.kube.current_context();
        let ns = self.kube.namespace_display();
        let rt_name = self.selected_resource_type.map(|r| r.display_name()).unwrap_or("None");
        let count = self.resources.len();
        self.status = format!("ctx: {} | ns: {} | {}: {}", ctx, ns, rt_name, count);
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

    /// Returns the last N status messages that are less than 30 seconds old.
    pub fn recent_status(&self, n: usize) -> Vec<&str> {
        let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(30);
        self.status_history
            .iter()
            .rev()
            .filter(|(t, _)| *t > cutoff)
            .take(n)
            .map(|(_, s)| s.as_str())
            .collect()
    }

    pub fn selected_nav_resource_type(&self) -> Option<ResourceType> {
        let idx = self.nav_state.selected()?;
        match &self.nav_items.get(idx)?.kind {
            | NavItemKind::Resource(rt) => Some(*rt),
            | _ => None,
        }
    }

    fn is_nav_cluster_stats(&self) -> bool {
        self.nav_state
            .selected()
            .and_then(|idx| self.nav_items.get(idx))
            .map(|item| matches!(item.kind, NavItemKind::ClusterStats))
            .unwrap_or(false)
    }

    pub fn is_showing_cluster_stats(&self) -> bool {
        self.selected_resource_type.is_none() && self.cluster_stats.is_some()
    }

    fn is_secret_smart_view(&self) -> bool {
        self.detail_mode == DetailMode::Smart
            && self.selected_resource_type == Some(ResourceType::Secret)
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

    pub fn process_pending_load(&mut self) {
        if let Some(load) = self.pending_load.take() {
            match load {
                | PendingLoad::Resources => self.load_resources(),
                | PendingLoad::Namespaces => self.load_namespaces(),
                | PendingLoad::SwitchContext(ctx) => self.do_switch_context(&ctx),
                | PendingLoad::ResourceDetail { name, namespace } => self.load_resource_detail(&namespace, &name),
                | PendingLoad::Logs { name, namespace } => self.load_logs(&namespace, &name),
                | PendingLoad::ReloadLogs => self.reload_logs(),
                | PendingLoad::ClusterStats => self.load_cluster_stats(),
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

    fn load_resources(&mut self) {
        if let Some(rt) = self.selected_resource_type {
            let prev_selected = self
                .resource_state
                .selected()
                .and_then(|idx| self.resources.get(idx))
                .map(|e| (e.name.clone(), e.namespace.clone()));

            match self.rt.block_on(self.kube.list_resources(rt)) {
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
            && !self.showing_port_forwards
            && self.last_refresh.elapsed() >= std::time::Duration::from_secs(2)
        {
            if self.is_showing_cluster_stats() {
                self.pending_load = Some(PendingLoad::ClusterStats);
            } else {
                self.pending_load = Some(PendingLoad::Resources);
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

    fn load_namespaces(&mut self) {
        match self.rt.block_on(self.kube.list_namespaces()) {
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

    fn do_switch_context(&mut self, ctx: &str) {
        match self.rt.block_on(self.kube.switch_context(ctx)) {
            | Ok(()) => {
                self.showing_port_forwards = false;
                self.pending_load = Some(PendingLoad::Resources);
                self.error = None;
            },
            | Err(e) => {
                self.error = Some(format!("Failed to switch context: {}", e));
            },
        }
    }

    fn load_resource_detail(&mut self, ns: &str, name: &str) {
        if let Some(rt) = self.selected_resource_type {
            match self.rt.block_on(self.kube.get_resource(rt, ns, name)) {
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
                    self.related_events = self
                        .rt
                        .block_on(self.kube.fetch_related_events(ns, name))
                        .unwrap_or_default();
                    // Always re-fetch related resources on navigation to a different resource;
                    // only skip on watch-mode refresh of the same resource.
                    if !is_same_resource || !self.detail_auto_refresh {
                        self.related_resources =
                            self.rt
                                .block_on(self.kube.fetch_related_resources(rt, ns, name, &self.detail_value));
                        self.related_cursor = None;
                    }
                },
                | Err(e) => {
                    self.error = Some(format!("Failed to load resource: {}", e));
                },
            }
        }
    }

    fn load_logs(&mut self, ns: &str, name: &str) {
        let rt = match self.selected_resource_type {
            | Some(rt) if rt.supports_logs() => rt,
            | _ => return,
        };

        match self.rt.block_on(self.kube.find_pods(rt, ns, name)) {
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
                match self
                    .rt
                    .block_on(self.kube.fetch_logs_multi(ns, &pairs, 500, default_since))
                {
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

    fn reload_logs(&mut self) {
        let log_state = match &mut self.log_state {
            | Some(s) => s,
            | None => return,
        };

        log_state.stop_following();

        let pairs = log_state.active_streams();
        let ns = log_state.namespace.clone();
        let since = log_state.since_seconds;

        match self.rt.block_on(self.kube.fetch_logs_multi(&ns, &pairs, 500, since)) {
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

    fn load_cluster_stats(&mut self) {
        match self.rt.block_on(self.kube.fetch_cluster_stats()) {
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

    pub fn handle_key(&mut self, key: KeyEvent) {
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.should_quit = true;
            return;
        }

        if key.code == KeyCode::Char('p') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.toggle_palette(false);
            return;
        }

        if self.help_open {
            self.handle_help_key(key);
            return;
        }

        if self.palette_open {
            self.handle_palette_key(key);
            return;
        }

        // [p] Toggle pause — global, works in any view (except popups/filter
        // editing/port forwards/logs)
        if key.code == KeyCode::Char('p') && self.popup.is_none() && !self.resource_filter_editing {
            // Don't consume 'p' in: log view (pod selector), edit diff, port forwards view
            // (pause/resume)
            if self.view != View::Logs && self.view != View::EditDiff && !self.showing_port_forwards {
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
            return;
        }

        if self.popup.is_some() {
            self.handle_popup_key(key);
            return;
        }

        if self.view == View::Detail {
            self.handle_detail_key(key);
            return;
        }

        if self.view == View::Logs {
            self.handle_log_key(key);
            return;
        }

        if self.view == View::EditDiff {
            self.handle_edit_diff_key(key);
            return;
        }

        // Resource filter editing captures all input in main view
        if self.resource_filter_editing {
            self.handle_resource_filter_key(key);
            return;
        }

        match key.code {
            | KeyCode::Char('q') => {
                self.should_quit = true;
            },
            | KeyCode::Char('r') => {
                self.focus = Focus::Resources;
            },
            | KeyCode::Char('c') => {
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
                    | Focus::Resources => self.handle_resource_key(key),
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
        if self.is_nav_port_forwards() {
            self.selected_resource_type = None;
            self.showing_port_forwards = true;
            self.clear_resource_filter();
            self.view = View::Main;
        } else if self.is_nav_cluster_stats() {
            self.showing_port_forwards = false;
            if !self.is_showing_cluster_stats() {
                self.selected_resource_type = None;
                self.clear_resource_filter();
                self.cluster_stats_scroll = 0;
                self.pending_load = Some(PendingLoad::ClusterStats);
            }
        } else if let Some(rt) = self.selected_nav_resource_type() {
            self.showing_port_forwards = false;
            if self.selected_resource_type != Some(rt) {
                self.selected_resource_type = Some(rt);
                self.resource_table_state = TableState::default();
                self.events_scroll = 0;
                self.events_cursor = 0;
                self.events_auto_scroll = true;
                self.clear_resource_filter();
                self.pending_load = Some(PendingLoad::Resources);
            }
        }
    }

    fn is_nav_port_forwards(&self) -> bool {
        self.nav_state
            .selected()
            .and_then(|idx| self.nav_items.get(idx))
            .map(|item| matches!(item.kind, NavItemKind::PortForwards))
            .unwrap_or(false)
    }

    pub fn is_showing_port_forwards(&self) -> bool {
        self.showing_port_forwards
    }

    fn is_selectable_nav(kind: &NavItemKind) -> bool {
        matches!(
            kind,
            NavItemKind::Resource(_) | NavItemKind::ClusterStats | NavItemKind::PortForwards
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

    fn handle_resource_key(&mut self, key: KeyEvent) {
        if self.showing_port_forwards {
            self.handle_port_forwards_key(key);
            return;
        }
        if self.is_showing_cluster_stats() {
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
        }
        if self.selected_resource_type == Some(ResourceType::Event) {
            self.handle_events_key(key);
            return;
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
                self.start_edit_from_list();
            },
            | KeyCode::Char('y') => {
                self.copy_resource_name();
            },
            | KeyCode::Char('F') => {
                self.open_port_forward_dialog();
            },
            | KeyCode::Char('D') => {
                self.open_delete_confirm();
            },
            | KeyCode::Char('R') => {
                self.restart_selected_workload();
            },
            | KeyCode::Char('d') => {
                self.handle_diff_mark();
            },
            | KeyCode::Char('C') => {
                self.start_create_resource();
            },
            | KeyCode::Char('S') => {
                self.open_scale_input();
            },
            | KeyCode::Char('x') if self.experimental && self.resource_filter_text.is_empty() => {
                self.open_exec_shell();
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

    fn handle_detail_key(&mut self, key: KeyEvent) {
        let secret_smart = self.is_secret_smart_view();

        match key.code {
            | KeyCode::Esc | KeyCode::Char('q') => {
                self.dict_cursor = None;
                self.view = View::Main;
                self.focus = Focus::Resources;
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
            // [s] Enter/leave label/annotation selection mode
            | KeyCode::Char('s') => {
                if secret_smart {
                    // no-op for secrets
                } else if self.dict_cursor.is_some() {
                    self.dict_cursor = None;
                } else if !self.dict_entries.is_empty() {
                    self.dict_cursor = Some(0);
                    self.scroll_to_dict_cursor();
                }
            },
            // [e] Edit resource
            | KeyCode::Char('e') => {
                self.start_edit_from_detail();
            },
            // [l] Open logs (for workload resources)
            | KeyCode::Char('l') => {
                self.open_logs_for_selected();
            },
            // [F] Port forward
            | KeyCode::Char('F') => {
                self.open_port_forward_dialog();
            },
            | KeyCode::Char('D') => {
                self.open_delete_confirm_detail();
            },
            | KeyCode::Char('R') => {
                self.restart_selected_workload();
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
            | KeyCode::Char('x') if self.experimental => {
                self.open_exec_shell();
            },
            // [Tab] Toggle related resource selection
            | KeyCode::Tab => {
                if self.related_resources.is_empty() {
                    // no-op
                } else if self.related_cursor.is_some() {
                    self.related_cursor = None;
                } else {
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
                        self.navigate_to_related(rel.resource_type, rel.name.clone(), rel.namespace.clone());
                    }
                }
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

    fn navigate_to_related(&mut self, rt: ResourceType, name: String, namespace: String) {
        // Switch to the related resource's type and load its detail
        self.selected_resource_type = Some(rt);
        self.showing_port_forwards = false;
        self.related_cursor = None;
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
        if let Ok(entries) = self.rt.block_on(self.kube.list_resources(rt)) {
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
                self.dict_cursor = None;
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
                self.dict_cursor = None;
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
                    }
                } else {
                    if let Some(state) = &mut self.log_state {
                        state.stop_following();
                    }
                    self.log_state = None;
                    self.log_detail_line = None;
                    self.view = View::Main;
                    self.focus = Focus::Resources;
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
                        state.start_following(self.kube.client().clone(), &self.rt);
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
            | KeyCode::Up | KeyCode::Char('k') => {
                if let Some(state) = &mut self.log_state {
                    match state.selected_line {
                        | Some(sel) if sel > 0 => {
                            state.selected_line = Some(sel - 1);
                            // Scroll to keep selection visible
                            if sel - 1 < state.scroll {
                                state.scroll = sel - 1;
                            }
                            state.auto_scroll = false;
                        },
                        | None => {
                            // Start selection at current scroll position
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
            | KeyCode::Down | KeyCode::Char('j') => {
                if let Some(state) = &mut self.log_state {
                    let vis_count = state.visible_lines().len();
                    match state.selected_line {
                        | Some(sel) if sel + 1 < vis_count => {
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
                    state.scroll_to_top();
                }
            },
            | KeyCode::End | KeyCode::Char('G') => {
                if let Some(state) = &mut self.log_state {
                    state.scroll_to_bottom(visible_count);
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

    fn handle_popup_key(&mut self, key: KeyEvent) {
        // Port forward create popup has its own handler
        if matches!(self.popup, Some(Popup::PortForwardCreate(_))) {
            self.handle_pf_create_popup_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::ConfirmDelete { .. })) {
            self.handle_confirm_delete_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::ScaleInput { .. })) {
            self.handle_scale_input_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::ExecShell { .. })) {
            self.handle_exec_shell_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::KubeconfigInput { .. })) {
            self.handle_kubeconfig_input_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::TimeFilter { .. })) {
            self.handle_time_filter_key(key);
            return;
        }
        if matches!(self.popup, Some(Popup::MetadataEdit { .. })) {
            self.handle_metadata_edit_key(key);
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
                        | Popup::ScaleInput { .. }
                        | Popup::ExecShell { .. }
                        | Popup::KubeconfigInput { .. }
                        | Popup::TimeFilter { .. }
                        | Popup::MetadataEdit { .. } => unreachable!(),
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
                        | Popup::ScaleInput { .. }
                        | Popup::ExecShell { .. }
                        | Popup::KubeconfigInput { .. }
                        | Popup::TimeFilter { .. }
                        | Popup::MetadataEdit { .. } => unreachable!(),
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
                        state.selected().and_then(|idx| {
                            items.get(idx).map(|ns| {
                                if ns == ALL_NAMESPACES_LABEL {
                                    self.kube.set_namespace(None);
                                } else {
                                    self.kube.set_namespace(Some(ns.clone()));
                                }
                                PendingLoad::Resources
                            })
                        })
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
                    | Some(Popup::ScaleInput { .. })
                    | Some(Popup::ExecShell { .. })
                    | Some(Popup::KubeconfigInput { .. })
                    | Some(Popup::TimeFilter { .. })
                    | Some(Popup::MetadataEdit { .. }) => unreachable!(),
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
        if let Some(rt) = self.selected_resource_type {
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
            // [p] Pause/resume selected forward
            | KeyCode::Char('p') => {
                if let Some(entry) = self.pf_manager.entries().get(self.pf_cursor) {
                    let id = entry.id;
                    if matches!(entry.status, portforward::PortForwardStatus::Paused) {
                        self.pf_manager.resume(id);
                    } else if entry.status.is_running() {
                        self.pf_manager.pause(id);
                    }
                }
            },
            // [d] Cancel (delete) selected forward
            | KeyCode::Char('d') => {
                if let Some(entry) = self.pf_manager.entries().get(self.pf_cursor) {
                    let id = entry.id;
                    self.pf_manager.cancel(id);
                    self.pf_manager.remove_cancelled();
                    let new_count = self.pf_manager.entries().len();
                    if new_count == 0 {
                        self.pf_cursor = 0;
                    } else {
                        self.pf_cursor = self.pf_cursor.min(new_count.saturating_sub(1));
                    }
                }
            },
            | _ => {},
        }
    }

    fn open_port_forward_dialog(&mut self) {
        let rt = match self.selected_resource_type {
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
        let value = match self.rt.block_on(self.kube.get_resource(rt, &namespace, &name)) {
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
            &self.rt,
            namespace,
            pod_name,
            context,
            resource_label.clone(),
            local_port,
            remote_port,
            target,
        );

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

    fn restart_selected_workload(&mut self) {
        let rt = match self.selected_resource_type {
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
        match self.rt.block_on(self.kube.restart_workload(rt, &namespace, &name)) {
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

    fn copy_logs_to_clipboard(&mut self) {
        if let Some(state) = &self.log_state {
            let visible = state.visible_lines();
            let text = visible.join("\n");
            let count = visible.len();
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

    fn handle_diff_mark(&mut self) {
        let rt = match self.selected_resource_type {
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
            match self.rt.block_on(self.kube.get_resource(rt, &namespace, &name)) {
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
            match self.rt.block_on(self.kube.get_resource(rt, &namespace, &name)) {
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

    pub fn handle_create_result(&mut self, yaml: String) {
        if yaml.trim().is_empty() {
            self.push_status("Empty YAML, nothing created");
            return;
        }
        match self.rt.block_on(self.kube.create_resource_yaml(&yaml)) {
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

    fn toggle_palette(&mut self, global: bool) {
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
                        if let Ok(entries) = self.rt.block_on(self.kube.list_resources(rt)) {
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
            let commands: Vec<(PaletteCommand, &str)> = vec![
                (PaletteCommand::Restart, "Restart Workload"),
                (PaletteCommand::Scale, "Scale Workload"),
                (PaletteCommand::Delete, "Delete Resource"),
                (PaletteCommand::PortForward, "Port Forward"),
                (PaletteCommand::Create, "Create Resource"),
                (PaletteCommand::SwitchContext, "Switch Context"),
                (PaletteCommand::SwitchNamespace, "Switch Namespace"),
                (PaletteCommand::OpenKubeconfig, "Open Kubeconfig"),
            ];
            for (cmd, label) in &commands {
                if cmd_query.is_empty() || label.to_lowercase().contains(cmd_query) {
                    self.palette_results.push(PaletteEntry {
                        label: label.to_string(),
                        kind: PaletteEntryKind::Command(cmd.clone()),
                    });
                }
            }
            if self.experimental {
                let label = "Exec Shell";
                if cmd_query.is_empty() || label.to_lowercase().contains(cmd_query) {
                    self.palette_results.push(PaletteEntry {
                        label: label.to_string(),
                        kind: PaletteEntryKind::Command(PaletteCommand::Exec),
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
            let singular = self.selected_resource_type.map(|rt| rt.singular_name()).unwrap_or("");
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

    fn handle_palette_key(&mut self, key: KeyEvent) {
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
                                self.selected_resource_type = Some(rt);
                                self.showing_port_forwards = false;
                                self.view = View::Main;
                                // Load the resource list for this type so detail can work
                                if let Ok(entries) = self.rt.block_on(self.kube.list_resources(rt)) {
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
                        | PaletteEntryKind::Command(cmd) => {
                            let cmd = cmd.clone();
                            self.palette_open = false;
                            self.execute_palette_command(cmd);
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
                            if let Ok(entries) = self.rt.block_on(self.kube.list_resources(rt)) {
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

    fn execute_palette_command(&mut self, cmd: PaletteCommand) {
        match cmd {
            | PaletteCommand::Restart => self.restart_selected_workload(),
            | PaletteCommand::Scale => self.open_scale_input(),
            | PaletteCommand::Delete => self.open_delete_confirm(),
            | PaletteCommand::PortForward => self.open_port_forward_dialog(),
            | PaletteCommand::Exec => self.open_exec_shell(),
            | PaletteCommand::Create => self.start_create_resource(),
            | PaletteCommand::SwitchContext => self.open_context_selector(),
            | PaletteCommand::SwitchNamespace => {
                self.pending_load = Some(PendingLoad::Namespaces);
            },
            | PaletteCommand::OpenKubeconfig => {
                let default = self
                    .kube
                    .kubeconfig_path()
                    .map(|s| s.to_string())
                    .or_else(|| std::env::var("KUBECONFIG").ok())
                    .unwrap_or_else(|| "~/.kube/config".to_string());
                self.popup = Some(Popup::KubeconfigInput { buf: default });
            },
        }
    }

    // -----------------------------------------------------------------------
    // Help palette
    // -----------------------------------------------------------------------

    pub fn help_entries(&self) -> Vec<(&'static str, &'static str, &'static str)> {
        // (key, description, context)
        let mut entries: Vec<(&str, &str, &str)> = vec![
            // Global
            ("Ctrl+C", "Force quit", "Global"),
            ("q", "Quit / back", "Global"),
            ("Esc", "Back / dismiss popup or selection", "Global"),
            ("?", "Help (this screen)", "Global"),
            ("Ctrl+P", "Command palette", "Global"),
            ("p", "Pause/resume auto-refresh", "Global"),
            // Main view - navigation
            ("j / Down", "Move down in sidebar or table", "Main"),
            ("k / Up", "Move up in sidebar or table", "Main"),
            ("Ctrl+d / PgDn", "Page down in table", "Main"),
            ("Ctrl+u / PgUp", "Page up in table", "Main"),
            ("g / Home", "Jump to top of list", "Main"),
            ("G / End", "Jump to bottom of list", "Main"),
            ("Enter", "Open detail / focus right panel", "Main"),
            ("Tab", "Toggle sidebar / table focus", "Main"),
            ("r", "Focus resource table", "Main"),
            // Main view - actions
            ("c", "Switch cluster context", "Main"),
            ("n", "Switch namespace", "Main"),
            ("O", "Open kubeconfig", "Main"),
            ("/", "Filter resources by regex", "Main"),
            ("x", "Clear filter", "Main"),
            ("e", "Edit resource ($EDITOR)", "Main"),
            ("l", "Open logs (workloads)", "Main"),
            ("F", "Port forward", "Main"),
            ("D", "Delete resource", "Main"),
            ("R", "Restart workload", "Main"),
            ("S", "Scale workload", "Main"),
            ("y", "Copy resource name", "Main"),
            ("d", "Mark / diff resources", "Main"),
            ("C", "Create new resource", "Main"),
            // Detail view - navigation
            ("j / Down", "Scroll down or navigate entries", "Detail"),
            ("k / Up", "Scroll up or navigate entries", "Detail"),
            ("Ctrl+d / PgDn", "Page down", "Detail"),
            ("Ctrl+u / PgUp", "Page up", "Detail"),
            ("Home", "Jump to top", "Detail"),
            // Detail view - actions
            ("v", "Cycle Smart / YAML view", "Detail"),
            ("s", "Enter/leave label selection", "Detail"),
            ("Enter", "Edit selected label/annotation", "Detail"),
            ("Space", "Expand/collapse or decode secret", "Detail"),
            ("y", "Copy value or YAML", "Detail"),
            ("e", "Edit resource ($EDITOR)", "Detail"),
            ("l", "Open logs (workloads)", "Detail"),
            ("F", "Port forward", "Detail"),
            ("D", "Delete resource", "Detail"),
            ("R", "Restart workload", "Detail"),
            ("S", "Scale workload", "Detail"),
            ("w", "Toggle watch mode (auto-refresh)", "Detail"),
            ("Tab", "Toggle related resources selection", "Detail"),
            // Log view - navigation
            ("j / Down", "Move cursor down", "Logs"),
            ("k / Up", "Move cursor up", "Logs"),
            ("Ctrl+d / PgDn", "Page down", "Logs"),
            ("Ctrl+u / PgUp", "Page up", "Logs"),
            ("g / Home", "Jump to top", "Logs"),
            ("G / End", "Jump to bottom", "Logs"),
            // Log view - actions
            ("f", "Toggle follow (live streaming)", "Logs"),
            ("/", "Filter by regex", "Logs"),
            ("x", "Clear filter", "Logs"),
            ("p", "Select pod", "Logs"),
            ("c", "Select container", "Logs"),
            ("w", "Toggle line wrapping", "Logs"),
            ("t", "Set time filter", "Logs"),
            ("Y", "Copy all logs to clipboard", "Logs"),
            ("Enter", "Open selected line detail", "Logs"),
            // Edit diff
            ("v", "Cycle inline / side-by-side diff", "Edit Diff"),
            ("j / Down", "Scroll down", "Edit Diff"),
            ("k / Up", "Scroll up", "Edit Diff"),
            ("Ctrl+d / PgDn", "Page down", "Edit Diff"),
            ("Ctrl+u / PgUp", "Page up", "Edit Diff"),
            ("Enter", "Apply changes to cluster", "Edit Diff"),
            ("e", "Re-edit in $EDITOR", "Edit Diff"),
            // Port forwards
            ("j / Down", "Navigate list", "Port Forwards"),
            ("k / Up", "Navigate list", "Port Forwards"),
            ("p", "Pause/resume selected forward", "Port Forwards"),
            ("d", "Cancel (delete) selected forward", "Port Forwards"),
        ];

        if self.experimental {
            entries.push(("x", "Exec into pod", "Main"));
            entries.push(("x", "Exec into pod", "Detail"));
        }

        entries
    }

    pub fn filtered_help_entries(&self) -> Vec<(&'static str, &'static str, &'static str)> {
        let query = self.help_buf.to_lowercase();
        self.help_entries()
            .into_iter()
            .filter(|(key, desc, ctx)| {
                if query.is_empty() {
                    return true;
                }
                let haystack = format!("{} {} {}", key, desc, ctx).to_lowercase();
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
    // Delete
    // -----------------------------------------------------------------------

    fn open_delete_confirm(&mut self) {
        let rt = match self.selected_resource_type {
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
        let rt = match self.selected_resource_type {
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

    fn handle_confirm_delete_key(&mut self, key: KeyEvent) {
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
                match self.rt.block_on(self.kube.delete_resource(rt, &namespace, &name)) {
                    | Ok(()) => {
                        self.push_status(format!("Deleted {}/{}", rt.display_name(), name));
                        self.error = None;
                        if self.view == View::Detail {
                            self.view = View::Main;
                            self.focus = Focus::Resources;
                        }
                        self.pending_load = Some(PendingLoad::Resources);
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

    // -----------------------------------------------------------------------
    // Scale
    // -----------------------------------------------------------------------

    fn open_scale_input(&mut self) {
        let rt = match self.selected_resource_type {
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
            .rt
            .block_on(self.kube.get_resource(rt, &namespace, &name))
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
        let rt = match self.selected_resource_type {
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

    fn handle_scale_input_key(&mut self, key: KeyEvent) {
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
                match self
                    .rt
                    .block_on(self.kube.scale_resource(rt, &namespace, &name, replicas))
                {
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

    fn handle_kubeconfig_input_key(&mut self, key: KeyEvent) {
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
                match self.rt.block_on(KubeClient::new(Some(expanded), None, None)) {
                    | Ok(new_client) => {
                        self.pf_manager.cancel_all();
                        self.kube = new_client;
                        self.selected_resource_type = None;
                        self.showing_port_forwards = false;
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

    fn open_exec_shell(&mut self) {
        let rt = match self.selected_resource_type {
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
        match self.rt.block_on(self.kube.find_pods(rt, &namespace, &name)) {
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
    fn start_edit_from_list(&mut self) {
        let rt = match self.selected_resource_type {
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
        match self.rt.block_on(self.kube.get_resource(rt, &namespace, &name)) {
            | Ok(value) => {
                let yaml = serde_yaml::to_string(&value).unwrap_or_default();
                self.pending_edit = Some(PendingEdit {
                    resource_type: rt,
                    name,
                    namespace,
                    yaml,
                });
            },
            | Err(e) => {
                self.error = Some(format!("Failed to fetch resource: {}", e));
            },
        }
    }

    /// Start edit from the detail view: use the already-loaded YAML.
    fn start_edit_from_detail(&mut self) {
        let rt = match self.selected_resource_type {
            | Some(rt) => rt,
            | None => return,
        };
        self.pending_edit = Some(PendingEdit {
            resource_type: rt,
            name: self.detail_name.clone(),
            namespace: self.detail_namespace.clone(),
            yaml: self.detail_yaml.clone(),
        });
    }

    /// Called by the event loop after the editor exits. Computes the diff.
    pub fn handle_edit_result(&mut self, edit: PendingEdit, edited_yaml: String) {
        if edited_yaml.trim() == edit.yaml.trim() {
            self.push_status("No changes");
            return;
        }

        let diff_lines = compute_diff(&edit.yaml, &edited_yaml);

        self.edit_ctx = Some(EditContext {
            resource_type: edit.resource_type,
            name: edit.name,
            namespace: edit.namespace,
            original_yaml: edit.yaml,
            edited_yaml,
            diff_lines,
            diff_mode: DiffMode::Inline,
            scroll: 0,
            error: None,
        });
        self.view = View::EditDiff;
    }

    /// Key handler for the diff preview view.
    pub fn handle_edit_diff_key(&mut self, key: KeyEvent) {
        match key.code {
            | KeyCode::Esc | KeyCode::Char('q') => {
                self.edit_ctx = None;
                // Return to previous view
                if self.detail_name.is_empty() {
                    self.view = View::Main;
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
                self.apply_edit();
            },
            | KeyCode::Char('e') => {
                // Re-edit: reopen editor with the edited YAML
                if let Some(ctx) = self.edit_ctx.take() {
                    self.pending_edit = Some(PendingEdit {
                        resource_type: ctx.resource_type,
                        name: ctx.name,
                        namespace: ctx.namespace,
                        yaml: ctx.edited_yaml,
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

    fn apply_edit(&mut self) {
        let ctx = match &self.edit_ctx {
            | Some(c) => c,
            | None => return,
        };
        let rt = ctx.resource_type;
        let ns = ctx.namespace.clone();
        let name = ctx.name.clone();
        let yaml = ctx.edited_yaml.clone();

        match self.rt.block_on(self.kube.replace_resource_yaml(rt, &ns, &name, &yaml)) {
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
        let rt = match self.selected_resource_type {
            | Some(rt) => rt,
            | None => return,
        };
        let field = match kind {
            | MetadataEditKind::Labels => "labels",
            | MetadataEditKind::Annotations => "annotations",
        };
        let entries: Vec<(String, String)> = self
            .detail_value
            .get("metadata")
            .and_then(|m| m.get(field))
            .and_then(|v| v.as_object())
            .map(|map| {
                let mut pairs: Vec<(String, String)> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                pairs
            })
            .unwrap_or_default();
        self.popup = Some(Popup::MetadataEdit {
            kind,
            resource_type: rt,
            name: self.detail_name.clone(),
            namespace: self.detail_namespace.clone(),
            entries,
            cursor: 0,
            mode: MetadataEditMode::Browse,
            key_buf: String::new(),
            value_buf: String::new(),
        });
    }

    fn handle_metadata_edit_key(&mut self, key: KeyEvent) {
        let mode = match &self.popup {
            | Some(Popup::MetadataEdit { mode, .. }) => *mode,
            | _ => return,
        };
        match mode {
            | MetadataEditMode::Browse => {
                match key.code {
                    | KeyCode::Esc => {
                        self.popup = None;
                    },
                    | KeyCode::Up | KeyCode::Char('k') => {
                        if let Some(Popup::MetadataEdit { cursor, .. }) = &mut self.popup {
                            *cursor = cursor.saturating_sub(1);
                        }
                    },
                    | KeyCode::Down | KeyCode::Char('j') => {
                        if let Some(Popup::MetadataEdit { entries, cursor, .. }) = &mut self.popup {
                            if *cursor + 1 < entries.len() {
                                *cursor += 1;
                            }
                        }
                    },
                    | KeyCode::Char('a') => {
                        if let Some(Popup::MetadataEdit {
                            mode,
                            key_buf,
                            value_buf,
                            ..
                        }) = &mut self.popup
                        {
                            *mode = MetadataEditMode::AddKey;
                            key_buf.clear();
                            value_buf.clear();
                        }
                    },
                    | KeyCode::Char('d') => {
                        self.delete_metadata_entry();
                    },
                    | KeyCode::Enter => {
                        if let Some(Popup::MetadataEdit {
                            entries,
                            cursor,
                            mode,
                            value_buf,
                            ..
                        }) = &mut self.popup
                        {
                            if let Some((_, val)) = entries.get(*cursor) {
                                *value_buf = val.clone();
                                *mode = MetadataEditMode::EditValue;
                            }
                        }
                    },
                    | _ => {},
                }
            },
            | MetadataEditMode::AddKey => {
                match key.code {
                    | KeyCode::Esc => {
                        if let Some(Popup::MetadataEdit { mode, .. }) = &mut self.popup {
                            *mode = MetadataEditMode::Browse;
                        }
                    },
                    | KeyCode::Enter => {
                        if let Some(Popup::MetadataEdit { mode, key_buf, .. }) = &mut self.popup {
                            if !key_buf.is_empty() {
                                *mode = MetadataEditMode::AddValue;
                            }
                        }
                    },
                    | KeyCode::Backspace => {
                        if let Some(Popup::MetadataEdit { key_buf, .. }) = &mut self.popup {
                            key_buf.pop();
                        }
                    },
                    | KeyCode::Char(c) => {
                        if let Some(Popup::MetadataEdit { key_buf, .. }) = &mut self.popup {
                            key_buf.push(c);
                        }
                    },
                    | _ => {},
                }
            },
            | MetadataEditMode::AddValue | MetadataEditMode::EditValue => {
                match key.code {
                    | KeyCode::Esc => {
                        if let Some(Popup::MetadataEdit { mode, .. }) = &mut self.popup {
                            *mode = MetadataEditMode::Browse;
                        }
                    },
                    | KeyCode::Enter => {
                        self.apply_metadata_edit();
                    },
                    | KeyCode::Backspace => {
                        if let Some(Popup::MetadataEdit { value_buf, .. }) = &mut self.popup {
                            value_buf.pop();
                        }
                    },
                    | KeyCode::Char(c) => {
                        if let Some(Popup::MetadataEdit { value_buf, .. }) = &mut self.popup {
                            value_buf.push(c);
                        }
                    },
                    | _ => {},
                }
            },
        }
    }

    fn apply_metadata_edit(&mut self) {
        let (kind, rt, name, namespace, key, value) = match &self.popup {
            | Some(Popup::MetadataEdit {
                kind,
                resource_type,
                name,
                namespace,
                mode,
                key_buf,
                value_buf,
                entries,
                cursor,
            }) => {
                let key = match mode {
                    | MetadataEditMode::AddValue => key_buf.clone(),
                    | MetadataEditMode::EditValue => entries.get(*cursor).map(|(k, _)| k.clone()).unwrap_or_default(),
                    | _ => return,
                };
                (
                    *kind,
                    *resource_type,
                    name.clone(),
                    namespace.clone(),
                    key,
                    value_buf.clone(),
                )
            },
            | _ => return,
        };
        let mut map = serde_json::Map::new();
        map.insert(key.clone(), Value::String(value));
        let result = match kind {
            | MetadataEditKind::Labels => {
                self.rt
                    .block_on(self.kube.patch_metadata(rt, &namespace, &name, Some(&map), None))
            },
            | MetadataEditKind::Annotations => {
                self.rt
                    .block_on(self.kube.patch_metadata(rt, &namespace, &name, None, Some(&map)))
            },
        };
        match result {
            | Ok(_) => {
                let kind_label = if kind == MetadataEditKind::Labels {
                    "label"
                } else {
                    "annotation"
                };
                self.push_status(format!("Updated {} '{}' on {}", kind_label, key, name));
                self.error = None;
                self.pending_load = Some(PendingLoad::ResourceDetail {
                    name: name.clone(),
                    namespace: namespace.clone(),
                });
                self.popup = None;
            },
            | Err(e) => {
                self.error = Some(format!("Patch failed: {}", e));
            },
        }
    }

    fn delete_metadata_entry(&mut self) {
        let (kind, rt, name, namespace, key) = match &self.popup {
            | Some(Popup::MetadataEdit {
                kind,
                resource_type,
                name,
                namespace,
                entries,
                cursor,
                ..
            }) => {
                let key = entries.get(*cursor).map(|(k, _)| k.clone()).unwrap_or_default();
                (*kind, *resource_type, name.clone(), namespace.clone(), key)
            },
            | _ => return,
        };
        if key.is_empty() {
            return;
        }
        let mut map = serde_json::Map::new();
        map.insert(key.clone(), Value::Null); // null = remove in merge patch
        let result = match kind {
            | MetadataEditKind::Labels => {
                self.rt
                    .block_on(self.kube.patch_metadata(rt, &namespace, &name, Some(&map), None))
            },
            | MetadataEditKind::Annotations => {
                self.rt
                    .block_on(self.kube.patch_metadata(rt, &namespace, &name, None, Some(&map)))
            },
        };
        match result {
            | Ok(_) => {
                let kind_label = if kind == MetadataEditKind::Labels {
                    "label"
                } else {
                    "annotation"
                };
                self.push_status(format!("Removed {} '{}' from {}", kind_label, key, name));
                self.popup = None;
                self.pending_load = Some(PendingLoad::ResourceDetail { name, namespace });
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
