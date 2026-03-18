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

#[derive(Clone, Copy, PartialEq)]
pub enum DetailMode {
    Smart,
    Yaml,
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

    // Edit / Exec
    pub pending_edit: Option<PendingEdit>,
    pub pending_exec: Option<PendingExec>,
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
    /// Line offsets for each dict entry (for click mapping). Populated each
    /// render.
    pub dict_line_offsets: Vec<usize>,

    // UI state
    pub focus: Focus,
    pub view: View,
    pub popup: Option<Popup>,
    pub should_quit: bool,
    pub experimental: bool,
    pub status: String,
    pub error: Option<String>,
    pub pending_load: Option<PendingLoad>,

    // Log view
    pub log_state: Option<LogViewState>,

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

    // Port forwards
    pub pf_manager: PortForwardManager,
    pub showing_port_forwards: bool,
    pub pf_cursor: usize,
    pub pf_table_state: TableState,

    // Click areas — updated each render by ui.rs
    pub area_nav: ratatui::layout::Rect,
    pub area_resources: ratatui::layout::Rect,
    pub area_popup: ratatui::layout::Rect,
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
            pending_edit: None,
            pending_exec: None,
            exec_terminal_override: None,
            edit_ctx: None,
            expanded_keys: std::collections::HashSet::new(),
            dict_entries: Vec::new(),
            dict_cursor: None,
            dict_line_offsets: Vec::new(),
            log_state: None,
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
            error: None,
            pending_load: Some(PendingLoad::ClusterStats),
            last_refresh: std::time::Instant::now(),
            pf_manager: PortForwardManager::new(),
            showing_port_forwards: false,
            pf_cursor: 0,
            pf_table_state: TableState::default(),
            area_nav: ratatui::layout::Rect::default(),
            area_resources: ratatui::layout::Rect::default(),
            area_popup: ratatui::layout::Rect::default(),
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
            return (0..self.resources.len()).collect();
        }
        self.resources
            .iter()
            .enumerate()
            .filter(|(_, e)| self.resource_matches(e))
            .map(|(i, _)| i)
            .collect()
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
                    self.secret_state = if rt == ResourceType::Secret {
                        Some(SecretDetailState::from_value(&value))
                    } else {
                        None
                    };
                    self.detail_value = value;
                    self.detail_name = name.to_string();
                    self.detail_namespace = ns.to_string();
                    self.detail_scroll = 0;
                    self.detail_mode = DetailMode::Smart;
                    self.expanded_keys.clear();
                    self.dict_entries.clear();
                    self.dict_cursor = None;
                    self.dict_line_offsets.clear();
                    self.view = View::Detail;
                    self.error = None;
                    // Fetch related events for describe-style view
                    self.related_events = self
                        .rt
                        .block_on(self.kube.fetch_related_events(ns, name))
                        .unwrap_or_default();
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
                match self.rt.block_on(self.kube.fetch_logs_multi(ns, &pairs, 500)) {
                    | Ok(lines) => {
                        self.log_state = Some(LogViewState::new(pods, ns.to_string(), lines));
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

        match self.rt.block_on(self.kube.fetch_logs_multi(&ns, &pairs, 500)) {
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

    pub fn handle_mouse(&mut self, event: crossterm::event::MouseEvent) {
        use crossterm::event::{
            MouseButton,
            MouseEventKind,
        };

        let pos = ratatui::layout::Position {
            x: event.column,
            y: event.row,
        };

        match event.kind {
            | MouseEventKind::Down(MouseButton::Left) => self.handle_mouse_click(pos),
            | MouseEventKind::ScrollUp => self.handle_mouse_scroll(-3),
            | MouseEventKind::ScrollDown => self.handle_mouse_scroll(3),
            | _ => {},
        }
    }

    fn handle_mouse_click(&mut self, pos: ratatui::layout::Position) {
        // Popup takes priority — click inside selects + confirms, click outside
        // dismisses
        if self.popup.is_some() {
            if !self.area_popup.contains(pos) {
                self.popup = None;
                return;
            }
            // Row offset: skip top border (title is rendered in the border line)
            let inner_y = pos.y.saturating_sub(self.area_popup.y + 1) as usize;
            let action = match &self.popup {
                | Some(Popup::ContextSelect { items, .. }) => {
                    if inner_y < items.len() {
                        Some(PendingLoad::SwitchContext(items[inner_y].clone()))
                    } else {
                        None
                    }
                },
                | Some(Popup::NamespaceSelect { items, .. }) => {
                    if inner_y < items.len() {
                        let ns = &items[inner_y];
                        if ns == ALL_NAMESPACES_LABEL {
                            self.kube.set_namespace(None);
                        } else {
                            self.kube.set_namespace(Some(ns.clone()));
                        }
                        Some(PendingLoad::Resources)
                    } else {
                        None
                    }
                },
                | Some(Popup::PodSelect { items, .. }) => {
                    if inner_y < items.len() {
                        if let Some(log_state) = &mut self.log_state {
                            log_state.selected_pod = if inner_y == 0 { None } else { Some(inner_y - 1) };
                            log_state.selected_container = None;
                        }
                        Some(PendingLoad::ReloadLogs)
                    } else {
                        None
                    }
                },
                | Some(Popup::ContainerSelect { items, .. }) => {
                    if inner_y < items.len() {
                        if let Some(log_state) = &mut self.log_state {
                            log_state.selected_container = if inner_y == 0 { None } else { Some(inner_y - 1) };
                        }
                        Some(PendingLoad::ReloadLogs)
                    } else {
                        None
                    }
                },
                | Some(Popup::PortForwardCreate(_)) => {
                    // Handled by its own key handler; click inside dismissed
                    None
                },
                | Some(Popup::ConfirmDelete { .. })
                | Some(Popup::ScaleInput { .. })
                | Some(Popup::ExecShell { .. })
                | Some(Popup::KubeconfigInput { .. }) => None,
                | None => None,
            };
            self.popup = None;
            if let Some(load) = action {
                self.pending_load = Some(load);
            }
            return;
        }

        // Main view
        if self.view == View::Main {
            // Click on nav sidebar — select resource type and load it
            if self.area_nav.contains(pos) {
                self.focus = Focus::Nav;
                // Account for list scroll offset so clicks target the correct item
                let visual_y = pos.y.saturating_sub(self.area_nav.y + 1) as usize;
                let inner_y = visual_y + self.nav_state.offset();
                if inner_y < self.nav_items.len() {
                    match &self.nav_items[inner_y].kind {
                        | NavItemKind::Resource(_) => {
                            self.nav_state.select(Some(inner_y));
                            self.showing_port_forwards = false;
                            if let Some(rt) = self.selected_nav_resource_type() {
                                if self.selected_resource_type != Some(rt) {
                                    self.selected_resource_type = Some(rt);
                                    self.events_scroll = 0;
                                    self.events_cursor = 0;
                                    self.events_auto_scroll = true;
                                    self.clear_resource_filter();
                                    self.pending_load = Some(PendingLoad::Resources);
                                }
                            }
                        },
                        | NavItemKind::ClusterStats => {
                            self.nav_state.select(Some(inner_y));
                            self.showing_port_forwards = false;
                            self.selected_resource_type = None;
                            self.clear_resource_filter();
                            self.cluster_stats_scroll = 0;
                            self.pending_load = Some(PendingLoad::ClusterStats);
                        },
                        | NavItemKind::PortForwards => {
                            self.nav_state.select(Some(inner_y));
                            self.selected_resource_type = None;
                            self.showing_port_forwards = true;
                            self.clear_resource_filter();
                        },
                        | NavItemKind::SuperCategory | NavItemKind::Category => {},
                    }
                }
                return;
            }

            // Click on resources table/events log — select row or scroll
            if self.area_resources.contains(pos) {
                self.focus = Focus::Resources;
                if self.selected_resource_type == Some(ResourceType::Event) {
                    // Events log: click selects the clicked event in filtered view
                    let inner_y = pos.y.saturating_sub(self.area_resources.y + 1) as usize;
                    let clicked = self.events_scroll.saturating_add(inner_y);
                    let vis_len = self.visible_resource_indices().len();
                    self.events_cursor = clicked.min(vis_len.saturating_sub(1));
                    self.events_auto_scroll = self.events_cursor == vis_len.saturating_sub(1);
                } else {
                    // Skip border (1) + header row (1) = 2
                    let inner_y = pos.y.saturating_sub(self.area_resources.y + 2) as usize;
                    if inner_y < self.resources.len() {
                        self.resource_state.select(Some(inner_y));
                    }
                }
                return;
            }
        }

        // Detail view — click on secret keys in smart mode
        if self.view == View::Detail && self.is_secret_smart_view() {
            if let Some(state) = &mut self.secret_state {
                // Content line = click row minus top border, plus scroll offset
                let content_line = pos.y.saturating_sub(1) as usize + self.detail_scroll as usize;
                // Walk key_line_offsets (populated during render) to find which key was clicked
                for (i, &offset) in state.key_line_offsets.iter().enumerate().rev() {
                    if content_line >= offset {
                        state.selected = i;
                        break;
                    }
                }
            }
        }
    }

    fn handle_mouse_scroll(&mut self, delta: i32) {
        if self.popup.is_some() {
            // Scroll popup list
            if let Some(popup) = &mut self.popup {
                match popup {
                    | Popup::ContextSelect { items, state }
                    | Popup::NamespaceSelect { items, state }
                    | Popup::PodSelect { items, state }
                    | Popup::ContainerSelect { items, state } => {
                        let current = state.selected().unwrap_or(0) as i32;
                        let next = (current + delta).clamp(0, items.len().saturating_sub(1) as i32) as usize;
                        state.select(Some(next));
                    },
                    | Popup::PortForwardCreate(d) => {
                        let current = d.port_cursor as i32;
                        let next = (current + delta).clamp(0, d.ports.len().saturating_sub(1) as i32) as usize;
                        d.port_cursor = next;
                        d.local_port_buf = d.ports[next].container_port.to_string();
                    },
                    | Popup::ConfirmDelete { .. }
                    | Popup::ScaleInput { .. }
                    | Popup::ExecShell { .. }
                    | Popup::KubeconfigInput { .. } => {},
                }
            }
            return;
        }

        if self.view == View::Detail {
            if self.is_secret_smart_view() {
                // Scroll navigates secret keys
                if let Some(state) = &mut self.secret_state {
                    let current = state.selected as i32;
                    let next = (current + delta).clamp(0, state.keys.len().saturating_sub(1) as i32) as usize;
                    state.selected = next;
                }
            } else {
                // Scroll the detail view
                if delta < 0 {
                    self.detail_scroll = self.detail_scroll.saturating_sub(delta.unsigned_abs() as u16);
                } else {
                    self.detail_scroll = self.detail_scroll.saturating_add(delta as u16);
                }
            }
            return;
        }

        // Main view — scroll the focused list/table
        match self.focus {
            | Focus::Nav => {
                // Scroll nav by moving selection
                if delta < 0 {
                    for _ in 0..delta.unsigned_abs() {
                        self.nav_up();
                    }
                } else {
                    for _ in 0..delta {
                        self.nav_down();
                    }
                }
            },
            | Focus::Resources => {
                if self.is_showing_cluster_stats() {
                    if delta < 0 {
                        self.cluster_stats_scroll =
                            self.cluster_stats_scroll.saturating_sub(delta.unsigned_abs() as u16);
                    } else {
                        self.cluster_stats_scroll = self.cluster_stats_scroll.saturating_add(delta as u16);
                    }
                } else if self.selected_resource_type == Some(ResourceType::Event) {
                    let max = self.visible_resource_indices().len().saturating_sub(1);
                    if delta < 0 {
                        self.events_cursor = self.events_cursor.saturating_sub(delta.unsigned_abs() as usize);
                        self.events_auto_scroll = false;
                    } else {
                        self.events_cursor = (self.events_cursor + delta as usize).min(max);
                        self.events_auto_scroll = self.events_cursor == max;
                    }
                } else {
                    let current = self.resource_state.selected().unwrap_or(0) as i32;
                    let next = (current + delta).clamp(0, self.resources.len().saturating_sub(1) as i32) as usize;
                    if !self.resources.is_empty() {
                        self.resource_state.select(Some(next));
                    }
                }
            },
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) {
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.should_quit = true;
            return;
        }

        // [p] Toggle pause — global, works in any view (except popups/filter
        // editing/port forwards/logs)
        if key.code == KeyCode::Char('p') && self.popup.is_none() && !self.resource_filter_editing {
            // Don't consume 'p' in: log view (pod selector), edit diff, port forwards view
            // (pause/resume)
            if self.view != View::Logs && self.view != View::EditDiff && !self.showing_port_forwards {
                self.paused = !self.paused;
                self.status = if self.paused {
                    "Paused — auto-refresh disabled".into()
                } else {
                    "Resumed — auto-refresh enabled".into()
                };
                return;
            }
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
                | KeyCode::PageUp => {
                    self.cluster_stats_scroll = self.cluster_stats_scroll.saturating_sub(20);
                },
                | KeyCode::PageDown => {
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
            | KeyCode::Char('F') => {
                self.open_port_forward_dialog();
            },
            | KeyCode::Char('D') => {
                self.open_delete_confirm();
            },
            | KeyCode::Char('S') => {
                self.open_scale_input();
            },
            | KeyCode::Char('x') if self.experimental && self.resource_filter_text.is_empty() => {
                self.open_exec_shell(false);
            },
            | KeyCode::Char('X') if self.experimental => {
                self.open_exec_shell(true);
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
            | KeyCode::PageUp => {
                self.events_cursor = self.events_cursor.saturating_sub(30);
                self.events_auto_scroll = false;
            },
            | KeyCode::PageDown => {
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
            | KeyCode::Up | KeyCode::Char('k') => {
                if secret_smart {
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
                if secret_smart {
                    if let Some(state) = &mut self.secret_state {
                        state.nav_down();
                    }
                } else if self.dict_cursor.is_some() {
                    self.dict_nav_down();
                } else {
                    self.detail_scroll = self.detail_scroll.saturating_add(1);
                }
            },
            | KeyCode::PageUp => {
                self.detail_scroll = self.detail_scroll.saturating_sub(20);
            },
            | KeyCode::PageDown => {
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
            | KeyCode::Char('S') => {
                self.open_scale_input_detail();
            },
            | KeyCode::Char('x') if self.experimental => {
                self.open_exec_shell(false);
            },
            | KeyCode::Char('X') if self.experimental => {
                self.open_exec_shell(true);
            },
            | _ => {},
        }
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
                self.status = format!("Copied YAML ({} bytes)", self.detail_yaml.len());
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
                        self.status = format!("Copied '{}' ({} bytes)", key, value.len());
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
                if let Some(state) = &mut self.log_state {
                    state.stop_following();
                }
                self.log_state = None;
                self.view = View::Main;
                self.focus = Focus::Resources;
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
            // [x] Clear filter
            | KeyCode::Char('x') => {
                if let Some(state) = &mut self.log_state {
                    state.clear_filter();
                }
            },
            // Navigation
            | KeyCode::Up | KeyCode::Char('k') => {
                if let Some(state) = &mut self.log_state {
                    state.scroll_up(1);
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if let Some(state) = &mut self.log_state {
                    state.scroll_down(1, visible_count);
                }
            },
            | KeyCode::PageUp => {
                if let Some(state) = &mut self.log_state {
                    state.scroll_up(30);
                }
            },
            | KeyCode::PageDown => {
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
                    self.status = format!("Copied '{}' to clipboard ({} bytes)", key_name, decoded.len());
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
                        | Popup::KubeconfigInput { .. } => unreachable!(),
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
                        | Popup::KubeconfigInput { .. } => unreachable!(),
                    };
                    let current = state.selected().unwrap_or(0);
                    if current + 1 < items_len {
                        state.select(Some(current + 1));
                    }
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
                    | Some(Popup::KubeconfigInput { .. }) => unreachable!(),
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
        self.status = format!("Port forward created: :{} -> :{}", local_port, remote_port);
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
                self.status = format!("Opened shell in {}: {}/{}", terminal_app, exec.pod_name, exec.container);
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
                        self.status = format!("Deleted {}/{}", rt.display_name(), name);
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
                        self.status = format!("Scaled {}/{} to {} replicas", rt.display_name(), name, replicas);
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
                        self.status = "Kubeconfig loaded".into();
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

    fn open_exec_shell(&mut self, custom_command: bool) {
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
                if custom_command {
                    // Show popup to choose container and enter command
                    self.popup = Some(Popup::ExecShell {
                        pod_name,
                        namespace,
                        containers,
                        container_cursor: 0,
                        command_buf: "/bin/sh".to_string(),
                        terminal_buf: resolve_terminal_app().unwrap_or_default(),
                        editing_terminal: false,
                    });
                } else {
                    // Direct exec with /bin/sh into first container
                    let container = containers.first().cloned().unwrap_or_default();
                    self.pending_exec = Some(PendingExec {
                        pod_name,
                        namespace,
                        container,
                        command: vec!["/bin/sh".to_string()],
                    });
                }
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
            self.status = "No changes".into();
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
            | KeyCode::PageUp => {
                if let Some(ctx) = &mut self.edit_ctx {
                    ctx.scroll = ctx.scroll.saturating_sub(20);
                }
            },
            | KeyCode::PageDown => {
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
                self.status = format!("Applied changes to {}/{}", rt.display_name(), name);
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
            | DiffKind::Context => "  ",
            | DiffKind::Added => "+ ",
            | DiffKind::Removed => "- ",
        };
        lines.push((kind, format!("{}{}", prefix, change.value().trim_end())));
    }
    lines
}
