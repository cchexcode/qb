use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::widgets::{ListState, TableState};
use regex::Regex;
use serde_json::Value;

use crate::k8s::{ClusterStatsData, KubeClient, ResourceEntry, ResourceType};
use super::logs::LogViewState;
use super::smart::SecretDetailState;

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
}

#[derive(Clone, Copy, PartialEq)]
pub enum DetailMode {
    Smart,
    Yaml,
}

#[allow(clippy::enum_variant_names)]
pub enum Popup {
    ContextSelect { items: Vec<String>, state: ListState },
    NamespaceSelect { items: Vec<String>, state: ListState },
    PodSelect { items: Vec<String>, state: ListState },
    ContainerSelect { items: Vec<String>, state: ListState },
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

    // Cluster stats (shown when "Overview" is selected)
    pub cluster_stats: Option<ClusterStatsData>,
    pub cluster_stats_scroll: u16,

    // Detail view
    pub detail_value: Value,
    pub detail_yaml: String,
    pub detail_scroll: u16,
    pub detail_mode: DetailMode,
    pub secret_state: Option<SecretDetailState>,
    /// Tracks which label/annotation keys are expanded (not truncated) in smart view.
    /// Keys are stored as "section:key", e.g. "Labels:app.kubernetes.io/name".
    pub expanded_keys: std::collections::HashSet<String>,

    // UI state
    pub focus: Focus,
    pub view: View,
    pub popup: Option<Popup>,
    pub should_quit: bool,
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
    pub last_refresh: std::time::Instant,

    // Click areas — updated each render by ui.rs
    pub area_nav: ratatui::layout::Rect,
    pub area_resources: ratatui::layout::Rect,
    pub area_popup: ratatui::layout::Rect,
}

impl App {
    pub fn new(kube: KubeClient, rt: tokio::runtime::Handle) -> Self {
        let nav_items = Self::build_nav_items();
        let mut nav_state = ListState::default();
        nav_state.select(Some(1));

        let mut app = Self {
            kube,
            rt,
            nav_items,
            nav_state,
            resources: Vec::new(),
            resource_state: TableState::default(),
            selected_resource_type: None,
            cluster_stats: None,
            cluster_stats_scroll: 0,
            detail_value: Value::Null,
            detail_yaml: String::new(),
            detail_scroll: 0,
            detail_mode: DetailMode::Smart,
            secret_state: None,
            expanded_keys: std::collections::HashSet::new(),
            log_state: None,
            resource_filter_text: String::new(),
            resource_filter_regex: None,
            resource_filter_editing: false,
            resource_filter_buf: String::new(),
            events_scroll: 0,
            events_cursor: 0,
            events_auto_scroll: true,
            focus: Focus::Nav,
            view: View::Main,
            popup: None,
            should_quit: false,
            status: String::new(),
            error: None,
            pending_load: Some(PendingLoad::ClusterStats),
            last_refresh: std::time::Instant::now(),
            area_nav: ratatui::layout::Rect::default(),
            area_resources: ratatui::layout::Rect::default(),
            area_popup: ratatui::layout::Rect::default(),
        };
        app.update_status();
        app
    }

    fn build_nav_items() -> Vec<NavItem> {
        let mut items = Vec::new();
        for (cat, types) in ResourceType::all_by_category() {
            items.push(NavItem {
                label: cat.display_name().to_string(),
                kind: NavItemKind::Category,
            });
            // Add "Overview" as the first item under Cluster
            if cat == crate::k8s::Category::Cluster {
                items.push(NavItem {
                    label: "  Overview".to_string(),
                    kind: NavItemKind::ClusterStats,
                });
            }
            for rt in types {
                items.push(NavItem {
                    label: format!("  {}", rt.display_name()),
                    kind: NavItemKind::Resource(rt),
                });
            }
        }
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
            re.is_match(&entry.name)
                || re.is_match(&entry.namespace)
                || entry.columns.iter().any(|c| re.is_match(c))
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

    /// Poll the log stream channel for new lines (called every event loop tick).
    pub fn poll_log_stream(&mut self) {
        if let Some(state) = &mut self.log_state {
            state.poll_stream();
        }
    }

    fn load_resources(&mut self) {
        if let Some(rt) = self.selected_resource_type {
            let prev_selected_name = self
                .resource_state
                .selected()
                .and_then(|idx| self.resources.get(idx))
                .map(|e| e.name.clone());

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
                    let new_idx = prev_selected_name
                        .and_then(|name| self.resources.iter().position(|e| e.name == name))
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
        if self.view == View::Main
            && self.popup.is_none()
            && self.pending_load.is_none()
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
                    self.detail_scroll = 0;
                    self.detail_mode = DetailMode::Smart;
                    self.expanded_keys.clear();
                    self.view = View::Detail;
                    self.error = None;
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
        use crossterm::event::{MouseButton, MouseEventKind};

        let pos = ratatui::layout::Position { x: event.column, y: event.row };

        match event.kind {
            | MouseEventKind::Down(MouseButton::Left) => self.handle_mouse_click(pos),
            | MouseEventKind::ScrollUp => self.handle_mouse_scroll(-3),
            | MouseEventKind::ScrollDown => self.handle_mouse_scroll(3),
            | _ => {},
        }
    }

    fn handle_mouse_click(&mut self, pos: ratatui::layout::Position) {
        // Popup takes priority — click inside selects + confirms, click outside dismisses
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
                let inner_y = pos.y.saturating_sub(self.area_nav.y + 1) as usize;
                if inner_y < self.nav_items.len() {
                    match &self.nav_items[inner_y].kind {
                        | NavItemKind::Resource(_) => {
                            self.nav_state.select(Some(inner_y));
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
                            self.selected_resource_type = None;
                            self.clear_resource_filter();
                            self.cluster_stats_scroll = 0;
                            self.pending_load = Some(PendingLoad::ClusterStats);
                        },
                        | NavItemKind::Category => {},
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
                let (items_len, state) = match popup {
                    | Popup::ContextSelect { items, state }
                    | Popup::NamespaceSelect { items, state }
                    | Popup::PodSelect { items, state }
                    | Popup::ContainerSelect { items, state } => (items.len(), state),
                };
                let current = state.selected().unwrap_or(0) as i32;
                let next = (current + delta).clamp(0, items_len.saturating_sub(1) as i32) as usize;
                state.select(Some(next));
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
                        self.cluster_stats_scroll =
                            self.cluster_stats_scroll.saturating_add(delta as u16);
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
                    let next =
                        (current + delta).clamp(0, self.resources.len().saturating_sub(1) as i32) as usize;
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
            | _ => match self.focus {
                | Focus::Nav => self.handle_nav_key(key),
                | Focus::Resources => self.handle_resource_key(key),
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

    /// Load whichever resource type or cluster stats is currently highlighted in the nav.
    fn load_nav_selection(&mut self) {
        if self.is_nav_cluster_stats() {
            if !self.is_showing_cluster_stats() {
                self.selected_resource_type = None;
                self.clear_resource_filter();
                self.cluster_stats_scroll = 0;
                self.pending_load = Some(PendingLoad::ClusterStats);
            }
        } else if let Some(rt) = self.selected_nav_resource_type() {
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

    fn is_selectable_nav(kind: &NavItemKind) -> bool {
        matches!(kind, NavItemKind::Resource(_) | NavItemKind::ClusterStats)
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
                self.view = View::Main;
                self.focus = Focus::Resources;
            },
            // [s] Smart view
            | KeyCode::Char('s') => {
                if self.detail_mode != DetailMode::Smart {
                    self.detail_mode = DetailMode::Smart;
                    self.detail_scroll = 0;
                }
            },
            // [y] YAML view — or copy in secret smart view
            | KeyCode::Char('y') => {
                if secret_smart {
                    self.copy_secret_to_clipboard();
                } else if self.detail_mode != DetailMode::Yaml {
                    self.detail_mode = DetailMode::Yaml;
                    self.detail_scroll = 0;
                }
            },
            // [d] Decode secret value
            | KeyCode::Char('d') => {
                if secret_smart {
                    if let Some(state) = &mut self.secret_state {
                        state.toggle_decode();
                    }
                }
            },
            // j/k: navigate secret keys in secret smart view, scroll otherwise
            | KeyCode::Up | KeyCode::Char('k') => {
                if secret_smart {
                    if let Some(state) = &mut self.secret_state {
                        state.nav_up();
                    }
                } else {
                    self.detail_scroll = self.detail_scroll.saturating_sub(1);
                }
            },
            | KeyCode::Down | KeyCode::Char('j') => {
                if secret_smart {
                    if let Some(state) = &mut self.secret_state {
                        state.nav_down();
                    }
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
            // [e] Expand/collapse all labels & annotations
            | KeyCode::Char('e') => {
                if self.expanded_keys.is_empty() {
                    // Expand all: collect all truncated keys from labels & annotations
                    self.expand_all_dict_keys();
                } else {
                    self.expanded_keys.clear();
                }
            },
            // [l] Open logs (for workload resources)
            | KeyCode::Char('l') => {
                self.open_logs_for_selected();
            },
            | _ => {},
        }
    }

    fn expand_all_dict_keys(&mut self) {
        for section_name in &["Labels", "Annotations"] {
            let path = if *section_name == "Labels" {
                "metadata.labels"
            } else {
                "metadata.annotations"
            };
            if let Some(map) = self
                .detail_value
                .get("metadata")
                .and_then(|m| m.get(path.rsplit('.').next().unwrap_or("")))
                .and_then(|v| v.as_object())
            {
                for k in map.keys() {
                    self.expanded_keys.insert(format!("{}:{}", section_name, k));
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
                        state.selected().and_then(|idx| items.get(idx).cloned()).map(PendingLoad::SwitchContext)
                    },
                    | Some(Popup::NamespaceSelect { items, state }) => state.selected().and_then(|idx| {
                        items.get(idx).map(|ns| {
                            if ns == ALL_NAMESPACES_LABEL {
                                self.kube.set_namespace(None);
                            } else {
                                self.kube.set_namespace(Some(ns.clone()));
                            }
                            PendingLoad::Resources
                        })
                    }),
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
}
