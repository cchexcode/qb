use {
    crate::k8s::{
        PodInfo,
        ResourceType,
    },
    k8s_openapi::api::core::v1::Pod,
    kube::{
        api::Api,
        Client,
    },
    ratatui::style::Color,
    regex::Regex,
    std::{
        collections::HashMap,
        sync::mpsc,
        time::Instant,
    },
};

// ---------------------------------------------------------------------------
// Structured log line
// ---------------------------------------------------------------------------

pub struct LogLine {
    pub pod: String,
    pub container: String,
    pub timestamp: String,
    pub message: String,
}

impl LogLine {
    /// Parse a raw prefixed log line: `[pod/container] 2024-01-15T10:30:00Z
    /// message`
    pub fn parse(raw: &str) -> Self {
        // Extract [pod/container] prefix
        if let Some(bracket_end) = raw.find("] ") {
            let inner = &raw[1..bracket_end]; // "pod/container"
            let rest = &raw[bracket_end + 2..]; // "timestamp message"
            let (pod, container) = inner.split_once('/').unwrap_or((inner, ""));
            // Split timestamp from message
            let (timestamp, message) = if let Some(space) = rest.find(' ') {
                let ts = &rest[..space];
                // Only treat as timestamp if it looks like one (starts with digit)
                if ts.starts_with(|c: char| c.is_ascii_digit()) {
                    (ts.to_string(), rest[space + 1..].to_string())
                } else {
                    (String::new(), rest.to_string())
                }
            } else {
                // No space — entire rest might be a timestamp-only line
                if rest.starts_with(|c: char| c.is_ascii_digit()) {
                    (rest.to_string(), String::new())
                } else {
                    (String::new(), rest.to_string())
                }
            };
            Self {
                pod: pod.to_string(),
                container: container.to_string(),
                timestamp,
                message,
            }
        } else {
            // No prefix — treat as plain message
            Self {
                pod: String::new(),
                container: String::new(),
                timestamp: String::new(),
                message: raw.to_string(),
            }
        }
    }

    /// Prefix shown to user: `[pod/container] `
    pub fn prefix(&self) -> String {
        if self.pod.is_empty() {
            String::new()
        } else {
            format!("[{}/{}] ", self.pod, self.container)
        }
    }

    /// Display text without the K8s timestamp (prefix + message).
    pub fn display_text(&self) -> String {
        let prefix = self.prefix();
        format!("{}{}", prefix, self.message)
    }
}

// ---------------------------------------------------------------------------
// Pod color palette
// ---------------------------------------------------------------------------

const POD_COLORS: &[Color] = &[
    Color::Cyan,
    Color::Green,
    Color::Yellow,
    Color::Magenta,
    Color::Blue,
    Color::LightRed,
    Color::LightGreen,
    Color::LightCyan,
    Color::LightMagenta,
    Color::LightBlue,
    Color::LightYellow,
    Color::Red,
];

// ---------------------------------------------------------------------------
// Log source context (for auto-discovery of new pods)
// ---------------------------------------------------------------------------

pub struct LogSource {
    pub resource_type: ResourceType,
    pub resource_name: String,
}

// ---------------------------------------------------------------------------
// Log view state
// ---------------------------------------------------------------------------

/// Tracks which pod/container subset to show.
/// `None` means "all".
pub struct LogFilter {
    pub text: String,
    pub regex: Option<Regex>,
    pub editing: bool,
    pub buf: String,
}

pub struct LogSelection {
    pub pod: Option<usize>,       // None = all pods
    pub container: Option<usize>, // None = all containers (scoped to selected pod)
}

pub struct LogViewState {
    pub lines: Vec<LogLine>,
    pub filter: LogFilter,

    // Follow
    pub following: bool,
    pub receivers: Vec<mpsc::Receiver<String>>,
    pub stream_handles: Vec<tokio::task::JoinHandle<()>>,
    /// Set of (pod, container) pairs that have active stream tasks.
    pub streaming_pairs: std::collections::HashSet<(String, String)>,

    // Scroll & selection
    pub scroll: usize,
    pub auto_scroll: bool,
    pub selected_line: Option<usize>,
    pub selection_anchor: Option<usize>,
    pub wrap: bool,

    // Pod / container
    pub pods: Vec<PodInfo>,
    pub selection: LogSelection,
    pub namespace: String,

    // Pod color assignments
    pub pod_colors: HashMap<String, Color>,
    pod_color_next: usize,

    // Time-based filter (only fetch logs from last N seconds)
    pub since_seconds: Option<i64>,

    // Auto-discovery of new pods during follow mode
    pub source: Option<LogSource>,
    pub last_pod_check: Instant,
}

impl LogViewState {
    pub fn new(pods: Vec<PodInfo>, namespace: String, initial_lines: Vec<LogLine>) -> Self {
        let mut state = Self {
            lines: Vec::new(),
            filter: LogFilter {
                text: String::new(),
                regex: None,
                editing: false,
                buf: String::new(),
            },
            following: false,
            receivers: Vec::new(),
            stream_handles: Vec::new(),
            streaming_pairs: std::collections::HashSet::new(),
            scroll: 0,
            auto_scroll: true,
            selected_line: None,
            selection_anchor: None,
            wrap: false,
            pods,
            selection: LogSelection {
                pod: None,
                container: None,
            },
            namespace,
            pod_colors: HashMap::new(),
            pod_color_next: 0,
            since_seconds: None,
            source: None,
            last_pod_check: Instant::now(),
        };
        // Register colors for initial lines
        for line in &initial_lines {
            state.ensure_pod_color(&line.pod);
        }
        state.lines = initial_lines;
        state
    }

    // -----------------------------------------------------------------------
    // Pod color management
    // -----------------------------------------------------------------------

    pub fn ensure_pod_color(&mut self, pod: &str) {
        if !pod.is_empty() && !self.pod_colors.contains_key(pod) {
            let color = POD_COLORS[self.pod_color_next % POD_COLORS.len()];
            self.pod_colors.insert(pod.to_string(), color);
            self.pod_color_next += 1;
        }
    }

    pub fn color_for_pod(&self, pod: &str) -> Color {
        self.pod_colors.get(pod).copied().unwrap_or(Color::DarkGray)
    }

    // -----------------------------------------------------------------------
    // Selection labels for the hotkey bar
    // -----------------------------------------------------------------------

    pub fn pod_label(&self) -> String {
        match self.selection.pod {
            | None => format!("All ({})", self.pods.len()),
            | Some(i) => self.pods.get(i).map(|p| p.name.clone()).unwrap_or_else(|| "?".into()),
        }
    }

    pub fn container_label(&self) -> String {
        match self.selection.container {
            | None => {
                let count = self.active_containers().len();
                format!("All ({})", count)
            },
            | Some(i) => self.active_containers().get(i).cloned().unwrap_or_else(|| "?".into()),
        }
    }

    /// Containers available given the current pod selection.
    pub fn active_containers(&self) -> Vec<String> {
        match self.selection.pod {
            | Some(i) => self.pods.get(i).map(|p| p.containers.clone()).unwrap_or_default(),
            | None => {
                let mut all: Vec<String> = self.pods.iter().flat_map(|p| p.containers.clone()).collect();
                all.sort();
                all.dedup();
                all
            },
        }
    }

    /// Returns the list of (pod_name, container_name) pairs to fetch logs from.
    pub fn active_streams(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        let pod_iter: Vec<&PodInfo> = match self.selection.pod {
            | Some(i) => self.pods.get(i).into_iter().collect(),
            | None => self.pods.iter().collect(),
        };
        for pod in pod_iter {
            let containers: Vec<&String> = match self.selection.container {
                | Some(ci) => {
                    let active = self.active_containers();
                    let name = active.get(ci);
                    pod.containers
                        .iter()
                        .filter(|c| name.map(|n| n == *c).unwrap_or(false))
                        .collect()
                },
                | None => pod.containers.iter().collect(),
            };
            for c in containers {
                pairs.push((pod.name.clone(), c.clone()));
            }
        }
        pairs
    }

    // -----------------------------------------------------------------------
    // Visible (filtered) lines
    // -----------------------------------------------------------------------

    pub fn visible_lines(&self) -> Vec<&LogLine> {
        if self.filter.text.is_empty() {
            self.lines.iter().collect()
        } else if let Some(re) = &self.filter.regex {
            self.lines.iter().filter(|l| re.is_match(&l.display_text())).collect()
        } else {
            self.lines
                .iter()
                .filter(|l| l.display_text().contains(&self.filter.text))
                .collect()
        }
    }

    // -----------------------------------------------------------------------
    // Follow mode — streams ALL active pod/container pairs
    // -----------------------------------------------------------------------

    pub fn start_following(&mut self, client: Client) {
        self.stop_following();

        let pairs = self.active_streams();
        let ns = self.namespace.clone();

        for (pod, container) in pairs {
            self.start_stream_for(client.clone(), &ns, pod, container);
        }
        self.following = true;
        self.auto_scroll = true;
    }

    /// Start a single stream task for a pod/container pair (if not already
    /// streaming).
    fn start_stream_for(&mut self, client: Client, ns: &str, pod: String, container: String) {
        let key = (pod.clone(), container.clone());
        if self.streaming_pairs.contains(&key) {
            return;
        }
        self.streaming_pairs.insert(key);
        self.ensure_pod_color(&pod);

        let (tx, rx) = mpsc::channel();
        let ns = ns.to_string();
        let pod_tag = pod.clone();
        let container_tag = container.clone();
        let handle = tokio::spawn(async move {
            log_stream_task(client, ns, pod, container, pod_tag, container_tag, tx).await;
        });
        self.receivers.push(rx);
        self.stream_handles.push(handle);
    }

    pub fn stop_following(&mut self) {
        for handle in self.stream_handles.drain(..) {
            handle.abort();
        }
        self.receivers.clear();
        self.streaming_pairs.clear();
        self.following = false;
    }

    pub fn poll_stream(&mut self) {
        let mut new_lines: Vec<LogLine> = Vec::new();
        for rx in &self.receivers {
            while let Ok(raw) = rx.try_recv() {
                new_lines.push(LogLine::parse(&raw));
            }
        }
        // Register colors for any new pods
        for line in &new_lines {
            if !line.pod.is_empty() && !self.pod_colors.contains_key(&line.pod) {
                let color = POD_COLORS[self.pod_color_next % POD_COLORS.len()];
                self.pod_colors.insert(line.pod.clone(), color);
                self.pod_color_next += 1;
            }
        }
        if new_lines.is_empty() {
            return;
        }
        // Sort new batch by timestamp
        new_lines.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        // Merge into existing sorted lines: find insertion point via binary search
        let first_ts = &new_lines[0].timestamp;
        let insert_at = if self.lines.is_empty()
            || self
                .lines
                .last()
                .map(|l| l.timestamp.as_str() <= first_ts.as_str())
                .unwrap_or(true)
        {
            // Common case: all new lines are newer — just append
            self.lines.len()
        } else {
            // Find where the first new timestamp belongs
            self.lines
                .partition_point(|l| l.timestamp.as_str() <= first_ts.as_str())
        };
        if insert_at == self.lines.len() {
            self.lines.extend(new_lines);
        } else {
            // Merge: split existing at insertion point, merge new batch with tail,
            // reassemble
            let tail: Vec<LogLine> = self.lines.drain(insert_at..).collect();
            let mut merged = Vec::with_capacity(new_lines.len() + tail.len());
            let mut ni = 0;
            let mut ti = 0;
            while ni < new_lines.len() && ti < tail.len() {
                if new_lines[ni].timestamp <= tail[ti].timestamp {
                    // Need to remove from new_lines; use swap_remove-like approach
                    // Actually, let's just use indices and push references — but we own both vecs
                    // We'll drain new_lines later; for now build order indices
                    merged.push(true); // true = from new_lines[ni]
                    ni += 1;
                } else {
                    merged.push(false); // false = from tail[ti]
                    ti += 1;
                }
            }
            // Rebuild: collect remaining
            let mut result = Vec::with_capacity(merged.len() + (new_lines.len() - ni) + (tail.len() - ti));
            let mut new_iter = new_lines.into_iter();
            let mut tail_iter = tail.into_iter();
            for from_new in merged {
                if from_new {
                    if let Some(l) = new_iter.next() {
                        result.push(l);
                    }
                } else if let Some(l) = tail_iter.next() {
                    result.push(l);
                }
            }
            result.extend(new_iter);
            result.extend(tail_iter);
            self.lines.extend(result);
        }
    }

    // -----------------------------------------------------------------------
    // Auto-discovery of new pods (for rolling updates)
    // -----------------------------------------------------------------------

    /// Check if new pods have appeared and start streams for them.
    /// Returns true if new pods were found (caller should re-discover).
    pub fn needs_pod_check(&self) -> bool {
        self.following && self.source.is_some() && self.last_pod_check.elapsed() >= std::time::Duration::from_secs(5)
    }

    /// Integrate newly discovered pods: update pod list, start streams for new
    /// ones.
    pub fn integrate_new_pods(&mut self, new_pods: Vec<PodInfo>, client: Client) {
        self.last_pod_check = Instant::now();

        let existing_names: std::collections::HashSet<String> = self.pods.iter().map(|p| p.name.clone()).collect();
        let mut added = false;

        // Collect new (pod, container) pairs to stream
        let mut to_start: Vec<(String, String)> = Vec::new();
        let active_container = self
            .selection
            .container
            .and_then(|ci| self.active_containers().get(ci).cloned());

        for pod in &new_pods {
            if !existing_names.contains(&pod.name) {
                added = true;
                self.ensure_pod_color(&pod.name);
                // Only start streams if in "all pods" mode
                if self.selection.pod.is_none() {
                    let containers: Vec<&String> = match &active_container {
                        | Some(name) => pod.containers.iter().filter(|c| *c == name).collect(),
                        | None => pod.containers.iter().collect(),
                    };
                    for c in containers {
                        to_start.push((pod.name.clone(), c.clone()));
                    }
                }
            }
        }

        // Start streams for new pods
        let ns = self.namespace.clone();
        for (pod, container) in to_start {
            self.start_stream_for(client.clone(), &ns, pod, container);
        }

        if added {
            self.pods = new_pods;
        }
    }

    // -----------------------------------------------------------------------
    // Filter
    // -----------------------------------------------------------------------

    pub fn begin_filter_edit(&mut self) {
        self.filter.editing = true;
        self.filter.buf = self.filter.text.clone();
    }

    pub fn apply_filter(&mut self) {
        self.filter.text = self.filter.buf.clone();
        self.filter.regex = if self.filter.text.is_empty() {
            None
        } else {
            Regex::new(&self.filter.text).ok()
        };
        self.filter.editing = false;
        self.scroll = 0;
    }

    pub fn cancel_filter_edit(&mut self) {
        self.filter.editing = false;
        self.filter.buf = self.filter.text.clone();
    }

    pub fn clear_filter(&mut self) {
        self.filter.text.clear();
        self.filter.regex = None;
        self.filter.buf.clear();
        self.scroll = 0;
    }

    // -----------------------------------------------------------------------
    // Navigation
    // -----------------------------------------------------------------------

    pub fn scroll_up(&mut self, n: usize) {
        self.scroll = self.scroll.saturating_sub(n);
        self.auto_scroll = false;
    }

    pub fn scroll_down(&mut self, n: usize, visible_count: usize) {
        self.scroll = (self.scroll + n).min(visible_count.saturating_sub(1));
    }

    /// Returns (start, end) inclusive range if a multi-line selection exists.
    pub fn selection_range(&self) -> Option<(usize, usize)> {
        let anchor = self.selection_anchor?;
        let cursor = self.selected_line?;
        Some((anchor.min(cursor), anchor.max(cursor)))
    }
}

impl Drop for LogViewState {
    fn drop(&mut self) {
        for handle in self.stream_handles.drain(..) {
            handle.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Async log streaming task — one per pod/container pair
// ---------------------------------------------------------------------------

async fn log_stream_task(
    client: Client,
    ns: String,
    pod: String,
    container: String,
    pod_tag: String,
    container_tag: String,
    tx: mpsc::Sender<String>,
) {
    use futures::{
        AsyncBufReadExt,
        StreamExt,
    };

    let api: Api<Pod> = Api::namespaced(client, &ns);
    let lp = kube::api::LogParams {
        follow: true,
        container: Some(container),
        tail_lines: Some(0),
        timestamps: true,
        ..Default::default()
    };

    let stream = match api.log_stream(&pod, &lp).await {
        | Ok(s) => s,
        | Err(_) => return,
    };

    let prefix = format!("[{}/{}] ", pod_tag, container_tag);
    let mut lines = stream.lines();
    while let Some(result) = lines.next().await {
        match result {
            | Ok(line) => {
                if tx.send(format!("{}{}", prefix, line)).is_err() {
                    return;
                }
            },
            | Err(_) => return,
        }
    }
}
