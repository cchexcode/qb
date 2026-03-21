use {
    crate::k8s::PodInfo,
    k8s_openapi::api::core::v1::Pod,
    kube::{
        api::Api,
        Client,
    },
    regex::Regex,
    std::sync::mpsc,
};

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
    pub lines: Vec<String>,
    pub filter: LogFilter,

    // Follow
    pub following: bool,
    pub receivers: Vec<mpsc::Receiver<String>>,
    pub stream_handles: Vec<tokio::task::JoinHandle<()>>,

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

    // Time-based filter (only fetch logs from last N seconds)
    pub since_seconds: Option<i64>,
}

impl LogViewState {
    pub fn new(pods: Vec<PodInfo>, namespace: String, initial_lines: Vec<String>) -> Self {
        Self {
            lines: initial_lines,
            filter: LogFilter {
                text: String::new(),
                regex: None,
                editing: false,
                buf: String::new(),
            },
            following: false,
            receivers: Vec::new(),
            stream_handles: Vec::new(),
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
            since_seconds: None,
        }
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

    pub fn visible_lines(&self) -> Vec<&str> {
        if self.filter.text.is_empty() {
            self.lines.iter().map(|s| s.as_str()).collect()
        } else if let Some(re) = &self.filter.regex {
            self.lines
                .iter()
                .filter(|l| re.is_match(l))
                .map(|s| s.as_str())
                .collect()
        } else {
            self.lines
                .iter()
                .filter(|l| l.contains(&self.filter.text))
                .map(|s| s.as_str())
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
            let (tx, rx) = mpsc::channel();
            let client = client.clone();
            let ns = ns.clone();
            let pod_tag = pod.clone();
            let container_tag = container.clone();
            let handle = tokio::spawn(async move {
                log_stream_task(client, ns, pod, container, pod_tag, container_tag, tx).await;
            });
            self.receivers.push(rx);
            self.stream_handles.push(handle);
        }
        self.following = true;
        self.auto_scroll = true;
    }

    pub fn stop_following(&mut self) {
        for handle in self.stream_handles.drain(..) {
            handle.abort();
        }
        self.receivers.clear();
        self.following = false;
    }

    pub fn poll_stream(&mut self) {
        for rx in &self.receivers {
            while let Ok(line) = rx.try_recv() {
                self.lines.push(line);
            }
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
