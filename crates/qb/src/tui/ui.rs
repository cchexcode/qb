use {
    super::{
        app::{
            App,
            DetailMode,
            Focus,
            NavItemKind,
            Popup,
            View,
            ALL_NAMESPACES_LABEL,
        },
        smart,
    },
    crate::{
        k8s::ResourceType,
        portforward::PortForwardStatus,
    },
    ratatui::{
        prelude::*,
        widgets::*,
    },
};

// ---------------------------------------------------------------------------
// Top-level render dispatch
// ---------------------------------------------------------------------------

pub fn render(f: &mut Frame, app: &mut App) {
    match app.view {
        | View::Main => render_main(f, app),
        | View::Detail => render_detail(f, app),
        | View::Logs => render_logs(f, app),
        | View::EditDiff => render_edit_diff(f, app),
    }

    if app.popup.is_some() {
        render_popup(f, app);
    }

    if app.palette_open {
        render_palette(f, app);
    }

    if app.help_open {
        render_help(f, app);
    }
}

// ---------------------------------------------------------------------------
// Breadcrumb bar
// ---------------------------------------------------------------------------

fn render_breadcrumb(f: &mut Frame, app: &App, area: Rect) {
    let sep_style = Style::default().fg(Color::DarkGray);
    let seg_style = Style::default().fg(Color::White);
    let active_style = Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD);

    let mut spans: Vec<Span> = vec![Span::styled(" ", Style::default())];

    let push_seg = |spans: &mut Vec<Span>, text: String, is_last: bool| {
        let style = if is_last { active_style } else { seg_style };
        spans.push(Span::styled(text, style));
        if !is_last {
            spans.push(Span::styled(" > ", sep_style));
        }
    };

    // Cluster context
    let ctx = app.kube.current_context().to_string();
    let is_main_only = app.view == View::Main;
    push_seg(&mut spans, ctx, is_main_only && app.selected_resource_type.is_none());

    // Namespace
    let ns = app.kube.namespace_display().to_string();
    push_seg(&mut spans, ns, false);

    // Resource type
    if let Some(rt) = app.selected_resource_type {
        let rt_name = rt.display_name().to_string();
        let is_last_rt = is_main_only;
        push_seg(&mut spans, rt_name, is_last_rt);

        // Selected resource name (in detail or log view)
        if app.view == View::Detail || app.view == View::Logs {
            let res_name = app
                .resource_state
                .selected()
                .and_then(|idx| app.resources.get(idx))
                .map(|e| e.name.clone())
                .unwrap_or_else(|| "?".into());
            let is_detail = app.view == View::Detail;
            push_seg(&mut spans, res_name, is_detail);
        }

        // Log view suffix
        if app.view == View::Logs {
            if let Some(log_state) = &app.log_state {
                push_seg(&mut spans, log_state.source_display(), false);
                push_seg(&mut spans, "logs".to_string(), true);
            }
        }
    }

    // Right-align the refresh indicator
    let elapsed = app.last_refresh.elapsed().as_secs();
    let refresh_text = if app.paused {
        " ⏸ paused ".to_string()
    } else if elapsed < 2 {
        " ⟳ just now ".to_string()
    } else {
        format!(" ⟳ {}s ago ", elapsed)
    };
    let left_width: usize = spans.iter().map(|s| s.content.chars().count()).sum();
    let area_width = area.width as usize;
    let pad = area_width.saturating_sub(left_width + refresh_text.len());
    spans.push(Span::styled(" ".repeat(pad), Style::default().bg(Color::DarkGray)));
    let refresh_style = if app.paused {
        Style::default().fg(Color::Yellow).bg(Color::DarkGray)
    } else {
        Style::default().fg(Color::White).bg(Color::DarkGray)
    };
    spans.push(Span::styled(refresh_text, refresh_style));

    let line = Line::from(spans);
    let bar = Paragraph::new(line).style(Style::default().bg(Color::DarkGray));
    f.render_widget(bar, area);
}

// ---------------------------------------------------------------------------
// Main view
// ---------------------------------------------------------------------------

fn render_main(f: &mut Frame, app: &mut App) {
    let has_filter_bar = app.resource_filter_editing || !app.resource_filter_text.is_empty();
    let filter_height = if has_filter_bar { 1 } else { 0 };

    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),             // breadcrumb
            Constraint::Min(3),                // main area
            Constraint::Length(filter_height), // filter bar
            Constraint::Length(1),             // error line
            Constraint::Length(2),             // hotkey tab bar (2 lines for wrapping)
        ])
        .split(f.area());

    render_breadcrumb(f, app, outer[0]);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(28), Constraint::Min(40)])
        .split(outer[1]);

    // Store areas for mouse click handling
    app.area_nav = cols[0];
    app.area_resources = cols[1];

    render_nav(f, app, cols[0]);
    render_resources(f, app, cols[1]);

    if has_filter_bar {
        render_resource_filter_bar(f, app, outer[2]);
    }

    render_error(f, app, outer[3]);
    render_hotkey_bar(f, app, outer[4]);
}

// ---------------------------------------------------------------------------
// Navigation sidebar
// ---------------------------------------------------------------------------

fn render_nav(f: &mut Frame, app: &mut App, area: Rect) {
    let items: Vec<ListItem> = app
        .nav_items
        .iter()
        .map(|item| {
            let style = match &item.kind {
                | NavItemKind::SuperCategory => {
                    Style::default()
                        .fg(Color::Magenta)
                        .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
                },
                | NavItemKind::Category => Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                | NavItemKind::Resource(rt) => {
                    if app.selected_resource_type == Some(*rt) && !app.showing_port_forwards {
                        Style::default().fg(Color::Green)
                    } else {
                        Style::default().fg(Color::White)
                    }
                },
                | NavItemKind::ClusterStats => {
                    if app.is_showing_cluster_stats() && !app.showing_port_forwards {
                        Style::default().fg(Color::Green)
                    } else {
                        Style::default().fg(Color::White)
                    }
                },
                | NavItemKind::PortForwards => {
                    if app.showing_port_forwards {
                        Style::default().fg(Color::Green)
                    } else {
                        Style::default().fg(Color::White)
                    }
                },
            };
            // Append resource count badge if available
            let label = if let NavItemKind::Resource(rt) = &item.kind {
                if let Some(&count) = app.resource_counts.get(rt) {
                    format!("{} ({})", item.label, count)
                } else {
                    item.label.clone()
                }
            } else {
                item.label.clone()
            };
            ListItem::new(label).style(style)
        })
        .collect();

    let focused = app.focus == Focus::Nav;
    let border_color = if focused { Color::Cyan } else { Color::DarkGray };

    // Show active port forward count in nav title
    let pf_count = app
        .pf_manager
        .entries()
        .iter()
        .filter(|e| e.status.is_running())
        .count();
    let title = if pf_count > 0 {
        format!(" Resources (PF:{}) ", pf_count)
    } else {
        " Resources ".to_string()
    };

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(border_color))
                .title(title),
        )
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED).fg(Color::Cyan))
        .highlight_symbol("▶ ")
        .scroll_padding(1);

    f.render_stateful_widget(list, area, &mut app.nav_state);
}

// ---------------------------------------------------------------------------
// Resource table
// ---------------------------------------------------------------------------

fn render_resources(f: &mut Frame, app: &mut App, area: Rect) {
    // Port forwards view
    if app.is_showing_port_forwards() {
        render_port_forwards(f, app, area);
        return;
    }

    // Cluster stats overview
    if app.is_showing_cluster_stats() {
        render_cluster_stats(f, app, area);
        return;
    }

    let rt = match app.selected_resource_type {
        | Some(rt) => rt,
        | None => {
            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray))
                .title(" Select a resource type ");
            f.render_widget(block, area);
            return;
        },
    };

    if rt == ResourceType::Event {
        render_events_log(f, app, area);
        return;
    }

    let visible_indices = app.visible_resource_indices();

    let all_ns = app.kube.is_all_namespaces();
    let base_headers = rt.column_headers();

    // Build logical columns: [NAME, (NAMESPACE)?, col1, col2, ...]
    let mut col_headers: Vec<&str> = vec![base_headers[0]];
    if all_ns {
        col_headers.push("NAMESPACE");
    }
    col_headers.extend(&base_headers[1..]);

    let header_style = Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
    let header = Row::new(
        col_headers
            .iter()
            .map(|h| Cell::from(*h).style(header_style))
            .collect::<Vec<_>>(),
    )
    .height(1);

    let rows: Vec<Row> = visible_indices
        .iter()
        .map(|&idx| {
            let entry = &app.resources[idx];
            let is_diff_marked = app
                .diff_mark
                .as_ref()
                .map(|(n, ns, _)| n == &entry.name && ns == &entry.namespace)
                .unwrap_or(false);

            let name_cell = if is_diff_marked {
                Cell::from(Span::styled(
                    format!("* {}", entry.name),
                    Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD),
                ))
            } else {
                Cell::from(entry.name.as_str())
            };

            let mut cells = vec![name_cell];
            if all_ns {
                cells.push(Cell::from(entry.namespace.as_str()));
            }
            for col in &entry.columns {
                cells.push(Cell::from(col.as_str()));
            }
            Row::new(cells)
        })
        .collect();

    // Compute column widths from header + data content
    let num_cols = col_headers.len();
    let mut max_widths: Vec<usize> = col_headers.iter().map(|h| h.len()).collect();
    for &idx in &visible_indices {
        let entry = &app.resources[idx];
        // Column 0 = NAME
        max_widths[0] = max_widths[0].max(entry.name.len());
        let data_start = if all_ns {
            max_widths[1] = max_widths[1].max(entry.namespace.len());
            2
        } else {
            1
        };
        for (i, col) in entry.columns.iter().enumerate() {
            let ci = data_start + i;
            if ci < num_cols {
                max_widths[ci] = max_widths[ci].max(col.len());
            }
        }
    }
    // Add padding (2 chars) and cap individual columns at 50 to prevent blowout
    for w in &mut max_widths {
        *w = (*w + 2).min(50);
    }
    // NAME column (first) is flexible; all others are fixed width
    let mut constraints: Vec<Constraint> = Vec::with_capacity(num_cols);
    constraints.push(Constraint::Min(max_widths[0] as u16));
    for &w in &max_widths[1..] {
        constraints.push(Constraint::Length(w as u16));
    }

    let focused = app.focus == Focus::Resources;
    let border_color = if focused { Color::Cyan } else { Color::DarkGray };
    let title = format!(" {} ", rt.display_name());

    // Map real selection index to filtered row position for highlight.
    // Preserve the table offset across renders for smooth edge-scrolling.
    if let Some(sel) = app.resource_state.selected() {
        if let Some(vis_pos) = visible_indices.iter().position(|&i| i == sel) {
            app.resource_table_state.select(Some(vis_pos));
        } else if !visible_indices.is_empty() {
            app.resource_table_state.select(Some(0));
            app.resource_state.select(Some(visible_indices[0]));
        }
    } else {
        app.resource_table_state.select(None);
    }

    let table = Table::new(rows, constraints)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(border_color))
                .title(title),
        )
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED).fg(Color::Cyan))
        .highlight_symbol("▶ ");

    f.render_stateful_widget(table, area, &mut app.resource_table_state);
}

/// Builds a text gauge bar: `[████████░░░░░░░░░░░░] 75%`
fn gauge_bar(filled: usize, total: usize, width: usize) -> Vec<Span<'static>> {
    let pct = if total == 0 { 0.0 } else { filled as f64 / total as f64 };
    let pct_int = (pct * 100.0) as u64;
    let filled_w = ((pct * width as f64) as usize).min(width);
    let empty_w = width - filled_w;

    let bar_color = if pct >= 0.95 {
        Color::Green
    } else if pct >= 0.80 {
        Color::Yellow
    } else {
        Color::Red
    };

    vec![
        Span::styled("[", Style::default().fg(Color::DarkGray)),
        Span::styled("█".repeat(filled_w), Style::default().fg(bar_color)),
        Span::styled("░".repeat(empty_w), Style::default().fg(Color::DarkGray)),
        Span::styled("] ", Style::default().fg(Color::DarkGray)),
        Span::styled(format!("{}%", pct_int), Style::default().fg(bar_color)),
    ]
}

/// Builds a mini stat card: ` ╭ LABEL ─────╮\n │  VALUE      │\n
/// ╰─────────────╯`
fn stat_card(label: &str, value: &str, value_style: Style, width: usize) -> Vec<Line<'static>> {
    let dim = Style::default().fg(Color::DarkGray);
    let inner = width.saturating_sub(4); // 2 border + 2 padding
    let bar_w = width.saturating_sub(2);
    let label_pad = bar_w.saturating_sub(label.len() + 2);

    vec![
        Line::from(vec![
            Span::styled(" ╭ ", dim),
            Span::styled(label.to_string(), Style::default().fg(Color::Cyan)),
            Span::styled(format!(" {}", "─".repeat(label_pad)), dim),
            Span::styled("╮", dim),
        ]),
        Line::from(vec![
            Span::styled(" │ ", dim),
            Span::styled(format!("{:<width$}", value, width = inner), value_style),
            Span::styled(" │", dim),
        ]),
        Line::from(vec![Span::styled(format!(" ╰{}╯", "─".repeat(bar_w)), dim)]),
    ]
}

/// Builds a box-drawn card for one node, returned as rows of Spans.
/// Each row is exactly `w` display-columns wide (including the box border).
fn build_node_card(node: &crate::k8s::NodeStats, w: usize) -> Vec<Vec<Span<'static>>> {
    let dim = Style::default().fg(Color::DarkGray);
    let lbl = Style::default().fg(Color::Cyan);
    let val = Style::default().fg(Color::White);
    let bold = Style::default().fg(Color::White).add_modifier(Modifier::BOLD);
    let is_ready = node.status == "Ready";
    let status_style = if is_ready {
        Style::default().fg(Color::Green)
    } else {
        Style::default().fg(Color::Red)
    };
    let status_icon = if is_ready { "●" } else { "○" };
    let inner = w.saturating_sub(4); // │ + space ... space + │

    // Helper: pad a set of spans to exactly `inner` visible chars, then wrap in │ …
    // │
    let row = |content: Vec<Span<'static>>| -> Vec<Span<'static>> {
        // Calculate visible width of content spans
        let content_w: usize = content.iter().map(|s| s.content.chars().count()).sum();
        let pad = inner.saturating_sub(content_w);
        let mut r = vec![Span::styled("│ ", dim)];
        r.extend(content);
        r.push(Span::styled(format!("{} │", " ".repeat(pad)), dim));
        r
    };

    let bar_w = w.saturating_sub(2); // ╭ ... ╯ border chars

    // Truncate name to fit, leaving room for status icon
    let name_max = inner.saturating_sub(2); // "● " prefix
    let name_display = if node.name.len() > name_max {
        format!("{}…", &node.name[..name_max.saturating_sub(1)])
    } else {
        node.name.clone()
    };

    // Truncate role to fit the remaining space on the role line
    let role_display = if node.roles.len() > inner {
        format!("{}…", &node.roles[..inner.saturating_sub(1)])
    } else {
        node.roles.clone()
    };

    let mut card = Vec::new();

    // Top border with status
    let top_label = format!(" {} {} ", status_icon, name_display);
    let top_pad = bar_w.saturating_sub(top_label.chars().count() + 1);
    card.push(vec![
        Span::styled("╭", dim),
        Span::styled(format!(" {} ", status_icon), status_style),
        Span::styled(name_display, bold),
        Span::styled(format!(" {}", "─".repeat(top_pad)), dim),
        Span::styled("╮", dim),
    ]);

    // Role
    card.push(row(vec![Span::styled(role_display, dim)]));

    // Separator
    card.push(vec![Span::styled(format!("├{}┤", "─".repeat(bar_w)), dim)]);

    let res_lbl_w = 8;
    let res_val_w = inner.saturating_sub(res_lbl_w);

    // Version row
    card.push(row(vec![
        Span::styled(format!("{:<w$}", "version", w = res_lbl_w), lbl),
        Span::styled(format!("{:<w$}", node.version, w = res_val_w), val),
    ]));

    // CPU row
    card.push(row(vec![
        Span::styled(format!("{:<w$}", "cpu", w = res_lbl_w), lbl),
        Span::styled(
            format!(
                "{:<w$}",
                format!("{} / {}", node.cpu_allocatable, node.cpu_capacity),
                w = res_val_w
            ),
            val,
        ),
    ]));

    // Memory row
    card.push(row(vec![
        Span::styled(format!("{:<w$}", "memory", w = res_lbl_w), lbl),
        Span::styled(
            format!(
                "{:<w$}",
                format!("{} / {}", node.mem_allocatable, node.mem_capacity),
                w = res_val_w
            ),
            val,
        ),
    ]));

    // Pods row
    card.push(row(vec![
        Span::styled(format!("{:<w$}", "pods", w = res_lbl_w), lbl),
        Span::styled(
            format!(
                "{:<w$}",
                format!("{} / {}", node.pods_allocatable, node.pods_capacity),
                w = res_val_w
            ),
            val,
        ),
    ]));

    // Separator
    card.push(vec![Span::styled(format!("├{}┤", "─".repeat(bar_w)), dim)]);

    // OS/arch & age
    card.push(row(vec![
        Span::styled(format!("{:<width$}", node.os_arch, width = inner / 2), dim),
        Span::styled(format!("{:>width$}", node.age, width = inner - inner / 2), dim),
    ]));

    // Bottom border
    card.push(vec![Span::styled(format!("╰{}╯", "─".repeat(bar_w)), dim)]);

    card
}

fn render_cluster_stats(f: &mut Frame, app: &mut App, area: Rect) {
    let focused = app.focus == Focus::Resources;
    let border_color = if focused { Color::Cyan } else { Color::DarkGray };

    let stats = match &app.cluster_stats {
        | Some(s) => s,
        | None => {
            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(border_color))
                .title(" Cluster Overview — Loading... ");
            f.render_widget(block, area);
            return;
        },
    };

    let heading = Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
    let label = Style::default().fg(Color::Cyan);
    let value = Style::default().fg(Color::White);
    let good = Style::default().fg(Color::Green);
    let bad = Style::default().fg(Color::Red);
    let dim = Style::default().fg(Color::DarkGray);

    let mut lines: Vec<Line> = Vec::new();

    // ── Top stat cards row ──────────────────────────────────
    let card_w = 20;
    let node_style = if stats.nodes_not_ready > 0 { bad } else { good };
    let cards: Vec<Vec<Line>> = vec![
        stat_card("K8s", &stats.server_version, value, card_w),
        stat_card(
            "Nodes",
            &format!("{} ready / {}", stats.nodes_ready, stats.node_count),
            node_style,
            card_w,
        ),
        stat_card("Namespaces", &stats.namespace_count.to_string(), value, card_w),
        stat_card("Deployments", &stats.deployment_count.to_string(), value, card_w),
        stat_card("Services", &stats.service_count.to_string(), value, card_w),
    ];

    // Render cards side by side (each card is 3 lines tall)
    for row in 0..3 {
        let mut spans = Vec::new();
        for card in &cards {
            if let Some(line) = card.get(row) {
                spans.extend(line.spans.iter().cloned());
            }
            spans.push(Span::styled("  ", Style::default())); // gap between
                                                              // cards
        }
        lines.push(Line::from(spans));
    }
    lines.push(Line::from(""));

    // ── Pod breakdown with gauge bar ────────────────────────
    lines.push(Line::from(Span::styled(
        format!(" Pods ({})", stats.pod_count),
        heading,
    )));
    lines.push(Line::from(Span::styled(
        " ──────────────────────────────────────────────────────────",
        dim,
    )));

    if stats.pod_count > 0 {
        let bar_width = 30;

        // Running
        let mut running_spans = vec![Span::styled(format!("  {:<12}", "Running"), label)];
        running_spans.extend(gauge_bar(stats.pods_running, stats.pod_count, bar_width));
        running_spans.push(Span::styled(
            format!("  {}/{}", stats.pods_running, stats.pod_count),
            dim,
        ));
        lines.push(Line::from(running_spans));

        // Pending
        if stats.pods_pending > 0 {
            lines.push(Line::from(vec![
                Span::styled(format!("  {:<12}", "Pending"), label),
                Span::styled(format!("{}", stats.pods_pending), Style::default().fg(Color::Yellow)),
            ]));
        }

        // Failed
        if stats.pods_failed > 0 {
            lines.push(Line::from(vec![
                Span::styled(format!("  {:<12}", "Failed"), label),
                Span::styled(format!("{}", stats.pods_failed), bad),
            ]));
        }
    } else {
        lines.push(Line::from(Span::styled("  No pods", dim)));
    }
    lines.push(Line::from(""));

    // ── Node grid ─────────────────────────────────────────
    if !stats.nodes.is_empty() {
        lines.push(Line::from(Span::styled(
            format!(" Nodes ({})", stats.nodes.len()),
            heading,
        )));
        lines.push(Line::from(Span::styled(
            " ──────────────────────────────────────────────────────────",
            dim,
        )));

        // Build node cards, then tile them in a grid
        let node_card_w: usize = 36;
        let gap = 1;
        let avail_w = area.width.saturating_sub(3) as usize; // inner width minus border + pad
        let cols = ((avail_w + gap) / (node_card_w + gap)).max(1);
        let node_cards: Vec<Vec<Vec<Span>>> = stats
            .nodes
            .iter()
            .map(|node| build_node_card(node, node_card_w))
            .collect();

        // Tile cards into grid rows
        for chunk in node_cards.chunks(cols) {
            let card_height = chunk.iter().map(|c| c.len()).max().unwrap_or(0);
            for row in 0..card_height {
                let mut spans: Vec<Span> = vec![Span::raw(" ")];
                for (ci, card) in chunk.iter().enumerate() {
                    if ci > 0 {
                        spans.push(Span::styled(" ", Style::default()));
                    }
                    if let Some(card_row) = card.get(row) {
                        spans.extend(card_row.iter().cloned());
                    } else {
                        // Pad empty rows to keep alignment
                        spans.push(Span::raw(" ".repeat(node_card_w)));
                    }
                }
                lines.push(Line::from(spans));
            }
            lines.push(Line::from("")); // gap between grid rows
        }
    }

    let paragraph = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(border_color))
                .title(" Cluster Overview "),
        )
        .scroll((app.cluster_stats_scroll, 0));
    f.render_widget(paragraph, area);
}

fn render_resource_filter_bar(f: &mut Frame, app: &App, area: Rect) {
    let visible = app.visible_resource_indices();
    let total = app.resources.len();
    let count_suffix = if visible.len() < total {
        format!(" ({}/{})", visible.len(), total)
    } else {
        String::new()
    };

    let display = if app.resource_filter_editing {
        format!(" /{}▏{}", app.resource_filter_buf, count_suffix)
    } else {
        format!(" /{}/{}", app.resource_filter_text, count_suffix)
    };
    let style = if app.resource_filter_editing {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    f.render_widget(Paragraph::new(Line::from(Span::styled(display, style))), area);
}

// ---------------------------------------------------------------------------
// Events log view (log-style rendering for events)
// ---------------------------------------------------------------------------

/// Known event reasons that indicate trouble — rendered with warning style.
fn is_warning_reason(reason: &str) -> bool {
    matches!(
        reason,
        "BackOff"
            | "Failed"
            | "FailedScheduling"
            | "FailedMount"
            | "FailedAttachVolume"
            | "FailedCreate"
            | "Unhealthy"
            | "Evicted"
            | "OOMKilling"
            | "ExceededGracePeriod"
            | "NodeNotReady"
            | "Rebooted"
            | "FailedSync"
            | "FailedValidation"
    )
}

fn render_events_log(f: &mut Frame, app: &mut App, area: Rect) {
    let focused = app.focus == Focus::Resources;
    let border_color = if focused { Color::Cyan } else { Color::DarkGray };
    let inner_height = area.height.saturating_sub(2) as usize; // minus top/bottom borders
    let all_ns = app.kube.is_all_namespaces();
    let visible_indices = app.visible_resource_indices();
    let total = visible_indices.len();
    let cursor = app.events_cursor.min(total.saturating_sub(1));
    app.events_cursor = cursor;

    // Auto-scroll keeps cursor at the bottom (newest event)
    if app.events_auto_scroll && total > 0 {
        app.events_cursor = total - 1;
    }

    // Scroll follows cursor: ensure cursor is always visible
    let mut scroll = app.events_scroll;
    if app.events_cursor < scroll {
        scroll = app.events_cursor;
    } else if inner_height > 0 && app.events_cursor >= scroll + inner_height {
        scroll = app.events_cursor - inner_height + 1;
    }
    scroll = scroll.min(total.saturating_sub(inner_height));
    app.events_scroll = scroll;

    let lines: Vec<Line> = visible_indices
        .iter()
        .enumerate()
        .skip(scroll)
        .take(inner_height)
        .map(|(vis_idx, &real_idx)| {
            let entry = &app.resources[real_idx];
            let is_selected = vis_idx == app.events_cursor;
            // columns: [0]=TYPE  [1]=REASON  [2]=OBJECT  [3]=AGE  [4]=MESSAGE  [5]=COUNT
            let event_type = entry.columns.first().map(|s| s.as_str()).unwrap_or("");
            let reason = entry.columns.get(1).map(|s| s.as_str()).unwrap_or("");
            let object = entry.columns.get(2).map(|s| s.as_str()).unwrap_or("");
            let age = entry.columns.get(3).map(|s| s.as_str()).unwrap_or("");
            let message = entry.columns.get(4).map(|s| s.as_str()).unwrap_or("");
            let count: i32 = entry.columns.get(5).and_then(|s| s.parse().ok()).unwrap_or(1);

            let is_warning = event_type == "Warning";
            let is_known_bad = is_warning_reason(reason);

            // Type indicator with icon
            let (type_icon, type_color) = if is_warning {
                ("⚠ ", Color::Yellow)
            } else {
                ("● ", Color::Green)
            };

            // Reason color: red for known-bad reasons, bold white otherwise
            let reason_style = if is_known_bad {
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
            } else if is_warning {
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD)
            };

            // Message color: dimmer for Normal, brighter for Warning
            let msg_color = if is_warning { Color::White } else { Color::Gray };

            let mut spans = vec![
                Span::styled(format!("{:>5} ", age), Style::default().fg(Color::DarkGray)),
                Span::styled(type_icon, Style::default().fg(type_color)),
            ];

            // Namespace prefix when in all-namespaces mode
            if all_ns && !entry.namespace.is_empty() {
                spans.push(Span::styled(
                    format!("{}/", entry.namespace),
                    Style::default().fg(Color::DarkGray),
                ));
            }

            spans.push(Span::styled(
                format!("{:<32}", object),
                Style::default().fg(Color::Cyan),
            ));
            spans.push(Span::styled(reason.to_string(), reason_style));

            // Count badge for repeated events
            if count > 1 {
                spans.push(Span::styled(
                    format!(" (x{})", count),
                    Style::default().fg(Color::Magenta),
                ));
            }

            spans.push(Span::styled(format!("  {}", message), Style::default().fg(msg_color)));

            let mut line = Line::from(spans);
            if is_selected {
                line = line.style(Style::default().add_modifier(Modifier::REVERSED));
            }
            line
        })
        .collect();

    let visible_end = scroll + lines.len();
    let count_info = format!(" {}/{} ", visible_end, total);
    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color))
            .title(" Events ")
            .title_bottom(count_info),
    );
    f.render_widget(paragraph, area);
}

// ---------------------------------------------------------------------------
// Detail view — dispatches between Smart and YAML mode
// ---------------------------------------------------------------------------

fn render_detail(f: &mut Frame, app: &mut App) {
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // breadcrumb
            Constraint::Min(3),    // content
            Constraint::Length(2), // hotkey bar
        ])
        .split(f.area());

    render_breadcrumb(f, app, outer[0]);

    let mode_label = match app.detail_mode {
        | DetailMode::Smart => "Smart",
        | DetailMode::Yaml => "YAML",
    };
    let title = format!(" [{}] ", mode_label);

    let lines: Vec<Line> = match app.detail_mode {
        | DetailMode::Smart => render_smart_lines(app),
        | DetailMode::Yaml => render_yaml_lines(&app.detail_yaml),
    };

    let paragraph = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(title),
        )
        .scroll((app.detail_scroll, 0));
    f.render_widget(paragraph, outer[1]);

    let bar = build_detail_hotkey_bar(app);
    f.render_widget(Paragraph::new(bar).wrap(Wrap { trim: false }), outer[2]);
}

// ---------------------------------------------------------------------------
// Edit diff view
// ---------------------------------------------------------------------------

fn render_edit_diff(f: &mut Frame, app: &mut App) {
    use super::app::{
        DiffKind,
        DiffMode,
    };

    let ctx = match &app.edit_ctx {
        | Some(c) => c,
        | None => return,
    };

    let has_error = ctx.error.is_some();
    let error_height = if has_error { 2 } else { 0 };

    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),            // breadcrumb
            Constraint::Min(3),               // diff content
            Constraint::Length(error_height), // error
            Constraint::Length(2),            // hotkey bar
        ])
        .split(f.area());

    // Breadcrumb
    let mode_label = match ctx.diff_mode {
        | DiffMode::Inline => "Inline",
        | DiffMode::SideBySide => "Side-by-Side",
    };
    let breadcrumb = Line::from(vec![
        Span::styled(" ", Style::default()),
        Span::styled(
            format!(" Edit: {}/{} ", ctx.resource_type.display_name(), ctx.name),
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" — Review changes [{}]", mode_label),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    f.render_widget(
        Paragraph::new(breadcrumb).style(Style::default().bg(Color::DarkGray)),
        outer[0],
    );

    // Diff content
    let added = ctx.diff_lines.iter().filter(|(k, _)| *k == DiffKind::Added).count();
    let removed = ctx.diff_lines.iter().filter(|(k, _)| *k == DiffKind::Removed).count();
    let summary = format!(" +{} -{} ", added, removed);

    match ctx.diff_mode {
        | DiffMode::Inline => {
            let lines: Vec<Line> = ctx
                .diff_lines
                .iter()
                .map(|(kind, text)| {
                    let style = match kind {
                        | DiffKind::Added => Style::default().fg(Color::Green),
                        | DiffKind::Removed => Style::default().fg(Color::Red),
                        | DiffKind::Context => Style::default().fg(Color::DarkGray),
                    };
                    Line::from(Span::styled(text.clone(), style))
                })
                .collect();

            let paragraph = Paragraph::new(lines)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Cyan))
                        .title(" Diff ")
                        .title_bottom(summary),
                )
                .scroll((ctx.scroll, 0));
            f.render_widget(paragraph, outer[1]);
        },
        | DiffMode::SideBySide => {
            // Split the diff into left (removed/context) and right (added/context) columns
            let mut left: Vec<(DiffKind, String)> = Vec::new();
            let mut right: Vec<(DiffKind, String)> = Vec::new();

            let mut i = 0;
            let dl = &ctx.diff_lines;
            while i < dl.len() {
                match dl[i].0 {
                    | DiffKind::Context => {
                        let text = dl[i]
                            .1
                            .strip_prefix("  ")
                            .or_else(|| dl[i].1.strip_prefix(" "))
                            .unwrap_or(&dl[i].1)
                            .to_string();
                        left.push((DiffKind::Context, text.clone()));
                        right.push((DiffKind::Context, text));
                        i += 1;
                    },
                    | DiffKind::Removed => {
                        let mut removes = Vec::new();
                        while i < dl.len() && dl[i].0 == DiffKind::Removed {
                            removes.push(
                                dl[i]
                                    .1
                                    .strip_prefix("- ")
                                    .or_else(|| dl[i].1.strip_prefix("-"))
                                    .unwrap_or(&dl[i].1)
                                    .to_string(),
                            );
                            i += 1;
                        }
                        let mut adds = Vec::new();
                        while i < dl.len() && dl[i].0 == DiffKind::Added {
                            adds.push(
                                dl[i]
                                    .1
                                    .strip_prefix("+ ")
                                    .or_else(|| dl[i].1.strip_prefix("+"))
                                    .unwrap_or(&dl[i].1)
                                    .to_string(),
                            );
                            i += 1;
                        }
                        // Pair them up
                        let max_len = removes.len().max(adds.len());
                        for j in 0..max_len {
                            left.push(
                                removes
                                    .get(j)
                                    .map(|s| (DiffKind::Removed, s.clone()))
                                    .unwrap_or((DiffKind::Context, String::new())),
                            );
                            right.push(
                                adds.get(j)
                                    .map(|s| (DiffKind::Added, s.clone()))
                                    .unwrap_or((DiffKind::Context, String::new())),
                            );
                        }
                    },
                    | DiffKind::Added => {
                        left.push((DiffKind::Context, String::new()));
                        right.push((
                            DiffKind::Added,
                            dl[i]
                                .1
                                .strip_prefix("+ ")
                                .or_else(|| dl[i].1.strip_prefix("+"))
                                .unwrap_or(&dl[i].1)
                                .to_string(),
                        ));
                        i += 1;
                    },
                }
            }

            // Render side-by-side
            let content_area = outer[1];
            let inner_w = content_area.width.saturating_sub(2) as usize; // minus borders
            let half_w = inner_w / 2;
            let sep_dim = Style::default().fg(Color::DarkGray);

            let lines: Vec<Line> = left
                .iter()
                .zip(right.iter())
                .map(|((lk, lt), (rk, rt))| {
                    let left_style = match lk {
                        | DiffKind::Removed => Style::default().fg(Color::Red),
                        | _ => Style::default().fg(Color::DarkGray),
                    };
                    let right_style = match rk {
                        | DiffKind::Added => Style::default().fg(Color::Green),
                        | _ => Style::default().fg(Color::DarkGray),
                    };
                    // Truncate/pad each side to half width
                    let left_text = if lt.len() > half_w.saturating_sub(1) {
                        format!("{:.w$}", lt, w = half_w.saturating_sub(1))
                    } else {
                        format!("{:<w$}", lt, w = half_w.saturating_sub(1))
                    };
                    let right_text = if rt.len() > half_w {
                        format!("{:.w$}", rt, w = half_w)
                    } else {
                        format!("{:<w$}", rt, w = half_w)
                    };
                    Line::from(vec![
                        Span::styled(left_text, left_style),
                        Span::styled("│", sep_dim),
                        Span::styled(right_text, right_style),
                    ])
                })
                .collect();

            let paragraph = Paragraph::new(lines)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Cyan))
                        .title(" Diff — Side by Side ")
                        .title_bottom(summary),
                )
                .scroll((ctx.scroll, 0));
            f.render_widget(paragraph, content_area);
        },
    }

    // Error display
    if let Some(err) = &ctx.error {
        let err_lines = vec![
            Line::from(vec![
                Span::styled(" ERROR: ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
                Span::styled(err.clone(), Style::default().fg(Color::Red)),
            ]),
            Line::from(Span::styled(
                " Press [e] to re-edit or [Esc] to cancel",
                Style::default().fg(Color::Yellow),
            )),
        ];
        f.render_widget(Paragraph::new(err_lines), outer[2]);
    }

    // Hotkey bar
    let key_style = Style::default().fg(Color::Black).bg(Color::Yellow);
    let label_style = Style::default().fg(Color::White);
    let sep = Span::styled("  ", Style::default());
    let view_label = match ctx.diff_mode {
        | DiffMode::Inline => " Side-by-Side",
        | DiffMode::SideBySide => " Inline",
    };
    let bar = Line::from(vec![
        Span::styled(" Enter ", key_style),
        Span::styled(" Apply", label_style),
        sep.clone(),
        Span::styled(" v ", key_style),
        Span::styled(view_label, label_style),
        sep.clone(),
        Span::styled(" e ", key_style),
        Span::styled(" Re-edit", label_style),
        sep.clone(),
        Span::styled(" j/k ", key_style),
        Span::styled(" Scroll", label_style),
        sep.clone(),
        Span::styled(" Esc ", key_style),
        Span::styled(" Cancel", label_style),
    ]);
    f.render_widget(Paragraph::new(bar).wrap(Wrap { trim: false }), outer[3]);
}

fn render_smart_lines(app: &mut App) -> Vec<Line<'static>> {
    let rt = match app.selected_resource_type {
        | Some(rt) => rt,
        | None => return vec![],
    };
    let mut ds = smart::DictState {
        entries: Vec::new(),
        line_offsets: Vec::new(),
        cursor: app.dict_cursor,
        expanded: app.expanded_keys.clone(),
    };
    let lines = smart::render(rt, &app.detail_value, app.secret_state.as_mut(), &mut ds);
    // Sync state back to App
    app.dict_entries = ds.entries;
    app.dict_line_offsets = ds.line_offsets;
    app.expanded_keys = ds.expanded;
    // Clamp cursor if entries changed
    if let Some(c) = app.dict_cursor {
        if c >= app.dict_entries.len() {
            app.dict_cursor = if app.dict_entries.is_empty() {
                None
            } else {
                Some(app.dict_entries.len() - 1)
            };
        }
    }

    // Append related events (describe-style)
    if !app.related_events.is_empty() {
        let mut all_lines = lines;
        all_lines.push(Line::from(""));
        all_lines.push(Line::from(Span::styled(
            "Events:",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        )));

        // Header
        all_lines.push(Line::from(vec![
            Span::styled(
                format!("  {:<8} {:<6} {:<18} ", "AGE", "COUNT", "REASON"),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled("MESSAGE", Style::default().fg(Color::DarkGray)),
        ]));

        for ev in &app.related_events {
            let type_style = if ev.type_ == "Warning" {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default().fg(Color::Green)
            };
            let icon = if ev.type_ == "Warning" { "⚠" } else { "●" };
            let count_str = if ev.count > 1 {
                format!("x{}", ev.count)
            } else {
                String::new()
            };
            all_lines.push(Line::from(vec![
                Span::styled(format!("  {:<8} ", ev.last_seen), Style::default().fg(Color::DarkGray)),
                Span::styled(format!("{:<6} ", count_str), Style::default().fg(Color::White)),
                Span::styled(format!("{} {:<16} ", icon, ev.reason), type_style),
                Span::styled(ev.message.clone(), Style::default().fg(Color::White)),
            ]));
        }

        return all_lines;
    }

    lines
}

fn render_yaml_lines(yaml: &str) -> Vec<Line<'_>> {
    yaml.lines()
        .map(|l| {
            if l.starts_with("---") {
                Line::from(Span::styled(l, Style::default().fg(Color::DarkGray)))
            } else if l.contains(':') && !l.trim_start().starts_with('-') {
                let parts: Vec<&str> = l.splitn(2, ':').collect();
                if parts.len() == 2 {
                    Line::from(vec![
                        Span::styled(parts[0], Style::default().fg(Color::Cyan)),
                        Span::styled(":", Style::default().fg(Color::DarkGray)),
                        Span::styled(parts[1], Style::default().fg(Color::White)),
                    ])
                } else {
                    Line::from(l)
                }
            } else {
                Line::from(Span::styled(l, Style::default().fg(Color::White)))
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Log view
// ---------------------------------------------------------------------------

fn render_logs(f: &mut Frame, app: &mut App) {
    let state = match &app.log_state {
        | Some(s) => s,
        | None => return,
    };

    // Layout: breadcrumb + log content + filter bar + hotkey bar
    let has_filter_bar = state.filter_editing || !state.filter_text.is_empty();
    let filter_height = if has_filter_bar { 1 } else { 0 };

    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),             // breadcrumb
            Constraint::Min(3),                // log content
            Constraint::Length(filter_height), // filter bar
            Constraint::Length(2),             // hotkey bar
        ])
        .split(f.area());

    render_breadcrumb(f, app, outer[0]);

    let mut title_parts = Vec::new();
    if state.following {
        title_parts.push("[Following]");
    }
    if state.wrap {
        title_parts.push("[Wrap]");
    }
    let title = if title_parts.is_empty() {
        String::new()
    } else {
        format!(" {} ", title_parts.join(" "))
    };

    // Log lines (filtered)
    let visible = state.visible_lines();
    let area_height = outer[1].height.saturating_sub(2) as usize;

    // Auto-scroll: if at bottom, keep scroll at end
    let scroll_offset = if state.auto_scroll && visible.len() > area_height {
        visible.len().saturating_sub(area_height)
    } else {
        state.scroll
    };

    let lines: Vec<Line> = visible
        .iter()
        .enumerate()
        .skip(scroll_offset)
        .take(area_height)
        .map(|(idx, l)| {
            let is_selected = state.selected_line == Some(idx);
            let base_style = if is_selected {
                Style::default().fg(Color::Cyan).add_modifier(Modifier::REVERSED)
            } else {
                Style::default().fg(Color::White)
            };
            // Highlight filter matches
            if !is_selected {
                if let Some(re) = &state.filter_regex {
                    if let Some(m) = re.find(l) {
                        return Line::from(vec![
                            Span::styled(&l[..m.start()], Style::default().fg(Color::White)),
                            Span::styled(
                                l[m.start()..m.end()].to_string(),
                                Style::default().fg(Color::Black).bg(Color::Yellow),
                            ),
                            Span::styled(&l[m.end()..], Style::default().fg(Color::White)),
                        ]);
                    }
                }
            }
            Line::from(Span::styled(*l, base_style))
        })
        .collect();

    let line_info = format!(" {}/{} ", scroll_offset + lines.len(), visible.len());
    let mut paragraph = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan))
            .title(title)
            .title_bottom(line_info),
    );
    if state.wrap {
        paragraph = paragraph.wrap(Wrap { trim: false });
    }
    f.render_widget(paragraph, outer[1]);

    // Log detail popup (selected line expanded)
    if let Some(detail) = &app.log_detail_line {
        let popup_area = centered_rect(80, 50, f.area());
        f.render_widget(Clear, popup_area);
        let detail_paragraph = Paragraph::new(detail.as_str()).wrap(Wrap { trim: false }).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow))
                .title(" Log Line Detail — Esc to close "),
        );
        f.render_widget(detail_paragraph, popup_area);
    }

    // Filter bar
    if has_filter_bar {
        let filter_display = if state.filter_editing {
            format!(" /{}▏", state.filter_buf)
        } else {
            format!(" /{}/", state.filter_text)
        };
        let filter_style = if state.filter_editing {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let filter_line = Paragraph::new(Line::from(Span::styled(filter_display, filter_style)));
        f.render_widget(filter_line, outer[2]);
    }

    // Hotkey bar
    let bar = build_log_hotkey_bar(state);
    f.render_widget(Paragraph::new(bar).wrap(Wrap { trim: false }), outer[3]);
}

fn build_log_hotkey_bar(state: &super::logs::LogViewState) -> Line<'static> {
    let key_style = Style::default().fg(Color::Black).bg(Color::Yellow);
    let label_style = Style::default().fg(Color::White);
    let sep = Span::styled("  ", Style::default());

    let follow_label = if state.following { " Unfollow" } else { " Follow" };

    let mut spans = vec![
        Span::styled(" Esc ", key_style),
        Span::styled(" Back", label_style),
        sep.clone(),
        Span::styled(" / ", key_style),
        Span::styled(" Filter", label_style),
        sep.clone(),
        Span::styled(" f ", key_style),
        Span::styled(follow_label.to_string(), label_style),
        sep.clone(),
    ];

    if state.pods.len() > 1 {
        spans.extend([
            Span::styled(" p ", key_style),
            Span::styled(format!(" Pod: {}", state.pod_label()), label_style),
            sep.clone(),
        ]);
    }

    if state.active_containers().len() > 1 {
        spans.extend([
            Span::styled(" c ", key_style),
            Span::styled(format!(" Container: {}", state.container_label()), label_style),
            sep.clone(),
        ]);
    }

    if !state.filter_text.is_empty() {
        spans.extend([
            Span::styled(" x ", key_style),
            Span::styled(" Clear filter", label_style),
            sep.clone(),
        ]);
    }

    let wrap_label = if state.wrap { " Unwrap" } else { " Wrap" };
    spans.extend([
        Span::styled(" w ", key_style),
        Span::styled(wrap_label, label_style),
        sep.clone(),
    ]);

    if state.selected_line.is_some() {
        spans.extend([Span::styled(" Enter ", key_style), Span::styled(" Open", label_style)]);
    }

    Line::from(spans)
}

// ---------------------------------------------------------------------------
// Error bar
// ---------------------------------------------------------------------------

fn render_error(f: &mut Frame, app: &App, area: Rect) {
    if let Some(err) = &app.error {
        let text = Paragraph::new(Line::from(vec![
            Span::styled(" ERROR: ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
            Span::styled(err.as_str(), Style::default().fg(Color::Red)),
        ]));
        f.render_widget(text, area);
    }
}

// ---------------------------------------------------------------------------
// Gitui-style hotkey tab bar
// ---------------------------------------------------------------------------

fn render_hotkey_bar(f: &mut Frame, app: &App, area: Rect) {
    let ctx = app.kube.current_context();
    let ns_display = app.kube.namespace_display();

    let key_style = Style::default().fg(Color::Black).bg(Color::Yellow);
    let label_style = Style::default().fg(Color::White);
    let sep = Span::styled("  ", Style::default());

    let mut spans = vec![
        Span::styled(" r ", key_style),
        Span::styled(" Resources", label_style),
        sep.clone(),
        Span::styled(" c ", key_style),
        Span::styled(format!(" {}", ctx), label_style),
        sep.clone(),
        Span::styled(" n ", key_style),
        Span::styled(format!(" {}", ns_display), label_style),
        sep.clone(),
    ];

    // Show [l] Logs for workload resources
    if app.selected_resource_type.map(|rt| rt.supports_logs()).unwrap_or(false) {
        spans.extend([
            Span::styled(" l ", key_style),
            Span::styled(" Logs", label_style),
            sep.clone(),
        ]);
    }

    // Events log-style navigation hints
    if app.selected_resource_type == Some(ResourceType::Event) && app.focus == Focus::Resources {
        spans.extend([
            Span::styled(" j/k ", key_style),
            Span::styled(" Scroll", label_style),
            sep.clone(),
            Span::styled(" G ", key_style),
            Span::styled(" Bottom", label_style),
            sep.clone(),
            Span::styled(" Enter ", key_style),
            Span::styled(" Detail", label_style),
            sep.clone(),
        ]);
    }

    // Edit
    if app.selected_resource_type.is_some() && app.focus == Focus::Resources && !app.showing_port_forwards {
        spans.extend([
            Span::styled(" e ", key_style),
            Span::styled(" Edit", label_style),
            sep.clone(),
        ]);
    }

    // Port forward
    if app.selected_resource_type.is_some() && app.focus == Focus::Resources && !app.showing_port_forwards {
        let rt = app.selected_resource_type.unwrap();
        let supports_pf = matches!(
            rt,
            ResourceType::Service
                | ResourceType::Deployment
                | ResourceType::StatefulSet
                | ResourceType::DaemonSet
                | ResourceType::ReplicaSet
                | ResourceType::Pod
                | ResourceType::Job
                | ResourceType::CronJob
        );
        if supports_pf {
            spans.extend([
                Span::styled(" F ", key_style),
                Span::styled(" PortFwd", label_style),
                sep.clone(),
            ]);
        }

        // Scale
        if rt.supports_scale() {
            spans.extend([
                Span::styled(" S ", key_style),
                Span::styled(" Scale", label_style),
                sep.clone(),
            ]);
        }

        // Exec (experimental only, hide when filter active since x clears filter)
        if app.experimental && rt.supports_exec() && app.resource_filter_text.is_empty() {
            spans.extend([
                Span::styled(" x ", key_style),
                Span::styled(" Exec", label_style),
                sep.clone(),
            ]);
        }

        // Delete
        spans.extend([
            Span::styled(" D ", key_style),
            Span::styled(" Delete", label_style),
            sep.clone(),
        ]);

        // Restart
        if matches!(
            rt,
            ResourceType::Deployment | ResourceType::StatefulSet | ResourceType::DaemonSet
        ) {
            spans.extend([
                Span::styled(" R ", key_style),
                Span::styled(" Restart", label_style),
                sep.clone(),
            ]);
        }

        // Copy name
        spans.extend([
            Span::styled(" y ", key_style),
            Span::styled(" Copy", label_style),
            sep.clone(),
        ]);

        // Diff
        spans.extend([
            Span::styled(" d ", key_style),
            Span::styled(if app.diff_mark.is_some() { " Diff*" } else { " Diff" }, label_style),
            sep.clone(),
        ]);

        // Create
        spans.extend([
            Span::styled(" C ", key_style),
            Span::styled(" Create", label_style),
            sep.clone(),
        ]);
    }

    // Port forwards view hotkeys
    if app.showing_port_forwards && app.focus == Focus::Resources {
        if !app.pf_manager.entries().is_empty() {
            spans.extend([
                Span::styled(" p ", key_style),
                Span::styled(" Pause/Resume", label_style),
                sep.clone(),
                Span::styled(" d ", key_style),
                Span::styled(" Cancel", label_style),
                sep.clone(),
            ]);
        }
    }

    // Filter controls
    if !app.showing_port_forwards {
        spans.extend([
            Span::styled(" / ", key_style),
            Span::styled(" Filter", label_style),
            sep.clone(),
        ]);
        if !app.resource_filter_text.is_empty() {
            spans.extend([
                Span::styled(" x ", key_style),
                Span::styled(" Clear", label_style),
                sep.clone(),
            ]);
        }
    }

    // Pause toggle (auto-refresh, not port forward pause)
    if !app.showing_port_forwards {
        let pause_label = if app.paused { " Resume" } else { " Pause" };
        spans.extend([
            Span::styled(" p ", key_style),
            Span::styled(pause_label, label_style),
            sep.clone(),
        ]);
    }
    if app.paused {
        spans.push(Span::styled(
            " ⏸ PAUSED ",
            Style::default().fg(Color::Black).bg(Color::Yellow),
        ));
        spans.push(Span::styled("  ", Style::default()));
    }

    spans.extend([
        Span::styled(" ^p ", key_style),
        Span::styled(" Palette", label_style),
        sep.clone(),
        Span::styled(" ? ", key_style),
        Span::styled(" Help", label_style),
        sep.clone(),
        Span::styled(" O ", key_style),
        Span::styled(" Kubeconfig", label_style),
        sep,
        Span::styled(" q ", key_style),
        Span::styled(" Quit", label_style),
    ]);

    let bar = Line::from(spans);
    f.render_widget(Paragraph::new(bar).wrap(Wrap { trim: false }), area);
}

/// Context-sensitive hotkey bar for the detail view.
fn build_detail_hotkey_bar(app: &App) -> Line<'static> {
    let key_style = Style::default().fg(Color::Black).bg(Color::Yellow);
    let label_style = Style::default().fg(Color::White);
    let sep = Span::styled("  ", Style::default());

    let is_secret_smart = app.detail_mode == DetailMode::Smart
        && app.selected_resource_type == Some(ResourceType::Secret)
        && app.secret_state.is_some();

    let mut spans = vec![
        Span::styled(" Esc ", key_style),
        Span::styled(" Back", label_style),
        sep.clone(),
    ];

    // [v] cycle view
    let view_label = match app.detail_mode {
        | DetailMode::Smart => " YAML",
        | DetailMode::Yaml => " Smart",
    };
    spans.extend([
        Span::styled(" v ", key_style),
        Span::styled(view_label, label_style),
        sep.clone(),
    ]);

    if is_secret_smart {
        spans.extend([
            Span::styled(" j/k ", key_style),
            Span::styled(" Navigate", label_style),
            sep.clone(),
            Span::styled(" Space ", key_style),
            Span::styled(" Decode", label_style),
            sep.clone(),
            Span::styled(" y ", key_style),
            Span::styled(" Copy", label_style),
        ]);
    } else if app.detail_mode == DetailMode::Smart {
        let has_dict = !app.dict_entries.is_empty();
        if app.dict_cursor.is_some() {
            // In selection mode
            spans.extend([
                Span::styled(" j/k ", key_style),
                Span::styled(" Navigate", label_style),
                sep.clone(),
                Span::styled(" Space ", key_style),
                Span::styled(" Expand", label_style),
                sep.clone(),
                Span::styled(" y ", key_style),
                Span::styled(" Copy", label_style),
                sep.clone(),
                Span::styled(" s ", key_style),
                Span::styled(" Done", label_style),
                sep.clone(),
            ]);
        } else {
            spans.extend([
                Span::styled(" j/k ", key_style),
                Span::styled(" Scroll", label_style),
                sep.clone(),
            ]);
            if has_dict {
                spans.extend([
                    Span::styled(" s ", key_style),
                    Span::styled(" Select", label_style),
                    sep.clone(),
                ]);
            }
        }
    } else {
        spans.extend([
            Span::styled(" y ", key_style),
            Span::styled(" Copy", label_style),
            sep.clone(),
            Span::styled(" j/k ", key_style),
            Span::styled(" Scroll", label_style),
            sep.clone(),
        ]);
    }

    spans.extend([
        Span::styled(" PgUp/Dn ", key_style),
        Span::styled(" Page", label_style),
        sep.clone(),
        Span::styled(" e ", key_style),
        Span::styled(" Edit", label_style),
    ]);

    // Show [l] Logs for workload resources
    if app.selected_resource_type.map(|rt| rt.supports_logs()).unwrap_or(false) {
        spans.extend([
            sep.clone(),
            Span::styled(" l ", key_style),
            Span::styled(" Logs", label_style),
        ]);
    }

    // Watch toggle
    let watch_label = if app.detail_auto_refresh { " Unwatch" } else { " Watch" };
    spans.extend([
        sep.clone(),
        Span::styled(" w ", key_style),
        Span::styled(watch_label, label_style),
    ]);

    // Watch indicator
    if app.detail_auto_refresh {
        spans.extend([
            sep.clone(),
            Span::styled(" ⟳ WATCH ", Style::default().fg(Color::Black).bg(Color::Green)),
        ]);
    }

    // Pause indicator
    if app.paused {
        spans.extend([
            sep,
            Span::styled(" ⏸ PAUSED ", Style::default().fg(Color::Black).bg(Color::Yellow)),
        ]);
    }

    Line::from(spans)
}

// ---------------------------------------------------------------------------
// Popup overlay
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Port forwards view
// ---------------------------------------------------------------------------

fn render_port_forwards(f: &mut Frame, app: &mut App, area: Rect) {
    let entries = app.pf_manager.entries();

    let focused = app.focus == Focus::Resources;
    let border_color = if focused { Color::Cyan } else { Color::DarkGray };

    if entries.is_empty() {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color))
            .title(" Port Forwards ");
        let text = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                "  No active port forwards",
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "  Press [F] on a resource to create one",
                Style::default().fg(Color::DarkGray),
            )),
        ])
        .block(block);
        f.render_widget(text, area);
        return;
    }

    // Sync table state with cursor
    app.pf_table_state
        .select(if entries.is_empty() { None } else { Some(app.pf_cursor) });

    // Build table rows
    let header = Row::new(vec!["STATUS", "LOCAL", "REMOTE", "CLUSTER", "RESOURCE", "POD", "CONNS"])
        .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
        .bottom_margin(0);

    let rows: Vec<Row> = entries
        .iter()
        .map(|entry| {
            let status_style = match &entry.status {
                | PortForwardStatus::Active => Style::default().fg(Color::Green),
                | PortForwardStatus::Paused => Style::default().fg(Color::Yellow),
                | PortForwardStatus::Reconnecting { .. } => Style::default().fg(Color::Yellow),
                | PortForwardStatus::Error(_) => Style::default().fg(Color::Red),
                | PortForwardStatus::Starting => Style::default().fg(Color::Cyan),
                | PortForwardStatus::Cancelled => Style::default().fg(Color::DarkGray),
            };

            let status_text = match &entry.status {
                | PortForwardStatus::Reconnecting { attempt } => format!("Retry({})", attempt),
                | PortForwardStatus::Error(msg) => {
                    if msg.len() > 20 {
                        format!("Err:{:.20}", msg)
                    } else {
                        format!("Err:{}", msg)
                    }
                },
                | other => other.display().to_string(),
            };

            Row::new(vec![
                Cell::from(Span::styled(status_text, status_style)),
                Cell::from(format!(":{}", entry.local_port)),
                Cell::from(format!(":{}", entry.remote_port)),
                Cell::from(entry.context.as_str()),
                Cell::from(entry.resource_label.as_str()),
                Cell::from(entry.pod_name.as_str()),
                Cell::from(entry.connections.to_string()),
            ])
        })
        .collect();

    let table = Table::new(rows, [
        Constraint::Length(12),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Min(14),
        Constraint::Min(18),
        Constraint::Min(18),
        Constraint::Length(6),
    ])
    .header(header)
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED).fg(Color::Cyan))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color))
            .title(" Port Forwards "),
    );

    f.render_stateful_widget(table, area, &mut app.pf_table_state);
}

// ---------------------------------------------------------------------------
// Command palette
// ---------------------------------------------------------------------------

fn render_help(f: &mut Frame, app: &mut App) {
    let area = f.area();
    let width = (area.width * 70 / 100).max(50).min(area.width);
    let x = (area.width.saturating_sub(width)) / 2;
    let max_rows = 20u16;
    let height = (max_rows + 3).min(area.height);
    let help_area = ratatui::layout::Rect::new(x, 1, width, height);

    f.render_widget(Clear, help_area);

    let entries = app.filtered_help_entries();
    let mut lines: Vec<Line> = Vec::new();

    // Search input
    lines.push(Line::from(vec![
        Span::styled("  ", Style::default()),
        Span::styled(
            format!("{}|", app.help_buf),
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
    ]));

    let visible_rows = (height.saturating_sub(3)) as usize;
    // Edge-only scrolling: only scroll when cursor hits the boundary
    if app.help_cursor < app.help_scroll {
        app.help_scroll = app.help_cursor;
    } else if visible_rows > 0 && app.help_cursor >= app.help_scroll + visible_rows {
        app.help_scroll = app.help_cursor - visible_rows + 1;
    }
    app.help_scroll = app.help_scroll.min(entries.len().saturating_sub(visible_rows));

    for (i, (key, desc, ctx)) in entries.iter().skip(app.help_scroll).take(visible_rows).enumerate() {
        let actual_idx = i + app.help_scroll;
        let is_selected = actual_idx == app.help_cursor;
        let row_style = if is_selected {
            Style::default().add_modifier(Modifier::REVERSED).fg(Color::Cyan)
        } else {
            Style::default()
        };
        lines.push(Line::from(vec![
            Span::styled(
                format!("  {:<12}", key),
                if is_selected {
                    row_style
                } else {
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                },
            ),
            Span::styled(format!("{:<36}", desc), row_style),
            Span::styled(
                format!("{}", ctx),
                if is_selected {
                    row_style
                } else {
                    Style::default().fg(Color::DarkGray)
                },
            ),
        ]));
    }

    if entries.is_empty() && !app.help_buf.is_empty() {
        lines.push(Line::from(Span::styled(
            "    No matching keybinds",
            Style::default().fg(Color::DarkGray),
        )));
    }

    let count = entries.len();
    let title = format!(" Keybindings ({})  |  Esc to close ", count);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow))
        .title(title);

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, help_area);
}

fn render_palette(f: &mut Frame, app: &mut App) {
    // Top-centered palette, like VS Code
    let area = f.area();
    let width = (area.width * 60 / 100).max(40).min(area.width);
    let x = (area.width.saturating_sub(width)) / 2;
    let max_results = 12u16;
    let height = (max_results + 3).min(area.height); // input + border + results
    let palette_area = ratatui::layout::Rect::new(x, 1, width, height);

    f.render_widget(Clear, palette_area);

    let mut lines: Vec<Line> = Vec::new();

    // Input line
    let prefix = if app.palette_buf.starts_with('>') { "" } else { "  " };
    lines.push(Line::from(vec![
        Span::styled(prefix, Style::default()),
        Span::styled(
            format!("{}|", app.palette_buf),
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
    ]));

    // Results
    let visible_results = (height.saturating_sub(3)) as usize;
    // Scroll to keep cursor visible
    let scroll = if app.palette_cursor >= visible_results {
        app.palette_cursor - visible_results + 1
    } else {
        0
    };

    for (i, entry) in app
        .palette_results
        .iter()
        .skip(scroll)
        .take(visible_results)
        .enumerate()
    {
        let actual_idx = i + scroll;
        let is_selected = actual_idx == app.palette_cursor;
        let style = if is_selected {
            Style::default().fg(Color::Cyan).add_modifier(Modifier::REVERSED)
        } else {
            Style::default().fg(Color::White)
        };
        let marker = if is_selected { "▶ " } else { "  " };

        let kind_label = match &entry.kind {
            | super::app::PaletteEntryKind::Resource { .. } => "",
            | super::app::PaletteEntryKind::Command(_) => "  cmd",
        };
        let kind_span = Span::styled(kind_label, Style::default().fg(Color::DarkGray));

        lines.push(Line::from(vec![
            Span::styled(format!("  {}{}", marker, entry.label), style),
            kind_span,
        ]));
    }

    if app.palette_results.is_empty() && !app.palette_buf.is_empty() {
        lines.push(Line::from(Span::styled(
            "    No matches",
            Style::default().fg(Color::DarkGray),
        )));
    }

    let hint = if app.palette_buf.starts_with('>') {
        " Commands (type to filter) "
    } else if app.palette_global {
        " Search ALL resources  |  Tab=local  |  > commands "
    } else {
        " Search resources  |  Tab=all types  |  > commands "
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(hint);

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, palette_area);
}

// ---------------------------------------------------------------------------
// Popup overlays
// ---------------------------------------------------------------------------

fn render_popup(f: &mut Frame, app: &mut App) {
    let current_context = app.kube.current_context().to_string();
    let current_namespace = app.kube.current_namespace().map(|s| s.to_string());

    let popup = match &mut app.popup {
        | Some(p) => p,
        | None => return,
    };

    // PortForwardCreate / ConfirmDelete / ScaleInput / ExecShell use their own
    // smaller area
    let area = if matches!(popup, Popup::ExecShell { .. } | Popup::KubeconfigInput { .. }) {
        let a = centered_rect(60, 65, f.area());
        app.area_popup = a;
        f.render_widget(Clear, a);
        a
    } else if matches!(
        popup,
        Popup::PortForwardCreate(_) | Popup::ConfirmDelete { .. } | Popup::ScaleInput { .. }
    ) {
        let a = centered_rect(45, 50, f.area());
        app.area_popup = a;
        f.render_widget(Clear, a);
        a
    } else {
        let a = centered_rect(50, 60, f.area());
        app.area_popup = a;
        f.render_widget(Clear, a);
        a
    };

    match popup {
        | Popup::ContextSelect { items, state } => {
            let list_items: Vec<ListItem> = items
                .iter()
                .map(|i| {
                    let style = if *i == current_context {
                        Style::default().fg(Color::Green)
                    } else {
                        Style::default()
                    };
                    ListItem::new(i.as_str()).style(style)
                })
                .collect();

            let list = List::new(list_items)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Cyan))
                        .title(" Select Context "),
                )
                .highlight_style(Style::default().add_modifier(Modifier::REVERSED).fg(Color::Cyan))
                .highlight_symbol("▶ ");

            f.render_stateful_widget(list, area, state);
        },
        | Popup::NamespaceSelect { items, state } => {
            let list_items: Vec<ListItem> = items
                .iter()
                .map(|i| {
                    let is_selected = if *i == ALL_NAMESPACES_LABEL {
                        current_namespace.is_none()
                    } else {
                        current_namespace.as_deref() == Some(i.as_str())
                    };
                    let style = if is_selected {
                        Style::default().fg(Color::Green)
                    } else if *i == ALL_NAMESPACES_LABEL {
                        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };
                    ListItem::new(i.as_str()).style(style)
                })
                .collect();

            let list = List::new(list_items)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Cyan))
                        .title(" Select Namespace "),
                )
                .highlight_style(Style::default().add_modifier(Modifier::REVERSED).fg(Color::Cyan))
                .highlight_symbol("▶ ");

            f.render_stateful_widget(list, area, state);
        },
        | Popup::PodSelect { .. } | Popup::ContainerSelect { .. } => {
            let (title, items, state) = match popup {
                | Popup::PodSelect { items, state } => (" Select Pod ", items, state),
                | Popup::ContainerSelect { items, state } => (" Select Container ", items, state),
                | _ => unreachable!(),
            };
            let list_items: Vec<ListItem> = items
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    let style = if i == 0 {
                        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };
                    ListItem::new(item.as_str()).style(style)
                })
                .collect();

            let list = List::new(list_items)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Cyan))
                        .title(title),
                )
                .highlight_style(Style::default().add_modifier(Modifier::REVERSED).fg(Color::Cyan))
                .highlight_symbol("▶ ");

            f.render_stateful_widget(list, area, state);
        },
        | Popup::PortForwardCreate(dialog) => {
            let mut lines: Vec<Line> = Vec::new();
            lines.push(Line::from(Span::styled(
                format!(" {}/{}", dialog.resource_type.display_name(), dialog.resource_name),
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(""));

            // Port list
            for (i, port) in dialog.ports.iter().enumerate() {
                let marker = if i == dialog.port_cursor { "▶ " } else { "  " };
                let port_label = if port.name.is_empty() {
                    format!("{}{}/{}", marker, port.container_port, port.protocol)
                } else {
                    format!("{}{}/{} ({})", marker, port.container_port, port.protocol, port.name)
                };
                let style = if i == dialog.port_cursor {
                    Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::White)
                };
                lines.push(Line::from(Span::styled(port_label, style)));
            }

            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled(" Local port: ", Style::default().fg(Color::Yellow)),
                Span::styled(
                    format!("{}_", dialog.local_port_buf),
                    Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
                ),
            ]));
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                " Enter=Create  Esc=Cancel",
                Style::default().fg(Color::DarkGray),
            )));

            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(" Port Forward ");

            let paragraph = Paragraph::new(lines).block(block);
            f.render_widget(paragraph, area);
        },
        | Popup::ConfirmDelete {
            name, resource_type, ..
        } => {
            let mut lines: Vec<Line> = Vec::new();
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("  Delete {}/{}?", resource_type.display_name(), name),
                Style::default().fg(Color::Red),
            )));
            lines.push(Line::from(""));
            lines.push(Line::from("  Press [Enter/y] to confirm, [Esc/n] to cancel"));

            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Red))
                .title(" Confirm Delete ");

            let paragraph = Paragraph::new(lines).block(block);
            f.render_widget(paragraph, area);
        },
        | Popup::ScaleInput {
            name,
            resource_type,
            current,
            buf,
            ..
        } => {
            let mut lines: Vec<Line> = Vec::new();
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("  {}/{}", resource_type.display_name(), name),
                Style::default().fg(Color::Cyan),
            )));
            lines.push(Line::from(format!("  Current replicas: {}", current)));
            lines.push(Line::from(""));
            lines.push(Line::from(format!("  New replicas: {}▎", buf)));
            lines.push(Line::from(""));
            lines.push(Line::from("  Press [Enter] to apply, [Esc] to cancel"));

            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(" Scale ");

            let paragraph = Paragraph::new(lines).block(block);
            f.render_widget(paragraph, area);
        },
        | Popup::ExecShell {
            pod_name,
            containers,
            container_cursor,
            command_buf,
            terminal_buf,
            editing_terminal,
            ..
        } => {
            let cmd_style = if !*editing_terminal {
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let term_style = if *editing_terminal {
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };

            let mut lines: Vec<Line> = Vec::new();
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("  Pod: {}", pod_name),
                Style::default().fg(Color::Cyan),
            )));
            lines.push(Line::from(""));
            lines.push(Line::from("  Container:"));
            for (i, c) in containers.iter().enumerate() {
                let marker = if i == *container_cursor { "▶ " } else { "  " };
                let style = if i == *container_cursor {
                    Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                lines.push(Line::from(Span::styled(format!("    {}{}", marker, c), style)));
            }
            lines.push(Line::from(""));
            let cmd_cursor = if !*editing_terminal { "▎" } else { "" };
            lines.push(Line::from(vec![
                Span::styled("  Command:  ", Style::default().fg(Color::Yellow)),
                Span::styled(format!("{}{}", command_buf, cmd_cursor), cmd_style),
            ]));
            let term_cursor = if *editing_terminal { "▎" } else { "" };
            lines.push(Line::from(vec![
                Span::styled("  Terminal:  ", Style::default().fg(Color::Yellow)),
                Span::styled(format!("{}{}", terminal_buf, term_cursor), term_style),
            ]));
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "  [Tab] switch field  [Up/Down] container  [Enter] exec  [Esc] cancel",
                Style::default().fg(Color::DarkGray),
            )));

            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(" Exec Shell ");

            let paragraph = Paragraph::new(lines).block(block);
            f.render_widget(paragraph, area);
        },
        | Popup::KubeconfigInput { buf } => {
            let lines = vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Path: ", Style::default().fg(Color::Yellow)),
                    Span::styled(
                        format!("{}▎", buf),
                        Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
                    ),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "  [Enter] load  [Esc] cancel",
                    Style::default().fg(Color::DarkGray),
                )),
            ];

            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(" Open Kubeconfig ");

            let paragraph = Paragraph::new(lines).block(block);
            f.render_widget(paragraph, area);
        },
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}
