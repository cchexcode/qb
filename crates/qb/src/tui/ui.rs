use ratatui::{
    prelude::*,
    widgets::*,
};

use super::app::{App, DetailMode, Focus, NavItemKind, Popup, View, ALL_NAMESPACES_LABEL};
use super::smart;
use crate::k8s::ResourceType;

// ---------------------------------------------------------------------------
// Top-level render dispatch
// ---------------------------------------------------------------------------

pub fn render(f: &mut Frame, app: &mut App) {
    match app.view {
        | View::Main => render_main(f, app),
        | View::Detail => render_detail(f, app),
        | View::Logs => render_logs(f, app),
    }

    if app.popup.is_some() {
        render_popup(f, app);
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
            Constraint::Length(1),            // breadcrumb
            Constraint::Min(3),               // main area
            Constraint::Length(filter_height), // filter bar
            Constraint::Length(1),            // error line
            Constraint::Length(1),            // hotkey tab bar
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
                | NavItemKind::Category => Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                | NavItemKind::Resource(rt) => {
                    if app.selected_resource_type == Some(*rt) {
                        Style::default().fg(Color::Green)
                    } else {
                        Style::default().fg(Color::White)
                    }
                },
                | NavItemKind::ClusterStats => {
                    if app.is_showing_cluster_stats() {
                        Style::default().fg(Color::Green)
                    } else {
                        Style::default().fg(Color::White)
                    }
                },
            };
            ListItem::new(item.label.as_str()).style(style)
        })
        .collect();

    let focused = app.focus == Focus::Nav;
    let border_color = if focused { Color::Cyan } else { Color::DarkGray };

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(border_color))
                .title(" Resources "),
        )
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED).fg(Color::Cyan))
        .highlight_symbol("▶ ");

    f.render_stateful_widget(list, area, &mut app.nav_state);
}

// ---------------------------------------------------------------------------
// Resource table
// ---------------------------------------------------------------------------

fn render_resources(f: &mut Frame, app: &mut App, area: Rect) {
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

    let mut header_cells: Vec<Cell> = Vec::new();
    let header_style = Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
    header_cells.push(Cell::from(base_headers[0]).style(header_style));
    if all_ns {
        header_cells.push(Cell::from("NAMESPACE").style(header_style));
    }
    for h in &base_headers[1..] {
        header_cells.push(Cell::from(*h).style(header_style));
    }
    let header = Row::new(header_cells).height(1);

    let rows: Vec<Row> = visible_indices
        .iter()
        .map(|&idx| {
            let entry = &app.resources[idx];
            let mut cells = vec![Cell::from(entry.name.as_str())];
            if all_ns {
                cells.push(Cell::from(entry.namespace.as_str()));
            }
            for col in &entry.columns {
                cells.push(Cell::from(col.as_str()));
            }
            Row::new(cells)
        })
        .collect();

    let mut constraints: Vec<Constraint> = vec![Constraint::Min(20)];
    if all_ns {
        constraints.push(Constraint::Length(20));
    }
    for _ in 1..base_headers.len() {
        constraints.push(Constraint::Length(16));
    }

    let focused = app.focus == Focus::Resources;
    let border_color = if focused { Color::Cyan } else { Color::DarkGray };
    let title = format!(" {} ", rt.display_name());

    // Map real selection index to filtered row position for highlight
    let mut filtered_state = TableState::default();
    if let Some(sel) = app.resource_state.selected() {
        if let Some(vis_pos) = visible_indices.iter().position(|&i| i == sel) {
            filtered_state.select(Some(vis_pos));
        } else if !visible_indices.is_empty() {
            // Selection not in filtered view — select first visible
            filtered_state.select(Some(0));
            app.resource_state.select(Some(visible_indices[0]));
        }
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

    f.render_stateful_widget(table, area, &mut filtered_state);
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

/// Builds a mini stat card: ` ╭ LABEL ─────╮\n │  VALUE      │\n ╰─────────────╯`
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
        Line::from(vec![
            Span::styled(format!(" ╰{}╯", "─".repeat(bar_w)), dim),
        ]),
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

    // Helper: pad a set of spans to exactly `inner` visible chars, then wrap in │ … │
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
    card.push(row(vec![
        Span::styled(role_display, dim),
    ]));

    // Separator
    card.push(vec![
        Span::styled(format!("├{}┤", "─".repeat(bar_w)), dim),
    ]);

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
            format!("{:<w$}", format!("{} / {}", node.cpu_allocatable, node.cpu_capacity), w = res_val_w),
            val,
        ),
    ]));

    // Memory row
    card.push(row(vec![
        Span::styled(format!("{:<w$}", "memory", w = res_lbl_w), lbl),
        Span::styled(
            format!("{:<w$}", format!("{} / {}", node.mem_allocatable, node.mem_capacity), w = res_val_w),
            val,
        ),
    ]));

    // Pods row
    card.push(row(vec![
        Span::styled(format!("{:<w$}", "pods", w = res_lbl_w), lbl),
        Span::styled(
            format!("{:<w$}", format!("{} / {}", node.pods_allocatable, node.pods_capacity), w = res_val_w),
            val,
        ),
    ]));

    // Separator
    card.push(vec![
        Span::styled(format!("├{}┤", "─".repeat(bar_w)), dim),
    ]);

    // OS/arch & age
    card.push(row(vec![
        Span::styled(
            format!("{:<width$}", node.os_arch, width = inner / 2),
            dim,
        ),
        Span::styled(
            format!("{:>width$}", node.age, width = inner - inner / 2),
            dim,
        ),
    ]));

    // Bottom border
    card.push(vec![
        Span::styled(format!("╰{}╯", "─".repeat(bar_w)), dim),
    ]);

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
            spans.push(Span::styled("  ", Style::default())); // gap between cards
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
                Span::styled(
                    format!("{}", stats.pods_pending),
                    Style::default().fg(Color::Yellow),
                ),
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

            spans.push(Span::styled(
                format!("  {}", message),
                Style::default().fg(msg_color),
            ));

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
            Constraint::Length(1), // hotkey bar
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
    f.render_widget(Paragraph::new(bar), outer[2]);
}

fn render_smart_lines(app: &mut App) -> Vec<Line<'static>> {
    let rt = match app.selected_resource_type {
        | Some(rt) => rt,
        | None => return vec![],
    };
    smart::render(rt, &app.detail_value, app.secret_state.as_mut(), &app.expanded_keys)
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
            Constraint::Length(1),                  // breadcrumb
            Constraint::Min(3),                     // log content
            Constraint::Length(filter_height),       // filter bar
            Constraint::Length(1),                   // hotkey bar
        ])
        .split(f.area());

    render_breadcrumb(f, app, outer[0]);

    let title = if state.following { " [Following]" } else { "" };

    // Log lines (filtered)
    let visible = state.visible_lines();
    let area_height = outer[1].height.saturating_sub(2) as usize; // minus borders

    // Auto-scroll: if at bottom, keep scroll at end
    let scroll_offset = if state.auto_scroll && visible.len() > area_height {
        visible.len().saturating_sub(area_height)
    } else {
        state.scroll
    };

    let lines: Vec<Line> = visible
        .iter()
        .skip(scroll_offset)
        .take(area_height)
        .map(|l| {
            // Highlight filter matches
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
            Line::from(Span::styled(*l, Style::default().fg(Color::White)))
        })
        .collect();

    let line_info = format!(" {}/{} ", scroll_offset + lines.len(), visible.len());
    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan))
            .title(title)
            .title_bottom(line_info),
    );
    f.render_widget(paragraph, outer[1]);

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
    f.render_widget(Paragraph::new(bar), outer[3]);
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
            Span::styled(
                format!(" Container: {}", state.container_label()),
                label_style,
            ),
            sep.clone(),
        ]);
    }

    if !state.filter_text.is_empty() {
        spans.extend([
            Span::styled(" x ", key_style),
            Span::styled(" Clear filter", label_style),
        ]);
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

    // Filter controls
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

    spans.extend([Span::styled(" q ", key_style),
        Span::styled(" Quit", label_style),
    ]);

    let bar = Line::from(spans);
    f.render_widget(Paragraph::new(bar), area);
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

    if is_secret_smart {
        spans.extend([
            Span::styled(" d ", key_style),
            Span::styled(" Decode", label_style),
            sep.clone(),
            Span::styled(" y ", key_style),
            Span::styled(" Copy", label_style),
            sep.clone(),
            Span::styled(" j/k ", key_style),
            Span::styled(" Navigate", label_style),
        ]);
    } else {
        // Show view toggle
        match app.detail_mode {
            | DetailMode::Smart => {
                spans.extend([
                    Span::styled(" y ", key_style),
                    Span::styled(" YAML", label_style),
                    sep.clone(),
                ]);
            },
            | DetailMode::Yaml => {
                spans.extend([
                    Span::styled(" s ", key_style),
                    Span::styled(" Smart", label_style),
                    sep.clone(),
                ]);
            },
        }
        spans.extend([
            Span::styled(" j/k ", key_style),
            Span::styled(" Scroll", label_style),
        ]);
    }

    // [e] expand/collapse labels & annotations
    if app.detail_mode == DetailMode::Smart && !is_secret_smart {
        let expand_label = if app.expanded_keys.is_empty() { " Expand" } else { " Collapse" };
        spans.extend([
            sep.clone(),
            Span::styled(" e ", key_style),
            Span::styled(expand_label, label_style),
        ]);
    }

    spans.extend([sep.clone(), Span::styled(" PgUp/Dn ", key_style), Span::styled(" Page", label_style)]);

    // Show [l] Logs for workload resources
    if app.selected_resource_type.map(|rt| rt.supports_logs()).unwrap_or(false) {
        spans.extend([sep, Span::styled(" l ", key_style), Span::styled(" Logs", label_style)]);
    }

    Line::from(spans)
}

// ---------------------------------------------------------------------------
// Popup overlay
// ---------------------------------------------------------------------------

fn render_popup(f: &mut Frame, app: &mut App) {
    let current_context = app.kube.current_context().to_string();
    let current_namespace = app.kube.current_namespace().map(|s| s.to_string());

    let popup = match &mut app.popup {
        | Some(p) => p,
        | None => return,
    };

    let area = centered_rect(50, 60, f.area());
    app.area_popup = area;
    f.render_widget(Clear, area);

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
