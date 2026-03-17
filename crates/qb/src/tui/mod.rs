pub mod app;
pub mod logs;
pub mod smart;
pub mod ui;

use {
    crate::k8s::KubeClient,
    anyhow::Result,
    crossterm::{
        event::{
            self,
            DisableMouseCapture,
            EnableMouseCapture,
        },
        execute,
        terminal::{
            disable_raw_mode,
            enable_raw_mode,
            EnterAlternateScreen,
            LeaveAlternateScreen,
        },
    },
    ratatui::prelude::*,
    std::io,
};

pub async fn run(kubeconfig: Option<String>, context: Option<String>, namespace: Option<String>) -> Result<()> {
    let kube_client = KubeClient::new(kubeconfig, context, namespace).await?;

    tokio::task::block_in_place(|| {
        let rt = tokio::runtime::Handle::current();
        run_tui(kube_client, rt)
    })
}

fn run_tui(kube_client: KubeClient, rt: tokio::runtime::Handle) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_event_loop(&mut terminal, kube_client, rt);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;

    result
}

fn run_event_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    kube_client: KubeClient,
    rt: tokio::runtime::Handle,
) -> Result<()> {
    let mut app = app::App::new(kube_client, rt);

    loop {
        terminal.draw(|f| ui::render(f, &mut app))?;

        // Process deferred loads after rendering
        app.process_pending_load();

        // Poll log stream for new lines
        app.poll_log_stream();

        // Check if auto-refresh is due
        app.maybe_auto_refresh();

        if event::poll(std::time::Duration::from_millis(50))? {
            match event::read()? {
                | event::Event::Key(key) => {
                    if key.kind == event::KeyEventKind::Press {
                        app.handle_key(key);
                    }
                },
                | event::Event::Mouse(mouse) => {
                    app.handle_mouse(mouse);
                },
                | _ => {},
            }
        }

        // Handle pending editor invocation — suspend TUI, run editor, resume
        if let Some(edit) = app.pending_edit.take() {
            run_external_editor(terminal, &mut app, edit)?;
        }

        if app.should_quit {
            return Ok(());
        }
    }
}

fn run_external_editor(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut app::App,
    edit: app::PendingEdit,
) -> Result<()> {
    // Write YAML to temp file
    let tmp = tempfile::Builder::new().suffix(".yaml").tempfile()?;
    std::fs::write(tmp.path(), &edit.yaml)?;

    // Suspend TUI
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;

    // Resolve editor: $EDITOR → vim → vi
    let editor = std::env::var("EDITOR").unwrap_or_else(|_| {
        if std::process::Command::new("vim").arg("--version").output().is_ok() {
            "vim".into()
        } else {
            "vi".into()
        }
    });

    // Run editor (blocking)
    let status = std::process::Command::new(&editor).arg(tmp.path()).status();

    // Read back edited content before restoring TUI
    let edited_yaml = std::fs::read_to_string(tmp.path()).unwrap_or_default();

    // Restore TUI
    enable_raw_mode()?;
    execute!(terminal.backend_mut(), EnterAlternateScreen, EnableMouseCapture)?;
    // Force full redraw
    terminal.clear()?;

    match status {
        | Ok(s) if s.success() => {
            app.handle_edit_result(edit, edited_yaml);
        },
        | Ok(s) => {
            app.error = Some(format!("Editor exited with status: {}", s));
        },
        | Err(e) => {
            app.error = Some(format!("Failed to run editor '{}': {}", editor, e));
        },
    }

    Ok(())
}
