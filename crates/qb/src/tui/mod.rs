pub mod app;
pub mod logs;
pub mod smart;
pub mod ui;

use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::prelude::*;
use std::io;

use crate::k8s::KubeClient;

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

        if app.should_quit {
            return Ok(());
        }
    }
}
