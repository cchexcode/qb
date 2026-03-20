use {
    anyhow::Result,
    clap::Parser,
};

mod args;
mod config;
mod k8s;
mod portforward;
mod tui;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = args::Cli::parse();
    match cli.command {
        | Some(args::Command::Version) => {
            println!("qb {}", env!("CARGO_PKG_VERSION"));
        },
        | None => {
            let config = match config::QbConfig::load() {
                | Ok(c) => c,
                | Err(e) => {
                    eprintln!("Warning: Failed to load config: {}", e);
                    eprintln!("Using default configuration.");
                    config::QbConfig::default_config()
                },
            };
            let saved_kubeconfig = config.active_profile().kubeconfig.clone();
            let saved_context = config.active_profile().context.clone();
            tui::run(saved_kubeconfig, saved_context, None, cli.experimental, config).await?;
        },
    }
    Ok(())
}
