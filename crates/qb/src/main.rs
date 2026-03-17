use {
    anyhow::Result,
    clap::Parser,
};

mod args;
mod k8s;
mod tui;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = args::Cli::parse();
    match cli.command {
        | Some(args::Command::Version) => {
            println!("qb {}", env!("CARGO_PKG_VERSION"));
        },
        | None => {
            tui::run(cli.kubeconfig, cli.context, cli.namespace).await?;
        },
    }
    Ok(())
}
