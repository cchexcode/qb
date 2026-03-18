use clap::{
    Parser,
    Subcommand,
};

#[derive(Parser)]
#[command(name = "qb", about = "Kubernetes resource browser")]
pub struct Cli {
    /// Enable experimental features (exec shell, etc.)
    #[arg(short = 'e', long = "experimental")]
    pub experimental: bool,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Print version information
    Version,
}
