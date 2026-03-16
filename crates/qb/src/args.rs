use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "qb", about = "Kubernetes resource browser")]
pub struct Cli {
    /// Path to kubeconfig file
    #[arg(long, env = "KUBECONFIG")]
    pub kubeconfig: Option<String>,

    /// Kubernetes context to use
    #[arg(long, short = 'c')]
    pub context: Option<String>,

    /// Namespace to use
    #[arg(long, short = 'n')]
    pub namespace: Option<String>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Print version information
    Version,
}
