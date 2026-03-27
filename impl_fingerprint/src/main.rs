use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "impl_fingerprint", about = "Lightning node implementation fingerprinter")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Scrape LN implementation repos and build the fingerprint database.
    Scrape {
        /// Output path for the fingerprint database JSON file.
        #[arg(long, default_value = "fingerprint_db.json")]
        output: String,
    },
    /// Classify nodes by implementation using gossip dump data.
    Classify {
        /// Path to the gossip_analyze full_node_list dump file.
        #[arg(long)]
        nodes: String,
        /// Path to the gossip_analyze full_channel_list dump file.
        #[arg(long)]
        channels: String,
        /// Path to the fingerprint database JSON file.
        #[arg(long, default_value = "fingerprint_db.json")]
        db: String,
        /// Output path for classification results JSON.
        #[arg(long, default_value = "classifications.json")]
        output: String,
    },
    /// Validate classifier accuracy against a known training set.
    Validate {
        /// Path to a JSON file mapping pubkey → known implementation.
        #[arg(long)]
        training_set: String,
        /// Path to the gossip_analyze full_node_list dump file.
        #[arg(long)]
        nodes: String,
        /// Path to the gossip_analyze full_channel_list dump file.
        #[arg(long)]
        channels: String,
        /// Path to the fingerprint database JSON file.
        #[arg(long, default_value = "fingerprint_db.json")]
        db: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Scrape { output } => {
            eprintln!("scrape → {output}  (not yet implemented — Phase 2)");
        }
        Commands::Classify { nodes, channels, db, output } => {
            eprintln!(
                "classify  nodes={nodes}  channels={channels}  db={db}  output={output}  \
                 (not yet implemented — Phase 3/4)"
            );
        }
        Commands::Validate { training_set, nodes, channels, db } => {
            eprintln!(
                "validate  training_set={training_set}  nodes={nodes}  channels={channels}  \
                 db={db}  (not yet implemented — Phase 5)"
            );
        }
    }

    Ok(())
}
