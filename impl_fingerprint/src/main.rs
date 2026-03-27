use std::fs;

use clap::{Parser, Subcommand};
use impl_fingerprint::{classifier, input, scraper, validate};

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
            let db = scraper::build_db();
            let json = db.to_json()?;
            fs::write(&output, &json)?;
            eprintln!(
                "scrape → {output}  ({} records across {} implementations)",
                db.len(),
                db.entries.len(),
            );
        }
        Commands::Classify { nodes, channels, db, output } => {
            let db_json = fs::read_to_string(&db)?;
            let db = impl_fingerprint::db::FingerprintDb::from_json(&db_json)?;
            let node_list = input::load_nodes(&nodes)?;
            let channel_list = input::load_channels(&channels)?;
            let results = classifier::classify_all(&node_list, &db, &channel_list);
            let json = serde_json::to_string_pretty(&results)?;
            fs::write(&output, &json)?;
            eprintln!(
                "classify → {output}  ({} nodes classified)",
                results.len(),
            );
        }
        Commands::Validate { training_set, nodes, channels, db } => {
            let training = validate::TrainingSet::load(&training_set)?;
            let db_json = fs::read_to_string(&db)?;
            let db = impl_fingerprint::db::FingerprintDb::from_json(&db_json)?;
            let node_list = input::load_nodes(&nodes)?;
            let channel_list = input::load_channels(&channels)?;
            let report = validate::run_validation(&training, &node_list, &channel_list, &db);
            eprint!("{}", report.summary());
        }
    }

    Ok(())
}
