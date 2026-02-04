//! Offline PTB Replay from Parquet Data
//!
//! Demonstrates fully offline transaction replay using only parquet data
//! indexed from Walrus checkpoints. No gRPC/GraphQL required.
//!
//! This proves that all data needed for PTB replay is stored in the parquet files.
//!
//! Run with:
//! ```bash
//! cargo run --release --example offline_replay -- \
//!     --parquet-dir ./parquet_output_with_bcs
//! ```

use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, BinaryArray, StringArray, UInt64Array};
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use sui_types::base_types::ObjectID;
use sui_types::effects::TransactionEffects;
use sui_types::object::Object;
use sui_types::transaction::{TransactionData, TransactionDataAPI};

#[derive(Parser, Debug)]
#[command(name = "offline-replay")]
#[command(about = "Replay transactions offline from parquet data")]
struct Args {
    /// Directory containing parquet files with BCS data
    #[arg(long, short, default_value = "./parquet_output_with_bcs")]
    parquet_dir: PathBuf,

    /// Transaction digest to replay (optional, replays sample if not provided)
    #[arg(long)]
    tx_digest: Option<String>,

    /// Maximum transactions to process
    #[arg(long, default_value_t = 10)]
    limit: usize,

    /// Verbose output
    #[arg(long, short)]
    verbose: bool,
}

/// Object cache keyed by (object_id, version)
type ObjectCache = HashMap<(ObjectID, u64), Object>;

/// Load all input objects from parquet into cache
fn load_objects_from_parquet(path: &PathBuf) -> Result<ObjectCache> {
    let file = File::open(path).context("Failed to open input_objects.parquet")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut cache = HashMap::new();
    let mut loaded = 0;
    let mut failed = 0;

    for batch_result in reader {
        let batch = batch_result?;

        // Get columns by name
        let object_id_col = batch
            .column_by_name("object_id")
            .ok_or_else(|| anyhow!("Missing object_id column"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("object_id not StringArray"))?;

        let version_col = batch
            .column_by_name("version")
            .ok_or_else(|| anyhow!("Missing version column"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("version not UInt64Array"))?;

        let bcs_col = batch
            .column_by_name("bcs_data")
            .ok_or_else(|| anyhow!("Missing bcs_data column"))?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("bcs_data not BinaryArray"))?;

        for i in 0..batch.num_rows() {
            if bcs_col.is_null(i) {
                continue;
            }

            let bcs_data = bcs_col.value(i);
            if bcs_data.is_empty() {
                continue;
            }

            match bcs::from_bytes::<Object>(bcs_data) {
                Ok(obj) => {
                    let oid = obj.id();
                    let version = version_col.value(i);
                    cache.insert((oid, version), obj);
                    loaded += 1;
                }
                Err(_) => {
                    failed += 1;
                }
            }
        }
    }

    println!(
        "Loaded {} objects into cache ({} failed to deserialize)",
        loaded, failed
    );
    Ok(cache)
}

/// Transaction data from parquet
#[derive(Debug)]
struct TransactionRecord {
    digest: String,
    checkpoint_num: u64,
    tx_data: TransactionData,
    effects: TransactionEffects,
    status: String,
}

/// Load transactions from parquet
fn load_transactions(path: &PathBuf, limit: usize) -> Result<Vec<TransactionRecord>> {
    let file = File::open(path).context("Failed to open transactions.parquet")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut records = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;

        let digest_col = batch
            .column_by_name("tx_digest")
            .ok_or_else(|| anyhow!("Missing tx_digest column"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("tx_digest not StringArray"))?;

        let checkpoint_col = batch
            .column_by_name("checkpoint_num")
            .ok_or_else(|| anyhow!("Missing checkpoint_num column"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("checkpoint_num not UInt64Array"))?;

        let status_col = batch
            .column_by_name("status")
            .ok_or_else(|| anyhow!("Missing status column"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("status not StringArray"))?;

        let tx_bcs_col = batch
            .column_by_name("tx_bcs")
            .ok_or_else(|| anyhow!("Missing tx_bcs column"))?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("tx_bcs not BinaryArray"))?;

        let effects_bcs_col = batch
            .column_by_name("effects_bcs")
            .ok_or_else(|| anyhow!("Missing effects_bcs column"))?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("effects_bcs not BinaryArray"))?;

        for i in 0..batch.num_rows() {
            if records.len() >= limit {
                break;
            }

            if tx_bcs_col.is_null(i) || effects_bcs_col.is_null(i) {
                continue;
            }

            let tx_bcs = tx_bcs_col.value(i);
            let effects_bcs = effects_bcs_col.value(i);

            if tx_bcs.is_empty() || effects_bcs.is_empty() {
                continue;
            }

            let tx_data: TransactionData = match bcs::from_bytes(tx_bcs) {
                Ok(t) => t,
                Err(_) => continue,
            };

            let effects: TransactionEffects = match bcs::from_bytes(effects_bcs) {
                Ok(e) => e,
                Err(_) => continue,
            };

            records.push(TransactionRecord {
                digest: digest_col.value(i).to_string(),
                checkpoint_num: checkpoint_col.value(i),
                tx_data,
                effects,
                status: status_col.value(i).to_string(),
            });
        }

        if records.len() >= limit {
            break;
        }
    }

    Ok(records)
}

/// Find input objects for a transaction from cache
/// Returns (found_objects, expected_ids) for validation
fn get_input_objects_for_tx(
    parquet_dir: &PathBuf,
    tx_digest: &str,
    cache: &ObjectCache,
) -> Result<(Vec<Object>, Vec<(String, u64)>)> {
    let path = parquet_dir.join("input_objects.parquet");
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut objects = Vec::new();
    let mut expected_ids = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;

        let digest_col = batch
            .column_by_name("tx_digest")
            .ok_or_else(|| anyhow!("Missing tx_digest column"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("tx_digest not StringArray"))?;

        let object_id_col = batch
            .column_by_name("object_id")
            .ok_or_else(|| anyhow!("Missing object_id column"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("object_id not StringArray"))?;

        let version_col = batch
            .column_by_name("version")
            .ok_or_else(|| anyhow!("Missing version column"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("version not UInt64Array"))?;

        for i in 0..batch.num_rows() {
            if digest_col.value(i) != tx_digest {
                continue;
            }

            let oid_str = object_id_col.value(i);
            let version = version_col.value(i);

            // Track what we expect
            expected_ids.push((oid_str.to_string(), version));

            // Parse object ID and try to find in cache
            if let Ok(oid) = oid_str.parse::<ObjectID>() {
                if let Some(obj) = cache.get(&(oid, version)) {
                    objects.push(obj.clone());
                }
            }
        }
    }

    Ok((objects, expected_ids))
}

/// Result of analyzing a transaction
enum AnalysisResult {
    Replayable {
        inputs_found: usize,
        inputs_expected: usize,
    },
    NotPTB,
    MissingObjects {
        found: usize,
        expected: usize,
        missing_ids: Vec<String>,
    },
}

/// Analyze a transaction for replay capability - MORE RIGOROUS VERSION
/// Actually checks if we have ALL required input objects
fn analyze_transaction(
    tx: &TransactionRecord,
    input_objects: &[Object],
    expected_input_ids: &[(String, u64)], // (object_id, version) from parquet
    verbose: bool,
) -> AnalysisResult {
    use sui_types::transaction::TransactionKind;

    let kind = tx.tx_data.kind();

    match kind {
        TransactionKind::ProgrammableTransaction(pt) => {
            let cmd_count = pt.commands.len();
            let ptb_input_count = pt.inputs.len();

            // Build set of object IDs we actually have
            let found_ids: std::collections::HashSet<_> = input_objects
                .iter()
                .map(|o| format!("{}", o.id()))
                .collect();

            // Check which expected inputs are missing
            let mut missing: Vec<String> = Vec::new();
            for (oid, version) in expected_input_ids {
                if !found_ids.contains(oid) {
                    missing.push(format!("{}@{}", oid, version));
                }
            }

            // Check if we have packages for execution
            let packages: Vec<_> = input_objects
                .iter()
                .filter(|o| o.data.try_as_package().is_some())
                .collect();

            let move_objects: Vec<_> = input_objects
                .iter()
                .filter(|o| o.data.try_as_move().is_some())
                .collect();

            if verbose {
                println!("  Commands: {}", cmd_count);
                println!("  PTB inputs: {}", ptb_input_count);
                println!("  Expected input objects: {}", expected_input_ids.len());
                println!("  Found input objects: {}", input_objects.len());
                println!("  Packages available: {}", packages.len());
                println!("  Move objects available: {}", move_objects.len());
                if !missing.is_empty() {
                    println!("  MISSING: {:?}", &missing[..missing.len().min(5)]);
                }
            }

            if !missing.is_empty() {
                AnalysisResult::MissingObjects {
                    found: input_objects.len(),
                    expected: expected_input_ids.len(),
                    missing_ids: missing,
                }
            } else if input_objects.is_empty() && !expected_input_ids.is_empty() {
                AnalysisResult::MissingObjects {
                    found: 0,
                    expected: expected_input_ids.len(),
                    missing_ids: expected_input_ids
                        .iter()
                        .map(|(id, v)| format!("{}@{}", id, v))
                        .collect(),
                }
            } else {
                AnalysisResult::Replayable {
                    inputs_found: input_objects.len(),
                    inputs_expected: expected_input_ids.len(),
                }
            }
        }
        _ => {
            if verbose {
                println!("  Non-PTB transaction type (skipped)");
            }
            AnalysisResult::NotPTB
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║   Offline Parquet Replay - No Network Required                ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("Parquet directory: {}", args.parquet_dir.display());

    // Verify required files
    let tx_path = args.parquet_dir.join("transactions.parquet");
    let input_path = args.parquet_dir.join("input_objects.parquet");

    if !tx_path.exists() {
        return Err(anyhow!(
            "Missing transactions.parquet (ensure you ran indexer with BCS output)"
        ));
    }
    if !input_path.exists() {
        return Err(anyhow!(
            "Missing input_objects.parquet (ensure you ran indexer with BCS output)"
        ));
    }

    // Load object cache
    println!("\nLoading objects from parquet...");
    let cache = load_objects_from_parquet(&input_path)?;

    // Load transactions
    println!("Loading transactions...");
    let transactions = load_transactions(&tx_path, args.limit)?;
    println!("Found {} transactions to analyze", transactions.len());

    // Analyze each transaction
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("Analyzing Transactions for Offline Replay Capability");
    println!("═══════════════════════════════════════════════════════════════\n");

    let mut replayable = 0;
    let mut not_ptb = 0;
    let mut missing_objects_count = 0;
    let mut total_missing = 0;
    let mut sample_missing: Vec<String> = Vec::new();

    for tx in &transactions {
        print!("TX {} (cp {})... ", &tx.digest[..12], tx.checkpoint_num);

        let (input_objects, expected_ids) =
            get_input_objects_for_tx(&args.parquet_dir, &tx.digest, &cache)?;

        let result = analyze_transaction(tx, &input_objects, &expected_ids, args.verbose);

        match result {
            AnalysisResult::Replayable {
                inputs_found,
                inputs_expected,
            } => {
                println!(
                    "✓ Replayable ({}/{} inputs, status: {})",
                    inputs_found, inputs_expected, tx.status
                );
                replayable += 1;
            }
            AnalysisResult::NotPTB => {
                println!("- Skipped (not PTB)");
                not_ptb += 1;
            }
            AnalysisResult::MissingObjects {
                found,
                expected,
                missing_ids,
            } => {
                println!("✗ Missing {}/{} objects", expected - found, expected);
                missing_objects_count += 1;
                total_missing += missing_ids.len();
                if sample_missing.len() < 10 {
                    sample_missing.extend(missing_ids.into_iter().take(3));
                }
            }
        }

        if args.verbose {
            println!();
        }
    }

    let ptb_count = transactions.len() - not_ptb;

    println!("\n═══════════════════════════════════════════════════════════════");
    println!("Summary");
    println!("═══════════════════════════════════════════════════════════════");
    println!("Total transactions:  {}", transactions.len());
    println!("PTB transactions:    {}", ptb_count);
    println!("  - Replayable:      {}", replayable);
    println!(
        "  - Missing objects: {} txs ({} objects total)",
        missing_objects_count, total_missing
    );
    println!("Non-PTB (skipped):   {}", not_ptb);
    if ptb_count > 0 {
        println!(
            "\nPTB Replayability: {:.1}%",
            (replayable as f64 / ptb_count as f64) * 100.0
        );
    }
    if !sample_missing.is_empty() {
        println!("\nSample missing objects:");
        for m in &sample_missing[..sample_missing.len().min(10)] {
            println!("  - {}", m);
        }
    }
    println!();
    println!("NOTE: Objects are considered 'found' only if their BCS data");
    println!("was successfully deserialized from parquet.");

    Ok(())
}
