use duckdb::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    println!("\n=== COMPREHENSIVE DATA ANALYSIS ===\n");

    // Transaction Statistics
    println!("=== Transaction Statistics ===");
    let mut stmt = conn.prepare(
        "
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success,
            SUM(CASE WHEN status = 'failure' THEN 1 ELSE 0 END) as failed,
            AVG(gas_used) as avg_gas,
            COUNT(DISTINCT sender) as senders
        FROM read_parquet('./parquet_output/transactions.parquet')
    ",
    )?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let total: i64 = row.get(0)?;
        let success: i64 = row.get(1)?;
        let failed: i64 = row.get(2)?;
        let avg_gas: f64 = row.get(3)?;
        let senders: i64 = row.get(4)?;
        println!("  Total transactions:    {:>12}", total);
        println!("  Successful:            {:>12}", success);
        println!("  Failed:                {:>12}", failed);
        println!(
            "  Failure rate:          {:>11.2}%",
            100.0 * failed as f64 / total as f64
        );
        println!("  Avg gas per tx:        {:>12.0}", avg_gas);
        println!("  Unique senders:        {:>12}", senders);
    }

    // Top senders
    println!("\n=== Top 10 Most Active Senders ===");
    let mut stmt = conn.prepare(
        "
        SELECT sender, COUNT(*) as tx_count
        FROM read_parquet('./parquet_output/transactions.parquet')
        GROUP BY sender ORDER BY tx_count DESC LIMIT 10
    ",
    )?;
    let mut rows = stmt.query([])?;
    let mut i = 1;
    while let Some(row) = rows.next()? {
        let sender: String = row.get(0)?;
        let txs: i64 = row.get(1)?;
        println!("  {:2}. {}... txs={:>6}", i, &sender[..16], txs);
        i += 1;
    }

    // Move call stats
    println!("\n=== Move Call Statistics ===");
    let mut stmt = conn.prepare(
        "
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT package_id) as packages,
            COUNT(DISTINCT module) as modules,
            COUNT(DISTINCT function) as functions
        FROM read_parquet('./parquet_output/move_calls.parquet')
    ",
    )?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let total: i64 = row.get(0)?;
        let packages: i64 = row.get(1)?;
        let modules: i64 = row.get(2)?;
        let functions: i64 = row.get(3)?;
        println!("  Total move calls:      {:>12}", total);
        println!("  Unique packages:       {:>12}", packages);
        println!("  Unique modules:        {:>12}", modules);
        println!("  Unique functions:      {:>12}", functions);
    }

    // Top packages
    println!("\n=== Top 10 Called Packages ===");
    let mut stmt = conn.prepare(
        "
        SELECT package_id, COUNT(*) as calls
        FROM read_parquet('./parquet_output/move_calls.parquet')
        GROUP BY package_id ORDER BY calls DESC LIMIT 10
    ",
    )?;
    let mut rows = stmt.query([])?;
    let mut i = 1;
    while let Some(row) = rows.next()? {
        let pkg: String = row.get(0)?;
        let calls: i64 = row.get(1)?;
        let short_pkg = if pkg.len() > 24 { &pkg[..24] } else { &pkg };
        println!("  {:2}. {}... calls={:>8}", i, short_pkg, calls);
        i += 1;
    }

    // Top functions
    println!("\n=== Top 10 Called Functions ===");
    let mut stmt = conn.prepare(
        "
        SELECT module || '::' || function as func, COUNT(*) as calls
        FROM read_parquet('./parquet_output/move_calls.parquet')
        GROUP BY module, function ORDER BY calls DESC LIMIT 10
    ",
    )?;
    let mut rows = stmt.query([])?;
    let mut i = 1;
    while let Some(row) = rows.next()? {
        let func: String = row.get(0)?;
        let calls: i64 = row.get(1)?;
        println!("  {:2}. {:<45} calls={:>8}", i, func, calls);
        i += 1;
    }

    // Balance changes
    println!("\n=== Balance Changes Analysis ===");
    let mut stmt = conn.prepare(
        "
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT owner) as owners,
            COUNT(DISTINCT coin_type) as coin_types,
            SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) as positive,
            SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as negative,
            SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) as zero
        FROM read_parquet('./parquet_output/balance_changes.parquet')
    ",
    )?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let total: i64 = row.get(0)?;
        let owners: i64 = row.get(1)?;
        let coin_types: i64 = row.get(2)?;
        let positive: i64 = row.get(3)?;
        let negative: i64 = row.get(4)?;
        let zero: i64 = row.get(5)?;
        println!("  Total balance changes: {:>12}", total);
        println!("  Unique owners:         {:>12}", owners);
        println!("  Unique coin types:     {:>12}", coin_types);
        println!("  Positive (received):   {:>12}", positive);
        println!("  Negative (sent):       {:>12}", negative);
        println!("  Zero changes:          {:>12}", zero);
    }

    // Top coins
    println!("\n=== Top 10 Coin Types by Transfer Count ===");
    let mut stmt = conn.prepare(
        "
        SELECT coin_type, COUNT(*) as transfers
        FROM read_parquet('./parquet_output/balance_changes.parquet')
        GROUP BY coin_type ORDER BY transfers DESC LIMIT 10
    ",
    )?;
    let mut rows = stmt.query([])?;
    let mut i = 1;
    while let Some(row) = rows.next()? {
        let coin: String = row.get(0)?;
        let transfers: i64 = row.get(1)?;
        let short = coin.split("::").last().unwrap_or(&coin);
        println!("  {:2}. {:<30} transfers={:>8}", i, short, transfers);
        i += 1;
    }

    // Object operations
    println!("\n=== Object Operations ===");
    let mut stmt = conn.prepare(
        "
        SELECT operation, COUNT(*) as count
        FROM read_parquet('./parquet_output/objects.parquet')
        GROUP BY operation ORDER BY count DESC
    ",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let op: String = row.get(0)?;
        let count: i64 = row.get(1)?;
        println!("  {:<15} {:>12}", op, count);
    }

    // Event types
    println!("\n=== Top 10 Event Types ===");
    let mut stmt = conn.prepare(
        "
        SELECT module || '::' || event_type as event, COUNT(*) as count
        FROM read_parquet('./parquet_output/events.parquet')
        GROUP BY module, event_type ORDER BY count DESC LIMIT 10
    ",
    )?;
    let mut rows = stmt.query([])?;
    let mut i = 1;
    while let Some(row) = rows.next()? {
        let event: String = row.get(0)?;
        let count: i64 = row.get(1)?;
        let short_event = if event.len() > 50 {
            &event[..50]
        } else {
            &event
        };
        println!("  {:2}. {:<50} count={:>8}", i, short_event, count);
        i += 1;
    }

    // Checkpoint stats
    println!("\n=== Checkpoint Statistics ===");
    let mut stmt = conn.prepare(
        "
        SELECT 
            MIN(checkpoint_num), MAX(checkpoint_num),
            COUNT(DISTINCT epoch),
            SUM(transactions_count),
            AVG(transactions_count),
            SUM(CASE WHEN end_of_epoch THEN 1 ELSE 0 END)
        FROM read_parquet('./parquet_output/checkpoints.parquet')
    ",
    )?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let min_cp: i64 = row.get(0)?;
        let max_cp: i64 = row.get(1)?;
        let epochs: i64 = row.get(2)?;
        let total_txs: i64 = row.get(3)?;
        let avg_txs: f64 = row.get(4)?;
        let epoch_ends: i64 = row.get(5)?;
        println!("  Checkpoint range:      {} - {}", min_cp, max_cp);
        println!("  Epochs covered:        {:>12}", epochs);
        println!("  Total transactions:    {:>12}", total_txs);
        println!("  Avg txs/checkpoint:    {:>12.1}", avg_txs);
        println!("  Epoch end checkpoints: {:>12}", epoch_ends);
    }

    // Error types
    println!("\n=== Top 10 Execution Error Types ===");
    let mut stmt = conn.prepare(
        "
        SELECT error_type, COUNT(*) as count
        FROM read_parquet('./parquet_output/execution_errors.parquet')
        GROUP BY error_type ORDER BY count DESC LIMIT 10
    ",
    )?;
    let mut rows = stmt.query([])?;
    let mut i = 1;
    while let Some(row) = rows.next()? {
        let err: String = row.get(0)?;
        let count: i64 = row.get(1)?;
        let short_err = if err.len() > 55 { &err[..55] } else { &err };
        println!("  {:2}. {:<55} count={:>6}", i, short_err, count);
        i += 1;
    }

    // Swap detection
    println!("\n=== Swap Detection (Multi-Coin Transactions) ===");
    let mut stmt = conn.prepare(
        "
        WITH multi_coin AS (
            SELECT tx_digest, COUNT(DISTINCT coin_type) as coins
            FROM read_parquet('./parquet_output/balance_changes.parquet')
            GROUP BY tx_digest HAVING coins >= 2
        )
        SELECT coins, COUNT(*) as tx_count
        FROM multi_coin
        GROUP BY coins ORDER BY coins
    ",
    )?;
    let mut rows = stmt.query([])?;
    println!("  Coin Types | Transaction Count");
    println!("  -----------|------------------");
    while let Some(row) = rows.next()? {
        let coins: i64 = row.get(0)?;
        let txs: i64 = row.get(1)?;
        println!("  {:>10} | {:>17}", coins, txs);
    }

    println!("\n=== Analysis Complete ===\n");

    Ok(())
}
