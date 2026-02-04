#!/bin/bash
# Replay transactions from parquet data using sui-sandbox
#
# Usage: ./scripts/replay_from_parquet.sh [--count N] [--filter FILTER] [--dry-run]
#
# Examples:
#   ./scripts/replay_from_parquet.sh --count 10
#   ./scripts/replay_from_parquet.sh --count 5 --filter "move_calls_count > 5"
#   ./scripts/replay_from_parquet.sh --dry-run

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PARQUET_DIR="${PROJECT_DIR}/parquet_output"
SANDBOX_DIR="/home/evan/Documents/sui-move-interface-extractor"

# Defaults
COUNT=10
FILTER="move_calls_count > 0"
DRY_RUN=false
COMPARE=true
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --count)
            COUNT="$2"
            shift 2
            ;;
        --filter)
            FILTER="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --no-compare)
            COMPARE=false
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=== Replay Transactions from Parquet ==="
echo "Parquet dir: ${PARQUET_DIR}"
echo "Count: ${COUNT}"
echo "Filter: ${FILTER}"
echo ""

# Check if parquet files exist
if [ ! -f "${PARQUET_DIR}/transactions.parquet" ]; then
    echo "Error: transactions.parquet not found in ${PARQUET_DIR}"
    exit 1
fi

# Query transactions
echo "Querying transactions..."
DIGESTS=$(duckdb -list -noheader -c "
SELECT tx_digest
FROM read_parquet('${PARQUET_DIR}/transactions.parquet')
WHERE ${FILTER}
ORDER BY checkpoint_num DESC
LIMIT ${COUNT}
" 2>/dev/null)

if [ -z "$DIGESTS" ]; then
    echo "No transactions found matching filter."
    exit 0
fi

# Count transactions
TX_COUNT=$(echo "$DIGESTS" | wc -l)
echo "Found ${TX_COUNT} transactions to replay"
echo ""

if [ "$DRY_RUN" = true ]; then
    echo "Dry run - would replay these digests:"
    echo "$DIGESTS"
    exit 0
fi

# Check if sui-sandbox exists
SANDBOX_BIN="${SANDBOX_DIR}/target/release/sui-sandbox"
if [ ! -f "$SANDBOX_BIN" ]; then
    echo "Building sui-sandbox..."
    (cd "$SANDBOX_DIR" && cargo build --release --bin sui-sandbox)
fi

# Replay each transaction
SUCCESS=0
FAILED=0
ERRORS=""

echo "Starting replay..."
echo ""

while IFS= read -r digest; do
    [ -z "$digest" ] && continue

    echo "=== Replaying: ${digest} ==="

    ARGS="replay ${digest}"
    if [ "$COMPARE" = true ]; then
        ARGS="$ARGS --compare"
    fi
    if [ "$VERBOSE" = true ]; then
        ARGS="$ARGS --verbose"
    fi

    if $SANDBOX_BIN $ARGS 2>&1; then
        echo "✓ Success"
        ((SUCCESS++)) || true
    else
        echo "✗ Failed"
        ((FAILED++)) || true
        ERRORS="${ERRORS}\n${digest}"
    fi
    echo ""
done <<< "$DIGESTS"

echo "=== Summary ==="
echo "Total: ${TX_COUNT}"
echo "Success: ${SUCCESS}"
echo "Failed: ${FAILED}"

if [ -n "$ERRORS" ]; then
    echo ""
    echo "Failed digests:"
    echo -e "$ERRORS"
fi
