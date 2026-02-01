#!/bin/bash
# Full setup script for walrus-checkpoint-indexing
#
# Usage: ./scripts/setup.sh
#
# This script installs all required dependencies:
# - Walrus CLI (for downloading checkpoint blobs)
# - DuckDB CLI (for querying Parquet files)
# - Python packages (optional, for data analysis)

set -e

echo "=========================================="
echo "  Walrus Checkpoint Indexing - Setup"
echo "=========================================="
echo ""

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$ARCH" in
    x86_64|amd64) ARCH="x86_64" ;;
    aarch64|arm64) ARCH="aarch64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

case "$OS" in
    linux) PLATFORM="linux" ;;
    darwin) PLATFORM="macos" ;;
    *) echo "Unsupported OS: $OS"; exit 1 ;;
esac

# Determine install location
INSTALL_DIR="/usr/local/bin"
if [ ! -w "$INSTALL_DIR" ]; then
    INSTALL_DIR="$HOME/.local/bin"
    mkdir -p "$INSTALL_DIR"
fi

# ====================
# 1. Install Walrus CLI
# ====================
echo "[1/3] Checking Walrus CLI..."

if command -v walrus &> /dev/null; then
    echo "  Already installed: $(walrus --version 2>/dev/null || echo 'walrus')"
else
    echo "  Downloading Walrus CLI from GitHub..."

    # Get latest mainnet release URL
    WALRUS_VERSION=$(curl -sL "https://api.github.com/repos/MystenLabs/walrus/releases" 2>/dev/null | \
        grep -o '"tag_name": "mainnet-[^"]*"' | head -1 | cut -d'"' -f4)

    if [ -z "$WALRUS_VERSION" ]; then
        WALRUS_VERSION="mainnet-v1.40.3"  # fallback
    fi

    # Determine platform
    case "$PLATFORM-$ARCH" in
        linux-x86_64)
            WALRUS_FILE="walrus-${WALRUS_VERSION}-ubuntu-x86_64.tgz"
            ;;
        linux-aarch64)
            WALRUS_FILE="walrus-${WALRUS_VERSION}-ubuntu-aarch64.tgz"
            ;;
        macos-x86_64)
            WALRUS_FILE="walrus-${WALRUS_VERSION}-macos-x86_64.tgz"
            ;;
        macos-aarch64)
            WALRUS_FILE="walrus-${WALRUS_VERSION}-macos-arm64.tgz"
            ;;
        *)
            echo "  Unsupported platform: $PLATFORM-$ARCH"
            echo "  Install manually from: https://docs.wal.app/usage/setup.html"
            WALRUS_FILE=""
            ;;
    esac

    if [ -n "$WALRUS_FILE" ]; then
        WALRUS_URL="https://github.com/MystenLabs/walrus/releases/download/${WALRUS_VERSION}/${WALRUS_FILE}"
        TEMP_DIR=$(mktemp -d)

        echo "  Downloading: $WALRUS_URL"
        if curl -fsSL "$WALRUS_URL" -o "$TEMP_DIR/walrus.tgz" 2>/dev/null; then
            tar -xzf "$TEMP_DIR/walrus.tgz" -C "$TEMP_DIR"
            # Find the walrus binary in extracted files
            WALRUS_BIN=$(find "$TEMP_DIR" -name "walrus" -type f -executable 2>/dev/null | head -1)
            if [ -z "$WALRUS_BIN" ]; then
                WALRUS_BIN=$(find "$TEMP_DIR" -name "walrus" -type f 2>/dev/null | head -1)
            fi
            if [ -n "$WALRUS_BIN" ]; then
                chmod +x "$WALRUS_BIN"
                mv "$WALRUS_BIN" "$INSTALL_DIR/walrus"
                echo "  Installed to $INSTALL_DIR/walrus"
            else
                echo "  Failed to find walrus binary in archive"
            fi
        else
            echo "  Failed to download Walrus CLI"
            echo "  Install manually from: https://docs.wal.app/usage/setup.html"
        fi
        rm -rf "$TEMP_DIR"
    fi
fi

# ====================
# 2. Install DuckDB CLI
# ====================
echo ""
echo "[2/3] Checking DuckDB CLI..."

if command -v duckdb &> /dev/null; then
    echo "  Already installed: $(duckdb --version 2>/dev/null | head -1)"
else
    echo "  Downloading DuckDB CLI..."

    # DuckDB version and URL
    DUCKDB_VERSION="v1.2.0"
    case "$PLATFORM-$ARCH" in
        linux-x86_64)
            DUCKDB_FILE="duckdb_cli-linux-amd64.zip"
            ;;
        linux-aarch64)
            DUCKDB_FILE="duckdb_cli-linux-aarch64.zip"
            ;;
        macos-x86_64)
            DUCKDB_FILE="duckdb_cli-osx-universal.zip"
            ;;
        macos-aarch64)
            DUCKDB_FILE="duckdb_cli-osx-universal.zip"
            ;;
        *)
            echo "  Unsupported platform for DuckDB: $PLATFORM-$ARCH"
            DUCKDB_FILE=""
            ;;
    esac

    if [ -n "$DUCKDB_FILE" ]; then
        DUCKDB_URL="https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/${DUCKDB_FILE}"
        TEMP_DIR=$(mktemp -d)

        if curl -fsSL "$DUCKDB_URL" -o "$TEMP_DIR/duckdb.zip" 2>/dev/null; then
            unzip -q "$TEMP_DIR/duckdb.zip" -d "$TEMP_DIR"
            chmod +x "$TEMP_DIR/duckdb"
            mv "$TEMP_DIR/duckdb" "$INSTALL_DIR/duckdb"
            echo "  Installed to $INSTALL_DIR/duckdb"
        else
            echo "  Failed to download DuckDB"
            echo "  Install manually: brew install duckdb (macOS) or from https://duckdb.org"
        fi
        rm -rf "$TEMP_DIR"
    fi
fi

# ====================
# 3. Python packages (optional, using uv)
# ====================
echo ""
echo "[3/3] Checking Python packages..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/.."
REQUIREMENTS="$PROJECT_DIR/requirements.txt"

# Check if uv is installed, if not install it
if ! command -v uv &> /dev/null; then
    echo "  Installing uv (fast Python package manager)..."
    if curl -LsSf https://astral.sh/uv/install.sh | sh; then
        echo "  uv installed successfully"
    else
        echo "  Failed to install uv automatically"
        echo "  Install manually: curl -LsSf https://astral.sh/uv/install.sh | sh"
    fi
    # Add uv to PATH for this session
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
fi

if command -v uv &> /dev/null; then
    # Create venv if it doesn't exist
    if [ ! -d "$PROJECT_DIR/.venv" ]; then
        echo "  Creating Python virtual environment..."
        uv venv "$PROJECT_DIR/.venv" 2>/dev/null
    fi

    # Check if key packages are installed
    if VIRTUAL_ENV="$PROJECT_DIR/.venv" uv run python -c "import duckdb, polars" 2>/dev/null; then
        echo "  Python packages already installed (duckdb, polars)"
    else
        echo "  Installing Python packages with uv..."
        if [ -f "$REQUIREMENTS" ]; then
            uv pip install --python "$PROJECT_DIR/.venv/bin/python" -r "$REQUIREMENTS" 2>/dev/null && \
                echo "  Installed packages from requirements.txt" || \
                echo "  Failed to install some packages (optional)"
        else
            uv pip install --python "$PROJECT_DIR/.venv/bin/python" duckdb polars pyarrow 2>/dev/null && \
                echo "  Installed: duckdb, polars, pyarrow" || \
                echo "  Failed to install packages (optional)"
        fi
    fi
    echo "  Activate venv with: source .venv/bin/activate"
else
    echo "  uv not found, trying pip fallback..."
    if command -v python3 &> /dev/null || command -v python &> /dev/null; then
        PYTHON_CMD=$(command -v python3 || command -v python)
        if [ -f "$REQUIREMENTS" ]; then
            $PYTHON_CMD -m pip install -q -r "$REQUIREMENTS" 2>/dev/null && \
                echo "  Installed packages from requirements.txt" || \
                echo "  Failed to install (optional, run: uv pip install -r requirements.txt)"
        fi
    else
        echo "  Python not found (optional - skip Python packages)"
    fi
fi

# ====================
# Summary
# ====================
echo ""
echo "=========================================="
echo "  Setup Complete!"
echo "=========================================="
echo ""

# Check what's installed
echo "Installed tools:"
if command -v walrus &> /dev/null; then
    echo "  [x] walrus CLI"
else
    echo "  [ ] walrus CLI (REQUIRED - install manually)"
fi

if command -v duckdb &> /dev/null; then
    echo "  [x] duckdb CLI"
else
    echo "  [ ] duckdb CLI (optional)"
fi

if command -v uv &> /dev/null; then
    echo "  [x] uv (Python package manager)"
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_DIR="$SCRIPT_DIR/.."
    if [ -f "$PROJECT_DIR/.venv/bin/python" ] && "$PROJECT_DIR/.venv/bin/python" -c "import duckdb" 2>/dev/null; then
        echo "  [x] Python duckdb (in .venv)"
    else
        echo "  [ ] Python duckdb (optional, run: uv pip install -r requirements.txt)"
    fi
else
    echo "  [ ] uv (optional, install: curl -LsSf https://astral.sh/uv/install.sh | sh)"
fi

# PATH reminder
if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
    echo ""
    echo "Note: Add $INSTALL_DIR to your PATH:"
    echo "  export PATH=\"\$PATH:$INSTALL_DIR\""
fi

echo ""
echo "Next steps:"
echo "  cargo run --release --example alt_checkpoint_parquet -- \\"
echo "    --start 238954764 --end 238981764 \\"
echo "    --output-dir ./parquet_output \\"
echo "    --parallel-prefetch --prefetch-concurrency 3 --show-progress"
