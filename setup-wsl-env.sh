#!/bin/bash
# WSL Setup Script for stormhub Python Environment
# Run this script from within WSL

set -e

echo "=========================================="
echo "Setting up stormhub Python environment"
echo "=========================================="

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "1. Updating system packages..."
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv python3-dev

# Check Python version
PYTHON_VERSION=$(python3 --version)
echo "   Python version: $PYTHON_VERSION"

echo ""
echo "2. Creating virtual environment..."
if [ -d ".venv" ]; then
    echo "   Virtual environment already exists, removing..."
    rm -rf .venv
fi
python3 -m venv .venv
echo "   Virtual environment created at .venv/"

echo ""
echo "3. Activating virtual environment..."
source .venv/bin/activate

echo ""
echo "4. Upgrading pip..."
pip install --upgrade pip setuptools wheel

echo ""
echo "5. Installing stormhub in editable mode with dev dependencies..."
pip install -e ".[dev]"

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "To activate the environment in the future, run:"
echo "  source .venv/bin/activate"
echo ""
echo "To deactivate, run:"
echo "  deactivate"
echo ""
