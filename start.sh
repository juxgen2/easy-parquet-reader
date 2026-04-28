#!/usr/bin/env bash
set -e

VENV_DIR=".venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

echo "Activating virtual environment and installing requirements..."
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

echo "Starting Easy Parquet Reader at http://127.0.0.1:5000"
flask --app app run --host 127.0.0.1 --port 5000
