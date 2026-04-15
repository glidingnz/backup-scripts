#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Error: $PYTHON_BIN is not installed." >&2
  exit 1
fi

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtualenv in $VENV_DIR"
  if ! "$PYTHON_BIN" -m venv "$VENV_DIR"; then
    cat >&2 <<'EOF'
Error: failed to create the virtualenv.

On Debian/Ubuntu, install the venv package first:
  sudo apt install python3-venv

If your server uses a versioned package name, use:
  sudo apt install python3.12-venv
EOF
    exit 1
  fi
fi

echo "Installing Python dependencies"
"$VENV_DIR/bin/python" -m pip install -r "$SCRIPT_DIR/requirements.txt"

cat <<EOF

Setup complete.

Next steps:
  1. Copy .env.example to .env and fill in the real values
  2. Copy my.cnf.example to my.cnf and fill in the real values
  3. Make sure both files are owned by $(id -un):$(id -gn)
  4. Run: $VENV_DIR/bin/python $SCRIPT_DIR/backup.py --dry-run
EOF
