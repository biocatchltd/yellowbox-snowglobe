#!/bin/sh
# run various linters
set -e
echo "formatting..."
python -m ruff format yellowbox_snowglobe tests
echo "sorting imports with ruff..."
python -m ruff check yellowbox_snowglobe tests --select I,F401 --fix --show-fixes