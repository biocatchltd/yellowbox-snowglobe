#!/bin/sh

set -e

echo "running ruff check..."
poetry run ruff check .  --fix --show-fixes
echo "running ruff format..."
poetry run ruff format .
echo "running mypy..."
python3 -m mypy --show-error-codes yellowbox_snowglobe
