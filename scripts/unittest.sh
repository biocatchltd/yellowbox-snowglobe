#!/bin/sh
set -e
# run the unittests with branch coverage
python -m pytest --cov-branch --cov=./yellowbox_snowglobe --cov-report=xml --cov-report=html --cov-report=term-missing tests/ "$@"