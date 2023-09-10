#!/bin/sh
# install poetry and the dev-dependencies of the project
python -m pip install --upgrade setuptools
curl -sSL https://install.python-poetry.org | python3 - --version 1.5.1
poetry update --lock
poetry install
