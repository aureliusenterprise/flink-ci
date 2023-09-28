#!/bin/bash

# Install dependencies
poetry install

# Install pre-commit hooks
poetry run pre-commit install
