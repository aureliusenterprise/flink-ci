#!/bin/bash

# Check if Poetry is installed
if ! command -v poetry &> /dev/null; then
    exit 0
fi

# Ensure installed dependencies match the lock file
if [ -d ".venv" ]; then
    poetry install
fi
