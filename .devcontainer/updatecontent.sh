#!/bin/bash

# Copy Apache Atlas configuration to shared folder
SOURCE_DIR=docker/docker-compose-atlas/*
TARGET_DIR=/tmp/atlas-config

cp -avf ${SOURCE_DIR} ${TARGET_DIR}

# Check if Poetry is installed
if ! command -v poetry &> /dev/null; then
    exit 0
fi

# Ensure installed dependencies match the lock file
if [ -d ".venv" ]; then
    poetry install
fi
