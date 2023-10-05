#!/bin/bash

# Install dependencies
poetry install

# Download JAR files if not already present
while read -r url; do
    [ -z "$url" ] && continue
    filename=$(basename "$url")
    if [ -e jars/"$filename" ]; then
        echo "File jars/$filename already exists, skipping download."
        continue
    fi
    wget -P jars/ "$url"
done < jars/manifest

# Add current directory as safe repository location
git config --global --add safe.directory $PWD

# Install pre-commit hooks
poetry run pre-commit install
