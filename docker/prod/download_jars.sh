#!/bin/bash

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