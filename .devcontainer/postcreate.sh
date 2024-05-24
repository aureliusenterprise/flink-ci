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

# Ensure binaries are executable and add them to the PATH
chmod +x -R $PWD/bin
echo "export PATH=\"\$PATH:$PWD/bin\"" >> /home/dev/.bashrc
echo 'export PATH="$PATH:/home/linuxbrew/.linuxbrew/bin"' >> /home/dev/.bashrc
echo "source $PWD/.venv/bin/activate" >> /home/dev/.bashrc

# Install pre-commit hooks
poetry run pre-commit install

# Install local package for development
.venv/bin/pip install --no-deps -e .

# Prompt the user to set their git username and email if not already set
if [ -z "$(git config --global user.name)" ]; then
    read -p "Enter your Git username (full name): " git_username
    git config --global user.name "$git_username"
fi

if [ -z "$(git config --global user.email)" ]; then
    read -p "Enter your Git email: " git_email
    git config --global user.email "$git_email"
fi
