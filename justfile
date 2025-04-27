spark_submit_service := "iceberg-spark"

# Show help message
help:
    @just --list

# Build docker image with a local tag
build:
    uv pip compile pyproject.toml -o requirements.txt && \
    docker compose build

# Start jupyter lab server
jlab:
    docker compose up

# Start jupyter lab using without docker and spark
uv-jlab:
    uv run --with jupyter jupyter lab

# Lint Python files
lint-fix:
    ruff check --fix

# Format Python files
format:
    ruff format

# Clean Python build directories and artifacts
clean:
    rm -rf ./build ./dist ./src/*.egg-info

# Submit an Spark job
spark-submit script *args="":
    docker compose run --rm {{ spark_submit_service }} spark-submit "{{ script }}" {{ args }}

# Run bash inside Docker Compose spark-submit service
shell *args="":
    docker compose run --rm --entrypoint="/bin/bash {{ if args != "" { " -c '" + args + "'" } else { "" } }}" \
      {{ spark_submit_service }}

# Login to GitHub Container Registry
login-ghcr:
    #!/usr/bin/env bash
    set -euo pipefail
    if docker login ghcr.io > /dev/null 2>&1; then
        echo "You are currently logged in to GitHub Container Registry"
        read -p "Would you like to login again? (y/N): " relogin
        if [[ $relogin =~ ^[Yy]$ ]]; then
          read -p "GitHub username: " github_user
          read -sp "GitHub Personal Access Token: " github_pat
          echo
          echo "$github_pat" | docker login ghcr.io -u "$github_user" --password-stdin
        fi
    fi

