# MWAA Local Runner Commands

# Default recipe - show available commands
default:
    @just --list

# Project name for docker compose
project := "aws-mwaa-local-runner-2_10_3"
compose_file := "./docker/docker-compose-local.yml"
override_file := "./docker/docker-compose-local.override.yml"

# =============================================================================
# First-Time Setup
# =============================================================================

# First-time setup for new teammates
setup:
    #!/usr/bin/env bash
    set -e
    echo "🚀 Setting up MWAA local runner..."
    echo ""
    
    # Create personal env config
    if [ ! -f docker/config/.env.localrunner ]; then
        echo "📝 Let's configure your environment..."
        echo ""
        
        # Prompt for AWS profile
        echo "Available AWS profiles:"
        grep '^\[profile' ~/.aws/config 2>/dev/null | sed 's/\[profile /  - /g' | sed 's/\]//g' || echo "  (none found)"
        echo ""
        read -p "Enter your AWS SSO profile name: " aws_profile
        
        # Prompt for Snowflake username
        read -p "Enter your Snowflake username (email): " snowflake_user
        
        # Create .env.localrunner with user values
        sed -e "s/your-profile-name/${aws_profile}/g" \
            -e "s/your.email@springhealth.com/${snowflake_user}/g" \
            docker/config/.env.example > docker/config/.env.localrunner
        
        echo ""
        echo "✅ Created docker/config/.env.localrunner with your settings"
    else
        echo "⏭️  docker/config/.env.localrunner already exists"
    fi
    
    # Create personal DAG mounts from template
    if [ ! -f docker/docker-compose-local.override.yml ]; then
        cp docker/docker-compose-local.override.example.yml docker/docker-compose-local.override.yml
        echo "✅ Created docker/docker-compose-local.override.yml from template"
    else
        echo "⏭️  docker/docker-compose-local.override.yml already exists"
    fi
    
    echo ""
    just setup-zscaler
    echo ""
    echo "📝 Next steps:"
    echo "   1. Edit docker/docker-compose-local.override.yml to mount your DAGs"
    echo ""
    echo "   2. Run: just build"
    echo "   3. Run: just start"

# Export Zscaler certificate for SSL in Docker
setup-zscaler:
    @echo "🔐 Setting up Zscaler certificate..."
    @if security find-certificate -a -c "Zscaler" -p /Library/Keychains/System.keychain > docker/config/zscaler-root-ca.crt 2>/dev/null; then \
        echo "✅ Zscaler certificate exported to docker/config/zscaler-root-ca.crt"; \
    else \
        echo "⚠️  No Zscaler certificate found (OK if not on corporate network)"; \
        rm -f docker/config/zscaler-root-ca.crt; \
    fi

# =============================================================================
# Build & Run
# =============================================================================

# Build the MWAA local runner image
build:
    ./mwaa-local-env build-image

# Helper to build compose command with optional override file
[private]
compose *args:
    #!/usr/bin/env bash
    if [ -f {{override_file}} ]; then
        docker compose -p {{project}} -f {{compose_file}} -f {{override_file}} {{args}}
    else
        echo "⚠️  No override file found. Run 'just setup' first to create one."
        echo "   Or create manually: cp docker/docker-compose-local.override.example.yml docker/docker-compose-local.override.yml"
        exit 1
    fi

# Start the local runner (includes postgres)
start:
    @just compose up -d

# Stop the local runner
stop:
    @just compose down

# Restart the local runner (picks up env file changes)
restart:
    @just compose up -d --force-recreate local-runner
    @echo "Waiting for Airflow to start..."
    @sleep 15
    @echo "Ready at http://localhost:8080"

# Quick restart (no recreate, just restart)
restart-quick:
    @just compose restart local-runner
    @echo "Waiting for Airflow to start..."
    @sleep 10
    @echo "Ready at http://localhost:8080"

# View logs from the local runner
logs:
    @just compose logs -f local-runner

# Reset database and restart fresh
reset:
    @just compose down
    rm -rf db-data
    @just compose up -d

# Open Airflow UI in browser
open:
    open http://localhost:8080

# Shell into the running container
shell:
    docker exec -it {{project}}-local-runner-1 bash

# Check container status
status:
    @just compose ps

