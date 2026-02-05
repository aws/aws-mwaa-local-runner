#!/bin/sh

echo "Running startup script..."

# =============================================================================
# Load environment variables from dp_airflow_pipelines MWAA scripts
# =============================================================================
# This sources the same env vars used in actual MWAA, ensuring consistency.
# The startup-validation.sh file contains all secret names, bucket names, etc.

MWAA_ENV_FILE="/usr/local/airflow/mwaa_scripts/startup-validation.sh"

if [ -f "$MWAA_ENV_FILE" ]; then
    echo "Loading environment variables from dp_airflow_pipelines/mwaa..."
    
    # Extract and execute only 'export' statements (skip airflow commands, comments)
    eval "$(grep '^export ' "$MWAA_ENV_FILE")"
    
    echo "  ✓ Environment variables loaded from startup-validation.sh"
else
    echo "Warning: MWAA scripts not mounted. Environment variables may be missing."
    echo "         Expected: $MWAA_ENV_FILE"
fi

# =============================================================================
# Override for local development
# =============================================================================
# These override the MWAA values for local testing

export EXECUTION_ENVIRONMENT='local'

# Allow INGEST_CONFIG_URL to be overridden via .env.localrunner
# Default to local Config API if not set
if [ -z "$INGEST_CONFIG_URL" ] || [ "$INGEST_CONFIG_URL" = "http://ingest-config-nlb-validation-5dac583427c273d3.elb.us-east-1.amazonaws.com" ]; then
    export INGEST_CONFIG_URL="http://host.docker.internal:3000"
    echo "  ✓ INGEST_CONFIG_URL set to local: $INGEST_CONFIG_URL"
fi

# =============================================================================
# Install requirements from dp_airflow_pipelines (if mounted)
# =============================================================================
if [ -d /usr/local/airflow/dp_requirements ]; then
    echo "Installing requirements from dp_airflow_pipelines..."
    
    # Install MWAA requirements (main dependencies)
    if [ -f /usr/local/airflow/dp_requirements/mwaa-requirements.txt ]; then
        pip install --quiet --no-cache-dir -r /usr/local/airflow/dp_requirements/mwaa-requirements.txt
        echo "  ✓ mwaa-requirements.txt"
    fi
    
    echo "Requirements installed."
else
    echo "Warning: dp_requirements not mounted. Using local requirements only."
fi

echo "Startup script complete."
