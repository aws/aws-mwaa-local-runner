# MWAA Local Runner - Quick Start Guide

Get your local Airflow environment running in 5 minutes.

## When to Use This

| Scenario | Use Local Runner? | Why |
|----------|-------------------|-----|
| Developing/debugging DAGs in `dp_airflow_pipelines` | Yes | Faster iteration, see logs immediately |
| Testing DAG changes before pushing to validation | Yes | Catch errors locally first |
| Changes are in a separate service (e.g., `dp_ate`, `dp_config_api`) | No | Just trigger DAGs in MWAA Validation instead |
| Quick one-off DAG run | No | Faster to use MWAA Validation directly |

**Rule of thumb**: If your code changes are in `dp_airflow_pipelines`, use the local runner. If your changes are elsewhere and you just need to trigger a DAG, use MWAA Validation.

## Prerequisites

Before starting, ensure you have:

- [ ] Docker Desktop installed and running
- [ ] `just` command runner (`brew install just`)
- [ ] Access to this role AWSAdministratorAccess
- [ ] AWS CLI configured with SSO access
- [ ] Access to `dp_airflow_pipelines` repo

## Step 1: Clone the Repos

Both repos must be side-by-side in the same parent directory:

```bash
cd <your_git_repo_directory>

# Clone this repo
git clone git@github.com:SpringCare/dp-aws-mwaa-local-runner.git

# Clone the DAGs repo (if you haven't already)
git clone git@github.com:SpringCare/dp_airflow_pipelines.git
```

Your folder structure should look like:
```
<your_git_repo_directory_path>
├── dp-aws-mwaa-local-runner/   ← You are here
└── dp_airflow_pipelines/       ← DAG source code
```

## Step 2: Run Setup

```bash
cd dp-aws-mwaa-local-runner
just setup
```

This creates two files (both gitignored):
- `docker/config/.env.localrunner` - Your personal environment config
- `docker/docker-compose-local.override.yml` - Your personal DAG mounts

## Step 3: Configure Your Environment - if needed

Edit `docker/config/.env.localrunner` with your personal values:

**Note**: Most environment variables (secret names, bucket names, API URLs) are automatically loaded from `dp_airflow_pipelines/mwaa/startup-validation.sh`. You only need to set personal values here. See [Environment Variables](#environment-variables) for details.

## Step 4: Mount Your DAGs

Edit `docker/docker-compose-local.override.yml` to add the DAGs you want to work with:

```yaml
services:
  local-runner:
    volumes:
      # Add one line per DAG you want to work on
      - "${DP_AIRFLOW_PATH:-${PWD}/../dp_airflow_pipelines}/src/dags/your_dag_name:/usr/local/airflow/dags/your_dag_name"
```

**Example** - To work on `census_pipeline_execution`:
```yaml
services:
  local-runner:
    volumes:
      - "${DP_AIRFLOW_PATH:-${PWD}/../dp_airflow_pipelines}/src/dags/census_pipeline_execution:/usr/local/airflow/dags/census_pipeline_execution"
```

## Step 5: Login to AWS

Make sure you're logged into AWS using SSO.
Note: make sure your profile has a region in your `~./aws/config` file.

## Step 6: Build & Start

```bash
# Build the Docker image (takes a few minutes first time)
just build

# Start the containers
just start

# Open the Airflow UI
just open
```

## Step 7: Login to Airflow

- **URL**: http://localhost:8080
- **Username**: `admin`
- **Password**: `admin`

Your DAG should appear within ~30 seconds.

## Common Commands

| Command | What it does |
|---------|--------------|
| `just start` | Start containers |
| `just stop` | Stop containers |
| `just restart` | Restart (needed after env changes) |
| `just logs` | View container logs |
| `just shell` | Shell into the container |
| `just open` | Open Airflow UI in browser |
| `just reset` | Wipe database and restart fresh |
| `just sso-login` | Refresh AWS credentials |

## Environment Variables

### How It Works

Environment variables are loaded from **two sources** in this order:

1. **`docker/config/.env.localrunner`** - Your personal settings (loaded first by Docker)
2. **`dp_airflow_pipelines/mwaa/startup-validation.sh`** - Shared team config (sourced on startup, overwrites duplicates)

The MWAA startup script provides all secret names, bucket names, and API URLs automatically. You only need to set personal values that aren't in the startup script.

### What Goes Where

| Variable Type | Where to Set | Example |
|--------------|--------------|---------|
| Secret names, buckets, API URLs | `startup-validation.sh` (auto-loaded) | `SF_CREDS_DBT_SECRET_NAME` |
| Personal settings (not in startup script) | `.env.localrunner` | `AWS_PROFILE`, `SNOWFLAKE_USERNAME` |

**Note**: You cannot override variables from `startup-validation.sh` via `.env.localrunner` - the startup script runs later and overwrites them. The exception is `INGEST_CONFIG_URL`, which has special handling to preserve your local value.

## When to Restart

| Change | Restart needed? |
|--------|-----------------|
| DAG code changes | No - auto-reloads in ~30s |
| `.env.localrunner` changes | Yes - `just restart` |
| Adding new DAG mount | Yes - `just restart` |
| Requirements changes | Yes - `just restart` |
| `startup-validation.sh` changes | Yes - `just restart` |

## Airflow Variables
TBD

## Troubleshooting

### DAG not appearing
```bash
just logs  # Check for import errors
```

### AWS credentials expired
```bash
just sso-login  # No restart needed
```

### SSL certificate errors (Zscaler)
```bash
just setup-zscaler
just build
just restart
```

### Things completely broken
```bash
just reset  # Nuclear option - wipes database
```

## Next Steps

- See [SET_UP_MWAA.md](SET_UP_MWAA.md) for detailed configuration options
- See [README.md](README.md) for full documentation

