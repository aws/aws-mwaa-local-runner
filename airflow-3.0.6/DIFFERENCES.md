# Differences: MWAA Local Runner 3.0.6 vs 2.10.3

This document highlights the key differences between MWAA Local Runner 3.0.6 and 2.10.3.

## Quick Comparison

| Aspect | MWAA 2.10.3 | MWAA 3.0.6 |
|--------|-------------|------------|
| **Airflow Version** | 2.10.3 | 3.0.6 |
| **Python Version** | 3.11 | 3.12 |
| **Docker Image** | Custom build | `amazon-mwaa-docker-images/airflow:3.0.6-dev` |
| **Services** | Single `local-runner` | Separate `webserver` and `scheduler` |
| **Port (Webserver)** | 8080 | 8081 |
| **Port (Database)** | 5432 | 5433 |
| **Environment File** | `mwaa-local-env` | `.env` in `docker/` directory |
| **Execution API** | Not required | **Required** (even for LocalExecutor) |
| **DAG Syntax** | `schedule_interval`, `execution_date` | `schedule`, `logical_date` |

## Architecture Differences

### Service Structure

**2.10.3:**
- Single `local-runner` service handles webserver and scheduler

**3.0.6:**
- Separate `webserver` service (port 8081)
- Separate `scheduler` service
- Separate `migratedb` service (runs once)

### Execution API

**2.10.3:**
- Direct database access
- No Execution API required

**3.0.6:**
- **Execution API required** (even for LocalExecutor)
- All executors communicate via API
- Must configure `MWAA__CORE__API_SERVER_URL`
- JWT authentication required (`AIRFLOW__API_AUTH__JWT_SECRET`)

## Configuration Differences

### Environment Variables

**2.10.3:**
```bash
# Uses mwaa-local-env file
source mwaa-local-env
docker-compose up
```

**3.0.6:**
```bash
# Uses .env file (automatically read by Docker Compose)
# Create docker/.env file
docker-compose -f docker-compose-local.yml up
```

### Docker Compose File

**2.10.3:**
- Single service: `local-runner`
- Port 8080

**3.0.6:**
- Multiple services: `webserver`, `scheduler`, `migratedb`
- Port 8081 (to avoid conflicts)
- Requires Execution API configuration

## DAG Syntax Differences

### Schedule

**2.10.3:**
```python
dag = DAG(
    'my_dag',
    schedule_interval=timedelta(days=1),
    ...
)
```

**3.0.6:**
```python
with DAG(
    dag_id='my_dag',
    schedule="0 0 * * *",  # or timedelta(days=1)
    ...
) as dag:
```

### Date References

**2.10.3:**
```python
def my_task(**context):
    execution_date = context['execution_date']
    # ...
```

**3.0.6:**
```python
def my_task(**context):
    logical_date = context['logical_date']  # or data_interval_start
    # ...
```

### Operator Changes

**2.10.3:**
```python
from airflow.providers.http.operators.http import SimpleHttpOperator
```

**3.0.6:**
```python
from airflow.providers.http.operators.http import HttpOperator
```

**2.10.3:**
```python
PythonOperator(
    task_id='task',
    python_callable=func,
    provide_context=True,  # Explicit
)
```

**3.0.6:**
```python
PythonOperator(
    task_id='task',
    python_callable=func,
    # provide_context removed (always True)
)
```

## Port Configuration

| Service | 2.10.3 | 3.0.6 |
|---------|--------|-------|
| Webserver | 8080 | 8081 |
| Database | 5432 | 5433 |

Ports are different to allow running both versions simultaneously.

## Container Names

**2.10.3:**
- Database: `mwaa-db`
- Local Runner: `local-runner-1`

**3.0.6:**
- Database: `mwaa-306-db`
- Webserver: `docker-webserver-1`
- Scheduler: `docker-scheduler-1`

## File Structure

**2.10.3:**
```
aws-mwaa-local-runner-2.10.3/
├── docker/
│   ├── mwaa-local-env      # Environment variables
│   └── docker-compose-local.yml
└── ...
```

**3.0.6:**
```
aws-mwaa-local-runner-3.0.6/
├── docker/
│   ├── .env                 # Environment variables (equivalent to mwaa-local-env)
│   └── docker-compose-local.yml
└── ...
```

## Package Versions

### MWAA 2.10.3

**Constraint File:**
```
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt"
```

**Common Provider Versions:**
- `apache-airflow-providers-snowflake==5.8.0`
- `apache-airflow-providers-mysql==5.7.3`

### MWAA 3.0.6

**Constraint File:**
```
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt"
```

**Common Provider Versions:**
- `apache-airflow-providers-snowflake==5.5.0` (or check constraint file for latest compatible version)
- Note: Provider versions are determined by constraint files, not by Airflow version number

**Important Note on Provider Versions:**

Provider version numbers don't always increase with Airflow versions. This happens because:

1. **Constraint Files Control Compatibility**: Each Airflow version has a constraint file that specifies which provider versions are tested and compatible
2. **API Changes**: Airflow 3.0 introduced breaking changes that may require specific provider versions
3. **Testing & Stability**: Newer provider versions may not be tested with newer Airflow versions yet
4. **Dependency Conflicts**: Higher provider versions may have dependency conflicts with Airflow 3.0.6

**Always check the constraint file** for your specific Airflow version to find the compatible provider versions:
- For 2.10.3: Check `constraints-2.10.3/constraints-3.11.txt`
- For 3.0.6: Check `constraints-3.0.6/constraints-3.12.txt`

**Example:**
```bash
# View compatible versions for 3.0.6
curl https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt | grep snowflake
```

## Migration Checklist

When migrating DAGs from 2.10.3 to 3.0.6:

- [ ] Update `schedule_interval` → `schedule`
- [ ] Update `execution_date` → `logical_date` or `data_interval_start`
- [ ] Update `SimpleHttpOperator` → `HttpOperator`
- [ ] Remove `provide_context=True` from operators
- [ ] Update imports if using deprecated operators
- [ ] Test DAG parsing in 3.0.6 environment
- [ ] Update environment variable configuration (`.env` file)
- [ ] Update package versions and constraint files

## Running Both Versions

You can run both versions simultaneously:

- **MWAA 2.10.3:** http://localhost:8080
- **MWAA 3.0.6:** http://localhost:8081

Container names and ports are configured to avoid conflicts.

## Additional Resources

- [Airflow 3.0.6 New Features](docs/AIRFLOW_3.0.6_NEW_FEATURES.md)
- [Environment Variables Guide](docker/README_ENV.md)

