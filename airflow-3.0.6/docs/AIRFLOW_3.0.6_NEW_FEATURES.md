# Airflow 3.0.6 New Features (vs 2.10.1)

This document outlines the major new features and improvements in Apache Airflow 3.0.6 that were not available in Airflow 2.10.1.

## Sample DAGs

Sample DAGs demonstrating each feature are available in the `dags/` folder:

- **`sample_execution_api_dag.py`** - Execution API features and configuration
- **`sample_dag_versioning_dag.py`** - DAG versioning and change tracking
- **`sample_backfill_improvements_dag.py`** - Backfill improvements and data intervals
- **`sample_remote_execution_dag.py`** - Remote execution patterns
- **`sample_airflow_3_syntax_dag.py`** - New syntax and migration examples
- **`sample_security_improvements_dag.py`** - Security enhancements (JWT, secrets)

These DAGs are automatically loaded by Airflow and can be viewed in the Airflow UI.

## Table of Contents
1. [Execution API](#execution-api)
2. [DAG Versioning](#dag-versioning)
3. [Backfill Improvements](#backfill-improvements)
4. [Remote Execution](#remote-execution)
5. [Other Major Features](#other-major-features)
6. [Breaking Changes & Migration Notes](#breaking-changes--migration-notes)

---

## Execution API

### Overview
Airflow 3.0 introduces a **new Execution API** that replaces the direct database access pattern used in 2.x. This API provides a standardized way for executors to interact with Airflow's execution layer.

### Key Features
- **RESTful API**: Executors communicate via HTTP/HTTPS instead of direct database connections
- **JWT Authentication**: Secure token-based authentication for API access
- **Decoupled Architecture**: Better separation between scheduler, executor, and workers
- **Required for All Executors**: Even LocalExecutor now requires the Execution API to be configured

### Configuration
```python
# Required in Airflow 3.0.6
AIRFLOW__CORE__API_SERVER_URL = "http://webserver:8080"
AIRFLOW__API_AUTH__JWT_SECRET = "your-secret-key-min-32-chars"
```

### Benefits
- **Security**: Reduced direct database access
- **Scalability**: Better support for distributed execution
- **Consistency**: Unified execution interface across all executors
- **Cloud-Native**: Aligns with modern microservices architecture

### Impact on MWAA Local Runner
- Must configure `MWAA__CORE__API_SERVER_URL` in docker-compose
- Webserver must be running before scheduler starts
- JWT secret must be configured for API authentication

### Sample DAG
See `dags/sample_execution_api_dag.py` for a working example demonstrating Execution API configuration and usage.

---

## DAG Versioning

### Overview
Airflow 3.0 introduces improved DAG versioning and change tracking capabilities.

### Key Features

#### 1. **DAG Serialization Versioning**
- DAGs are serialized with version information
- Better tracking of DAG changes over time
- Improved conflict detection

#### 2. **DAG Bag Versioning**
- Each DAG bag (collection of DAGs) has a version
- Changes to DAG files trigger version updates
- Helps track when DAGs were last modified

#### 3. **Task Instance Versioning**
- Task instances track which DAG version they were created from
- Better audit trail for task execution history
- Helps with debugging and compliance

### Usage Example
```python
# Airflow 3.0 tracks DAG versions automatically
# You can query DAG version information via the API or UI
from airflow.models import DagBag

dag_bag = DagBag()
for dag_id, dag in dag_bag.dags.items():
    print(f"DAG {dag_id} version: {dag.version}")
```

### Benefits
- **Audit Trail**: Know exactly which DAG version ran at what time
- **Debugging**: Easier to identify when issues were introduced
- **Compliance**: Better tracking for regulated environments
- **Rollback**: Easier to identify and revert to previous DAG versions

### Sample DAG
See `dags/sample_dag_versioning_dag.py` for examples of DAG version tracking and task instance versioning.

---

## Backfill Improvements

### Overview
Airflow 3.0.6 includes significant improvements to the backfill functionality.

### Key Features

#### 1. **Improved Backfill Performance**
- Better parallelization of backfill runs
- Optimized database queries for large backfill operations
- Reduced memory footprint during backfills

#### 2. **Enhanced Backfill Control**
- Better handling of task dependencies during backfill
- Improved retry logic for failed backfill tasks
- More granular control over backfill execution

#### 3. **Backfill API Improvements**
- RESTful API endpoints for backfill operations
- Better status tracking and monitoring
- Programmatic backfill management

#### 4. **Data Interval Awareness**
- Airflow 3.0 uses `data_interval_start` and `data_interval_end` instead of `execution_date`
- Backfills respect data intervals more accurately
- Better alignment with data pipeline semantics

### Usage Example
```python
# Airflow 3.0 backfill with data intervals
from airflow.models import DagRun
from airflow.utils.dates import days_ago

# Backfill respects data intervals
dag_run = DagRun.create(
    dag_id="my_dag",
    run_type="backfill",
    execution_date=days_ago(1),
    data_interval_start=days_ago(1),
    data_interval_end=days_ago(0),
    state="running"
)
```

### Migration from 2.10.1
- Replace `execution_date` with `logical_date` or `data_interval_start`
- Update DAG code to use new interval-based scheduling
- Review backfill strategies to leverage new features

### Sample DAG
See `dags/sample_backfill_improvements_dag.py` for examples of data intervals and improved backfill logic.

---

## Remote Execution

### Overview
Airflow 3.0 introduces enhanced support for remote execution patterns.

### Key Features

#### 1. **Execution API for Remote Workers**
- Workers can execute tasks remotely via the Execution API
- No direct database access required for workers
- Better security and isolation

#### 2. **Kubernetes Executor Enhancements**
- Improved pod management and lifecycle
- Better resource allocation and scaling
- Enhanced integration with Kubernetes APIs

#### 3. **Celery Executor Improvements**
- Better message queue handling
- Improved worker registration and health checks
- Enhanced task routing and load balancing

#### 4. **Remote Task Execution**
- Tasks can be executed on remote systems
- Better support for distributed computing
- Improved integration with cloud services

### Configuration Example
```python
# Remote execution via Execution API
AIRFLOW__CORE__EXECUTOR = "CeleryExecutor"
AIRFLOW__CORE__API_SERVER_URL = "http://webserver:8080"
AIRFLOW__CELERY__BROKER_URL = "redis://redis:6379/0"
AIRFLOW__CELERY__RESULT_BACKEND = "db+postgresql://airflow:airflow@postgres/airflow"
```

### Benefits
- **Scalability**: Better support for distributed execution
- **Security**: Reduced attack surface with API-based communication
- **Flexibility**: Easier to integrate with external systems
- **Cloud-Native**: Better alignment with modern infrastructure

### Sample DAG
See `dags/sample_remote_execution_dag.py` for examples of remote execution patterns with Celery and Kubernetes executors.

---

## Other Major Features

### 1. **Python 3.12 Support**
- Full support for Python 3.12
- Improved performance with newer Python versions
- Better type hinting support

### 2. **Improved Task Scheduling**
- Better handling of task dependencies
- Improved scheduling algorithms
- Enhanced support for complex DAG patterns

### 3. **Enhanced UI/UX**
- Improved Airflow UI responsiveness
- Better visualization of DAG runs
- Enhanced task instance details view
- Improved search and filtering capabilities

### 4. **Better Error Handling**
- More descriptive error messages
- Improved stack traces
- Better logging and debugging tools

### 5. **Provider Package Improvements**
- Updated provider packages with new features
- Better compatibility with external systems
- Improved documentation and examples

### 6. **Security Enhancements**
- JWT-based authentication for APIs
- Improved secret management
- Better access control mechanisms

### 7. **Performance Improvements**
- Optimized database queries
- Better caching mechanisms
- Reduced memory footprint
- Improved scheduler performance

### 8. **DAG Syntax Changes**
- `schedule_interval` deprecated in favor of `schedule`
- `execution_date` deprecated in favor of `logical_date`
- Better support for modern Python features

### Example: New DAG Syntax
```python
# Airflow 2.10.1 (old)
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=True,
)

def my_task(**context):
    execution_date = context['execution_date']
    # ...

# Airflow 3.0.6 (new)
from airflow import DAG
import pendulum

with DAG(
    dag_id='my_dag',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 0 * * *",  # cron expression or timedelta
    catchup=True,
) as dag:
    
    def my_task(**context):
        logical_date = context['logical_date']  # or data_interval_start
        # ...
```

### Sample DAGs
- **`sample_airflow_3_syntax_dag.py`** - Complete examples of new syntax, schedule options, and data intervals
- **`sample_security_improvements_dag.py`** - JWT authentication, secret management, and access control

---

## Breaking Changes & Migration Notes

### 1. **Execution API Required**
- **Breaking**: All executors now require Execution API configuration
- **Action**: Configure `AIRFLOW__CORE__API_SERVER_URL` and JWT secret

### 2. **DAG Parameter Changes**
- **Breaking**: `schedule_interval` → `schedule`
- **Breaking**: `execution_date` → `logical_date` or `data_interval_start`
- **Action**: Update all DAG definitions

### 3. **HTTP Provider Changes**
- **Breaking**: `SimpleHttpOperator` removed in favor of `HttpOperator`
- **Action**: Update imports and operator usage

### 4. **Snowflake Provider Changes**
- **Breaking**: `SnowflakeOperator` deprecated in newer provider versions
- **Action**: Use `SQLExecuteQueryOperator` with Snowflake connection or `SnowflakeSqlApiOperator`

### 5. **Python Context Changes**
- **Breaking**: `provide_context` parameter removed (always True now)
- **Action**: Remove `provide_context=True` from operator calls

### 6. **Database Schema Changes**
- **Breaking**: New database schema with additional tables
- **Action**: Run `airflow db upgrade` when upgrading

### 7. **Constraint File Requirements**
- **Breaking**: Must use constraints file for installation
- **Action**: Always install with `--constraint` flag

---

## Migration Checklist

When migrating from Airflow 2.10.1 to 3.0.6:

- [ ] Update Python version (3.9, 3.10, 3.11, or 3.12)
- [ ] Configure Execution API (`AIRFLOW__CORE__API_SERVER_URL`)
- [ ] Set JWT secret (`AIRFLOW__API_AUTH__JWT_SECRET`)
- [ ] Update DAG syntax (`schedule_interval` → `schedule`)
- [ ] Replace `execution_date` with `logical_date` or `data_interval_start`
- [ ] Update HTTP operator imports (`SimpleHttpOperator` → `HttpOperator`)
- [ ] Review Snowflake provider usage
- [ ] Remove `provide_context=True` from operators
- [ ] Run database migrations (`airflow db upgrade`)
- [ ] Update requirements.txt with constraints
- [ ] Test all DAGs thoroughly
- [ ] Update documentation and runbooks

---

## Additional Resources

- [Airflow 3.0.6 Documentation](https://airflow.apache.org/docs/apache-airflow/3.0.6/)
- [Airflow Upgrade Guide](https://airflow.apache.org/docs/apache-airflow/stable/upgrading.html)
- [Execution API Documentation](https://airflow.apache.org/docs/apache-airflow/stable/executor/execution-api.html)
- [DAG Versioning Guide](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#dag-versioning)

---

## Summary

Airflow 3.0.6 represents a significant evolution from 2.10.1, with major architectural improvements focused on:

1. **Modern API-based execution** (Execution API)
2. **Better versioning and tracking** (DAG versioning)
3. **Improved backfill capabilities**
4. **Enhanced remote execution support**
5. **Modern Python and cloud-native patterns**

These changes provide better scalability, security, and maintainability, but require careful migration planning and code updates.

