# Requirements Documentation

This folder contains Python package requirements for MWAA Local Runner 3.0.6.

## Current Requirements

### Minimal Requirements (for sample DAGs)

- **`pendulum>=3.0.0`** - Date handling library (used by `sample_airflow_3_syntax_dag.py`)
- **`boto3==1.35.7`** - AWS SDK for Python (commonly used for AWS services)

## Adding Additional Packages

If you need additional packages for your own DAGs, add them to `requirements.txt`. Common packages include:

- `apache-airflow-providers-snowflake==5.5.0` - For Snowflake integration (3.0.6)
- `apache-airflow-providers-mysql==5.7.3` - For MySQL integration
- `slack-sdk==3.31.0` - For Slack notifications
- `pyyaml==6.0.2` - For YAML parsing
- `pyarrow==16.1.0` - For Parquet file handling
- `s3fs==2024.6.1` - For S3 file system operations

## Constraint Files

**Always use constraint files** when installing Airflow packages to ensure compatibility:

### MWAA 2.10.3
```bash
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt"
```

### MWAA 3.0.6
```bash
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt"
```

## Why Provider Versions Differ Between Airflow Versions

Provider version numbers don't always increase with Airflow versions. This happens because:

1. **Constraint Files Control Compatibility**: Each Airflow version has a constraint file that specifies which provider versions are tested and compatible
2. **API Changes**: Airflow 3.0 introduced breaking changes that may require specific provider versions
3. **Testing & Stability**: Newer provider versions may not be tested with newer Airflow versions yet
4. **Dependency Conflicts**: Higher provider versions may have dependency conflicts with Airflow 3.0.6

### Example: Snowflake Provider

- **2.10.3**: `apache-airflow-providers-snowflake==5.8.0`
- **3.0.6**: `apache-airflow-providers-snowflake==5.5.0`

Even though 5.8.0 is a higher version number, it may not be compatible with Airflow 3.0.6 due to API changes or dependency conflicts.

## Finding Compatible Versions

To find compatible provider versions for your Airflow version, check the constraint file:

```bash
# For Airflow 3.0.6
curl https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt | grep snowflake

# For Airflow 2.10.3
curl https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt | grep snowflake
```

## Installation

After adding packages to `requirements.txt`, restart the containers:

```bash
cd docker
docker-compose -f docker-compose-local.yml restart
```

The packages will be installed automatically from the `requirements/requirements.txt` file.

## Notes

- Always check constraint files for compatible versions
- Don't mix provider versions from different Airflow versions
- Test your DAGs after adding new packages
- Keep requirements.txt minimal - only add what you actually need

