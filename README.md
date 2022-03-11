# About aws-mwaa-local-runner

This repository provides a command line interface (CLI) utility that replicates an Amazon Managed Workflows for Apache Airflow (MWAA) environment locally.

## About the CLI

The CLI builds a Docker container image locally that’s similar to a MWAA production image. This allows you to run a local Apache Airflow environment to develop and test DAGs, custom plugins, and dependencies before deploying to MWAA.

## What this repo contains

```text
dags/
  requirements.txt
  tutorial.py
docker/
  .gitignore
  mwaa-local-env
  README.md
  config/
    airflow.cfg
    constraints.txt
    requirements.txt
    webserver_config.py
  script/
    bootstrap.sh
    entrypoint.sh
  docker-compose-dbonly.yml
  docker-compose-local.yml
  docker-compose-sequential.yml
  Dockerfile
```

## Prerequisites

- **macOS**: [Install Docker Desktop](https://docs.docker.com/desktop/).
- **Linux/Ubuntu**: [Install Docker Compose](https://docs.docker.com/compose/install/) and [Install Docker Engine](https://docs.docker.com/engine/install/).
- **Windows**: Windows Subsystem for Linux (WSL) to run the bash based command `mwaa-local-env`. Please follow [Windows Subsystem for Linux Installation (WSL)](https://docs.docker.com/docker-for-windows/wsl/) and [Using Docker in WSL 2](https://code.visualstudio.com/blogs/2020/03/02/docker-in-wsl2), to get started.

## Get started

```bash
git clone https://github.com/aws/aws-mwaa-local-runner.git
cd aws-mwaa-local-runner
```

### Step one: Building the Docker image

Build the Docker container image using the following command:

```bash
./mwaa-local-env build-image
```

**Note**: it takes several minutes to build the Docker image locally.

### Step two: Running Apache Airflow

Run Apache Airflow using one of the following database backends.

#### Local runner

Runs a local Apache Airflow environment that is a close representation of MWAA by configuration.

```bash
./mwaa-local-env start
```

To stop the local environment, Ctrl+C on the terminal and wait till the local runner and the postgres containers are stopped.

### Step three: Accessing the Airflow UI

By default, the `bootstrap.sh` script creates a username and password for your local Airflow environment.

- Username: `admin`
- Password: `test`

#### Airflow UI

- Open the Apache Airlfow UI: <http://localhost:8080/>.

### Step four: Add DAGs and supporting files

The following section describes where to add your DAG code and supporting files. We recommend creating a directory structure similar to your MWAA environment.

#### DAGs

1. Add DAG code to the `dags/` folder.
2. To run the sample code in this repository, see the `tutorial.py` file.

#### Requirements.txt

1. Add Python dependencies to `dags/requirements.txt`.  
2. To test a requirements.txt without running Apache Airflow, use the following script:

```bash
./mwaa-local-env test-requirements
```

Let's say you add `aws-batch==0.6` to your `dags/requirements.txt` file. You should see an output similar to:

```bash
Installing requirements.txt
Collecting aws-batch (from -r /usr/local/airflow/dags/requirements.txt (line 1))
  Downloading https://files.pythonhosted.org/packages/5d/11/3aedc6e150d2df6f3d422d7107ac9eba5b50261cf57ab813bb00d8299a34/aws_batch-0.6.tar.gz
Collecting awscli (from aws-batch->-r /usr/local/airflow/dags/requirements.txt (line 1))
  Downloading https://files.pythonhosted.org/packages/07/4a/d054884c2ef4eb3c237e1f4007d3ece5c46e286e4258288f0116724af009/awscli-1.19.21-py2.py3-none-any.whl (3.6MB)
    100% |████████████████████████████████| 3.6MB 365kB/s 
...
...
...
Installing collected packages: botocore, docutils, pyasn1, rsa, awscli, aws-batch
  Running setup.py install for aws-batch ... done
Successfully installed aws-batch-0.6 awscli-1.19.21 botocore-1.20.21 docutils-0.15.2 pyasn1-0.4.8 rsa-4.7.2
```

#### Custom plugins

- Create a directory at the root of this repository, and change directories into it. This should be at the same level as `dags/` and `docker`. For example:

```bash
mkdir plugins
cd plugins
```

- Create a file for your custom plugin. For example:

```bash
virtual_python_plugin.py
```

- (Optional) Add any Python dependencies to `dags/requirements.txt`.

**Note**: this step assumes you have a DAG that corresponds to the custom plugin. For examples, see [MWAA Code Examples](https://docs.aws.amazon.com/mwaa/latest/userguide/sample-code.html).

### Validating Airflow upgrade from 1.10 to 2.0

You can use the "upgrade check" script - [apache-airflow-upgrade-check](https://pypi.org/project/apache-airflow-upgrade-check/) - to identify breaking changes as you plan migrating from 1.10.12 to 2.0. Copy your dags and plugins to the appropriate directories and run the command below.

```bash
./mwaa-local-env upgrade-check
```

As an example the v1.10.12 [Amazon EMR job DAG](https://github.com/aws-samples/amazon-mwaa-examples/blob/main/dags/emr_job/1.10/emr_job.py) from the MWAA samples repository will generate the results bellow.

```bash
================================================================================================== STATUS ==================================================================================================

Check for latest versions of apache-airflow and checker...........................................................................................................................................SUCCESS
Remove airflow.AirflowMacroPlugin class...........................................................................................................................................................SUCCESS
Ensure users are not using custom metaclasses in custom operators.................................................................................................................................SUCCESS
Chain between DAG and operator not allowed........................................................................................................................................................SUCCESS
Connection.conn_type is not nullable..............................................................................................................................................................SUCCESS
Custom Executors now require full path............................................................................................................................................................SUCCESS
Check versions of PostgreSQL, MySQL, and SQLite to ease upgrade to Airflow 2.0....................................................................................................................SUCCESS
Hooks that run DB functions must inherit from DBApiHook...........................................................................................................................................SUCCESS
Fernet is enabled by default......................................................................................................................................................................SUCCESS
GCP service account key deprecation...............................................................................................................................................................SUCCESS
Unify hostname_callable option in core section....................................................................................................................................................SUCCESS
Changes in import paths of hooks, operators, sensors and others...................................................................................................................................FAIL
Legacy UI is deprecated by default................................................................................................................................................................SUCCESS
Logging configuration has been moved to new section...............................................................................................................................................FAIL
Removal of Mesos Executor.........................................................................................................................................................................SUCCESS
No additional argument allowed in BaseOperator....................................................................................................................................................SUCCESS
/usr/local/lib/python3.7/site-packages/airflow/configuration.py:436: DeprecationWarning: The max_threads option in [scheduler] has been renamed to parsing_processes - the old setting has been used, but please update your config.
  self.get(section, option, fallback=_UNSET)
Rename max_threads to parsing_processes...........................................................................................................................................................SUCCESS
Users must set a kubernetes.pod_template_file value...............................................................................................................................................SKIPPED
Ensure Users Properly Import conf from Airflow....................................................................................................................................................SUCCESS
SendGrid email uses old airflow.contrib module....................................................................................................................................................SUCCESS
Check Spark JDBC Operator default connection name.................................................................................................................................................SUCCESS
Changes in import path of remote task handlers....................................................................................................................................................SUCCESS
Connection.conn_id is not unique..................................................................................................................................................................SUCCESS
Use CustomSQLAInterface instead of SQLAInterface for custom data models...........................................................................................................................SUCCESS
Found 7 problems.
```

The recommendation section details how exactly to address the failures.

```bash
============================================================================================= RECOMMENDATIONS ==============================================================================================

Changes in import paths of hooks, operators, sensors and others
---------------------------------------------------------------
Many hooks, operators and other classes has been renamed and moved. Those changes were part of unifying names and imports paths as described in AIP-21.
The `contrib` folder has been replaced by `providers` directory and packages:
https://github.com/apache/airflow#backport-packages

Problems:

  1.  Please install `apache-airflow-backport-providers-amazon`
  2.  Using `airflow.contrib.operators.emr_add_steps_operator.EmrAddStepsOperator` should be replaced by `airflow.providers.amazon.aws.operators.emr_add_steps.EmrAddStepsOperator`. Affected file: /usr/local/airflow/dags/emr_job.py
  3.  Using `airflow.contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator` should be replaced by `airflow.providers.amazon.aws.operators.emr_create_job_flow.EmrCreateJobFlowOperator`. Affected file: /usr/local/airflow/dags/emr_job.py
  4.  Using `airflow.contrib.sensors.emr_step_sensor.EmrStepSensor` should be replaced by `airflow.providers.amazon.aws.sensors.emr_step.EmrStepSensor`. Affected file: /usr/local/airflow/dags/emr_job.py
```

## What's next?

- Learn how to upload the requirements.txt file to your Amazon S3 bucket in [Installing Python dependencies](https://docs.aws.amazon.com/mwaa/latest/userguide/working-dags-dependencies.html).
- Learn how to upload the DAG code to the dags folder in your Amazon S3 bucket in [Adding or updating DAGs](https://docs.aws.amazon.com/mwaa/latest/userguide/configuring-dag-folder.html).
- Learn more about how to upload the plugins.zip file to your Amazon S3 bucket in [Installing custom plugins](https://docs.aws.amazon.com/mwaa/latest/userguide/configuring-dag-import-plugins.html).

## FAQs

The following section contains common questions and answers you may encounter when using your Docker container image.

### Can I test execution role permissions using this repository?

- You can setup the local Airflow's boto with the intended execution role to test your DAGs with AWS operators before uploading to your Amazon S3 bucket. To setup aws connection for Airflow locally see [Airflow | AWS Connection](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html)
To learn more, see [Amazon MWAA Execution Role](https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-create-role.html).

### How do I add libraries to requirements.txt and test install?

- A `requirements.txt` file is included in the `/dags` folder of your local Docker container image. We recommend adding libraries to this file, and running locally.

### What if a library is not available on PyPi.org?

- If a library is not available in the Python Package Index (PyPi.org), add the `--index-url` flag to the package in your `dags/requirements.txt` file. To learn more, see [Managing Python dependencies in requirements.txt](https://docs.aws.amazon.com/mwaa/latest/userguide/best-practices-dependencies.html).

## Troubleshooting

The following section contains errors you may encounter when using the Docker container image in this repository.

## My environment is not starting - process failed with dag_stats_table already exists

- If you encountered [the following error](https://issues.apache.org/jira/browse/AIRFLOW-3678): `process fails with "dag_stats_table already exists"`, you'll need to reset your database using the following command:

```bash
./mwaa-local-env reset-db
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
