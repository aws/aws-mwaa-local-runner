# About aws-mwaa-local-runner

This repository provides a command line interface (CLI) utility that replicates an Amazon Managed Workflows for Apache Airflow (MWAA) environment locally.

*Please note: MWAA/AWS/DAG/Plugin issues should be raised through AWS Support or the Airflow Slack #airflow-aws channel.  Issues here should be focused on this local-runner repository.*


## About the CLI

The CLI builds a Docker container image locally that’s similar to a MWAA production image. This allows you to run a local Apache Airflow environment to develop and test DAGs, custom plugins, and dependencies before deploying to MWAA.

## What this repo contains

```text
dags/
  example_lambda.py
  example_dag_with_taskflow_api.py    
  example_redshift_data_execute_sql.py
docker/
  config/
    airflow.cfg
    constraints.txt
    mwaa-base-providers-requirements.txt
    webserver_config.py
    .env.localrunner
  script/
    bootstrap.sh
    entrypoint.sh
    systemlibs.sh
    generate_key.sh
  docker-compose-local.yml
  docker-compose-resetdb.yml
  docker-compose-sequential.yml
  Dockerfile
plugins/
  README.md
requirements/  
  requirements.txt
.gitignore
CODE_OF_CONDUCT.md
CONTRIBUTING.md
LICENSE
mwaa-local-env
README.md
VERSION
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
2. To run the sample code in this repository, see the `example_dag_with_taskflow_api.py` file.

#### Requirements.txt

1. Add Python dependencies to `requirements/requirements.txt`.  
2. To test a requirements.txt without running Apache Airflow, use the following script:

```bash
./mwaa-local-env test-requirements
```

Let's say you add `aws-batch==0.6` to your `requirements/requirements.txt` file. You should see an output similar to:

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

3. To package the necessary WHL files for your requirements.txt without running Apache Airflow, use the following script:

```bash
./mwaa-local-env package-requirements
```

For example usage see [Installing Python dependencies using PyPi.org Requirements File Format Option two: Python wheels (.whl)](https://docs.aws.amazon.com/mwaa/latest/userguide/best-practices-dependencies.html#best-practices-dependencies-python-wheels).

#### Custom plugins

- There is a directory at the root of this repository called plugins. 
- In this directory, create a file for your new custom plugin.
- Add any Python dependencies to `requirements/requirements.txt`.

**Note**: this step assumes you have a DAG that corresponds to the custom plugin. For example usage [MWAA Code Examples](https://docs.aws.amazon.com/mwaa/latest/userguide/sample-code.html).

## What's next?

- Learn how to upload the requirements.txt file to your Amazon S3 bucket in [Installing Python dependencies](https://docs.aws.amazon.com/mwaa/latest/userguide/working-dags-dependencies.html).
- Learn how to upload the DAG code to the dags folder in your Amazon S3 bucket in [Adding or updating DAGs](https://docs.aws.amazon.com/mwaa/latest/userguide/configuring-dag-folder.html).
- Learn more about how to upload the plugins.zip file to your Amazon S3 bucket in [Installing custom plugins](https://docs.aws.amazon.com/mwaa/latest/userguide/configuring-dag-import-plugins.html).

## FAQs

The following section contains common questions and answers you may encounter when using your Docker container image.

### Can I test execution role permissions using this repository?

- You can setup the local Airflow's boto with the intended execution role to test your DAGs with AWS operators before uploading to your Amazon S3 bucket. To setup aws connection for Airflow locally see [Airflow | AWS Connection](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html)
To learn more, see [Amazon MWAA Execution Role](https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-create-role.html).
- You can set AWS credentials via environment variables set in the `docker/config/.env.localrunner` env file. To learn more about AWS environment variables, see [Environment variables to configure the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html) and [Using temporary security credentials with the AWS CLI](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html#using-temp-creds-sdk-cli). Simply set the relevant environment variables in `.env.localrunner` and `./mwaa-local-env start`.

### How do I add libraries to requirements.txt and test install?

- A `requirements.txt` file is included in the `/requirements` folder of your local Docker container image. We recommend adding libraries to this file, and running locally.

### What if a library is not available on PyPi.org?

- If a library is not available in the Python Package Index (PyPi.org), add the `--index-url` flag to the package in your `requirements/requirements.txt` file. To learn more, see [Managing Python dependencies in requirements.txt](https://docs.aws.amazon.com/mwaa/latest/userguide/best-practices-dependencies.html).

## Troubleshooting

The following section contains errors you may encounter when using the Docker container image in this repository.

### My environment is not starting

- If you encountered [the following error](https://issues.apache.org/jira/browse/AIRFLOW-3678): `process fails with "dag_stats_table already exists"`, you'll need to reset your database using the following command:

```bash
./mwaa-local-env reset-db
```

- If you are moving from an older version of local-runner you may need to run the above reset-db command, or delete your `./db-data` folder. Note, too, that newer Airflow versions have newer provider packages, which may require updating your DAG code.

### Fernet Key InvalidToken

A Fernet Key is generated during image build (`./mwaa-local-env build-image`) and is durable throughout all
containers started from that image. This key is used to [encrypt connection passwords in the Airflow DB](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html).
If changes are made to the image and it is rebuilt, you may get a new key that will not match the key used when
the Airflow DB was initialized, in this case you will need to reset the DB (`./mwaa-local-env reset-db`).

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
