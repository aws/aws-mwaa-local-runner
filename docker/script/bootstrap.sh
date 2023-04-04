#!/bin/sh

set -e
set -x

# install adduser and add the airflow user
yum update -y
yum install -y shadow-utils
adduser -s /bin/bash -d "${AIRFLOW_USER_HOME}" airflow
yum install -y sudo

echo 'airflow ALL=(ALL)NOPASSWD:ALL' | sudo EDITOR='tee -a' visudo

# install basic python environment and other required dependencies
yum install -y python37 gcc gcc-g++ python3-devel python3-wheel

# Upgrade pip
sudo -u airflow pip3 install $PIP_OPTION --upgrade pip

# openjdk is required for JDBC to work with Airflow
yum install -y java-1.8.0-openjdk

# install minimal Airflow packages
echo "Installing minimal Airflow packages"
sudo -u airflow pip3 install $PIP_OPTION --constraint /constraints.txt apache-airflow[crypto,celery,statsd"${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}"]=="${AIRFLOW_VERSION}"

# install celery[sqs] and its dependencies
yum install -y libcurl-devel openssl-devel
# see https://stackoverflow.com/questions/49200056/pycurl-import-error-ssl-backend-mismatch
export PYCURL_SSL_LIBRARY=openssl
sudo -u airflow pip3 install $PIP_OPTION --compile pycurl
sudo -u airflow pip3 install $PIP_OPTION celery[sqs]

# install postgres Python driver and its dependencies
yum install -y postgresql-devel
sudo -u airflow pip3 install $PIP_OPTION psycopg2

# install unixODBC-devel to support pyodbc
yum install -y unixODBC-devel

# install additional python dependencies
if [ -n "${PYTHON_DEPS}" ]; then sudo -u airflow pip3 install $PIP_OPTION "${PYTHON_DEPS}"; fi

echo "Installing apache-airflow-providers-amazon with version ${PROVIDER_AMAZON_VERSION}"
sudo -u airflow pip3 install $PIP_OPTION apache-airflow-providers-amazon==${PROVIDER_AMAZON_VERSION}
sudo -u airflow pip3 install $PIP_OPTION watchtower==${WATCHTOWER_VERSION}

MWAA_BASE_PROVIDERS_FILE=/mwaa-base-providers-requirements.txt
echo "Installing providers supported for airflow version ${PROVIDER_AMAZON_VERSION}"
sudo -u airflow pip3 install $PIP_OPTION -r $MWAA_BASE_PROVIDERS_FILE

# install system dependency to enable the installation of most Airflow extras
yum install -y mariadb-devel gcc-c++ cyrus-sasl-devel

# jq is used to parse ECS-injected AWSSecretsManager secrets
yum install -y jq

# nc is used to check DB connectivity
yum install -y nc

# install archiving packages
yum install -y zip unzip bzip2 gzip # tar

# install awscli v2
zip_file="awscliv2.zip"
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o $zip_file
unzip $zip_file
./aws/install
rm $zip_file
rm -rf ./aws
cd -  # Return to previous directory

# snapshot the packages
if [ -n "$INDEX_URL" ]
then
  pip3 freeze > /requirements.txt
fi
