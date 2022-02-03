#!/bin/sh

set -e

# Upgrade pip version to latest
python3 -m pip install --upgrade pip

# Install wheel to avoid legacy setup.py install
pip3 install wheel

# On RHL and Centos based linux, openssl needs to be set as Python Curl SSL library
export PYCURL_SSL_LIBRARY=openssl
pip3 install --upgrade pip
pip3 install $PIP_OPTION --compile pycurl
pip3 install $PIP_OPTION celery[sqs]

# install postgres python client
pip3 install $PIP_OPTION psycopg2

# setuptools dropped support for use_2to3 in v58+ and psycopg2 will install the latest v59+ version
pip3 install $PIP_OPTION "setuptools<=57.*"

# install minimal Airflow packages
pip3 install $PIP_OPTION --constraint /constraints.txt apache-airflow[crypto,celery,statsd"${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}"]=="${AIRFLOW_VERSION}"

# install additional python dependencies
if [ -n "${PYTHON_DEPS}" ]; then pip3 install $PIP_OPTION "${PYTHON_DEPS}"; fi

# install adduser and add the airflow user
adduser -s /bin/bash -d "${AIRFLOW_USER_HOME}" airflow

# install watchtower for Cloudwatch logging
pip3 install $PIP_OPTION watchtower==1.0.1

pip3 install $PIP_OPTION apache-airflow-providers-tableau==1.0.0
pip3 install $PIP_OPTION apache-airflow-providers-databricks==1.0.1
pip3 install $PIP_OPTION apache-airflow-providers-ssh==1.3.0
pip3 install $PIP_OPTION apache-airflow-providers-postgres==1.0.2
pip3 install $PIP_OPTION apache-airflow-providers-docker==1.2.0
pip3 install $PIP_OPTION apache-airflow-providers-oracle==1.1.0
pip3 install $PIP_OPTION apache-airflow-providers-presto==1.0.2
pip3 install $PIP_OPTION apache-airflow-providers-sftp==1.2.0

# Install default providers
pip3 install --constraint /constraints.txt apache-airflow-providers-amazon

# Use symbolic link to ensure Airflow 2.0's backport packages are in the same namespace as Airflow itself
# see https://airflow.apache.org/docs/apache-airflow/stable/backport-providers.html#troubleshooting-installing-backport-packages
ln -s /usr/local/airflow/.local/lib/python3.7/site-packages/airflow/providers /usr/local/lib/python3.7/site-packages/airflow/providers

# install awscli v2, according to https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-linux.html#cliv2-linux-install
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
else
  # flask-swagger depends on PyYAML that are known to be vulnerable
  # even though Airflow 1.10 names flask-swagger as a dependency, it doesn't seem to use it.
  if [ "$AIRFLOW_VERSION" = "1.10.12" ]
  then
    pip3 uninstall -y flask-swagger
  fi
fi
