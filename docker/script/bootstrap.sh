#!/bin/sh

set -e

# On RHL and Centos based linux, openssl needs to be set as Python Curl SSL library
export PYCURL_SSL_LIBRARY=openssl
pip3 install $PIP_OPTION --compile pycurl
pip3 install $PIP_OPTION celery[sqs]

# install postgres python client
pip3 install $PIP_OPTION psycopg2

# install minimal Airflow packages
pip3 install $PIP_OPTION --constraint /constraints.txt apache-airflow[crypto,celery,statsd"${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}"]=="${AIRFLOW_VERSION}"

# install additional python dependencies
if [ -n "${PYTHON_DEPS}" ]; then pip3 install $PIP_OPTION "${PYTHON_DEPS}"; fi

# install adduser and add the airflow user
adduser -s /bin/bash -d "${AIRFLOW_USER_HOME}" airflow

# install watchtower for Cloudwatch logging
pip3 install $PIP_OPTION watchtower==1.0.1

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
  # even though Airflow names flask-swagger as a dependency, it doesn't seem to use it.
  pip3 uninstall -y flask-swagger
fi
