#!/bin/sh

set -e
set -x

# install adduser and add the airflow user
yum update -y
yum install -y shadow-utils
adduser -s /bin/bash -d "${AIRFLOW_USER_HOME}" airflow
yum install -y sudo

yum groupinstall "Development Tools" -y
yum erase openssl-devel -y
yum install openssl11 openssl11-devel libffi-devel bzip2-devel wget tar xz -y # PRIOR TO RELEASE REVIEW LICENSES AND REMOVE WHERE NEEDED
yum install glibc -y


# install sqlite to avoid local-runner Airflow error messages
yum install -y sqlite-devel

# Python 3.10 install
sudo mkdir python_install
python_file=Python-$PYTHON_VERSION
python_tar=$python_file.tar
python_xz=$python_tar.xz
sudo mkdir python_source
wget "https://www.python.org/ftp/python/$PYTHON_VERSION/$python_xz" -P /python_source
cp /python_source/$python_xz /python_install/$python_xz
unxz ./python_install/$python_xz
tar -xf ./python_install/$python_tar -C ./python_install

pushd /python_install/$python_file 
./configure --enable-optimizations --enable-loadable-sqlite-extensions --prefix=/usr ## Override the install at /usr/bin/python3
make install -j $(nproc) # use -j to set the cores for the build
popd

# Upgrade pip
pip3 install $PIP_OPTION --upgrade pip

# openjdk is required for JDBC to work with Airflow
yum install -y java-1.8.0-openjdk

# install minimal Airflow packages
sudo -u airflow pip3 install $PIP_OPTION --no-use-pep517 --constraint /constraints.txt poetry
sudo -u airflow pip3 install $PIP_OPTION --constraint /constraints.txt cached-property
sudo -u airflow pip3 install $PIP_OPTION --constraint /constraints.txt wheel 
sudo -u airflow pip3 install $PIP_OPTION --constraint /constraints.txt --use-deprecated legacy-resolver apache-airflow[celery,statsd"${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}"]=="${AIRFLOW_VERSION}"

# install celery[sqs] and its dependencies
yum install -y libcurl-devel 
# see https://stackoverflow.com/questions/49200056/pycurl-import-error-ssl-backend-mismatch
export PYCURL_SSL_LIBRARY=openssl11
sudo -u airflow pip3 install $PIP_OPTION --compile pycurl
sudo -u airflow pip3 install $PIP_OPTION celery[sqs]

# install postgres Python driver and its dependencies
yum install -y postgresql-devel
sudo -u airflow pip3 install $PIP_OPTION psycopg2

# install unixODBC-devel to support pyodbc
yum install -y unixODBC-devel

# install additional python dependencies
if [ -n "${PYTHON_DEPS}" ]; then sudo -u airflow pip3 install $PIP_OPTION "${PYTHON_DEPS}"; fi

MWAA_BASE_PROVIDERS_FILE=/mwaa-base-providers-requirements.txt
echo "Installing providers supported for airflow version ${AIRFLOW_VERSION}"
sudo -u airflow pip3 install $PIP_OPTION -r $MWAA_BASE_PROVIDERS_FILE

# install system dependency to enable the installation of most Airflow extras
yum install -y gcc-c++ cyrus-sasl-devel

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
  find /packages/ -maxdepth 1 -not -name "watchtower-2.0.1-py3-none-any.whl" -exec rm -f {} \;
fi

yum clean all