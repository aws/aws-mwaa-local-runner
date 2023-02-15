#!/bin/sh

set -e
set -x

# install adduser and add the airflow user
yum update -y
yum install -y shadow-utils
adduser -s /bin/bash -d "${AIRFLOW_USER_HOME}" airflow
yum install -y sudo

yum erase openssl-devel -y
yum install openssl11 openssl11-devel libffi-devel bzip2-devel wget tar xz -y
# Install python optional standard libary module dependencies 
yum install ncurses-devel gdbm-devel readline-devel xz-libs xz-devel uuid-devel libuuid-devel -y 
yum install glibc -y

# install system dependency to enable the installation of most Airflow extras
yum install -y gcc gcc-c++ cyrus-sasl-devel python3-devel python3-wheel make

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

# Installing mariadb-devel dependency for apache-airflow-providers-mysql.
# The mariadb-devel provided by AL2 conflicts with openssl11 which is required Python 3.10
# so a newer version of the dependency must be installed from source.
sudo mkdir mariadb_rpm
sudo chown airflow /mariadb_rpm
wget https://dlm.mariadb.com/2596575/MariaDB/mariadb-10.8.6/yum/rhel7-amd64/rpms/MariaDB-common-10.8.6-1.el7.centos.x86_64.rpm -P /mariadb_rpm
wget https://dlm.mariadb.com/2596577/MariaDB/mariadb-10.8.6/yum/rhel7-amd64/rpms/MariaDB-compat-10.8.6-1.el7.centos.x86_64.rpm -P /mariadb_rpm
wget https://dlm.mariadb.com/2596582/MariaDB/mariadb-10.8.6/yum/rhel7-amd64/rpms/MariaDB-shared-10.8.6-1.el7.centos.x86_64.rpm -P /mariadb_rpm
wget https://dlm.mariadb.com/2596593/MariaDB/mariadb-10.8.6/yum/rhel7-amd64/rpms/MariaDB-devel-10.8.6-1.el7.centos.x86_64.rpm -P /mariadb_rpm

# install mariadb_devel and its dependencies
sudo rpm -ivh /mariadb_rpm/*

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

# jq is used to parse json
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

yum clean all