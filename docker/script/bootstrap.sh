#!/bin/sh

set -e
set -x

# install adduser and add the airflow user
dnf update -y
dnf install -y shadow-utils
adduser -s /bin/bash -d "${AIRFLOW_USER_HOME}" airflow
dnf install -y sudo

echo 'airflow ALL=(ALL)NOPASSWD:ALL' | sudo EDITOR='tee -a' visudo

dnf erase openssl-devel -y
dnf install openssl openssl-devel libffi-devel sqlite-devel bzip2-devel wget tar xz -y
# Install python optional standard libary module dependencies 
dnf install ncurses-devel gdbm-devel readline-devel xz-libs xz-devel uuid-devel libuuid-devel -y 
dnf install glibc -y

# install system dependency to enable the installation of most Airflow extras
dnf install -y gcc gcc-c++ cyrus-sasl-devel python3-devel python3-wheel make

# Python 3.11 install
sudo mkdir python_install
python_file=Python-$PYTHON_VERSION
python_tar=$python_file.tar
python_xz=$python_tar.xz
sudo mkdir python_source
wget "https://www.python.org/ftp/python/$PYTHON_VERSION/$python_xz" -P /python_source
cp /python_source/$python_xz /python_install/$python_xz
unxz ./python_install/$python_xz
tar -xf ./python_install/$python_tar -C ./python_install

dnf install -y dnf-plugins-core
dnf builddep -y python3


pushd /python_install/$python_file 
./configure 
make install -j $(nproc) # use -j to set the cores for the build
popd

# Upgrade pip
pip3 install $PIP_OPTION --upgrade 'pip<23'

# openjdk is required for JDBC to work with Airflow
dnf install -y java-17-amazon-corretto

# Installing mariadb-devel dependency for apache-airflow-providers-mysql.
# The mariadb-devel provided by AL2 conflicts with openssl11 which is required Python 3.10
# so a newer version of the dependency must be installed from source.
sudo mkdir mariadb_rpm
sudo chown airflow /mariadb_rpm

if [[ $(uname -p) == "aarch64" ]]; then
  wget https://mirror.mariadb.org/yum/11.4/fedora38-aarch64/rpms/MariaDB-common-11.4.2-1.fc38.$(uname -p).rpm -P /mariadb_rpm
  wget https://mirror.mariadb.org/yum/11.4/fedora38-aarch64/rpms/MariaDB-shared-11.4.2-1.fc38.$(uname -p).rpm -P /mariadb_rpm
  wget https://mirror.mariadb.org/yum/11.4/fedora38-aarch64/rpms/MariaDB-devel-11.4.2-1.fc38.$(uname -p).rpm -P /mariadb_rpm
else
  wget https://mirror.mariadb.org/yum/11.4/fedora38-amd64/rpms/MariaDB-common-11.4.2-1.fc38.$(uname -p).rpm -P /mariadb_rpm
  wget https://mirror.mariadb.org/yum/11.4/fedora38-amd64/rpms/MariaDB-shared-11.4.2-1.fc38.$(uname -p).rpm -P /mariadb_rpm
  wget https://mirror.mariadb.org/yum/11.4/fedora38-amd64/rpms/MariaDB-devel-11.4.2-1.fc38.$(uname -p).rpm -P /mariadb_rpm
fi

# install mariadb_devel and its dependencies
sudo rpm -ivh /mariadb_rpm/*

sudo -u airflow pip3 install $PIP_OPTION --no-use-pep517 --constraint /constraints.txt poetry
sudo -u airflow pip3 install $PIP_OPTION --constraint /constraints.txt cached-property
sudo -u airflow pip3 install $PIP_OPTION --constraint /constraints.txt wheel 
sudo -u airflow pip3 install $PIP_OPTION --constraint /constraints.txt --use-deprecated legacy-resolver apache-airflow[celery,statsd"${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}"]=="${AIRFLOW_VERSION}"

dnf install -y libxml2-devel libxslt-devel
# install celery[sqs] and its dependencies
dnf install -y libcurl-devel 
# see https://stackoverflow.com/questions/49200056/pycurl-import-error-ssl-backend-mismatch
export PYCURL_SSL_LIBRARY=openssl11
sudo -u airflow pip3 install $PIP_OPTION --compile pycurl
sudo -u airflow pip3 install $PIP_OPTION celery[sqs]

# install postgres Python driver and its dependencies
dnf install -y postgresql-devel
sudo -u airflow pip3 install $PIP_OPTION psycopg2

# install unixODBC-devel to support pyodbc
dnf install -y unixODBC-devel

# install additional python dependencies
if [ -n "${PYTHON_DEPS}" ]; then sudo -u airflow pip3 install $PIP_OPTION "${PYTHON_DEPS}"; fi

MWAA_BASE_PROVIDERS_FILE=/mwaa-base-providers-requirements.txt
echo "Installing providers supported for airflow version ${AIRFLOW_VERSION}"
sudo -u airflow pip3 install --constraint /constraints.txt $PIP_OPTION -r $MWAA_BASE_PROVIDERS_FILE

# jq is used to parse json
dnf install -y jq

# nc is used to check DB connectivity
dnf install -y nc

# install archiving packages
dnf install -y zip unzip bzip2 gzip # tar

# install awscli v2
zip_file="awscliv2.zip"
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o $zip_file
unzip $zip_file
./aws/install
rm $zip_file
rm -rf ./aws
cd -  # Return to previous directory

dnf clean all
