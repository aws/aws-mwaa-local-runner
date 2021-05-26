#!/bin/sh

set -e
yum update -y

# install basic python environment
yum install -y python37 gcc gcc-g++ python3-devel

# JDBC and PyODBC dependencies
yum install -y java-1.8.0-openjdk unixODBC-devel 

# Database clients
yum install -y mariadb-devel postgresql-devel

# Archiving Libraries
yum install -y zip unzip bzip2 gzip

# Airflow extras
yum install -y gcc-c++ cyrus-sasl-devel libcurl-devel openssl-devel shadow-utils

#### Required Libraries for entrypoint.sh script

# jq is used to parse ECS-injected AWSSecretsManager secrets
yum install -y jq

# nc is used to check DB connectivity
yum install -y nc

# Needed for generating fernet key for local runner
yum install -y python2-cryptography

# Install additional system library dependencies. Provided as a string of libraries separated by space
if [ -n "${SYSTEM_DEPS}" ]; then yum install -y "${SYSTEM_DEPS}"; fi

yum clean all