#!/bin/sh

set -e
dnf update -y

# install basic python environment
dnf install -y python3 gcc gcc-g++ python3-devel

# JDBC and PyODBC dependencies
# openjdk is required for JDBC to work with Airflow
dnf install -y java-17-amazon-corretto unixODBC-devel 

# Database clients
dnf install -y postgresql-devel

# Archiving Libraries
dnf install -y zip unzip bzip2 gzip

# Airflow extras
dnf install -y gcc-c++ cyrus-sasl-devel libcurl-devel openssl openssl-devel shadow-utils

#### Required Libraries for entrypoint.sh script

# jq is used to parse ECS-injected AWSSecretsManager secrets
dnf install -y jq

# nc is used to check DB connectivity
dnf install -y nc

# Needed for generating fernet key for local runner
dnf install -y python3-cryptography

# Install additional system library dependencies. Provided as a string of libraries separated by space
if [ -n "${SYSTEM_DEPS}" ]; then dnf install -y "${SYSTEM_DEPS}"; fi

dnf clean all