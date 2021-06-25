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

# build sqlite3 > 3.15.0 as needed by airflow cli
# https://github.com/ferruzzi/airflow/blob/61c3020c0eca9599fc736d3fdecbdb049ddf6ff5/docs/apache-airflow/howto/set-up-database.rst
yum -y install wget tar gzip gcc make expect

wget https://www.sqlite.org/src/tarball/sqlite.tar.gz
tar xzf sqlite.tar.gz
cd sqlite/
export CFLAGS="-DSQLITE_ENABLE_FTS3 \
    -DSQLITE_ENABLE_FTS3_PARENTHESIS \
    -DSQLITE_ENABLE_FTS4 \
    -DSQLITE_ENABLE_FTS5 \
    -DSQLITE_ENABLE_JSON1 \
    -DSQLITE_ENABLE_LOAD_EXTENSION \
    -DSQLITE_ENABLE_RTREE \
    -DSQLITE_ENABLE_STAT4 \
    -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT \
    -DSQLITE_SOUNDEX \
    -DSQLITE_TEMP_STORE=3 \
    -DSQLITE_USE_URI \
    -O2 \
    -fPIC"
export PREFIX="/usr/local"
LIBS="-lm" ./configure --disable-tcl --enable-shared --enable-tempstore=always --prefix="$PREFIX"
make
make install

# Install additional system library dependencies. Provided as a string of libraries separated by space
if [ -n "${SYSTEM_DEPS}" ]; then yum install -y "${SYSTEM_DEPS}"; fi

yum clean all