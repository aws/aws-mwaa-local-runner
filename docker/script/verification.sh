#!/usr/bin/env bash

EXPECTED_AIRFLOW_VERSION="2.6.3"
EXPECTED_WATCHTOWER_VERSION="2.0.1"

validate_version_with_pip() {
  if [[ $(pip3 show "$1" | grep 'Version' | grep -o '[0-9].*') != "$2" ]]
  then
    echo "Validation Error: $1 version $2 expected"
  fi
}

if ! python3 -c 'import sys; assert sys.version_info[:2] == (3,10)' > /dev/null;
then
  echo "Validation Error: Python version 3.10 expected"
fi

validate_version_with_pip "apache-airflow" "$EXPECTED_AIRFLOW_VERSION"

validate_version_with_pip "watchtower" "$EXPECTED_WATCHTOWER_VERSION"