#!/usr/bin/env bash

source "$AIRFLOW_HOME/startup/startup.sh"
declare -p | grep -v '^declare \-[aAilnrtux]*r ' > stored_env