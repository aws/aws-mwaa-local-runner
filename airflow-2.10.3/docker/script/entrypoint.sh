#!/usr/bin/env bash

TRY_LOOP="20"

# Global defaults
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(cat /usr/local/etc/airflow_fernet_key)}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"
: "${REQUIREMENTS_FILE:="requirements/requirements.txt"}"

# Load DAGs examples (default: Yes)
if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]; then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
fi

export \
  AIRFLOW_HOME \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \

# Install custom python package if requirements.txt is present
install_requirements() {
    # Install custom python package if requirements.txt is present
    if [[ -e "$AIRFLOW_HOME/$REQUIREMENTS_FILE" ]]; then
      if ! grep -- "-c " "$AIRFLOW_HOME/$REQUIREMENTS_FILE"
      then
          if ! grep -- "--constraint " "$AIRFLOW_HOME/$REQUIREMENTS_FILE"
          then
              echo "WARNING: Constraints should be specified for requirements.txt. Please see https://docs.aws.amazon.com/mwaa/latest/userguide/working-dags-dependencies.html#working-dags-dependencies-test-create"
          fi
      fi    
        echo "Installing requirements.txt"
        pip3 install --user -r "$AIRFLOW_HOME/$REQUIREMENTS_FILE"
    fi
}

# Download custom python WHL files and package as ZIP if requirements.txt is present
package_requirements() {
    # Download custom python WHL files and package as ZIP if requirements.txt is present
    if [[ -e "$AIRFLOW_HOME/$REQUIREMENTS_FILE" ]]; then
        echo "Packaging requirements.txt into plugins"
        pip3 download -r "$AIRFLOW_HOME/$REQUIREMENTS_FILE" -d "$AIRFLOW_HOME/plugins"
        cd "$AIRFLOW_HOME/plugins"
        zip "$AIRFLOW_HOME/requirements/plugins.zip" *
        printf '%s\n%s\n' "--no-index" "$(cat $AIRFLOW_HOME/$REQUIREMENTS_FILE)" > "$AIRFLOW_HOME/requirements/packaged_requirements.txt"
        printf '%s\n%s\n' "--find-links /usr/local/airflow/plugins" "$(cat $AIRFLOW_HOME/requirements/packaged_requirements.txt)" > "$AIRFLOW_HOME/requirements/packaged_requirements.txt"
    fi
}

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

execute_startup_script() {
  # Execute customer provided shell script
  if [[ -e "$AIRFLOW_HOME/startup/startup.sh" ]]; then
    bash /shell-launch-script.sh
    source stored_env
    export AIRFLOW_HOME="/usr/local/airflow"
    export AIRFLOW__CORE__LOAD_EXAMPLES="False"
    cd "$AIRFLOW_HOME"
  else
    echo "No startup script found, skipping execution."
  fi
}

# Other executors than SequentialExecutor drive the need for an SQL database, here PostgreSQL is used
if [ "$AIRFLOW__CORE__EXECUTOR" != "SequentialExecutor" ]; then
  # Check if the user has provided explicit Airflow configuration concerning the database
  if [ -z "$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" ]; then
    # Default values corresponding to the default compose files
    : "${POSTGRES_HOST:="postgres"}"
    : "${POSTGRES_PORT:="5432"}"
    : "${POSTGRES_USER:="airflow"}"
    : "${POSTGRES_PASSWORD:="airflow"}"
    : "${POSTGRES_DB:="airflow"}"
    : "${POSTGRES_EXTRAS:-""}"

    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
  else
    # Derive useful variables from the AIRFLOW__ variables provided explicitly by the user
    POSTGRES_ENDPOINT=$(echo -n "$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" | cut -d '/' -f3 | sed -e 's,.*@,,')
    POSTGRES_HOST=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f1)
    POSTGRES_PORT=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f2)
  fi

  wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
fi


case "$1" in
  local-runner)
    # if S3_PLUGINS_PATH
    if [ -n "$S3_PLUGINS_PATH" ]; then
      echo "Downloading $S3_PLUGINS_PATH"
      mkdir -p $AIRFLOW_HOME/plugins
      cd $AIRFLOW_HOME/plugins
      aws s3 cp $S3_PLUGINS_PATH plugins.zip
      unzip -o plugins.zip 
      rm plugins.zip
    fi
    # if S3_DAGS_PATH
    if [ -n "$S3_DAGS_PATH" ]; then
      echo "Syncing $S3_DAGS_PATH"   
      mkdir -p $AIRFLOW_HOME/dags
      cd $AIRFLOW_HOME/dags
      aws s3 sync --exact-timestamp --delete $S3_DAGS_PATH .
    fi    
    # if S3_REQUIREMENTS_PATH
    if [ -n "$S3_REQUIREMENTS_PATH" ]; then
      echo "Downloading $S3_REQUIREMENTS_PATH"
      mkdir -p $AIRFLOW_HOME/requirements
      aws s3 cp $S3_REQUIREMENTS_PATH $AIRFLOW_HOME/$REQUIREMENTS_FILE
    fi


    execute_startup_script
    source stored_env
    export AIRFLOW_HOME="/usr/local/airflow"
    export AIRFLOW__CORE__LOAD_EXAMPLES="False"

    install_requirements
    airflow db init
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
      # With the "Local" and "Sequential" executors it should all run in one container.
      airflow scheduler &
      airflow triggerer &
      sleep 2
    fi
    airflow users create -r Admin -u admin -e admin@example.com -f admin -l user -p $DEFAULT_PASSWORD
    exec airflow webserver
    ;;
  resetdb)
    airflow db reset -y
    sleep 2
    airflow db init
    ;;
  test-requirements)
    # if S3_REQUIREMENTS_PATH
    if [ -n "$S3_REQUIREMENTS_PATH" ]; then
      echo "Downloading $S3_REQUIREMENTS_PATH"
      mkdir -p $AIRFLOW_HOME/requirements
      aws s3 cp $S3_REQUIREMENTS_PATH $AIRFLOW_HOME/$REQUIREMENTS_FILE
    fi      
    install_requirements
    ;;
  package-requirements)
    # if S3_REQUIREMENTS_PATH
    if [ -n "$S3_REQUIREMENTS_PATH" ]; then
      echo "Downloading $S3_REQUIREMENTS_PATH"
      mkdir -p $AIRFLOW_HOME/requirements
      aws s3 cp $S3_REQUIREMENTS_PATH $AIRFLOW_HOME/$REQUIREMENTS_FILE
    fi      
    package_requirements
    ;;
  test-startup-script)
    execute_startup_script
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac
