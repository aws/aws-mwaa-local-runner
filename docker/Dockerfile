# VERSION 2.4
# AUTHOR: John Jackson
# DESCRIPTION: Amazon MWAA Local Dev Environment
# BUILD: docker build --rm -t amazon/mwaa-local .

FROM amazonlinux
LABEL maintainer="amazon"

# Airflow
## Version specific ARGs
ARG AIRFLOW_VERSION=2.4.3
ARG WATCHTOWER_VERSION=2.0.1
ARG PROVIDER_AMAZON_VERSION=6.0.0

## General ARGs
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ARG SYSTEM_DEPS=""
ARG INDEX_URL=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ENV PATH="$PATH:/usr/local/airflow/.local/bin:/root/.local/bin:/usr/local/airflow/.local/lib/python3.10/site-packages"
ENV PYTHON_VERSION=3.10.9
ENV AIRFLOW_VAR_AIRFLOW_ENVIRONMENT=internal
ENV AIRFLOW_VAR_DBT_ROLE=cnn-di-zion-dbt-airflow-internal
ENV AIRFLOW_VAR_DBT_AWSLOGS='{"awslogs_group":"/aws/fargate/cnn-di-zion-dbt-internal","awslogs_region":"us-east-1","awslogs_stream_prefix":"cnn-di-zion-dbt-internal/cnn-di-zion-dbt-internal"}'
ENV AIRFLOW_VAR_DBT_CLUSTER=cnn-di-zion-dbt-internal
ENV AIRFLOW_VAR_DBT_TASK_DEFINITION=cnn-di-zion-dbt-internal
ENV AIRFLOW_VAR_PUBLIC_SUBNET_IDS='["subnet-03c5f9c674e3bed75","subnet-041a9d019e0154abe","subnet-08a6ddf446caafe30","subnet-0a28cf2d773001897","subnet-0c73fc97eca7d862d","subnet-0f6a85eb80e7bb819"]'
ENV AIRFLOW_VAR_PRIVATE_SUBNET_IDS='["subnet-016338c0b4a0d2253","subnet-0549104d24efcaaa1","subnet-074acdff83fce7af8","subnet-07ccf392fd9760a50","subnet-0876c63b197605a6c","subnet-0b0afe39a56ad76bd"]'

COPY script/bootstrap.sh /bootstrap.sh
COPY script/systemlibs.sh /systemlibs.sh
COPY script/generate_key.sh /generate_key.sh
COPY config/constraints.txt /constraints.txt
COPY config/mwaa-base-providers-requirements.txt /mwaa-base-providers-requirements.txt

RUN chmod u+x /systemlibs.sh && /systemlibs.sh
RUN chmod u+x /bootstrap.sh && /bootstrap.sh
RUN chmod u+x /generate_key.sh && /generate_key.sh

# Post bootstrap to avoid expensive docker rebuilds
COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
COPY config/webserver_config.py ${AIRFLOW_USER_HOME}/webserver_config.py

RUN chown -R airflow: ${AIRFLOW_USER_HOME}
RUN chmod +x /entrypoint.sh

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["local-runner"]
