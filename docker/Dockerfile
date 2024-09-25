# VERSION 2.4
# AUTHOR: John Jackson
# DESCRIPTION: Amazon MWAA Local Dev Environment
# BUILD: docker build --rm -t amazon/mwaa-local .

FROM amazonlinux:2023
LABEL maintainer="amazon"

# Airflow
## Version specific ARGs
ARG AIRFLOW_VERSION=2.10.1
ARG WATCHTOWER_VERSION=3.3.1
ARG PROVIDER_AMAZON_VERSION=8.28.0

## General ARGs
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ARG SYSTEM_DEPS=""
ARG INDEX_URL=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ENV PATH="/usr/local/airflow/.local/bin:/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/airflow/.local/lib/python3.11/site-packages:/usr/local/airflow/.local/bin:/usr/local/airflow/.local/lib/python3.11/site-packages:/usr/local/airflow/.local/bin:/usr/local/airflow/.local/lib/python3.11/site-packages"
ENV PYTHON_VERSION=3.11.7

COPY script/bootstrap.sh /bootstrap.sh
COPY script/systemlibs.sh /systemlibs.sh
COPY script/generate_key.sh /generate_key.sh
COPY script/run-startup.sh /run-startup.sh
COPY script/shell-launch-script.sh /shell-launch-script.sh
COPY script/verification.sh /verification.sh
COPY config/constraints.txt /constraints.txt
COPY config/mwaa-base-providers-requirements.txt /mwaa-base-providers-requirements.txt

RUN chmod u+x /systemlibs.sh && /systemlibs.sh
RUN chmod u+x /bootstrap.sh && /bootstrap.sh
RUN chmod u+x /generate_key.sh && /generate_key.sh
RUN chmod u+x /run-startup.sh
RUN chmod u+x /shell-launch-script.sh
RUN chmod u+x /verification.sh

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
