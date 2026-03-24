#!/bin/bash

export JAVA_HOME="$(jrunscript -e 'java.lang.System.out.println(java.lang.System.getProperty("java.home"));')"
export AIRFLOW_HOME=/opt/airflow
export KYUUBI_HOME=/opt/kyuubi

# spark
mkdir -p /tmp/spark-events
start-master.sh -p 7077 --webui-port 8061
start-worker.sh spark://spark:7077 --webui-port 8062
start-history-server.sh
${KYUUBI_HOME}/bin/kyuubi start

# airflow - initialise DB and create admin user on first run, then start services
airflow db migrate
# Airflow 3.x Simple Auth Manager: create user then explicitly reset password to known value
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@ngods.com \
    --password admin 2>/dev/null || true
airflow users reset-password --username admin --password admin 2>/dev/null || true
airflow api-server -p 8080 &
airflow scheduler &

# Entrypoint, for example notebook, pyspark or spark-sql
if [[ $# -gt 0 ]] ; then
    eval "$1"
fi
