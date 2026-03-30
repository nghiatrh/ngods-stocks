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

# Initialise Iceberg namespaces in the warehouse catalog.
# spark.sql.defaultCatalog=warehouse means Spark looks for warehouse.default
# on every session start — the session fails if the namespace doesn't exist.
spark-sql -e "
  CREATE NAMESPACE IF NOT EXISTS warehouse.default;
  CREATE NAMESPACE IF NOT EXISTS warehouse.bronze;
  CREATE NAMESPACE IF NOT EXISTS warehouse.silver;
  CREATE NAMESPACE IF NOT EXISTS warehouse.gold;
" 2>/dev/null || true

# airflow - initialise DB and write fixed credentials before starting services
airflow db migrate

# Airflow 3.x Simple Auth Manager stores plaintext passwords in a JSON file.
# Pre-writing the file prevents the random password from being generated.
# Password is configurable via AIRFLOW_ADMIN_PASSWORD env var (default: admin).
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD:-admin}
mkdir -p "${AIRFLOW_HOME}"
echo "{\"admin\": \"${AIRFLOW_ADMIN_PASSWORD}\"}" > "${AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated"
echo "Airflow admin password set to: ${AIRFLOW_ADMIN_PASSWORD}"
airflow api-server -p 8080 &
airflow dag-processor &
airflow scheduler &

# Entrypoint, for example notebook, pyspark or spark-sql
if [[ $# -gt 0 ]] ; then
    eval "$1"
fi
