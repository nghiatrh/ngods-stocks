-- OpenMetadata server backend + its own Airflow metadata DB.
-- On a fresh volume this runs automatically at container startup.
-- On an existing volume run manually (CREATE DATABASE cannot be inside a transaction,
-- so each statement needs its own -c invocation):
--   docker exec postgres psql -U ngods -c "CREATE DATABASE openmetadata;"
--   docker exec postgres psql -U ngods -c "GRANT ALL PRIVILEGES ON DATABASE openmetadata TO ngods;"
--   docker exec postgres psql -U ngods -c "CREATE DATABASE openmetadata_airflow;"
--   docker exec postgres psql -U ngods -c "GRANT ALL PRIVILEGES ON DATABASE openmetadata_airflow TO ngods;"
CREATE DATABASE openmetadata;
GRANT ALL PRIVILEGES ON DATABASE openmetadata TO ngods;

CREATE DATABASE openmetadata_airflow;
GRANT ALL PRIVILEGES ON DATABASE openmetadata_airflow TO ngods;
