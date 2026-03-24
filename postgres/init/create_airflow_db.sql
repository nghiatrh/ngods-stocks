-- Creates a separate database for Airflow metadata
-- This runs automatically on first postgres container startup
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO ngods;
