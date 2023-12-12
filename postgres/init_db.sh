#!/bin/bash

psql -v ON_ERROR_STOP=1 --username "$PG_USER" --dbname=template1 <<-EOSQL
    CREATE USER $POSTGRES_AIRFLOW_USER WITH PASSWORD '$POSTGRES_AIRFLOW_PASSWORD';
    CREATE DATABASE $POSTGRES_AIRFLOW_DB;
    GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_AIRFLOW_DB TO $POSTGRES_AIRFLOW_USER;
    ALTER DATABASE $POSTGRES_AIRFLOW_DB OWNER TO $POSTGRES_AIRFLOW_USER;
    REVOKE ALL PRIVILEGES ON DATABASE $POSTGRES_DB FROM $POSTGRES_AIRFLOW_USER;
EOSQL

exit