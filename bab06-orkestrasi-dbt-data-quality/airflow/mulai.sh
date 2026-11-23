#!/usr/bin/env bash
# Titik masuk container airflow lab Bab 6: tulis berkas sandi dari .env, migrasi DB, lalu standalone.
set -euo pipefail
SANDI=${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-/opt/airflow/sandi/passwords.json}
mkdir -p "$(dirname "$SANDI")" /opt/lab/dbt_target/target /opt/lab/dbt_target/runs
printf '{"%s": "%s"}\n' "${AIRFLOW_ADMIN_USER:-admin}" "${AIRFLOW_ADMIN_PASSWORD:?isi AIRFLOW_ADMIN_PASSWORD}" > "$SANDI"
airflow db migrate
exec airflow standalone
