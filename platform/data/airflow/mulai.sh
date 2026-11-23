#!/usr/bin/env bash
# Titik masuk semua container yang memakai image de-platform-data.
# Peran: init | apiserver | scheduler | dag-processor | lake-engine | data-init | muat-awal | bronze-dev
set -euo pipefail
PERAN=${1:?peran wajib}
SANDI=/opt/airflow/sandi/simple_auth_manager_passwords.json

tulis_sandi() {
  # Simple auth manager membaca sandi dari berkas JSON; isinya dari .env, bukan dari repo.
  mkdir -p "$(dirname "$SANDI")"
  printf '{"%s": "%s"}\n' "$AIRFLOW_ADMIN_USER" "$AIRFLOW_ADMIN_PASSWORD" > "$SANDI"
}

case "$PERAN" in
  init)
    airflow db migrate
    tulis_sandi
    ;;
  apiserver)
    tulis_sandi
    exec airflow api-server --port 8080
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  dag-processor)
    exec airflow dag-processor
    ;;
  lake-engine)
    cd /opt/data && exec "$PY_DATA" -m pipeline.lake_engine
    ;;
  data-init)
    cd /opt/data && exec "$PY_DATA" -m pipeline.siapkan
    ;;
  muat-awal)
    cd /opt/data && exec "$PY_DATA" -m pipeline.muat_awal
    ;;
  bronze-dev)
    cd /opt/data && exec "$PY_DATA" dev/bangkitkan_bronze.py
    ;;
  *)
    echo "peran tidak dikenal: $PERAN" >&2; exit 2;;
esac
