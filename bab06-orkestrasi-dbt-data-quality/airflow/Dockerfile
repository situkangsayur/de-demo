# Airflow 3.3.2 (image slim yang sama dengan platform) + dbt-clickhouse di virtualenv terpisah.
# Mode standalone: api-server, scheduler, dag-processor, dan triggerer dalam satu container (hanya untuk lab).
FROM apache/airflow:slim-3.3.2-python3.12

# Driver PostgreSQL untuk metadata Airflow, sama persis dengan platform (layer dapat dipakai bersama).
RUN pip install --no-cache-dir "psycopg2-binary==2.9.13" "asyncpg==0.31.0" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"

COPY --chown=airflow:root requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/airflow/venv-dbt \
 && PIP_USER=false /opt/airflow/venv-dbt/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# Perbaikan bug Airflow 3.3.2: clear (jalankan ulang) run yang punya Deadline Alert berbasis queued_at dan belum
# terlewat gagal 500, karena interval tersimpan sebagai dict {"__data__": detik}. Build gagal bila baris berubah.
RUN F=/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py \
 && grep -q 'timedelta(seconds=deadline_alert.interval)' "$F" \
 && sed -i 's/timedelta(seconds=deadline_alert.interval)/timedelta(seconds=(deadline_alert.interval or {}).get("__data__", 0) if isinstance(deadline_alert.interval, dict) else deadline_alert.interval)/' "$F"

USER root
RUN mkdir -p /opt/lab/dbt_target /data && chown -R airflow:root /opt/lab /data
USER airflow
COPY --chown=airflow:root mulai.sh /opt/lab/mulai.sh

ENV DBT_BIN=/opt/airflow/venv-dbt/bin/dbt \
    PY_DBT=/opt/airflow/venv-dbt/bin/python \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/lab/mulai.sh"]
