# Satu image untuk Airflow (api-server, scheduler, dag-processor) dan semua pekerjaan data:
# Polars, DuckDB, PyArrow, PyIceberg, dbt-clickhouse, serta lake-engine (DuckDB atas Iceberg).
# Image slim (+driver PostgreSQL) menghemat ±400 MB dibanding image penuh.
FROM apache/airflow:slim-3.3.2-python3.12

# Virtualenv data terpisah dari Airflow agar dependensi tidak bentrok. Ekstensi DuckDB diunduh saat build.
COPY --chown=airflow:root airflow/requirements-data.txt /tmp/requirements-data.txt
RUN python -m venv /opt/airflow/venv-data \
 && PIP_USER=false /opt/airflow/venv-data/bin/pip install --no-cache-dir -r /tmp/requirements-data.txt \
 && /opt/airflow/venv-data/bin/python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('httpfs', 'iceberg', 'avro')]"

# Driver PostgreSQL (sync + async) untuk metadata Airflow, mengikuti constraints resmi Airflow 3.3.2.
RUN pip install --no-cache-dir "psycopg2-binary==2.9.13" "asyncpg==0.31.0" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"

COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root pipeline/ /opt/data/pipeline/
COPY --chown=airflow:root dbt/ /opt/data/dbt/
COPY --chown=airflow:root tiering/ /opt/data/tiering/
COPY --chown=airflow:root dev/bangkitkan_bronze.py /opt/data/dev/bangkitkan_bronze.py
COPY --chown=airflow:root airflow/mulai.sh /opt/data/mulai.sh

ENV DATA_DIR=/opt/data \
    PY_DATA=/opt/airflow/venv-data/bin/python \
    DBT_BIN=/opt/airflow/venv-data/bin/dbt \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor
WORKDIR /opt/data
