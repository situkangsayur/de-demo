# API lab Bab 4 (Python): PyIceberg, Polars, DuckDB, PyArrow, FastAPI.
# Base image dan requirements.txt identik dengan bab05 agar layer pip dipakai bersama.
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && useradd -m -u 10001 lab && mkdir -p /data && chown lab:lab /data
USER lab
# ekstensi DuckDB diunduh saat build supaya container tidak butuh internet saat demo
RUN python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('httpfs', 'iceberg', 'avro')]"
WORKDIR /app
COPY --chown=lab:lab app/ /app/app/
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
