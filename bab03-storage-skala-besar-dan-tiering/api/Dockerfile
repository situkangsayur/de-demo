# API lab Bab 3 (Python: boto3, PyArrow, DuckDB). Layer pip sama dengan lab Bab 2.
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && useradd -m -u 10001 lab
RUN mkdir -p /data && chown lab /data
USER lab
# baris berikut sama dengan lab Bab 2 agar layer dipakai bersama (ekstensi avro tidak dipakai di Bab 3)
RUN python -c "import duckdb; duckdb.sql('INSTALL avro')" || echo "ekstensi avro tidak terpasang, memakai fastavro"
WORKDIR /app
COPY --chown=lab *.py /app/
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=60 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2)"
CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--no-access-log", "--log-level", "warning"]
