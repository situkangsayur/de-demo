# API + benchmark lab Bab 5 (Python): Arrow, Polars, DuckDB, DataFusion, pandas.
# Base image dan requirements.txt identik dengan bab04 agar layer pip dipakai bersama.
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && useradd -m -u 10001 lab && mkdir -p /data && chown lab:lab /data
USER lab
WORKDIR /app
COPY --chown=lab:lab app/ /app/app/
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
