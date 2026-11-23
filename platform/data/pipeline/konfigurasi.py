"""Konfigurasi bersama: katalog Iceberg, object storage (RustFS), ClickHouse, dan log JSON.

Semua nilai datang dari environment (diisi compose dari .env), tidak ada secret di kode.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
import uuid
from functools import lru_cache

ZONA = "Asia/Jakarta"
BUCKET = os.environ.get("S3_BUCKET", "lake")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
ICEBERG_URI = os.environ.get("ICEBERG_CATALOG_URI", "")
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CH_USER = os.environ.get("CLICKHOUSE_USER", "de")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
SERVICE = os.environ.get("SERVICE_NAME", "data-pipeline")


def log(level: str, msg: str, **kv) -> None:
    """Log JSON satu baris (time, level, msg, service, run_id) ke stdout."""
    rec = {"time": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"),
           "level": level, "msg": msg, "service": SERVICE,
           "run_id": os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "-")}
    rec.update(kv)
    print(json.dumps(rec, default=str, ensure_ascii=False), file=sys.stdout, flush=True)


@lru_cache(maxsize=1)
def katalog():
    """Katalog Iceberg SQL (tabel metadata di PostgreSQL), warehouse di s3://lake/."""
    from pyiceberg.catalog.sql import SqlCatalog
    return SqlCatalog("lake", **{
        "uri": ICEBERG_URI,
        "warehouse": f"s3://{BUCKET}/",
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "s3.region": S3_REGION,
        "s3.path-style-access": "true",
        "init_catalog_tables": "true",
    })


@lru_cache(maxsize=1)
def s3fs():
    """FileSystem PyArrow ke RustFS (dipakai untuk membaca bronze Parquet)."""
    from pyarrow import fs
    sch, host = S3_ENDPOINT.split("://", 1)
    return fs.S3FileSystem(access_key=S3_ACCESS_KEY, secret_key=S3_SECRET_KEY, endpoint_override=host,
                           scheme=sch, region=S3_REGION, force_virtual_addressing=False)


def boto():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS_KEY,
                        aws_secret_access_key=S3_SECRET_KEY, region_name=S3_REGION,
                        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}))


def clickhouse():
    import clickhouse_connect
    return clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER,
                                         password=CH_PASSWORD, connect_timeout=10,
                                         send_receive_timeout=300)


def duckdb_lake():
    """Koneksi DuckDB yang bisa membaca s3://lake (httpfs) dan tabel Iceberg (iceberg_scan)."""
    import duckdb
    con = duckdb.connect()
    con.execute("LOAD httpfs; LOAD iceberg;")
    host = S3_ENDPOINT.split("://", 1)[1]
    con.execute(f"""CREATE OR REPLACE SECRET lake (TYPE s3, KEY_ID '{S3_ACCESS_KEY}',
                    SECRET '{S3_SECRET_KEY}', ENDPOINT '{host}', REGION '{S3_REGION}',
                    URL_STYLE 'path', USE_SSL {str(S3_ENDPOINT.startswith('https')).lower()})""")
    return con


def tanggal(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def rentang(awal: dt.date, akhir: dt.date) -> list[dt.date]:
    return [awal + dt.timedelta(days=i) for i in range((akhir - awal).days + 1)]


def id_baru() -> str:
    return uuid.uuid4().hex[:12]


class Stopwatch:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *a):
        self.ms = round((time.perf_counter() - self.t0) * 1000, 1)
