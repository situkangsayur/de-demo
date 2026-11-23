"""Konfigurasi lab Bab 4: object storage RustFS, katalog Iceberg SQL (SQLite), dan log JSON.

Semua nilai datang dari environment (diisi compose dari .env). Tidak ada secret di kode.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
from functools import lru_cache

BUCKET = os.environ.get("S3_BUCKET", "lake")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
KATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "sqlite:////data/katalog.db")
TOKEN_PEPPER = os.environ.get("TOKEN_PEPPER", "")
SERVICE = os.environ.get("SERVICE_NAME", "bab04-api")
ZONA = "Asia/Jakarta"

# Opsional: bronze platform end-to-end (bila platform menyala). Kosong = tidak dipakai.
PLATFORM_S3_ENDPOINT = os.environ.get("PLATFORM_S3_ENDPOINT", "")
PLATFORM_S3_ACCESS_KEY = os.environ.get("PLATFORM_S3_ACCESS_KEY", "")
PLATFORM_S3_SECRET_KEY = os.environ.get("PLATFORM_S3_SECRET_KEY", "")
PLATFORM_S3_BUCKET = os.environ.get("PLATFORM_S3_BUCKET", "lake")


def log(level: str, msg: str, **kv) -> None:
    """Log JSON satu baris (time, level, msg, service, request_id) ke stdout."""
    rec = {"time": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"),
           "level": level, "msg": msg, "service": SERVICE}
    rec.update(kv)
    print(json.dumps(rec, default=str, ensure_ascii=False), file=sys.stdout, flush=True)


def _s3fs(endpoint: str, ak: str, sk: str):
    from pyarrow import fs
    sch, host = endpoint.split("://", 1)
    return fs.S3FileSystem(access_key=ak, secret_key=sk, endpoint_override=host, scheme=sch,
                           region=S3_REGION, force_virtual_addressing=False, allow_bucket_creation=True)


@lru_cache(maxsize=1)
def s3fs():
    """FileSystem PyArrow ke RustFS lab."""
    return _s3fs(S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY)


def s3fs_platform():
    if not PLATFORM_S3_ENDPOINT:
        raise RuntimeError("PLATFORM_S3_ENDPOINT belum diisi di .env; platform tidak dipakai")
    return _s3fs(PLATFORM_S3_ENDPOINT, PLATFORM_S3_ACCESS_KEY, PLATFORM_S3_SECRET_KEY)


def buat_bucket() -> None:
    """Membuat bucket lab bila belum ada (RustFS butuh beberapa detik saat start)."""
    f = s3fs()
    for i in range(30):
        try:
            f.create_dir(BUCKET)
            return
        except OSError as e:
            if "BucketAlready" in str(e) or "already" in str(e).lower():
                return
            log("warn", "menunggu RustFS", percobaan=i + 1, error=str(e)[:160])
            time.sleep(2)
    raise RuntimeError("RustFS tidak dapat dijangkau")


@lru_cache(maxsize=1)
def katalog():
    """Katalog Iceberg SQL. Di platform tabelnya di PostgreSQL; di lab cukup SQLite di volume."""
    from pyiceberg.catalog.sql import SqlCatalog
    return SqlCatalog("lake", **{
        "uri": KATALOG_URI,
        "warehouse": f"s3://{BUCKET}/",
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "s3.region": S3_REGION,
        "s3.force-virtual-addressing": "false",
        "init_catalog_tables": "true",
    })


def duckdb_lake():
    """DuckDB yang dapat membaca s3://lake (httpfs) dan metadata Iceberg (iceberg_scan)."""
    import duckdb
    con = duckdb.connect()
    con.execute("LOAD httpfs; LOAD iceberg;")
    host = S3_ENDPOINT.split("://", 1)[1]
    con.execute(f"""CREATE OR REPLACE SECRET lake (TYPE s3, KEY_ID '{S3_ACCESS_KEY}',
                    SECRET '{S3_SECRET_KEY}', ENDPOINT '{host}', REGION '{S3_REGION}',
                    URL_STYLE 'path', USE_SSL {str(S3_ENDPOINT.startswith('https')).lower()})""")
    return con


class Stopwatch:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *a):
        self.ms = round((time.perf_counter() - self.t0) * 1000, 1)
