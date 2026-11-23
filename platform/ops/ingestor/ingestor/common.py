"""Bagian bersama: konfigurasi dari environment, logger JSON satu baris, filesystem S3 (RustFS)."""

import json
import logging
import os
import sys
import time

SERVICE = os.environ.get("SERVICE_NAME", "ingestor")


class JsonFormatter(logging.Formatter):
    """Log JSON satu baris: time, level, msg, service, + field tambahan (extra={"f": {...}})."""

    def format(self, record: logging.LogRecord) -> str:
        out = {
            "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "msg": record.getMessage(),
            "service": SERVICE,
        }
        out.update(getattr(record, "f", {}) or {})
        if record.exc_info:
            out["err"] = self.formatException(record.exc_info)
        return json.dumps(out, ensure_ascii=False, default=str)


def get_logger(name: str = SERVICE) -> logging.Logger:
    log = logging.getLogger(name)
    if not log.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(JsonFormatter())
        log.addHandler(h)
        log.setLevel(logging.INFO)
        log.propagate = False
    return log


def env(key: str, default: str) -> str:
    v = os.environ.get(key, "")
    return v if v else default


def s3fs(allow_bucket_creation: bool = False):
    """Filesystem S3 PyArrow yang menunjuk ke RustFS."""
    from pyarrow import fs

    endpoint = env("S3_ENDPOINT", "http://rustfs:9000")
    scheme = "https" if endpoint.startswith("https://") else "http"
    host = endpoint.split("://", 1)[-1].rstrip("/")
    return fs.S3FileSystem(
        access_key=os.environ["S3_ACCESS_KEY"],
        secret_key=os.environ["S3_SECRET_KEY"],
        endpoint_override=host,
        scheme=scheme,
        region=env("S3_REGION", "us-east-1"),
        allow_bucket_creation=allow_bucket_creation,
        connect_timeout=5,
        request_timeout=30,
    )


def bucket() -> str:
    return env("S3_BUCKET", "lake")
