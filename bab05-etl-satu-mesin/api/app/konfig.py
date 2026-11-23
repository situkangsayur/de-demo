"""Konfigurasi lab Bab 5: direktori data (volume), log JSON, batas memori container."""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

DATA = os.environ.get("DATA_DIR", "/data")
TOKEN_PEPPER = os.environ.get("TOKEN_PEPPER", "")
SERVICE = os.environ.get("SERVICE_NAME", "bab05-api")
UKURAN = [int(x) for x in os.environ.get("UKURAN_DATASET", "200000,1000000,2000000,5000000,10000000").split(",")]


def log(level: str, msg: str, **kv) -> None:
    rec = {"time": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"),
           "level": level, "msg": msg, "service": SERVICE}
    rec.update(kv)
    print(json.dumps(rec, default=str, ensure_ascii=False), file=sys.stdout, flush=True)


def batas_memori_mb() -> int | None:
    """mem_limit container (cgroup v2), dipakai UI untuk menggambar garis batas."""
    for p in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            v = open(p).read().strip()
            if v.isdigit() and int(v) < 1 << 50:
                return int(v) // (1024 * 1024)
        except OSError:
            pass
    return None


def cpu() -> int:
    try:
        q, p = open("/sys/fs/cgroup/cpu.max").read().split()
        if q != "max":
            return max(1, int(int(q) / int(p)))
    except (OSError, ValueError):
        pass
    return os.cpu_count() or 1
