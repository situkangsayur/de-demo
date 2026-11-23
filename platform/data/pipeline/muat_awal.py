"""Sekali saat platform menyala: picu run muat awal (semua tanggal bronze s.d. kemarin), lalu
aktifkan jadwal harian DAG bank_harian. Dilewati bila run muat awal sudah pernah dibuat.
MUAT_AWAL=0 mematikan langkah ini (uji-data.sh mengendalikan run sendiri).
"""
from __future__ import annotations

import datetime as dt
import os
import time
import urllib.error

from .airflow_api import panggil
from .konfigurasi import log

DAG = "bank_harian"


def main() -> None:
    if os.environ.get("MUAT_AWAL", "1") != "1":
        log("info", "muat awal dimatikan (MUAT_AWAL=0)")
        return
    for _ in range(120):
        try:
            panggil("GET", f"/dags/{DAG}")
            break
        except urllib.error.HTTPError:
            time.sleep(3)
    kemarin = (dt.datetime.now(dt.timezone(dt.timedelta(hours=7))).date() - dt.timedelta(days=1)).isoformat()
    run_id = f"muat-awal-{kemarin}"
    try:
        panggil("POST", f"/dags/{DAG}/dagRuns", {"dag_run_id": run_id, "logical_date": None,
                                                 "conf": {"tgl": kemarin, "penuh": True},
                                                 "note": "muat awal: semua tanggal bronze"})
        log("info", "run muat awal dipicu", run_id=run_id)
    except urllib.error.HTTPError as e:
        log("info", "run muat awal sudah ada", run_id=run_id, status=e.code)
    panggil("PATCH", f"/dags/{DAG}", {"is_paused": False})
    log("info", "jadwal harian aktif", dag=DAG)


if __name__ == "__main__":
    main()
