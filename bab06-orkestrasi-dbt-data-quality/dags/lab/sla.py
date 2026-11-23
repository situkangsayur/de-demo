"""Callback Deadline Alert Airflow 3 (pengganti SLA Airflow 2). Berjalan di triggerer."""
from __future__ import annotations

import datetime
import json

from lab import ch


async def catat_sla_terlewat(context: dict | None = None, **kwargs) -> None:
    run = (context or {}).get("dag_run", {}) or {}
    batas = str(((context or {}).get("deadline") or {}).get("deadline_time", ""))
    tgl = ""
    if run.get("logical_date"):  # tanggal data dalam WIB, sama dengan kolom tgl di UI
        ld = datetime.datetime.fromisoformat(str(run["logical_date"]).replace("Z", "+00:00"))
        tgl = (ld + datetime.timedelta(hours=7)).strftime("%Y-%m-%d")
    ch.sisip_json("meta.sla_terlewat", [{
        "run_id": run.get("dag_run_id") or run.get("run_id") or "",
        "tgl": tgl, "batas": batas,
        "dicatat": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:23],
    }])
    ch.log("deadline DAG run terlewat", level="warning", run_id=run.get("dag_run_id"), batas=batas,
           detail=json.dumps(kwargs)[:200])
