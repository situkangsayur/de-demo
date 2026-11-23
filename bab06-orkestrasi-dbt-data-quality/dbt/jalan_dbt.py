"""Pembungkus dbt untuk Airflow: run / test / docs untuk satu tanggal, lalu catat hasil test ke ClickHouse.

Pemakaian: python jalan_dbt.py <run|test|docs> <ds> <run_id>
Kode keluar sama dengan kode keluar dbt sehingga task Airflow gagal bila ada test berkeparahan error gagal.
"""
from __future__ import annotations

import datetime
import json
import os
import re
import shutil
import subprocess
import sys

sys.path.insert(0, "/opt/airflow/dags")
from lab import ch  # noqa: E402

TARGET = "/opt/lab/dbt_target/target"
RUNS = "/opt/lab/dbt_target/runs"


def jalankan(perintah: str, ds: str) -> int:
    dbt = os.environ.get("DBT_BIN", "dbt")
    sub = {"run": ["run"], "test": ["test"], "docs": ["docs", "generate"]}[perintah]
    args = [dbt, *sub, "--vars", json.dumps({"tgl": ds}), "--profiles-dir", ".", "--target-path", TARGET,
            "--log-path", "/tmp/dbt-logs", "--no-use-colors"]
    print("$", " ".join(args), flush=True)
    return subprocess.call(args)


def catat_test(ds: str, run_id: str) -> None:
    with open(f"{TARGET}/run_results.json", encoding="utf-8") as f:
        hasil = json.load(f)
    with open(f"{TARGET}/manifest.json", encoding="utf-8") as f:
        nodes = json.load(f)["nodes"]
    waktu = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]
    rows = []
    for r in hasil["results"]:
        n = nodes.get(r["unique_id"], {})
        meta = (n.get("config") or {}).get("meta") or n.get("meta") or {}
        model = ""
        deps = (n.get("depends_on") or {}).get("nodes") or []
        if deps:
            model = deps[-1].split(".")[-1] if n.get("column_name") else ",".join(d.split(".")[-1] for d in deps)
        rows.append({
            "run_id": run_id, "tgl": ds, "waktu": waktu, "unique_id": r["unique_id"],
            "nama": n.get("name", r["unique_id"]), "model": model, "kolom": n.get("column_name") or "",
            "dimensi": meta.get("dimensi", "lainnya"),
            "keparahan": (n.get("config") or {}).get("severity", "ERROR").lower(),
            "status": r["status"], "failures": int(r.get("failures") or 0),
            "pesan": (r.get("message") or "")[:500],
        })
    ch.sisip_json("meta.dq_hasil", rows)
    gagal = [x for x in rows if x["status"] in ("fail", "error")]
    ch.log("hasil dbt test dicatat", tgl=ds, run_id=run_id, jumlah=len(rows), gagal=len(gagal),
           level="error" if gagal else "info")


def main() -> int:
    perintah, ds, run_id = sys.argv[1], sys.argv[2], sys.argv[3]
    kode = jalankan(perintah, ds)
    os.makedirs(RUNS, exist_ok=True)
    aman = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    if os.path.exists(f"{TARGET}/run_results.json"):
        shutil.copy(f"{TARGET}/run_results.json", f"{RUNS}/{aman}-{perintah}.json")
    if perintah == "test" and os.path.exists(f"{TARGET}/run_results.json"):
        catat_test(ds, run_id)
    return kode


if __name__ == "__main__":
    sys.exit(main())
