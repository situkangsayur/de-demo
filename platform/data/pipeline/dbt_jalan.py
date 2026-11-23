"""ELT langkah T: `dbt build` di ClickHouse (raw -> stg -> gold), lalu hasil test disimpan.

Dua tahap: (1) model staging + test-nya sebagai gerbang, (2) seed + model gold + test gold.
Bila tahap 1 gagal, tahap 2 tidak dijalankan sama sekali sehingga database gold tidak berubah (W8). Hasil setiap test (lulus/gagal/peringatan/
dilewati) disimpan ke meta.dq_hasil dengan jalur 'elt' agar tampil di dashboard bersama hasil ETL.
Kode keluar mengikuti dbt: gagal -> task Airflow gagal.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys

from .konfigurasi import clickhouse, log

PROYEK = os.path.join(os.environ.get("DATA_DIR", "/opt/data"), "dbt", "bank_elt")
DBT = os.environ.get("DBT_BIN", "dbt")
DIMENSI_BAWAAN = {"not_null": "completeness", "unique": "uniqueness", "accepted_values": "validity",
                  "relationships": "consistency", "rentang_nilai": "validity"}
STATUS = {"pass": "lulus", "fail": "gagal", "error": "gagal", "warn": "peringatan", "skipped": "dilewati"}


def main() -> None:
    tgl = sys.argv[1] if len(sys.argv) > 1 else dt.date.today().isoformat()
    env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false")
    dasar = [DBT, "build", "--profiles-dir", ".", "--target", "lab", "--log-format", "text"]
    hasil_rr: list = []

    def jalan(argumen: list[str]) -> int:
        p = subprocess.run(dasar + argumen, cwd=PROYEK, env=env)
        with open(os.path.join(PROYEK, "target", "run_results.json")) as f:
            hasil_rr.extend(json.load(f)["results"])
        return p.returncode

    # Tahap 1 (gerbang): model staging + test yang induknya hanya staging.
    # Tahap 2 hanya jalan bila tahap 1 lulus: seed, dim_tanggal, model gold, dan test gold.
    # Dengan begitu tidak ada satu pun tabel gold (termasuk seed) yang ditulis ulang saat data rusak.
    rc = jalan(["--select", "path:models/staging", "--indirect-selection", "cautious"])
    if rc == 0:
        rc = jalan(["--exclude", "path:models/staging"])
    else:
        print("test staging gagal: tahap gold tidak dijalankan", file=sys.stderr)
    p = type("Hasil", (), {"returncode": rc})
    rr = {"results": hasil_rr}
    with open(os.path.join(PROYEK, "target", "manifest.json")) as f:
        nodes = json.load(f)["nodes"]
    run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    rows, model_dilewati = [], []
    for r in rr["results"]:
        n = nodes.get(r["unique_id"], {})
        if n.get("resource_type") == "model" and r["status"] == "skipped":
            model_dilewati.append(n["name"])
        if n.get("resource_type") != "test":
            continue
        meta = (n.get("config") or {}).get("meta") or {}
        tm = n.get("test_metadata") or {}
        dimensi = meta.get("dimensi") or DIMENSI_BAWAAN.get(tm.get("name", ""), "validity")
        induk = (n.get("depends_on") or {}).get("nodes") or []
        lapisan = "gold" if any(".gold." in x or x.split(".")[-1].startswith(("dim_", "fakta_", "fitur_")) for x in induk) \
            else "stg"
        kolom = n.get("column_name") or ""
        aturan = f"{tm.get('name', 'singular')} {kolom}".strip() + (
            f" {json.dumps(tm.get('kwargs', {}).get('values', ''), ensure_ascii=False)}" if tm.get("name") == "accepted_values" else "")
        status = STATUS.get(r["status"], r["status"])
        rows.append([now, run_id, "elt", lapisan, n["name"], dimensi, aturan, status,
                     (n.get("config") or {}).get("severity", "ERROR").lower().replace("error", "error"),
                     int(r.get("failures") or 0), 0, None, (r.get("message") or "")[:2000], dt.date.fromisoformat(tgl),
                     dt.date.fromisoformat(tgl)])
        if status in ("gagal", "peringatan"):
            log("error" if status == "gagal" else "warn", f"DQ {status.upper()}: {n['name']}", dimensi=dimensi,
                gagal=r.get("failures"), pesan=r.get("message"))
    if rows:
        clickhouse().insert("meta.dq_hasil", rows, column_names=[
            "waktu", "run_id", "jalur", "lapisan", "tes", "dimensi", "aturan", "status", "keparahan", "gagal",
            "total", "nilai", "contoh", "tgl_awal", "tgl_akhir"])
    if p.returncode != 0:
        print(f"\n=== dbt build GAGAL: gold tidak diperbarui {('(dilewati: ' + ', '.join(model_dilewati) + ')') if model_dilewati else ''} ===",
              file=sys.stderr)
        sys.exit(p.returncode)
    log("info", "dbt build lulus", jumlah_test=len(rows))


if __name__ == "__main__":
    main()
