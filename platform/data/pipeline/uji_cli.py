"""Alat bantu uji-data.sh (dijalankan di container airflow-scheduler). Keluaran JSON satu baris.

  trigger TGL [penuh]        picu run (conf tgl), tunggu selesai, cetak status run + status task
  backfill DARI SAMPAI       backfill Airflow 3 (reprocess completed), tunggu selesai
  ringkas DARI SAMPAI        per tgl: baris, total, trx_unik di lake dan warehouse + snapshot gold + part ClickHouse
  sabotase sisipkan|pulihkan TGL
  aktifkan                   unpause DAG (run manual pada DAG yang di-pause tetap queued)
  dq_gagal                   test gagal terakhir dari meta.dq_hasil
"""
from __future__ import annotations

import datetime as dt
import json
import sys
import time

import polars as pl
from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual, And

from . import iceberg_util as ice, sabotase
from .airflow_api import panggil
from .konfigurasi import clickhouse

DAG = "bank_harian"


def _tasks(run_id: str) -> dict:
    ti = panggil("GET", f"/dags/{DAG}/dagRuns/{run_id}/taskInstances?limit=100")["task_instances"]
    return {t["task_id"]: t["state"] for t in ti}


def tunggu_run(run_id: str, batas: int = 1800) -> str:
    t0 = time.time()
    while time.time() - t0 < batas:
        st = panggil("GET", f"/dags/{DAG}/dagRuns/{run_id}")["state"]
        if st in ("success", "failed"):
            return st
        time.sleep(5)
    return "timeout"


def trigger(tgl: str, penuh: bool) -> dict:
    run_id = f"uji-{tgl}-{int(time.time())}"
    panggil("POST", f"/dags/{DAG}/dagRuns", {"dag_run_id": run_id, "logical_date": None,
                                             "conf": {"tgl": tgl, "penuh": penuh}})
    t0 = time.time()
    st = tunggu_run(run_id)
    return {"run_id": run_id, "state": st, "detik": round(time.time() - t0, 1), "tasks": _tasks(run_id)}


def rentang_backfill(dari: str, sampai: str) -> dict:
    """Isi permintaan backfill agar run yang dibuat tepat untuk tgl WIB dari..sampai.

    Logical date run tgl D = D 00.00 WIB. Batas bawah backfill Airflow tidak selalu inklusif terhadap
    logical date, jadi dicek dengan dry run dan digeser satu hari bila tanggal pertama tidak ikut.
    """
    d0 = dt.date.fromisoformat(dari)
    for geser in (0, 1):
        isi = {"dag_id": DAG, "from_date": f"{d0 - dt.timedelta(days=geser)}T00:00:00+07:00",
               "to_date": f"{sampai}T00:00:00+07:00", "reprocess_behavior": "completed",
               "max_active_runs": 1, "run_backwards": False, "dag_run_conf": {}}
        coba = panggil("POST", "/backfills/dry_run", isi).get("backfills", [])
        tgl = {(dt.datetime.fromisoformat(x["logical_date"].replace("Z", "+00:00")) + dt.timedelta(hours=7)).date()
               for x in coba if x.get("logical_date")}
        if d0 in tgl:
            return isi
    return isi


def backfill(dari: str, sampai: str) -> dict:
    t0 = time.time()
    isi = rentang_backfill(dari, sampai)
    b = panggil("POST", "/backfills", isi)
    while time.time() - t0 < 3600:
        x = panggil("GET", f"/backfills/{b['id']}")
        if x.get("completed_at"):
            break
        time.sleep(5)
    # run backfill: run_id backfill__..., logical_date di dalam rentang (dibuat ulang tiap backfill)
    awal = dt.datetime.fromisoformat(f"{dari}T00:00:00+07:00").astimezone(dt.timezone.utc).isoformat()[:19]
    akhir = dt.datetime.fromisoformat(f"{sampai}T00:00:00+07:00").astimezone(dt.timezone.utc).isoformat()[:19]
    runs = panggil("GET", f"/dags/{DAG}/dagRuns?limit=100&order_by=-run_after")["dag_runs"]
    runs = [r for r in runs if r["dag_run_id"].startswith("backfill__") and r.get("logical_date")
            and awal <= r["logical_date"][:19] <= akhir]
    return {"backfill_id": b["id"], "detik": round(time.time() - t0, 1),
            "runs": sorted((r["logical_date"][:19], r["state"]) for r in runs)}


def ringkas(dari: str, sampai: str) -> dict:
    a, z = dt.date.fromisoformat(dari), dt.date.fromisoformat(sampai)
    f = And(GreaterThanOrEqual("tgl", dari), LessThanOrEqual("tgl", sampai))
    lk = (ice.baca("gold.fakta_transaksi", f, kolom=("tgl", "trx_id", "nominal")).group_by("tgl")
            .agg(pl.len().alias("baris"), pl.col("nominal").cast(pl.Float64).sum().round(2).alias("total"),
                 pl.col("trx_id").n_unique().alias("trx_unik")).sort("tgl"))
    ch = clickhouse()
    wh = ch.query(f"""select toString(tgl), count(), round(toFloat64(sum(nominal)), 2), uniqExact(trx_id)
                      from gold.fakta_transaksi where tgl between '{a}' and '{z}' group by tgl order by tgl""").result_rows
    sv = (ice.baca("silver.transaksi", f, kolom=("tgl", "trx_id")).group_by("tgl").len().sort("tgl"))
    snap = {}
    for t in ("fakta_transaksi", "dim_nasabah", "fakta_pembiayaan", "fitur_nasabah"):
        s = ice.muat(f"gold.{t}").current_snapshot()
        snap[t] = s.snapshot_id if s else None
    parts = ch.query("""select table, toString(max(modification_time)), sum(rows) from system.parts
                        where database = 'gold' and active group by table order by table""").result_rows
    return {"lake": [[str(r[0]), r[1], r[2], r[3]] for r in lk.iter_rows()],
            "warehouse": [list(r) for r in wh],
            "silver": [[str(r[0]), r[1]] for r in sv.iter_rows()],
            "snapshot_gold": snap, "part_gold_clickhouse": [list(r) for r in parts]}


def dq_gagal() -> list:
    return [list(r) for r in clickhouse().query(
        """select jalur, tes, dimensi, gagal, substring(aturan, 1, 120), substring(contoh, 1, 300)
           from meta.dq_hasil where status = 'gagal' and waktu > now() - interval 30 minute
           order by waktu desc limit 15""").result_rows]


def main() -> None:
    k = sys.argv[1]
    if k == "trigger":
        out = trigger(sys.argv[2], len(sys.argv) > 3 and sys.argv[3] == "penuh")
    elif k == "backfill":
        out = backfill(sys.argv[2], sys.argv[3])
    elif k == "ringkas":
        out = ringkas(sys.argv[2], sys.argv[3])
    elif k == "sabotase":
        d = dt.date.fromisoformat(sys.argv[3])
        out = sabotase.sisipkan(d) if sys.argv[2] == "sisipkan" else sabotase.pulihkan(d)
    elif k == "aktifkan":
        # run manual pada DAG yang di-pause tetap queued; jadwal harian diaktifkan dulu
        out = {"is_paused": panggil("PATCH", f"/dags/{DAG}", {"is_paused": False})["is_paused"]}
    elif k == "dq_gagal":
        out = dq_gagal()
    else:
        raise SystemExit(f"perintah tidak dikenal: {k}")
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
