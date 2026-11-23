"""DAG harian bank syariah digital: bronze -> (ETL ke lake) dan (ELT ke warehouse) -> rekonsiliasi.

Graph view memperlihatkan dua jalur berdampingan:

  tentukan_tgl -> cek_bronze -+-> etl.* : Polars/DuckDB -> silver Iceberg -> dq_silver -> gold staging
                              |           -> dq_gold -> gold_publikasi      (lake.gold.*)
                              +-> elt.* : muat_raw ClickHouse -> dbt_build (stg + test -> gold)
                                                                            (ClickHouse gold.*)
                  etl + elt -> rekonsiliasi_lake_vs_warehouse -> selesai

W6: jadwal harian (00.00 WIB untuk data hari sebelumnya), retries=2.
W7: setiap task menimpa partisi tanggalnya sendiri (Iceberg overwrite partisi, ClickHouse REPLACE
    PARTITION/EXCHANGE TABLES); run ulang dan backfill tidak menggandakan data.
W8: dq_silver/dq_gold/dbt test gagal -> task gold berikutnya upstream_failed -> gold tidak berubah.
    Task DQ tidak di-retry: data rusak tidak membaik dengan dicoba ulang.

Tanggal yang diproses (tgl): dag_run.conf["tgl"] bila ada, selain itu logical_date dalam WIB.
dag_run.conf["penuh"]=true memproses semua tanggal bronze s.d. tgl (muat awal).
"""
from __future__ import annotations

import datetime
import os

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import CronDataIntervalTimetable, TaskGroup, dag, get_current_context, task

PY = os.environ.get("PY_DATA", "python")
DATA = os.environ.get("DATA_DIR", "/opt/data")
ZONA = "Asia/Jakarta"
TGL = "{{ ti.xcom_pull(task_ids='tentukan_tgl')['tgl'] }}"
PENUH = "{{ '--penuh' if ti.xcom_pull(task_ids='tentukan_tgl')['penuh'] else '' }}"


def langkah(task_id: str, modul: str, argumen: str = "", dq: bool = False, **kw) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {DATA} && {PY} -m pipeline.{modul} {argumen}",
        env={"SERVICE_NAME": f"airflow-{task_id}"},
        append_env=True,
        retries=0 if dq else 2,
        **kw,
    )


@dag(
    dag_id="bank_harian",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=ZONA),
    start_date=pendulum.datetime(2026, 10, 1, tz=ZONA),
    catchup=False,           # hari yang terlewat dijalankan lewat backfill eksplisit (Q3)
    max_active_runs=1,       # SCD-2 dan commit Iceberg berurutan; backfill berjalan satu per satu
    default_args={"retries": 2, "retry_delay": pendulum.duration(seconds=30), "owner": "data-eng"},
    tags=["bank-syariah", "medallion", "etl", "elt"],
    params={"tgl": None, "penuh": False},
    doc_md=__doc__,
)
def bank_harian():
    @task
    def tentukan_tgl() -> dict:
        ctx = get_current_context()
        conf = (ctx["dag_run"].conf or {}) if ctx.get("dag_run") else {}
        ld = ctx.get("logical_date") or (ctx["dag_run"].run_after - datetime.timedelta(days=1))
        tgl = conf.get("tgl") or ctx["params"].get("tgl") or pendulum.instance(ld).in_timezone(ZONA).to_date_string()
        penuh = bool(conf.get("penuh") or ctx["params"].get("penuh"))
        print(f"tgl diproses = {tgl}, penuh = {penuh}")
        return {"tgl": tgl, "penuh": penuh}

    t = tentukan_tgl()
    cek = langkah("cek_bronze", "cek_bronze", f"--tgl {TGL}")

    with TaskGroup("etl", tooltip="ETL: transformasi di Python (Polars/DuckDB) lalu dimuat ke lake Iceberg") as etl:
        dims = [langkah(f"silver_{e}", "silver", f"--entitas {e} --tgl {TGL}")
                for e in ("cabang", "nasabah", "rekening", "akad_pembiayaan")]
        trx = langkah("silver_transaksi", "silver", f"--entitas transaksi --tgl {TGL} {PENUH}")
        dq_silver = langkah("dq_silver", "dq", f"--lapisan silver --tgl {TGL} {PENUH}", dq=True)
        gold_siapkan = langkah("gold_siapkan_staging", "gold", f"--tahap siapkan --tgl {TGL} {PENUH}")
        dq_gold = langkah("dq_gold", "dq", f"--lapisan gold --tgl {TGL} {PENUH}", dq=True)
        publikasi = langkah("gold_publikasi", "gold", "--tahap publikasi")
        dims >> trx >> dq_silver >> gold_siapkan >> dq_gold >> publikasi

    with TaskGroup("elt", tooltip="ELT: muat mentah ke ClickHouse, transformasi di warehouse dengan dbt") as elt:
        raw = langkah("muat_raw_clickhouse", "elt_raw", f"--tgl {TGL} {PENUH}")
        dbt = BashOperator(task_id="dbt_build_gold", bash_command=f"cd {DATA} && {PY} -m pipeline.dbt_jalan {TGL}",
                           env={"SERVICE_NAME": "airflow-dbt_build_gold"}, append_env=True, retries=0)
        raw >> dbt

    rekon = langkah("rekonsiliasi_lake_vs_warehouse", "rekonsiliasi", f"--tgl {TGL} {PENUH}", dq=True)

    @task
    def selesai() -> str:
        return "gold lake dan warehouse diperbarui"

    t >> cek >> [etl, elt]
    [etl, elt] >> rekon >> selesai()


bank_harian()
