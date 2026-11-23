"""DAG harian lab Bab 6: transaksi bank syariah dari bronze ke gold ClickHouse dengan dbt.

Graph:

  tunggu_bronze (sensor) -+-> ambil_kontrol_core_banking (sumber flaky: retry + exponential backoff)
                          +-> muat_raw_transaksi (timpa partisi dt)  -+-> gold_naif (INSERT biasa, tanpa gerbang)
                          +-> muat_raw_dimensi (full load)            |
                          +-> muat_raw_akad (timpa partisi dt)        |
        semua muat_* + kontrol -> dbt_run (WRITE ke database audit)
                               -> dbt_test (AUDIT: gerbang mutu, tanpa retry)
                               -> publikasi_gold (PUBLISH: REPLACE PARTITION audit -> gold)
        dbt_test selesai (lulus/gagal) -> dbt_docs (manifest + catalog untuk lineage dan dokumentasi)

Konsep slide P06 yang diperlihatkan:
- dependensi, sensor (mode reschedule), retry dengan exponential backoff;
- tanggal logis: run 16 Nov selalu mengolah data 16 Nov kapan pun dijalankan (tanggal WIB dari
  logical_date, bukan {{ ds }} yang ber-zona UTC);
- idempotency: muat_raw dan publikasi menimpa partisi tanggalnya; gold_naif menambah baris (pembanding);
- backfill: POST /api/v2/backfills atau `airflow backfill create`;
- SLA: Deadline Alert Airflow 3 (run harus selesai <= SLA_MENIT sejak masuk antrean);
- write-audit-publish: gold hanya berubah bila dbt test lulus.
"""
from __future__ import annotations

import datetime
import glob
import json
import os
import urllib.request

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import CronDataIntervalTimetable, dag, get_current_context, task
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference

from lab import ch

ZONA = "Asia/Jakarta"
BRONZE = os.environ.get("BRONZE_DIR", "/data/bronze")
API = os.environ.get("LAB_API_URL", "http://api:8080")
DBT_DIR = "/opt/lab/dbt/trx_lab"
SLA_MENIT = float(os.environ.get("SLA_MENIT", "5"))
DBT = (f"cd {DBT_DIR} && $PY_DBT /opt/lab/dbt/jalan_dbt.py")
# Tanggal data (WIB). Perhatian: template {{ ds }} adalah logical_date dalam UTC; untuk interval
# 16 Nov 00.00 WIB nilainya "2026-11-15". Karena itu tanggal diambil dari logical_date dalam WIB.
TGL = "{{ logical_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}"


def tgl_wib(logical_date) -> str:
    return pendulum.instance(logical_date).in_timezone(ZONA).to_date_string()


def _partisi(entitas: str, ds: str) -> str:
    return f"{BRONZE}/app/{entitas}/dt={ds}"


def _baca_jsonl(pola: str, ds: str | None = None) -> list[dict]:
    rows = []
    for f in sorted(glob.glob(pola)):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    r = json.loads(line)
                    if ds:
                        r["dt"] = ds
                    rows.append(r)
    return rows


def bronze_siap(ds: str) -> bool:
    """Sensor: partisi bronze dt=ds dianggap lengkap bila penanda _SUCCESS sudah ada."""
    ada = os.path.exists(f"{_partisi('transaksi', ds)}/_SUCCESS")
    ch.log("cek penanda bronze", tgl=ds, ada=ada)
    return ada


@dag(
    dag_id="trx_harian",
    # Interval data harian WIB: run berlogika 16 Nov 00.00 WIB mengolah data 16 Nov, dijalankan 17 Nov 00.00.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=ZONA),
    start_date=pendulum.datetime(2026, 10, 1, tz=ZONA),
    catchup=False,             # hari lampau diisi dengan backfill eksplisit
    max_active_runs=1,         # model audit dipakai bergantian; backfill berjalan berurutan
    is_paused_upon_creation=False,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": datetime.timedelta(seconds=5),
    },
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=datetime.timedelta(minutes=SLA_MENIT),
        callback=AsyncCallback("lab.sla.catat_sla_terlewat"),
    ),
    tags=["bab06", "dbt", "data-quality", "write-audit-publish"],
    doc_md=__doc__,
)
def trx_harian():
    tunggu = PythonSensor(
        task_id="tunggu_bronze",
        python_callable=bronze_siap,
        op_kwargs={"ds": TGL},
        mode="reschedule",      # slot worker dilepas selama menunggu
        poke_interval=10,
        timeout=15 * 60,
        retries=0,
    )

    @task(retries=4, retry_delay=datetime.timedelta(seconds=4), retry_exponential_backoff=2.0,
          max_retry_delay=datetime.timedelta(seconds=60))
    def ambil_kontrol_core_banking(logical_date=None) -> dict:
        """Ambil total kontrol harian dari API core banking (bisa gagal sementara: 503)."""
        ds = tgl_wib(logical_date)
        ti = get_current_context()["ti"]
        ch.log("panggil core banking", tgl=ds, percobaan=ti.try_number)
        with urllib.request.urlopen(f"{API}/api/core-banking/kontrol?tgl={ds}", timeout=10) as r:
            k = json.loads(r.read())
        ch.kueri(f"ALTER TABLE raw.kontrol DROP PARTITION '{ds}'")
        ch.sisip_json("raw.kontrol", [{"dt": ds, "jumlah_trx": k["jumlah_trx"], "total_nominal": k["total_nominal"],
                                       "sumber": "core-banking",
                                       "diambil_at": pendulum.now("UTC").strftime("%Y-%m-%d %H:%M:%S")}])
        return k

    @task
    def muat_raw_transaksi(logical_date=None) -> int:
        """Timpa partisi dt=ds: DROP PARTITION lalu INSERT. Run ulang = hasil sama (idempotent)."""
        ds = tgl_wib(logical_date)
        rows = _baca_jsonl(f"{_partisi('transaksi', ds)}/part-*.jsonl", ds)
        ch.kueri(f"ALTER TABLE raw.transaksi DROP PARTITION '{ds}'")
        n = ch.sisip_json("raw.transaksi", rows)
        ch.log("raw transaksi dimuat", tgl=ds, baris=n)
        return n

    @task
    def muat_raw_akad(logical_date=None) -> int:
        ds = tgl_wib(logical_date)
        rows = _baca_jsonl(f"{_partisi('akad_pembiayaan', ds)}/part-*.jsonl", ds)
        ch.kueri(f"ALTER TABLE raw.akad_pembiayaan DROP PARTITION '{ds}'")
        return ch.sisip_json("raw.akad_pembiayaan", rows)

    @task
    def muat_raw_dimensi() -> dict:
        """Full load tabel kecil (cabang, rekening): kosongkan lalu isi ulang."""
        hasil = {}
        for e in ("cabang", "rekening"):
            rows = _baca_jsonl(f"{BRONZE}/app/{e}/snapshot*.jsonl")
            ch.kueri(f"TRUNCATE TABLE raw.{e}")
            hasil[e] = ch.sisip_json(f"raw.{e}", rows)
        return hasil

    @task(retries=0)
    def gold_naif(logical_date=None, run_id=None) -> int:
        """Pembanding yang SALAH: INSERT biasa langsung dari raw, tanpa uji dan tanpa timpa partisi."""
        ds = tgl_wib(logical_date)
        ch.kueri(f"""
            INSERT INTO gold.harian_cabang_naif
            SELECT dt, toUInt16OrZero(cabang_id), count(), sum(toInt64OrZero(nominal)), now(), '{run_id}'
            FROM raw.transaksi WHERE dt = '{ds}' GROUP BY dt, cabang_id""")
        return 1

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=DBT + " run " + TGL + " {{ run_id }}",
        retries=1,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT + " test " + TGL + " {{ run_id }}",
        retries=0,              # data rusak tidak membaik dengan dicoba ulang
    )

    @task(retries=1)
    def publikasi_gold(logical_date=None, run_id=None) -> dict:
        """PUBLISH: ganti partisi tgl di gold dengan isi audit secara atomik (REPLACE PARTITION)."""
        ds = tgl_wib(logical_date)
        hasil = {}
        for t in ("fakta_transaksi", "harian_cabang", "pembiayaan_harian"):
            ch.kueri(f"CREATE TABLE IF NOT EXISTS gold.{t} AS audit.{t}")
            ch.kueri(f"ALTER TABLE gold.{t} REPLACE PARTITION '{ds}' FROM audit.{t}")
            kol = "total_nominal" if t == "harian_cabang" else ("nominal" if t == "fakta_transaksi" else "total_pokok")
            agg = "sum(jumlah_trx)" if t == "harian_cabang" else "count()"
            r = ch.baris(f"SELECT toUInt64({agg}) AS baris, toInt64(sum({kol})) AS total FROM gold.{t} WHERE tgl = '{ds}'")[0]
            ch.sisip_json("meta.publikasi", [{"run_id": run_id, "tgl": ds, "tabel": f"gold.{t}",
                                              "waktu": pendulum.now("UTC").strftime("%Y-%m-%d %H:%M:%S.%f")[:23],
                                              "baris": int(r["baris"]), "total_nominal": int(r["total"])}])
            hasil[t] = r
        ch.log("gold dipublikasikan", tgl=ds, run_id=run_id)
        return hasil

    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=DBT + " docs " + TGL + " {{ run_id }}",
        trigger_rule="all_done",   # dokumentasi dan lineage tetap diperbarui walau uji gagal
        retries=0,
    )

    kontrol = ambil_kontrol_core_banking()
    raw_trx = muat_raw_transaksi()
    raw_akad = muat_raw_akad()
    raw_dim = muat_raw_dimensi()
    tunggu >> [kontrol, raw_trx, raw_akad, raw_dim]
    raw_trx >> gold_naif()
    [kontrol, raw_trx, raw_akad, raw_dim] >> dbt_run >> dbt_test >> publikasi_gold()
    dbt_test >> dbt_docs


trx_harian()
