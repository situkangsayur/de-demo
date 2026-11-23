"""Uji Q2 (slide P08): menyisipkan data rusak ke bronze transaksi satu tanggal, atau menghapusnya.

Data rusak yang disisipkan (event aplikasi palsu):
  1. duplikat trx_id yang sudah ada dengan nominal berbeda  -> consistency (app vs cdc) gagal
  2. kanal di luar domain ('satelit')                         -> validity gagal
  3. rekening_id kosong                                       -> completeness gagal
  4. nominal negatif                                          -> validity gagal
Pemakaian: python -m pipeline.sabotase sisipkan|pulihkan 2026-11-22
"""
from __future__ import annotations

import datetime as dt
import sys
import uuid

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from . import bronze
from .konfigurasi import BUCKET, log, s3fs

NAMA = "part-sabotase-uji-q2.parquet"


def _path(d: dt.date) -> str:
    # dt bronze = tanggal UTC; transaksi siang hari WIB jatuh di dt yang sama, berkas ditaruh di dt=d
    return f"{BUCKET}/bronze/app/transaksi/dt={d.isoformat()}/{NAMA}"


def sisipkan(d: dt.date) -> dict:
    # transaksi tgl WIB = d yang tercatat di CDC, agar duplikat app bernominal beda memicu konflik
    asli = bronze.baca("transaksi", [d - dt.timedelta(days=1), d, d + dt.timedelta(days=1)]).filter(
        (pl.col("waktu").dt.convert_time_zone("Asia/Jakarta").dt.date() == d) & (pl.col("_source") == "cdc"))
    if asli.height == 0:
        raise SystemExit(f"tidak ada transaksi cdc bertanggal {d} di bronze")
    contoh = asli.sort("id").row(0, named=True)
    now = dt.datetime.now(dt.timezone.utc)
    waktu = contoh["waktu"]
    base = {"_ingested_at": now, "_source": "app", "_batch_id": "sabotase-q2", "_op": None,
            "_event_type": "bank.transaksi.dibuat", "_occurred_at": waktu.isoformat(),
            "_ts_ms": str(int(now.timestamp() * 1000)), "cabang_id": str(contoh["cabang_id"]),
            "jenis": "transfer_keluar", "occurred_at": waktu.isoformat(), "created_at": waktu.isoformat()}
    rows = [
        {**base, "_event_id": str(uuid.uuid4()), "id": contoh["id"], "rekening_id": str(contoh["rekening_id"]),
         "kanal": "mobile", "nominal": str(float(contoh["nominal"]) + 999_000)},
        {**base, "_event_id": str(uuid.uuid4()), "id": f"SAB{d:%Y%m%d}0002", "rekening_id": str(contoh["rekening_id"]),
         "kanal": "satelit", "nominal": "150000"},
        {**base, "_event_id": str(uuid.uuid4()), "id": f"SAB{d:%Y%m%d}0003", "rekening_id": None,
         "kanal": "atm", "nominal": "200000"},
        {**base, "_event_id": str(uuid.uuid4()), "id": f"SAB{d:%Y%m%d}0004", "rekening_id": str(contoh["rekening_id"]),
         "kanal": "web", "nominal": "-5000"},
    ]
    skema = pa.schema([("_ingested_at", pa.timestamp("us", "UTC"))] +
                      [(k, pa.string()) for k in rows[0] if k != "_ingested_at"])
    tbl = pa.Table.from_pylist(rows, schema=skema)
    pq.write_table(tbl, _path(d), filesystem=s3fs())
    log("warn", "data rusak disisipkan ke bronze", path=_path(d), baris=len(rows), trx_duplikat=contoh["id"])
    return {"path": _path(d), "baris": len(rows), "trx_duplikat": contoh["id"]}


def pulihkan(d: dt.date) -> dict:
    f = s3fs()
    try:
        f.delete_file(_path(d))
        ada = True
    except (FileNotFoundError, OSError):
        ada = False
    log("info", "berkas sabotase dihapus", path=_path(d), ada=ada)
    return {"path": _path(d), "dihapus": ada}


def ada_sabotase() -> list[str]:
    return [p for p, _ in bronze.daftar_berkas("transaksi") if p.endswith(NAMA)]


if __name__ == "__main__":
    aksi, tgl = sys.argv[1], dt.date.fromisoformat(sys.argv[2])
    print(sisipkan(tgl) if aksi == "sisipkan" else pulihkan(tgl))
