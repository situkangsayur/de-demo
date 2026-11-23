"""ELT langkah E+L: bronze Parquet dimuat MENTAH ke ClickHouse database `raw`.

ClickHouse sendiri yang membaca s3://lake/bronze lewat table function s3() dan named collection
lake_s3 (Python hanya mengirim SQL). Tidak ada transformasi: semua kolom data disimpan sebagai
Nullable(String) persis seperti di Parquet, ditambah dt (dari path), _file, dan _dimuat_pada.
Transformasi (T) terjadi sesudahnya di dalam warehouse dengan dbt.

Idempotent:
- dimensi (kecil): seluruh bronze dimuat ke tabel sementara lalu EXCHANGE TABLES (atomik);
- transaksi: per partisi dt, dimuat ke tabel sementara lalu REPLACE PARTITION (atomik).
Menjalankan ulang tanggal yang sama tidak pernah menggandakan baris.

Pemakaian: python -m pipeline.elt_raw --tgl 2026-11-20 [--penuh]
"""
from __future__ import annotations

import argparse
import datetime as dt

import pyarrow.parquet as pq

from . import bronze
from .kolom import META, SKEMA
from .konfigurasi import clickhouse, log, s3fs, tanggal

DIMENSI = ("cabang", "nasabah", "rekening", "akad_pembiayaan")
SET = {"schema_inference_mode": "union", "input_format_parquet_allow_missing_columns": 1,
       "input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference": 1}


def _glob(entitas: str, d: dt.date | None) -> str:
    return f"bronze/{{app,cdc}}/{entitas}/{'dt=' + d.isoformat() if d else '*'}/*.parquet"


def _kolom_bronze(entitas: str, d: dt.date | None) -> list[str]:
    """Gabungan nama kolom semua berkas (hanya footer Parquet yang dibaca, bukan isinya)."""
    f = s3fs()
    nama: dict[str, None] = {}
    for path, _ in bronze.daftar_berkas(entitas, [d] if d else None):
        for k in pq.read_schema(path, filesystem=f).names:
            nama.setdefault(k, None)
    return list(nama)


def _siapkan_tabel(ch, entitas: str, kolom: list[str]) -> None:
    ch.command(f"""CREATE TABLE IF NOT EXISTS raw.{entitas}
                   (dt Date, _file String, _dimuat_pada DateTime64(3, 'UTC'))
                   ENGINE = MergeTree PARTITION BY dt ORDER BY tuple()""")
    # kolom kanonik yang dipakai dbt selalu ada (NULL bila sumber tidak mengirimnya)
    # (termasuk nama alternatif di kolom.py, sehingga dbt cukup memakai coalesce)
    alias = [a for (al, _, _) in SKEMA[entitas].values() for a in al]
    kolom = list(dict.fromkeys([*kolom, *SKEMA[entitas].keys(), *alias, *META]))
    for k in kolom:  # schema evolution: kolom baru di bronze menjadi kolom baru di raw
        ch.command(f"ALTER TABLE raw.{entitas} ADD COLUMN IF NOT EXISTS `{k}` Nullable(String)")


def _isi_sementara(ch, entitas: str, glob: str, kolom: list[str]) -> int:
    """ClickHouse membaca Parquet langsung dari RustFS; setiap kolom dibaca sebagai String
    (app dan cdc boleh berbeda tipe untuk kolom yang sama: string RFC3339 vs epoch mikrodetik)."""
    tmp = f"raw._muat_{entitas}"
    ch.command(f"DROP TABLE IF EXISTS {tmp}")
    ch.command(f"CREATE TABLE {tmp} AS raw.{entitas}")
    daftar = ", ".join(f"`{k}`" for k in kolom)
    struktur = ", ".join(f"`{k}` Nullable(String)" for k in kolom)
    ch.command(f"""INSERT INTO {tmp} (dt, _file, _dimuat_pada, {daftar})
                   SELECT toDate(extract(_path, 'dt=(\\\\d{{4}}-\\\\d{{2}}-\\\\d{{2}})')), _path, now64(3), {daftar}
                   FROM s3(lake_s3, filename='{glob}', format='Parquet', structure='{struktur}')""", settings=SET)
    return int(ch.command(f"SELECT count() FROM {tmp}"))


def muat_penuh(ch, entitas: str) -> int:
    if not bronze.daftar_berkas(entitas):
        log("warn", "bronze kosong", entitas=entitas)
        return 0
    glob = _glob(entitas, None)
    kolom = _kolom_bronze(entitas, None)
    _siapkan_tabel(ch, entitas, kolom)
    n = _isi_sementara(ch, entitas, glob, kolom)
    ch.command(f"EXCHANGE TABLES raw.{entitas} AND raw._muat_{entitas}")
    ch.command(f"DROP TABLE IF EXISTS raw._muat_{entitas}")
    return n


def muat_partisi(ch, entitas: str, d: dt.date) -> int:
    if not bronze.daftar_berkas(entitas, [d]):
        # partisi yang tidak lagi punya berkas (mis. berkas sabotase dihapus) ikut dikosongkan
        if ch.command(f"EXISTS TABLE raw.{entitas}") == 1:
            ch.command(f"ALTER TABLE raw.{entitas} DROP PARTITION '{d.isoformat()}'")
        return 0
    glob = _glob(entitas, d)
    kolom = _kolom_bronze(entitas, d)
    _siapkan_tabel(ch, entitas, kolom)
    n = _isi_sementara(ch, entitas, glob, kolom)
    ch.command(f"ALTER TABLE raw.{entitas} REPLACE PARTITION '{d.isoformat()}' FROM raw._muat_{entitas}")
    ch.command(f"DROP TABLE IF EXISTS raw._muat_{entitas}")
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tgl", required=True)
    ap.add_argument("--penuh", action="store_true")
    a = ap.parse_args()
    ch = clickhouse()
    hasil = {}
    for e in DIMENSI:
        hasil[e] = muat_penuh(ch, e)
    if a.penuh:
        hasil["transaksi"] = muat_penuh(ch, "transaksi")
    else:
        d = tanggal(a.tgl)
        hasil["transaksi"] = sum(muat_partisi(ch, "transaksi", d + dt.timedelta(days=k)) for k in (-1, 0, 1))
    log("info", "raw ClickHouse dimuat", baris=hasil, tgl=a.tgl, penuh=a.penuh)


if __name__ == "__main__":
    main()
