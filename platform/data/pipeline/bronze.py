"""Membaca bronze Parquet dari s3://lake/bronze/<sumber>/<entitas>/dt=YYYY-MM-DD/part-*.parquet."""
from __future__ import annotations

import datetime as dt
import re

import polars as pl
import pyarrow.parquet as pq
from pyarrow import fs as pafs

from .konfigurasi import BUCKET, s3fs
from .kolom import normalisasi

SUMBER = ("app", "cdc")
_DT = re.compile(r"dt=(\d{4}-\d{2}-\d{2})")


def daftar_berkas(entitas: str, dts: list[dt.date] | None = None) -> list[tuple[str, dt.date]]:
    f = s3fs()
    hasil = []
    for sumber in SUMBER:
        akar = f"{BUCKET}/bronze/{sumber}/{entitas}"
        try:
            infos = f.get_file_info(pafs.FileSelector(akar, recursive=True, allow_not_found=True))
        except FileNotFoundError:
            continue
        for i in infos:
            if i.type != pafs.FileType.File or not i.path.endswith(".parquet"):
                continue
            m = _DT.search(i.path)
            if not m:
                continue
            d = dt.date.fromisoformat(m.group(1))
            if dts is None or d in dts:
                hasil.append((i.path, d))
    return sorted(hasil)


def daftar_dt(entitas: str) -> list[dt.date]:
    return sorted({d for _, d in daftar_berkas(entitas)})


def baca(entitas: str, dts: list[dt.date] | None = None) -> pl.DataFrame:
    """Gabungan semua berkas (app + cdc) yang cocok, sudah dinormalisasi ke kolom kanonik."""
    f = s3fs()
    bagian = []
    for path, d in daftar_berkas(entitas, dts):
        tbl = pq.read_table(path, filesystem=f)
        df = pl.from_arrow(tbl)
        if isinstance(df, pl.Series):
            df = df.to_frame()
        n = normalisasi(df, entitas, path)
        bagian.append(n.with_columns(pl.lit(d).alias("_dt")))
    if not bagian:
        return normalisasi(pl.DataFrame(), entitas, "").with_columns(pl.lit(None, dtype=pl.Date).alias("_dt")).clear()
    return pl.concat(bagian, how="vertical_relaxed")
