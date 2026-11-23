"""Eksperimen zero-copy Arrow dalam proses terpisah: python -m app.zerocopy <folder>.

Setiap langkah mencatat waktu, kenaikan RSS, dan apakah alamat buffer kolom `nominal` sama dengan
buffer Arrow asal (sama = tidak ada salinan data).
"""
from __future__ import annotations

import gc
import json
import sys
import time

from .kerja import rss_mb


def alamat_arrow(arr) -> int:
    """Alamat buffer data (bukan validity) dari chunk pertama ChunkedArray/Array PyArrow."""
    a = arr.chunks[0] if hasattr(arr, "chunks") else arr
    return a.buffers()[1].address


def main() -> None:
    folder = sys.argv[1]
    import duckdb
    import pandas as pd
    import polars as pl
    import pyarrow.parquet as pq

    langkah = []

    def catat(nama: str, fn, asal: int | None = None, ket: str = ""):
        gc.collect()
        r0 = rss_mb()
        t0 = time.perf_counter()
        hasil, alamat = fn()
        ms = round((time.perf_counter() - t0) * 1000, 1)
        langkah.append({"langkah": nama, "ms": ms, "tambah_mb": round(rss_mb() - r0, 1),
                        "alamat_sama": None if asal is None or alamat is None else alamat == asal, "ket": ket})
        return hasil

    tbl = catat("pq.read_table: Parquet → Arrow (dekompresi, satu-satunya salinan wajib)",
                lambda: (pq.read_table(folder, columns=["nominal", "kanal", "tanggal", "nasabah_id"]), None))
    asal = alamat_arrow(tbl.column("nominal"))
    langkah[-1]["baris"] = tbl.num_rows

    df_pl = catat("pl.from_arrow(tbl, rechunk=False)",
                  lambda: (lambda d: (d, alamat_arrow(d["nominal"].get_chunks()[0].to_arrow())))(
                      pl.from_arrow(tbl, rechunk=False)),
                  asal, "kolom angka memakai buffer Arrow yang sama; kolom string diubah ke string view (disalin)")
    catat("pl.from_arrow(tbl).rechunk()",
          lambda: (lambda d: (d, alamat_arrow(d["nominal"].get_chunks()[0].to_arrow())))(pl.from_arrow(tbl).rechunk()),
          asal, "30 chunk (satu per berkas) digabung menjadi satu buffer: data disalin")
    con = duckdb.connect()
    con.register("df_pl", df_pl)
    con.register("tbl", tbl)
    catat("duckdb: SELECT kanal, SUM(nominal) FROM df_pl (Polars)",
          lambda: (con.execute("SELECT kanal, SUM(nominal) s FROM df_pl GROUP BY kanal").fetchall(), None),
          None, "DuckDB memindai DataFrame Polars lewat Arrow tanpa memuat ulang")
    catat("duckdb: hasil kembali sebagai Arrow (.to_arrow_table)",
          lambda: (con.sql("SELECT * FROM tbl WHERE kanal = 'mobile'").to_arrow_table(), None))
    catat("tbl.to_pandas(types_mapper=pd.ArrowDtype)",
          lambda: (lambda d: (d, alamat_arrow(d["nominal"].array._pa_array)))(tbl.to_pandas(types_mapper=pd.ArrowDtype)),
          asal, "kolom pandas berbasis Arrow: tanpa salin")
    catat("tbl.to_pandas() (NumPy, bawaan)",
          lambda: (lambda d: (d, d["nominal"].to_numpy().__array_interface__["data"][0]))(tbl.to_pandas()),
          asal, "kolom disalin ke array NumPy")
    del df_pl
    print(json.dumps({"langkah": langkah, "baris": tbl.num_rows}), flush=True)


if __name__ == "__main__":
    main()
