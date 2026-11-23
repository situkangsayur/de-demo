"""Gerbang awal: partisi bronze transaksi untuk tgl harus sudah ada (app atau cdc).

Bila belum ada, task gagal dan Airflow mencoba ulang (retries=2): data terlambat diberi waktu.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys

from . import bronze
from .konfigurasi import log, tanggal


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tgl", required=True)
    a = ap.parse_args()
    d = tanggal(a.tgl)
    berkas = bronze.daftar_berkas("transaksi", [d - dt.timedelta(days=1), d, d + dt.timedelta(days=1)])
    per_sumber = {s: sum(1 for p, x in berkas if f"/{s}/" in p and x == d) for s in bronze.SUMBER}
    log("info", "bronze transaksi", tgl=a.tgl, berkas=per_sumber)
    if not berkas:
        print(f"bronze transaksi untuk dt sekitar {a.tgl} belum ada", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
