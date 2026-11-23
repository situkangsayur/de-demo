"""Satu pekerjaan benchmark dalam proses terpisah: python -m app.kerja '<json>'.

Proses baru untuk setiap pengukuran supaya memori puncak (VmHWM dari /proc/self/status) milik satu
engine saja dan cache Python tidak terbawa. Hasil dicetak sebagai satu baris JSON di stdout.

Transformasi yang sama untuk semua engine (slide P05 "Query yang sama, tiga engine satu node"):
  transaksi tanggal >= 2026-10-11, JOIN nasabah, GROUP BY tanggal, cabang: jumlah dan total nominal.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time

MULAI = dt.date(2026, 10, 11)
THREADS = int(os.environ.get("THREADS", "4"))  # sama untuk semua engine (POLARS_MAX_THREADS di compose)
SQL = """
SELECT t.tanggal, n.cabang, COUNT(*) AS jml, SUM(t.nominal) AS total
FROM {trx} t JOIN {nas} n ON t.nasabah_id = n.nasabah_id
WHERE t.tanggal >= DATE '2026-10-11'
GROUP BY t.tanggal, n.cabang
"""
SQL_BERAT = """
SELECT rekening_id, COUNT(*) AS jml, SUM(nominal) AS total, MAX(occurred_at) AS terakhir,
       COUNT(DISTINCT keterangan) AS ragam
FROM {trx}
GROUP BY rekening_id
ORDER BY total DESC
"""


def vmhwm_mb() -> float:
    with open("/proc/self/status") as f:
        for baris in f:
            if baris.startswith("VmHWM:"):
                return round(int(baris.split()[1]) / 1024, 1)
    return 0.0


def rss_mb() -> float:
    with open("/proc/self/status") as f:
        for baris in f:
            if baris.startswith("VmRSS:"):
                return round(int(baris.split()[1]) / 1024, 1)
    return 0.0


# ---------------------------------------------------------------- transformasi per engine
def pandas_(folder: str, nas: str):
    import pandas as pd
    t = pd.read_parquet(folder, columns=["tanggal", "nasabah_id", "nominal"])
    t = t[t["tanggal"] >= MULAI]
    n = pd.read_parquet(nas, columns=["nasabah_id", "cabang"])
    m = t.merge(n, on="nasabah_id")
    r = m.groupby(["tanggal", "cabang"], as_index=False).agg(jml=("nominal", "size"), total=("nominal", "sum"))
    return len(r), int(r["total"].sum())


def polars_eager(folder: str, nas: str):
    import polars as pl
    t = pl.read_parquet(f"{folder}/*.parquet")          # eager: semua kolom dibaca dulu
    t = t.filter(pl.col("tanggal") >= MULAI)
    n = pl.read_parquet(nas)
    r = t.join(n, on="nasabah_id").group_by("tanggal", "cabang").agg(
        pl.len().alias("jml"), pl.col("nominal").sum().alias("total"))
    return r.height, int(r["total"].sum())


def _lazy(folder: str, nas: str):
    import polars as pl
    return (pl.scan_parquet(f"{folder}/*.parquet")
            .filter(pl.col("tanggal") >= MULAI)
            .join(pl.scan_parquet(nas), on="nasabah_id")
            .group_by("tanggal", "cabang")
            .agg(pl.len().alias("jml"), pl.col("nominal").sum().alias("total")))


def polars_lazy(folder: str, nas: str):
    r = _lazy(folder, nas).collect()
    return r.height, int(r["total"].sum())


def polars_streaming(folder: str, nas: str):
    r = _lazy(folder, nas).collect(engine="streaming")
    return r.height, int(r["total"].sum())


def duckdb_(folder: str, nas: str, memory_limit: str | None = None):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET threads={THREADS}")
    if memory_limit:
        con.execute(f"SET memory_limit='{memory_limit}'")
    r = con.execute(SQL.format(trx=f"read_parquet('{folder}/*.parquet')", nas=f"read_parquet('{nas}')")).fetchall()
    return len(r), int(sum(x[3] for x in r))


def datafusion_(folder: str, nas: str):
    from datafusion import SessionConfig, SessionContext
    ctx = SessionContext(SessionConfig().with_target_partitions(THREADS))
    ctx.register_parquet("trx", folder + "/")
    ctx.register_parquet("nas", nas)
    tbl = ctx.sql(SQL.format(trx="trx", nas="nas")).to_arrow_table()
    return tbl.num_rows, int(sum(tbl.column("total").to_pylist()))


ENGINE = {"pandas": pandas_, "polars_eager": polars_eager, "polars_lazy": polars_lazy,
          "polars_streaming": polars_streaming, "duckdb": duckdb_, "datafusion": datafusion_}


# ---------------------------------------------------------------- batas memori (query berat)
def berat(engine: str, folder: str, anggaran_mb: int, tmp: str):
    if engine == "pandas":
        import pandas as pd
        t = pd.read_parquet(folder)                       # semua kolom ke memori
        r = (t.groupby("rekening_id").agg(jml=("nominal", "size"), total=("nominal", "sum"),
                                          terakhir=("occurred_at", "max"), ragam=("keterangan", "nunique"))
             .sort_values("total", ascending=False))
        return len(r), {}
    if engine in ("polars_eager", "polars_streaming"):
        import polars as pl
        agg = [pl.len().alias("jml"), pl.col("nominal").sum().alias("total"),
               pl.col("occurred_at").max().alias("terakhir"), pl.col("keterangan").n_unique().alias("ragam")]
        if engine == "polars_eager":
            t = pl.read_parquet(f"{folder}/*.parquet")    # semua kolom ke memori
            r = t.group_by("rekening_id").agg(agg).sort("total", descending=True)
        else:
            r = (pl.scan_parquet(f"{folder}/*.parquet").group_by("rekening_id").agg(agg)
                 .sort("total", descending=True).collect(engine="streaming"))
        return r.height, {}
    import threading

    import duckdb
    con = duckdb.connect()
    os.makedirs(tmp, exist_ok=True)
    puncak = [0]
    selesai = threading.Event()

    def pantau():  # ukuran folder spill diukur berkala; berkas dihapus DuckDB sesudah query selesai
        while not selesai.is_set():
            n = 0
            for a, _, fs in os.walk(tmp):
                for f in fs:
                    try:
                        n += os.path.getsize(os.path.join(a, f))
                    except OSError:
                        pass
            puncak[0] = max(puncak[0], n)
            time.sleep(0.05)
    threading.Thread(target=pantau, daemon=True).start()
    con.execute(f"SET memory_limit='{anggaran_mb}MB'; SET temp_directory='{tmp}'; SET threads=2;"
                "SET preserve_insertion_order=false")
    # hasil diambil sebagai Arrow (kolom timestamptz tidak butuh pytz)
    r = con.sql(SQL_BERAT.format(trx=f"read_parquet('{folder}/*.parquet')")).to_arrow_table().to_pylist()
    selesai.set()
    return len(r), {"memory_limit": f"{anggaran_mb}MB", "spill_puncak_byte": puncak[0]}


PUSTAKA = {"pandas": ["pandas", "pyarrow.parquet"], "polars_eager": ["polars"], "polars_lazy": ["polars"],
           "polars_streaming": ["polars"], "duckdb": ["duckdb"], "datafusion": ["datafusion"]}


def main() -> None:
    import importlib
    a = json.loads(sys.argv[1])
    ti = time.perf_counter()
    for m in PUSTAKA.get(a["engine"], []):   # waktu import tidak ikut diukur
        importlib.import_module(m)
    import_ms = round((time.perf_counter() - ti) * 1000, 1)
    awal_rss = rss_mb()
    t0 = time.perf_counter()
    try:
        if a.get("jenis") == "berat":
            baris, extra = berat(a["engine"], a["folder"], a["anggaran_mb"], a["tmp"])
            hasil = {"baris": baris, **extra}
        else:
            baris, total = ENGINE[a["engine"]](a["folder"], a["nasabah"])
            hasil = {"baris": baris, "checksum_total": total}
        hasil.update({"ok": True})
    except MemoryError as e:
        hasil = {"ok": False, "error": f"MemoryError: {e}"}
    except Exception as e:  # noqa: BLE001  (kesalahan engine dilaporkan ke UI apa adanya)
        hasil = {"ok": False, "error": f"{type(e).__name__}: {' '.join(str(e).split())[:300]}"}
    hasil.update({"engine": a["engine"], "ms": round((time.perf_counter() - t0) * 1000, 1),
                  "puncak_mb": vmhwm_mb(), "rss_awal_mb": awal_rss, "import_ms": import_ms})
    print(json.dumps(hasil), flush=True)


if __name__ == "__main__":
    main()
