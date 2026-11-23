"""Pipeline ETL satu mesin seperti platform (pipeline/silver.py, gold.py), versi Parquet lokal.

bronze (app + cdc, kolom string) --Polars lazy--> silver/transaksi/tgl=... --DuckDB SQL--> gold/fakta_trx_harian
- silver: cast tipe, dedup app vs cdc (_source = app / cdc / app+cdc), kanal dibakukan, rekening ditoken;
- pemeriksaan ringan sebelum menulis (not null, unik, rentang, domain): gagal = tidak ada yang ditulis;
- penulisan idempotent: partisi tanggal ditulis ke folder sementara lalu ditukar (os.replace),
  sehingga menjalankan ulang tanggal yang sama memberi hasil yang sama, tanpa duplikat.
Tokenization dibandingkan dua cara: UDF Python per baris (map_elements, seperti platform) dan
fungsi vektor DuckDB sha256(); keduanya harus menghasilkan token identik.
"""
from __future__ import annotations

import datetime as dt
import glob
import hashlib
import json
import os
import shutil
import time

import duckdb
import polars as pl

from .konfig import DATA, TOKEN_PEPPER, log

BRONZE = f"{DATA}/bronze"
SILVER = f"{DATA}/silver/transaksi"
GOLD = f"{DATA}/gold/fakta_trx_harian"
RIWAYAT = f"{DATA}/riwayat-pipeline.json"
TERAKHIR = f"{DATA}/pipeline-terakhir.json"
KANAL = {"mobile": "mobile", "mobile banking": "mobile", "m-banking": "mobile", "atm": "atm", "teller": "teller",
         "web": "web", "ecommerce": "ecommerce", "e-commerce": "ecommerce"}
KANAL_VALID = sorted(set(KANAL.values()))


def token_py(v: str | None) -> str | None:
    """Sama dengan platform: tok_ + 20 hex pertama SHA-256(pepper|nilai)."""
    return None if v is None or v == "" else "tok_" + hashlib.sha256(f"{TOKEN_PEPPER}|{v}".encode()).hexdigest()[:20]


def silver_lazy() -> pl.LazyFrame:
    raw = pl.scan_parquet(f"{BRONZE}/*/transaksi/*/*.parquet", hive_partitioning=False, missing_columns="insert",
                          extra_columns="ignore")
    ts = pl.col("occurred_at").str.replace("Z$", "+00:00").str.to_datetime(
        "%Y-%m-%dT%H:%M:%S%.f%:z", time_unit="us", strict=False).dt.convert_time_zone("UTC")
    sumber = raw.group_by("id").agg(pl.col("_source").unique().sort().str.join("+").alias("_sumber"))
    akhir = (raw.sort(["id", "_ingested_at", "_source"]).group_by("id", maintain_order=True).last()
             .join(sumber, on="id"))
    return akhir.select(
        pl.col("id").alias("trx_id"), pl.col("rekening_id"), pl.col("nasabah_id").cast(pl.Int32, strict=False),
        pl.col("cabang_id").cast(pl.Int16, strict=False),
        pl.col("kanal").str.strip_chars().str.to_lowercase().replace(KANAL).alias("kanal"),
        pl.col("jenis"), pl.col("nominal").str.strip_chars().cast(pl.Int64, strict=False).alias("nominal"),
        ts.alias("waktu"), ts.dt.date().alias("tgl"), pl.col("_sumber").alias("_source"), "_ingested_at")


def periksa(df: pl.DataFrame) -> list[dict]:
    aturan = [
        ("trx_id tidak null", df["trx_id"].null_count()),
        ("trx_id unik", df.height - df["trx_id"].n_unique()),
        ("nominal > 0", df.filter(pl.col("nominal").is_null() | (pl.col("nominal") <= 0)).height),
        ("waktu terbaca", df["waktu"].null_count()),
        (f"kanal dalam domain {'/'.join(KANAL_VALID)}", df.filter(~pl.col("kanal").is_in(KANAL_VALID)).height),
    ]
    return [{"aturan": a, "pelanggar": int(n), "lulus": n == 0} for a, n in aturan]


def tulis_partisi(df: pl.DataFrame, akar: str, kolom: str) -> list[str]:
    """Overwrite per partisi secara atomik: tulis ke folder .tmp, lalu os.replace menggantikan yang lama."""
    hasil = []
    for (nilai,), bagian in df.group_by([kolom], maintain_order=True):
        tujuan = f"{akar}/{kolom}={nilai}"
        tmp, lama = tujuan + ".tmp", tujuan + ".lama"
        shutil.rmtree(tmp, ignore_errors=True)
        os.makedirs(tmp)
        bagian.drop(kolom).write_parquet(f"{tmp}/part-0.parquet", compression="zstd")
        if os.path.exists(tujuan):
            os.replace(tujuan, lama)
        os.replace(tmp, tujuan)
        shutil.rmtree(lama, ignore_errors=True)
        hasil.append(str(nilai))
    return hasil


SQL_GOLD = """
SELECT tgl, cabang_id, kanal, COUNT(*) AS jml_trx, SUM(nominal) AS total_nominal,
       COUNT(DISTINCT rekening_token) AS rekening_aktif
FROM read_parquet('{silver}/*/*.parquet', hive_partitioning = true)
GROUP BY ALL ORDER BY ALL
"""


def jalankan() -> dict:
    if not os.path.isdir(BRONZE):
        raise ValueError("bronze belum ada; bangkitkan bronze dulu")
    langkah = []
    t0 = time.perf_counter()
    lazy = silver_lazy()
    rencana = lazy.explain()
    df = lazy.collect()
    langkah.append({"langkah": "bronze → silver (Polars lazy: cast, dedup, kanal)",
                    "ms": round((time.perf_counter() - t0) * 1000, 1), "baris": df.height})
    n_bronze = pl.scan_parquet(f"{BRONZE}/*/transaksi/*/*.parquet", hive_partitioning=False,
                               missing_columns="insert", extra_columns="ignore").select(pl.len()).collect().item()

    # tokenization: DuckDB sha256 vektor (dipakai), dibandingkan dengan UDF Python per baris
    t1 = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET VARIABLE pepper = ?", [TOKEN_PEPPER])
    df = pl.from_arrow(con.sql("""SELECT * EXCLUDE (rekening_id),
            'tok_' || left(sha256(getvariable('pepper') || '|' || rekening_id), 20) AS rekening_token
            FROM df""").to_arrow_table())
    ms_duck = round((time.perf_counter() - t1) * 1000, 1)
    sampel = pl.from_arrow(con.sql("SELECT DISTINCT rekening_id FROM read_parquet('"
                                   f"{BRONZE}/*/transaksi/*/*.parquet', union_by_name=true) LIMIT 20000").to_arrow_table())
    t2 = time.perf_counter()
    tok_py = sampel.with_columns(pl.col("rekening_id").map_elements(token_py, return_dtype=pl.Utf8).alias("tok"))
    ms_py = (time.perf_counter() - t2) * 1000
    t3 = time.perf_counter()
    tok_dk = pl.from_arrow(con.sql("""SELECT rekening_id, 'tok_' || left(sha256(getvariable('pepper') || '|' ||
        rekening_id), 20) AS tok FROM sampel""").to_arrow_table())
    ms_dk_sampel = (time.perf_counter() - t3) * 1000
    sama = tok_py.sort("rekening_id")["tok"].equals(tok_dk.sort("rekening_id")["tok"])
    langkah.append({"langkah": "tokenization rekening (DuckDB sha256 vektor)", "ms": ms_duck, "baris": df.height})

    cek = periksa(df)
    lulus = all(c["lulus"] for c in cek)
    tgl = []
    if lulus:
        t4 = time.perf_counter()
        tgl = tulis_partisi(df, SILVER, "tgl")
        langkah.append({"langkah": "tulis silver per tgl (atomik, idempotent)",
                        "ms": round((time.perf_counter() - t4) * 1000, 1), "partisi": tgl})
        t5 = time.perf_counter()
        gold = pl.from_arrow(con.sql(SQL_GOLD.format(silver=SILVER)).to_arrow_table())
        tulis_partisi(gold, GOLD, "tgl")
        langkah.append({"langkah": "silver → gold (DuckDB SQL)", "ms": round((time.perf_counter() - t5) * 1000, 1),
                        "baris": gold.height})
    hitung = hitung_lapisan()
    hasil = {
        "waktu": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "lulus": lulus,
        "langkah": langkah, "cek": cek, "rencana_silver": rencana,
        "dedup": {"baris_bronze": n_bronze, "transaksi_unik": df.height,
                  "sumber": {r["_source"]: r["len"] for r in df.group_by("_source").len().sort("_source").to_dicts()}},
        "token": {"sampel": sampel.height, "ms_udf_python": round(ms_py, 1), "ms_duckdb": round(ms_dk_sampel, 1),
                  "identik": bool(sama), "contoh": tok_py.head(3).to_dicts(),
                  "per_juta_udf_detik": round(ms_py / max(sampel.height, 1) * 1000, 1),
                  "per_juta_duckdb_detik": round(ms_dk_sampel / max(sampel.height, 1) * 1000, 2)},
        "lapisan": hitung, "sql_gold": SQL_GOLD.format(silver=SILVER).strip(),
    }
    riwayat = muat_riwayat()
    riwayat.insert(0, {"waktu": hasil["waktu"], "lulus": lulus, "lapisan": hitung})
    with open(RIWAYAT, "w") as f:
        json.dump(riwayat[:10], f)
    with open(TERAKHIR, "w") as f:
        json.dump(hasil, f, default=str)
    log("info", "pipeline selesai", lulus=lulus, silver=hitung.get("silver"), gold=hitung.get("gold"))
    return hasil


def hitung_lapisan() -> dict:
    out = {}
    for nama, akar in (("silver", SILVER), ("gold", GOLD)):
        if glob.glob(f"{akar}/*/*.parquet"):
            r = duckdb.sql(f"SELECT tgl::VARCHAR, COUNT(*) FROM read_parquet('{akar}/*/*.parquet', "
                           "hive_partitioning = true) GROUP BY 1 ORDER BY 1").fetchall()
            out[nama] = sum(x[1] for x in r)
            out[f"{nama}_per_tgl"] = {x[0]: x[1] for x in r}
    return out


def terakhir() -> dict | None:
    try:
        with open(TERAKHIR) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def muat_riwayat() -> list:
    try:
        with open(RIWAYAT) as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def gold_tabel() -> dict:
    if not glob.glob(f"{GOLD}/*/*.parquet"):
        return {"kolom": [], "baris": []}
    r = duckdb.sql(f"SELECT tgl::VARCHAR AS tgl, cabang_id, kanal, jml_trx, total_nominal, rekening_aktif FROM "
                   f"read_parquet('{GOLD}/*/*.parquet', hive_partitioning = true) ORDER BY 1, 2, 3 LIMIT 120")
    return {"kolom": r.columns, "baris": [list(x) for x in r.fetchall()]}
