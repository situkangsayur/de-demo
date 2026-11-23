"""Dataset lab Bab 5, berskema platform (transaksi, nasabah, cabang), dibangkitkan vektor dengan Polars.

1. Dataset benchmark: /data/trx_<N>/part-YYYY-MM-DD.parquet (30 berkas, satu per tanggal) dan
   /data/nasabah.parquet. Kolom transaksi dibuat lebar (13 kolom) seperti tabel sungguhan, sedangkan
   query benchmark hanya memakai 4 kolom; dengan begitu projection pushdown terlihat dampaknya.
2. Bronze untuk pipeline: /data/bronze/<app|cdc>/<entitas>/dt=YYYY-MM-DD/part-<batch>.parquet,
   8 kolom metadata platform + kolom data string; 75% transaksi datang dari app dan cdc.
Semua dataset hidup di volume Docker dan hilang dengan `docker compose down -v`.
"""
from __future__ import annotations

import datetime as dt
import os
import shutil

import numpy as np
import polars as pl

from .konfig import DATA, log

HARI = 30
AWAL = dt.date(2026, 10, 1)
N_NASABAH = 100_000
CABANG = ["JKT-SDR", "JKT-01", "BGR-01", "BDG-01", "SBY-01", "MDN-01", "YGY-01", "MKS-01"]
KANAL = ["mobile", "atm", "teller", "web", "ecommerce"]
P_KANAL = [0.45, 0.18, 0.09, 0.13, 0.15]
JENIS = ["setor", "tarik", "transfer_masuk", "transfer_keluar", "bayar_belanja", "angsuran"]
KANAL_RAW = {"mobile": ["mobile", "Mobile Banking", "m-banking"], "atm": ["ATM", "atm"],
             "teller": ["teller", "teller "], "web": ["web", "WEB"], "ecommerce": ["ecommerce", "e-commerce"]}


def folder(n: int) -> str:
    return f"{DATA}/trx_{n}"


def nasabah(rng: np.random.Generator | None = None) -> pl.DataFrame:
    rng = rng or np.random.default_rng(7)
    i = np.arange(1, N_NASABAH + 1)
    return pl.DataFrame({
        "nasabah_id": i.astype(np.int32),
        "cif": [f"CIF{x:07d}" for x in i],
        "cabang": np.array(CABANG)[rng.integers(0, len(CABANG), N_NASABAH)],
        "segmen": np.array(["ritel", "ritel", "prioritas", "umkm"])[rng.integers(0, 4, N_NASABAH)],
        "kota": np.array(["Jakarta", "Bogor", "Bandung", "Surabaya", "Medan", "Yogyakarta", "Makassar"])[
            rng.integers(0, 7, N_NASABAH)],
    })


def bangkitkan(n: int) -> dict:
    """n transaksi tersebar rata di 30 hari, ditulis per tanggal (berkas ±n/30 baris)."""
    os.makedirs(DATA, exist_ok=True)
    if not os.path.exists(f"{DATA}/nasabah.parquet"):
        nasabah().write_parquet(f"{DATA}/nasabah.parquet")
    tmp = folder(n) + ".tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    rng = np.random.default_rng(n)
    per_hari = n // HARI
    nomor = 0
    for h in range(HARI):
        m = per_hari + (n - per_hari * HARI if h == HARI - 1 else 0)
        d = AWAL + dt.timedelta(days=h)
        detik = rng.integers(0, 86400, m)
        nas = rng.integers(1, N_NASABAH + 1, m).astype(np.int32)
        df = pl.DataFrame({
            "trx_id": np.arange(nomor + 1, nomor + m + 1, dtype=np.int64),
            "rekening_id": (nas.astype(np.int64) * 10 + rng.integers(0, 3, m)),
            "nasabah_id": nas,
            "cabang_id": rng.integers(1, 9, m).astype(np.int16),
            "kanal": np.array(KANAL)[rng.choice(len(KANAL), m, p=P_KANAL)],
            "jenis": np.array(JENIS)[rng.integers(0, len(JENIS), m)],
            "nominal": (rng.choice([5, 10, 20, 25, 50, 75, 100, 150, 250, 500, 1000], m) * 10_000).astype(np.int64),
            "saldo_setelah": (rng.integers(1, 900, m) * 100_000).astype(np.int64),
            "detik": detik,
            "keterangan": np.array(["transfer antarbank", "pembayaran tagihan listrik", "belanja daring",
                                    "setoran tunai cabang", "tarik tunai ATM", "angsuran murabahah",
                                    "zakat dan infak", "isi ulang dompet digital"])[rng.integers(0, 8, m)],
            "acak": rng.integers(0, 2**31, m),
        }).with_columns(
            (pl.lit(dt.datetime.combine(d, dt.time(0), dt.timezone.utc)) + pl.duration(seconds=pl.col("detik")))
            .dt.cast_time_unit("us").alias("occurred_at"),
            pl.lit(d).alias("tanggal"),
            pl.format("req-{}", pl.col("acak")).alias("request_id"),
            pl.when(pl.col("kanal") == "ecommerce").then(pl.format("ord-{}", pl.col("trx_id"))).alias("ref_order_id"),
        ).drop("detik", "acak")
        df.select("trx_id", "rekening_id", "nasabah_id", "cabang_id", "kanal", "jenis", "nominal", "saldo_setelah",
                  "occurred_at", "tanggal", "keterangan", "request_id", "ref_order_id") \
          .write_parquet(f"{tmp}/part-{d}.parquet", compression="zstd", row_group_size=128_000, statistics=True)
        nomor += m
    shutil.rmtree(folder(n), ignore_errors=True)
    os.replace(tmp, folder(n))
    log("info", "dataset dibangkitkan", baris=n)
    return info(n)


def info(n: int) -> dict:
    f = folder(n)
    if not os.path.isdir(f):
        return {"baris": n, "ada": False}
    berkas = sorted(os.listdir(f))
    return {"baris": n, "ada": True, "berkas": len(berkas),
            "byte": sum(os.path.getsize(f"{f}/{b}") for b in berkas), "path": f}


def daftar() -> list[dict]:
    from .konfig import UKURAN
    return [info(n) for n in UKURAN]


def hapus(n: int) -> dict:
    shutil.rmtree(folder(n), ignore_errors=True)
    return {"pesan": f"dataset {n} baris dihapus"}


# ---------------------------------------------------------------- bronze bergaya platform
META = ["_ingested_at", "_source", "_batch_id", "_event_id", "_event_type", "_occurred_at", "_ts_ms", "_op"]


def bangkitkan_bronze(n: int, hari: int = 3) -> dict:
    """Bronze transaksi + nasabah seperti platform: kolom data string, app dan cdc tumpang tindih."""
    akar = f"{DATA}/bronze"
    shutil.rmtree(akar, ignore_errors=True)
    rng = np.random.default_rng(2026)
    # nasabah (PII mentah: nik, no_hp, email), satu berkas cdc snapshot (op=r)
    nas = nasabah(rng)
    nas_b = nas.with_columns(
        pl.col("nasabah_id").cast(pl.Utf8).alias("id"),
        pl.format("3{}{}", pl.lit(171), (pl.col("nasabah_id") * 7919 % 10**9).cast(pl.Utf8).str.zfill(9)).alias("nik"),
        pl.format("08{}", (pl.col("nasabah_id") * 104729 % 10**10).cast(pl.Utf8).str.zfill(10)).alias("no_hp"),
        pl.format("nasabah{}@contoh.id", pl.col("nasabah_id")).alias("email"),
        pl.lit("Nama Contoh").alias("nama"),
    ).select("id", "cif", "nama", "nik", "no_hp", "email", "kota", "segmen", pl.col("cabang").alias("cabang_kode"))
    t0 = dt.datetime.combine(AWAL, dt.time(0), dt.timezone.utc)
    nas_b = _meta(nas_b, "cdc", "nasabah", t0, "r").with_columns(pl.lit(None, pl.Utf8).alias("_before"))
    os.makedirs(f"{akar}/cdc/nasabah/dt={AWAL}", exist_ok=True)
    _tulis(nas_b, f"{akar}/cdc/nasabah/dt={AWAL}/part-init.parquet")
    per_hari = n // hari
    total = {"app": 0, "cdc": 0}
    for h in range(hari):
        d = AWAL + dt.timedelta(days=h)
        m = per_hari
        detik = rng.integers(0, 86400, m)
        kanal = np.array(KANAL)[rng.choice(len(KANAL), m, p=P_KANAL)]
        raw = np.array([KANAL_RAW[k][i % len(KANAL_RAW[k])] for i, k in enumerate(kanal)])
        base = pl.DataFrame({
            "id": [f"TRX{d:%Y%m%d}{i:07d}" for i in range(m)],
            "rekening_id": (rng.integers(1, N_NASABAH + 1, m) * 10).astype(str),
            "nasabah_id": rng.integers(1, N_NASABAH + 1, m).astype(str),
            "cabang_id": rng.integers(1, 9, m).astype(str),
            "kanal": raw, "jenis": np.array(JENIS)[rng.integers(0, len(JENIS), m)],
            "nominal": (rng.choice([5, 10, 20, 50, 100, 250, 500], m) * 10_000).astype(str),
            "detik": detik, "pilih": rng.random(m),
        }).with_columns(
            (pl.lit(dt.datetime.combine(d, dt.time(0), dt.timezone.utc)) + pl.duration(seconds=pl.col("detik")))
            .dt.cast_time_unit("us").alias("_waktu"))
        base = base.with_columns(pl.col("_waktu").dt.strftime("%Y-%m-%dT%H:%M:%S%.3fZ").alias("occurred_at"),
                                 (pl.col("_waktu") + pl.duration(milliseconds=1))
                                 .dt.strftime("%Y-%m-%dT%H:%M:%S%.3fZ").alias("created_at"))
        # 75% app+cdc, 15% hanya cdc, 10% hanya app (sama dengan generator platform)
        cdc = base.filter(pl.col("pilih") < 0.90)
        app = base.filter((pl.col("pilih") < 0.75) | (pl.col("pilih") >= 0.90))
        for sumber, df in (("app", app), ("cdc", cdc)):
            out = _meta(df.drop("detik", "pilih"), sumber, "transaksi", None, "c")
            if sumber == "cdc":
                out = out.with_columns(pl.lit(None, pl.Utf8).alias("_before"))
            os.makedirs(f"{akar}/{sumber}/transaksi/dt={d}", exist_ok=True)
            # dua batch per partisi, seperti ingestor yang flush berkala
            half = out.height // 2
            for b, part in enumerate((out.head(half), out.tail(out.height - half))):
                _tulis(part.with_columns(pl.lit(f"{d:%Y%m%d}-{sumber}-{b}").alias("_batch_id")),
                       f"{akar}/{sumber}/transaksi/dt={d}/part-{d:%Y%m%d}-{b}.parquet")
            total[sumber] += out.height
    log("info", "bronze dibangkitkan", transaksi=per_hari * hari, **total)
    return {"transaksi_unik": per_hari * hari, "baris_app": total["app"], "baris_cdc": total["cdc"],
            "hari": hari, "nasabah": nas.height}


def _meta(df: pl.DataFrame, sumber: str, entitas: str, t0, op: str) -> pl.DataFrame:
    waktu = pl.col("_waktu") if "_waktu" in df.columns else pl.lit(t0).dt.cast_time_unit("us")
    jeda = pl.duration(seconds=(pl.int_range(pl.len()) % 90) + (5 if sumber == "app" else 20))
    df = df.with_columns(
        (waktu + jeda).dt.replace_time_zone("UTC").alias("_ingested_at") if "_waktu" in df.columns
        else (waktu + jeda).dt.replace_time_zone("UTC").alias("_ingested_at"),
        pl.lit(sumber).alias("_source"), pl.lit("").alias("_batch_id"),
        (pl.format("{}:{}:{}", pl.lit(entitas), pl.int_range(pl.len()) + 1000, pl.col("id")) if sumber == "cdc"
         else pl.format("evt-{}", pl.col("id"))).alias("_event_id"),
        pl.lit(f"bank.{entitas}.dibuat" if sumber == "app" else f"{entitas}.{op}").alias("_event_type"),
        (pl.col("occurred_at") if "occurred_at" in df.columns
         else pl.lit(t0.isoformat().replace("+00:00", "Z"))).alias("_occurred_at"),
        pl.lit(None, pl.Int64).alias("_ts_ms"),
        pl.lit(op if sumber == "cdc" else None, pl.Utf8).alias("_op"))
    df = df.with_columns((pl.col("_ingested_at").dt.epoch("ms")).alias("_ts_ms"))
    data = [c for c in df.columns if c not in META and not c.startswith("_waktu")]
    return df.select(META + data)


def _tulis(df: pl.DataFrame, path: str) -> None:
    df.write_parquet(path, compression="zstd")


def ringkas_bronze() -> dict:
    akar = f"{DATA}/bronze"
    if not os.path.isdir(akar):
        return {"ada": False}
    berkas = []
    for r, _, fs in os.walk(akar):
        berkas += [os.path.join(r, f) for f in fs if f.endswith(".parquet")]
    return {"ada": True, "berkas": len(berkas), "byte": sum(os.path.getsize(b) for b in berkas),
            "contoh": sorted(b.removeprefix(DATA + "/") for b in berkas)[:6]}
