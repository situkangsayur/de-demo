"""ETL bronze -> silver (Polars), ditulis sebagai tabel Iceberg lake.silver.*.

Aturan silver (W2):
- tipe benar (kolom kanonik di kolom.py), waktu UTC, tgl bisnis = tanggal WIB;
- dedup: satu transaksi bisa datang dua kali (event aplikasi dan CDC). Versi terakhir menang,
  kolom _source mencatat 'app', 'cdc', atau 'app+cdc'; konflik nominal antar-sumber ditandai;
- standardisasi kode kanal dan kode akad; kode akad tidak valid dikarantina ke silver.akad_karantina;
- tokenization PII: NIK, nomor HP, email, dan alamat diganti token (SHA-256 dengan pepper rahasia),
  nama diganti samaran. Nilai asli tidak pernah sampai ke silver.

Pemakaian: python -m pipeline.silver --entitas transaksi --tgl 2026-11-20 [--penuh]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os

import polars as pl

from . import bronze, iceberg_util as ice
from .kolom import AKAD, AKAD_VALID, KANAL
from .konfigurasi import ZONA, log, rentang, tanggal

PEPPER = os.environ.get("TOKEN_PEPPER", "")


def token(e: pl.Expr) -> pl.Expr:
    """tok_ + 20 hex pertama SHA-256(pepper | nilai). Sama persis dengan macro dbt `token()`."""
    return e.map_elements(lambda v: None if v is None or v == "" else
                          "tok_" + hashlib.sha256(f"{PEPPER}|{v}".encode()).hexdigest()[:20],
                          return_dtype=pl.Utf8)


def norm_hp(e: pl.Expr) -> pl.Expr:
    d = e.str.replace_all(r"[^0-9]", "")
    return pl.when(d.str.starts_with("0")).then(pl.lit("62") + d.str.slice(1)).otherwise(d)


def samaran(e: pl.Expr) -> pl.Expr:
    return e.str.strip_chars().str.split(" ").list.eval(
        pl.element().str.slice(0, 1) + pl.lit("***")).list.join(" ")


def terakhir(df: pl.DataFrame) -> pl.DataFrame:
    """Versi terakhir per id: updated_at terbaru, lalu _ingested_at, _ts_ms, cdc mengalahkan app, _event_id.

    Satu batch ingestor memberi _ingested_at yang sama untuk semua barisnya (mis. beberapa UPDATE saldo
    rekening dalam 10 detik), jadi updated_at dan _ts_ms dipakai sebagai pemutus seri.
    """
    sumber = df.group_by("id").agg(pl.col("_source").unique().sort().str.join("+").alias("_sumber"))
    df = df.with_columns(pl.col("_ts_ms").cast(pl.Int64, strict=False).alias("_ts_urut"))
    akhir = (df.sort(["id", "updated_at", "_ingested_at", "_ts_urut", "_source", "_event_id"], nulls_last=False)
               .group_by("id", maintain_order=True).last())
    return akhir.join(sumber, on="id", how="left")


def _tgl_wib(c: str) -> pl.Expr:
    return pl.col(c).dt.convert_time_zone(ZONA).dt.date()


# ---------------------------------------------------------------- entitas dimensi (dibangun ulang penuh)
def cabang() -> int:
    df = terakhir(bronze.baca("cabang")).filter(pl.col("_op").fill_null("") != "d")
    out = df.select(pl.col("id").alias("cabang_id"), pl.col("kode").str.strip_chars().str.to_uppercase(),
                    pl.col("nama").str.strip_chars(), pl.col("kota").str.strip_chars().str.to_titlecase(),
                    pl.col("wilayah").str.strip_chars(), pl.col("_sumber").alias("_source"),
                    pl.col("_ingested_at"))
    ice.timpa("silver.cabang", out.sort("cabang_id"))
    return out.height


def rekening() -> int:
    df = terakhir(bronze.baca("rekening")).filter(pl.col("_op").fill_null("") != "d")
    out = df.select(pl.col("id").alias("rekening_id"), "nasabah_id", "cabang_id", "no_rekening",
                    pl.col("produk").str.strip_chars().str.to_lowercase(),
                    pl.col("status").str.strip_chars().str.to_lowercase(), "dibuka_pada",
                    pl.col("_sumber").alias("_source"), "_ingested_at")
    ice.timpa("silver.rekening", out.sort("rekening_id"))
    return out.height


def nasabah() -> int:
    """Semua versi profil nasabah (bahan SCD-2), PII sudah di-tokenize."""
    df = bronze.baca("nasabah").filter(pl.col("_op").fill_null("") != "d")
    df = df.with_columns(
        pl.coalesce("updated_at", "terdaftar_pada", "_ingested_at").alias("versi_ts"))
    # app dan cdc untuk perubahan yang sama -> satu versi
    df = (df.sort(["id", "versi_ts", "_ingested_at", "_source", "_event_id"])
            .group_by(["id", "versi_ts"], maintain_order=True).last())
    df = df.with_columns(
        token(pl.col("nik").str.replace_all(r"[^0-9]", "")).alias("nik_token"),
        token(norm_hp(pl.col("no_hp"))).alias("no_hp_token"),
        token(pl.col("alamat").str.strip_chars().str.to_lowercase()).alias("alamat_token"),
        token(pl.col("email").str.strip_chars().str.to_lowercase()).alias("email_token"),
        samaran(pl.col("nama")).alias("nama_samaran"),
        pl.col("kota").str.strip_chars().str.to_titlecase().alias("kota"),
        pl.col("segmen").str.strip_chars().str.to_lowercase().alias("segmen"),
        pl.col("tanggal_lahir").dt.year().cast(pl.Int32).alias("tahun_lahir"),
    )
    # perubahan yang tidak mengubah atribut yang dilacak tidak membuat versi baru
    lacak = ["kota", "segmen", "cabang_id", "no_hp_token", "alamat_token"]
    df = df.sort(["id", "versi_ts"]).with_columns(
        pl.concat_str([pl.col(c).fill_null("∅") for c in lacak], separator="|").alias("_sidik"))
    df = df.filter((pl.col("_sidik") != pl.col("_sidik").shift(1).over("id")).fill_null(True))
    out = df.select(pl.col("id").alias("nasabah_id"), "cif", "nama_samaran", "nik_token", "no_hp_token",
                    "email_token", "alamat_token", "tahun_lahir", "kota", "segmen", "cabang_id",
                    pl.coalesce("terdaftar_pada", "versi_ts").alias("terdaftar_pada"), "versi_ts",
                    pl.col("_source"), "_ingested_at")
    # terdaftar_pada mengikuti versi pertama (nilai bisa kosong di event perubahan)
    out = out.with_columns(pl.col("terdaftar_pada").min().over("nasabah_id"))
    ice.timpa("silver.nasabah", out.sort(["nasabah_id", "versi_ts"]))
    return out.height


def akad_pembiayaan() -> int:
    df = terakhir(bronze.baca("akad_pembiayaan")).filter(pl.col("_op").fill_null("") != "d")
    df = df.with_columns(
        pl.col("kode_akad").alias("kode_akad_asli"),
        pl.col("kode_akad").str.strip_chars().replace_strict(AKAD, default=None)
          .alias("kode_akad"))
    cols = [pl.col("id").alias("akad_id"), "nasabah_id", "rekening_id", "cabang_id", "kode_akad",
            "kode_akad_asli", pl.col("plafon").round(2).cast(pl.Decimal(18, 2)),
            pl.col("margin").round(2).cast(pl.Decimal(18, 2)),
            pl.col("terbayar").round(2).cast(pl.Decimal(18, 2)), "tenor_bulan",
            pl.col("status").str.strip_chars().str.to_lowercase(), "tanggal_cair",
            _tgl_wib("tanggal_cair").alias("tgl_cair"), pl.col("_sumber").alias("_source"), "_ingested_at"]
    valid = df.filter(pl.col("kode_akad").is_in(AKAD_VALID)).select(cols)
    karantina = (df.filter(~pl.col("kode_akad").is_in(AKAD_VALID).fill_null(False) | pl.col("kode_akad").is_null())
                   .select(cols).with_columns(
                       pl.format("kode akad '{}' di luar domain {}", pl.col("kode_akad_asli").fill_null("NULL"),
                                 pl.lit("/".join(AKAD_VALID))).alias("alasan"),
                       pl.lit(dt.datetime.now(dt.timezone.utc)).alias("dikarantina_pada")))
    ice.timpa("silver.akad_pembiayaan", valid.sort("akad_id"))
    ice.timpa("silver.akad_karantina", karantina.sort("akad_id"))
    log("info", "akad dikarantina", jumlah=karantina.height, valid=valid.height)
    return valid.height


# ---------------------------------------------------------------- fakta per partisi tanggal
def transaksi(tgl: dt.date, penuh: bool = False) -> tuple[int, list[dt.date]]:
    """Silver transaksi untuk satu tanggal WIB (atau semua tanggal s.d. tgl bila penuh)."""
    if penuh:
        df = bronze.baca("transaksi").with_columns(_tgl_wib("waktu").alias("tgl"))
        awal = df["tgl"].min() if df.height else tgl
        tgls = rentang(min(awal, tgl), tgl)
    else:
        # partisi dt tetangga ikut dibaca: dt bronze memakai tanggal UTC, tgl bisnis memakai WIB
        tgls = [tgl]
        df = bronze.baca("transaksi", [tgl - dt.timedelta(days=1), tgl, tgl + dt.timedelta(days=1)]) \
            .with_columns(_tgl_wib("waktu").alias("tgl"))
    df = df.filter(pl.col("tgl").is_in(tgls))
    # konflik nominal antar-sumber: dicatat, lalu diperiksa data quality test (consistency)
    nom = (df.sort(["id", "_ingested_at"]).group_by(["id", "_source"], maintain_order=True)
             .agg(pl.col("nominal").last())
             .pivot(on="_source", index="id", values="nominal"))
    for s in ("app", "cdc"):
        if s not in nom.columns:
            nom = nom.with_columns(pl.lit(None, dtype=pl.Float64).alias(s))
    nom = nom.select("id", (pl.col("app").is_not_null() & pl.col("cdc").is_not_null()
                            & ((pl.col("app") - pl.col("cdc")).abs() > 0.005)).alias("_konflik_nominal"))
    akhir = terakhir(df).filter(pl.col("_op").fill_null("") != "d").join(nom, on="id", how="left")
    rek = ice.baca("silver.rekening", kolom=("rekening_id", "nasabah_id")).rename({"nasabah_id": "_nas_rek"})
    akhir = akhir.join(rek, on="rekening_id", how="left")
    out = akhir.select(
        pl.col("id").alias("trx_id"), "rekening_id", pl.coalesce("nasabah_id", "_nas_rek").alias("nasabah_id"),
        "cabang_id", "akad_id", "ref_order_id",
        pl.col("kanal").str.strip_chars().str.to_lowercase().replace(KANAL).alias("kanal"),
        pl.col("kanal").alias("kanal_asli"),
        pl.col("jenis").str.strip_chars().str.to_lowercase(),
        pl.col("nominal").round(2).cast(pl.Decimal(18, 2)).alias("nominal"),
        "waktu", "tgl", "transfer_id", "idempotency_key",
        # data terlambat: dicatat server > 1 menit sesudah occurred_at (kontrak: created_at bisa ~1 ms lebih awal)
        ((pl.col("dicatat_pada") - pl.col("waktu")).dt.total_seconds() > 60).fill_null(False).alias("_terlambat"),
        pl.col("_sumber").alias("_source"), "_batch_id", "_event_id", "_ingested_at",
        pl.col("_konflik_nominal").fill_null(False))
    ice.timpa("silver.transaksi", out.sort(["tgl", "trx_id"]), partisi="tgl", nilai_partisi=tgls)
    return out.height, tgls


def tgl_proses(tgl: str, penuh: bool) -> list[dt.date]:
    """Tanggal yang diproses satu run. Mode penuh (muat awal): rentang tanggal pertama di
    silver.transaksi s.d. tgl, tanpa celah, sehingga hari yang kosong tetap terdeteksi DQ."""
    akhir = tanggal(tgl)
    if not penuh:
        return [akhir]
    if ice.ada("silver.transaksi"):
        ada = ice.baca("silver.transaksi", kolom=("tgl",))["tgl"]
        if ada.len():
            return rentang(min(ada.min(), akhir), akhir)
    return [akhir]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entitas", required=True)
    ap.add_argument("--tgl", required=True)
    ap.add_argument("--penuh", action="store_true", help="muat awal: semua tanggal bronze s.d. --tgl")
    a = ap.parse_args()
    fungsi = {"cabang": cabang, "rekening": rekening, "nasabah": nasabah, "akad_pembiayaan": akad_pembiayaan}
    if a.entitas == "transaksi":
        n, tgls = transaksi(tanggal(a.tgl), a.penuh)
        log("info", "silver selesai", entitas="transaksi", baris=n, tgl_awal=str(tgls[0]), tgl_akhir=str(tgls[-1]))
    else:
        n = fungsi[a.entitas]()
        log("info", "silver selesai", entitas=a.entitas, baris=n)


if __name__ == "__main__":
    main()
