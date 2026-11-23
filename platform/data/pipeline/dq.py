"""Data quality test jalur ETL sebagai gerbang (W8).

Lima dimensi: completeness, uniqueness, validity, consistency, timeliness.
- --lapisan silver : memeriksa lake.silver.* untuk tanggal yang diproses (gerbang sebelum gold)
- --lapisan gold   : memeriksa lake.staging.* (gold yang belum dipublikasikan)
Setiap hasil disimpan ke ClickHouse meta.dq_hasil (dibaca dashboard). Bila ada test berkeparahan
'error' yang gagal, proses keluar dengan kode 1 sehingga task Airflow gagal dan task gold
berikutnya tidak dijalankan (upstream_failed): gold tidak berubah.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from dataclasses import dataclass, field

import polars as pl
from pyiceberg.expressions import In

from . import iceberg_util as ice
from .kolom import AKAD_VALID, KANAL_VALID, SKEMA
from .konfigurasi import clickhouse, log
from .silver import tgl_proses

LATENSI_MAKS_MENIT = float(os.environ.get("DQ_LATENSI_MAKS_MENIT", "60"))
KARANTINA_MAKS = float(os.environ.get("DQ_KARANTINA_MAKS_PERSEN", "10"))
NOMINAL_MAKS = float(os.environ.get("DQ_NOMINAL_MAKS", "5000000000"))
MIN_BARIS = int(os.environ.get("DQ_MIN_BARIS_PER_HARI", "1"))
# Latensi ingest bermakna untuk data live; data backfill historis (occurred_at lampau) wajar berlatensi
# besar, jadi bawaannya peringatan. Gerbang timeliness berkeparahan error ada di dq_gold.
LATENSI_KEPARAHAN = os.environ.get("DQ_LATENSI_KEPARAHAN", "warn")


@dataclass
class Hasil:
    tes: str
    dimensi: str
    aturan: str
    gagal: int
    total: int
    keparahan: str = "error"
    contoh: list = field(default_factory=list)
    nilai: float | None = None

    @property
    def status(self) -> str:
        if self.gagal == 0:
            return "lulus"
        return "gagal" if self.keparahan == "error" else "peringatan"


def _contoh(df: pl.DataFrame, kol: list[str], n: int = 5) -> list:
    return df.select(list(dict.fromkeys(c for c in kol if c in df.columns))).head(n).to_dicts()


# ------------------------------------------------------------------ silver
def tes_silver(tgls: list[dt.date]) -> list[Hasil]:
    f = In("tgl", [d.isoformat() for d in tgls])
    trx = ice.baca("silver.transaksi", f)
    cab = ice.baca("silver.cabang", kolom=("cabang_id",))
    nas = ice.baca("silver.nasabah")
    akad = ice.baca("silver.akad_pembiayaan", kolom=("akad_id",))
    kar = ice.baca("silver.akad_karantina", kolom=("akad_id", "kode_akad_asli", "alasan"))
    h: list[Hasil] = []

    wajib = [k if k != "id" else "trx_id" for k, (_, _, w) in SKEMA["transaksi"].items() if w]
    kosong = trx.filter(pl.any_horizontal([pl.col(c).is_null() for c in wajib]))
    h.append(Hasil("silver_transaksi_kolom_wajib_terisi", "completeness",
                   f"kolom {', '.join(wajib)} tidak boleh NULL", kosong.height, trx.height,
                   contoh=_contoh(kosong, ["trx_id", *wajib])))

    per_hari = trx.group_by("tgl").len()
    kurang = [d.isoformat() for d in tgls
              if per_hari.filter(pl.col("tgl") == d)["len"].sum() < MIN_BARIS]
    h.append(Hasil("silver_transaksi_volume_per_hari", "completeness",
                   f"setiap tanggal yang diproses punya >= {MIN_BARIS} transaksi", len(kurang), len(tgls),
                   contoh=[{"tgl": d} for d in kurang[:5]]))

    ganda = trx.filter(pl.col("trx_id").is_duplicated())
    h.append(Hasil("silver_transaksi_trx_id_unik", "uniqueness", "trx_id unik di silver.transaksi",
                   ganda.height, trx.height, contoh=_contoh(ganda, ["trx_id", "tgl", "_source"])))

    luar = trx.filter(~pl.col("kanal").is_in(KANAL_VALID) & pl.col("kanal").is_not_null())
    h.append(Hasil("silver_transaksi_kanal_valid", "validity", f"kanal ∈ {{{', '.join(KANAL_VALID)}}}",
                   luar.height, trx.height, contoh=_contoh(luar, ["trx_id", "kanal_asli", "kanal"])))

    rentang = trx.filter((pl.col("nominal").cast(pl.Float64) <= 0) | (pl.col("nominal").cast(pl.Float64) > NOMINAL_MAKS))
    h.append(Hasil("silver_transaksi_nominal_dalam_rentang", "validity",
                   f"0 < nominal <= {NOMINAL_MAKS:,.0f}", rentang.height, trx.height,
                   contoh=_contoh(rentang, ["trx_id", "nominal"])))

    pola_nik = nas.filter(pl.concat_str([pl.col(c).cast(pl.Utf8).fill_null("") for c in nas.columns
                                         if not c.endswith("_token")],
                                        separator=" ").str.contains(r"(^|\D)\d{16}(\D|$)"))
    h.append(Hasil("silver_nasabah_tanpa_pii_mentah", "validity",
                   "tidak ada NIK mentah (16 digit) di silver.nasabah; PII hanya berupa token",
                   pola_nik.height, nas.height, contoh=_contoh(pola_nik, ["nasabah_id"])))

    total_akad = akad.height + kar.height
    persen = 100 * kar.height / total_akad if total_akad else 0.0
    h.append(Hasil("silver_akad_rasio_karantina", "validity",
                   f"kode akad di luar {'/'.join(AKAD_VALID)} dikarantina; rasio <= {KARANTINA_MAKS}%",
                   int(persen > KARANTINA_MAKS), 1, keparahan="warn", nilai=round(persen, 2),
                   contoh=_contoh(kar, ["akad_id", "kode_akad_asli"])))

    # duplikat bisnis: request dikirim ulang tanpa idempotency key -> trx_id berbeda, isi sama, <= 60 detik
    urut = trx.sort(["rekening_id", "nominal", "jenis", "kanal", "waktu"])
    kunci = ["rekening_id", "nominal", "jenis", "kanal"]
    dup = urut.filter(pl.all_horizontal([pl.col(k) == pl.col(k).shift(1) for k in kunci])
                      & ((pl.col("waktu") - pl.col("waktu").shift(1)).dt.total_seconds() <= 60))
    h.append(Hasil("silver_transaksi_duplikat_bisnis", "uniqueness",
                   "tidak ada dua transaksi dengan rekening, nominal, jenis, kanal sama dalam 60 detik "
                   "(indikasi request ulang tanpa idempotency key)", dup.height, trx.height, keparahan="warn",
                   contoh=_contoh(dup, ["trx_id", "rekening_id", "nominal", "waktu"])))

    telat = trx.filter(pl.col("_terlambat"))
    h.append(Hasil("silver_transaksi_data_terlambat", "timeliness",
                   "occurred_at lebih dari 1 menit sebelum created_at (data terlambat, masuk partisi tanggal aslinya)",
                   telat.height, trx.height, keparahan="warn", nilai=round(100 * telat.height / max(1, trx.height), 2),
                   contoh=_contoh(telat, ["trx_id", "waktu", "tgl"])))

    konflik = trx.filter(pl.col("_konflik_nominal"))
    h.append(Hasil("silver_transaksi_app_cdc_konsisten", "consistency",
                   "transaksi yang tercatat di event aplikasi dan CDC harus bernominal sama",
                   konflik.height, trx.filter(pl.col("_source") == "app+cdc").height,
                   contoh=_contoh(konflik, ["trx_id", "nominal", "_source"])))

    yatim = trx.join(cab, on="cabang_id", how="anti")
    h.append(Hasil("silver_transaksi_cabang_terdaftar", "consistency",
                   "cabang_id transaksi ada di silver.cabang", yatim.height, trx.height,
                   contoh=_contoh(yatim, ["trx_id", "cabang_id"])))

    lat = trx.select(((pl.col("_ingested_at") - pl.col("waktu")).dt.total_seconds() / 60).alias("m")).drop_nulls()
    p95 = float(lat["m"].quantile(0.95)) if lat.height else 0.0
    h.append(Hasil("silver_transaksi_latensi_ingest_p95", "timeliness",
                   f"p95 (_ingested_at - waktu) <= {LATENSI_MAKS_MENIT:.0f} menit", int(p95 > LATENSI_MAKS_MENIT),
                   1, keparahan=LATENSI_KEPARAHAN, nilai=round(p95, 2)))
    return h


# ------------------------------------------------------------------ gold (staging)
def tes_gold(tgls: list[dt.date]) -> list[Hasil]:
    ft = ice.baca("staging.fakta_transaksi")
    fp = ice.baca("staging.fakta_pembiayaan")
    dn = ice.baca("staging.dim_nasabah")
    dc = ice.baca("staging.dim_cabang", kolom=("cabang_key",))
    sv = ice.baca("silver.transaksi", In("tgl", [d.isoformat() for d in tgls]), kolom=("tgl", "nominal"))
    h: list[Hasil] = []

    ganda = ft.filter(pl.col("trx_id").is_duplicated())
    h.append(Hasil("gold_fakta_transaksi_grain_unik", "uniqueness",
                   "grain fakta_transaksi = satu transaksi (trx_id unik)", ganda.height, ft.height,
                   contoh=_contoh(ganda, ["trx_id", "tgl"])))

    tanpa = dn.group_by("nasabah_id").agg(pl.col("is_current").sum().alias("n")).filter(pl.col("n") != 1)
    h.append(Hasil("gold_dim_nasabah_satu_versi_aktif", "uniqueness",
                   "setiap nasabah_id punya tepat satu baris is_current", tanpa.height, dn["nasabah_id"].n_unique(),
                   contoh=_contoh(tanpa, ["nasabah_id", "n"])))

    tumpang = (dn.sort(["nasabah_id", "berlaku_dari"])
                 .with_columns(pl.col("berlaku_dari").shift(-1).over("nasabah_id").alias("_dari_berikut"))
                 .filter((pl.col("berlaku_dari") >= pl.col("berlaku_sampai"))
                         | (pl.col("_dari_berikut").is_not_null() & (pl.col("_dari_berikut") != pl.col("berlaku_sampai")))))
    h.append(Hasil("gold_dim_nasabah_scd2_tanpa_celah", "validity",
                   "interval SCD-2 per nasabah berurutan: berlaku_sampai versi n = berlaku_dari versi n+1",
                   tumpang.height, dn.height, contoh=_contoh(tumpang, ["nasabah_sk", "berlaku_dari", "berlaku_sampai"])))

    tanpa_sk = ft.filter(pl.col("nasabah_sk").is_null())
    yatim_c = ft.join(dc, on="cabang_key", how="anti")
    h.append(Hasil("gold_fakta_kunci_dimensi_lengkap", "consistency",
                   "setiap fakta menemukan versi nasabah yang berlaku (SCD-2) dan cabang di dim_cabang",
                   tanpa_sk.height + yatim_c.height, ft.height,
                   contoh=_contoh(pl.concat([tanpa_sk, yatim_c], how="diagonal_relaxed"), ["trx_id", "nasabah_id", "cabang_key"])))

    a = sv.group_by("tgl").agg(pl.len().alias("n_silver"), pl.col("nominal").cast(pl.Float64).sum().alias("t_silver"))
    b = ft.group_by("tgl").agg(pl.len().alias("n_gold"), pl.col("nominal").cast(pl.Float64).sum().alias("t_gold"))
    r = a.join(b, on="tgl", how="full", coalesce=True).fill_null(0).filter(
        (pl.col("n_silver") != pl.col("n_gold")) | ((pl.col("t_silver") - pl.col("t_gold")).abs() > 0.01))
    h.append(Hasil("gold_rekonsiliasi_silver_vs_gold", "consistency",
                   "jumlah baris dan total nominal per tgl sama di silver dan gold", r.height, len(tgls),
                   contoh=r.head(5).to_dicts()))

    luar = fp.filter(~pl.col("akad_key").is_in(AKAD_VALID))
    h.append(Hasil("gold_fakta_pembiayaan_akad_valid", "validity", f"akad_key ∈ {'/'.join(AKAD_VALID)}",
                   luar.height, fp.height, contoh=_contoh(luar, ["akad_id", "akad_key"])))

    fn = ice.baca("staging.fitur_nasabah")
    ganda_f = fn.filter(pl.struct("nasabah_id", "tgl_fitur").is_duplicated())
    h.append(Hasil("gold_fitur_nasabah_grain_unik", "uniqueness",
                   "fitur_nasabah: satu baris per (nasabah_id, tgl_fitur)", ganda_f.height, fn.height,
                   contoh=_contoh(ganda_f, ["nasabah_id", "tgl_fitur"])))
    bocor = fn.filter((pl.col("hari_sejak_trx_terakhir") < 0) | (pl.col("trx_7h") > pl.col("trx_30h"))
                      | (pl.col("umur_nasabah_hari") < 0) | pl.col("nasabah_sk_saat_itu").is_null())
    h.append(Hasil("gold_fitur_nasabah_point_in_time", "validity",
                   "fitur hanya memakai data s.d. tgl_fitur: recency >= 0, umur >= 0, trx_7h <= trx_30h, "
                   "versi profil (SCD-2) yang berlaku pada akhir hari ditemukan", bocor.height, fn.height,
                   contoh=_contoh(bocor, ["nasabah_id", "tgl_fitur", "hari_sejak_trx_terakhir", "trx_7h", "trx_30h"])))

    hilang = sorted(set(tgls) - set(ft["tgl"].to_list()))
    h.append(Hasil("gold_fakta_transaksi_partisi_lengkap", "timeliness",
                   "setiap tanggal yang diproses sudah punya partisi di gold (SLA H-1 sebelum 06.00)",
                   len(hilang), len(tgls), contoh=[{"tgl": d.isoformat()} for d in hilang[:5]]))
    return h


def simpan(hasil: list[Hasil], lapisan: str, tgls: list[dt.date]) -> None:
    run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    rows = [[now, run_id, "etl", lapisan, x.tes, x.dimensi, x.aturan, x.status, x.keparahan, x.gagal, x.total,
             x.nilai, json.dumps(x.contoh, default=str, ensure_ascii=False), min(tgls), max(tgls)] for x in hasil]
    clickhouse().insert("meta.dq_hasil", rows, column_names=[
        "waktu", "run_id", "jalur", "lapisan", "tes", "dimensi", "aturan", "status", "keparahan", "gagal", "total",
        "nilai", "contoh", "tgl_awal", "tgl_akhir"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lapisan", choices=["silver", "gold"], required=True)
    ap.add_argument("--tgl", required=True)
    ap.add_argument("--penuh", action="store_true")
    a = ap.parse_args()
    tgls = tgl_proses(a.tgl, a.penuh)
    hasil = tes_silver(tgls) if a.lapisan == "silver" else tes_gold(tgls)
    simpan(hasil, a.lapisan, tgls)
    gagal = [x for x in hasil if x.status == "gagal"]
    for x in hasil:
        log("error" if x.status == "gagal" else "warn" if x.status == "peringatan" else "info",
            f"DQ {x.status.upper()}: {x.tes}", dimensi=x.dimensi, aturan=x.aturan, gagal=x.gagal, total=x.total,
            nilai=x.nilai, contoh=x.contoh[:3])
    if gagal:
        print("\n=== DATA QUALITY GAGAL: pembaruan gold dihentikan ===", file=sys.stderr)
        for x in gagal:
            print(f"- [{x.dimensi}] {x.tes}: {x.gagal}/{x.total} baris melanggar aturan '{x.aturan}'. "
                  f"Contoh: {json.dumps(x.contoh[:3], default=str, ensure_ascii=False)}", file=sys.stderr)
        sys.exit(1)
    log("info", "DQ lulus semua", lapisan=a.lapisan, jumlah_tes=len(hasil))


if __name__ == "__main__":
    main()
