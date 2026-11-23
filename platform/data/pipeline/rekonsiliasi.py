"""Rekonsiliasi gold lake (ETL) vs gold warehouse (ELT) untuk tanggal yang diproses.

Dua jalur yang dibangun terpisah harus menghasilkan angka yang sama. Selisih berarti salah satu
aturan transformasi berbeda, dan itu ditangkap di sini, bukan oleh direksi di dashboard.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

import polars as pl
from pyiceberg.expressions import In

from . import iceberg_util as ice
from .konfigurasi import clickhouse, log
from .silver import tgl_proses


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tgl", required=True)
    ap.add_argument("--penuh", action="store_true")
    a = ap.parse_args()
    tgls = tgl_proses(a.tgl, a.penuh)
    ch = clickhouse()
    iso = [d.isoformat() for d in tgls]

    lake = (ice.baca("gold.fakta_transaksi", In("tgl", iso), kolom=("tgl", "trx_id", "nominal", "nasabah_sk"))
              .group_by("tgl").agg(pl.len().alias("n"), pl.col("nominal").cast(pl.Float64).sum().round(2).alias("t"),
                                   pl.col("nasabah_sk").n_unique().alias("sk")))
    daftar = ",".join(f"'{d}'" for d in iso)
    wh = ch.query_arrow(f"""select tgl, count() as n, round(toFloat64(sum(nominal)), 2) as t, uniqExact(nasabah_sk) as sk
                                from gold.fakta_transaksi where tgl in ({daftar}) group by tgl""")
    wh = pl.from_arrow(wh)
    if wh.height == 0:
        wh = pl.DataFrame(schema={"tgl": pl.Date, "n": pl.UInt64, "t": pl.Float64, "sk": pl.UInt64})
    gab = (lake.join(wh.with_columns(pl.col("tgl").cast(pl.Date)), on="tgl", how="full", coalesce=True, suffix="_wh")
               .fill_null(0))
    selisih = gab.filter((pl.col("n") != pl.col("n_wh")) | ((pl.col("t") - pl.col("t_wh")).abs() > 0.01)
                         | (pl.col("sk") != pl.col("sk_wh")))

    dn_lake = ice.baca("gold.dim_nasabah", kolom=("nasabah_sk", "is_current"))
    dn_wh = ch.query("select count(), countIf(is_current) from gold.dim_nasabah").result_rows[0]
    dim_beda = (dn_lake.height, int(dn_lake["is_current"].sum())) != (int(dn_wh[0]), int(dn_wh[1]))

    fp_lake = ice.baca("gold.fakta_pembiayaan", In("tgl_cair", iso), kolom=("akad_id", "plafon"))
    fp_wh = ch.query(f"""select count(), round(toFloat64(coalesce(sum(plafon), 0)), 2) from gold.fakta_pembiayaan
                         where tgl_cair in ({daftar})""").result_rows[0]
    fp_beda = fp_lake.height != int(fp_wh[0]) or abs(float(fp_lake["plafon"].cast(pl.Float64).sum() or 0) - float(fp_wh[1] or 0)) > 0.01

    # Fitur memakai jendela 30 hari ke belakang. Bandingkan hanya tanggal yang riwayat jendelanya
    # lengkap di lake: silver berisi tanggal yang sudah diproses saja, sedangkan raw ClickHouse ikut
    # memuat partisi dt tetangga. Sesudah muat awal (penuh) semua tanggal memenuhi syarat.
    ada = set(ice.baca("silver.transaksi", kolom=("tgl",))["tgl"].unique().to_list())
    awal_wh = ch.query("select min(tgl) from gold.fakta_transaksi").result_rows[0][0]
    awal_lake = min(ada) if ada else None

    def jendela_lengkap(d: dt.date) -> bool:
        if awal_lake is None:
            return False
        mulai = d - dt.timedelta(days=29)
        if awal_lake > mulai and awal_wh is not None and awal_lake > awal_wh:
            return False  # warehouse punya riwayat lebih awal yang belum diproses lake
        hari = [max(mulai, awal_lake) + dt.timedelta(days=k) for k in range((d - max(mulai, awal_lake)).days + 1)]
        return all(x in ada for x in hari)

    iso_f = [d.isoformat() for d in tgls if jendela_lengkap(d)]
    daftar_f = ",".join(f"'{d}'" for d in iso_f) or "'1900-01-01'"
    fn_lake = (ice.baca("gold.fitur_nasabah", In("tgl_fitur", iso_f or ["1900-01-01"]), kolom=("tgl_fitur", "trx_30h", "nominal_30h"))
                 .group_by("tgl_fitur").agg(pl.len().alias("n"), pl.col("trx_30h").sum().alias("trx"),
                                            pl.col("nominal_30h").sum().round(2).alias("nom")))
    fn_wh = pl.from_arrow(ch.query_arrow(f"""select tgl_fitur, count() as n, sum(trx_30h) as trx,
                                round(sum(nominal_30h), 2) as nom from gold.fitur_nasabah
                                where tgl_fitur in ({daftar_f}) group by tgl_fitur"""))
    fn_gab = fn_lake.join(fn_wh.with_columns(pl.col("tgl_fitur").cast(pl.Date), pl.col("n").cast(pl.UInt32),
                                             pl.col("trx").cast(pl.Int64)),
                          on="tgl_fitur", how="full", coalesce=True, suffix="_wh").fill_null(0)
    fn_beda = fn_gab.filter((pl.col("n") != pl.col("n_wh")) | (pl.col("trx") != pl.col("trx_wh"))
                            | ((pl.col("nom") - pl.col("nom_wh")).abs() > 0.05))

    contoh = {"fakta_transaksi": selisih.head(5).to_dicts(), "fitur_nasabah": fn_beda.head(5).to_dicts(),
              "fitur_tgl_dibandingkan": len(iso_f),
              "dim_nasabah": {"lake": [dn_lake.height, int(dn_lake["is_current"].sum())], "warehouse": list(map(int, dn_wh))},
              "fakta_pembiayaan": {"lake": fp_lake.height, "warehouse": int(fp_wh[0])}}
    gagal = selisih.height + int(dim_beda) + int(fp_beda) + fn_beda.height
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    ch.insert("meta.dq_hasil", [[now, os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual"), "etl_vs_elt", "gold",
                                 "rekonsiliasi_lake_vs_warehouse", "consistency",
                                 "fakta per tgl (baris, total nominal, nasabah_sk unik), dim_nasabah, fakta_pembiayaan, fitur_nasabah sama di kedua jalur",
                                 "lulus" if gagal == 0 else "gagal", "error", gagal, len(tgls) + 2, None,
                                 json.dumps(contoh, default=str), min(tgls), max(tgls)]],
              column_names=["waktu", "run_id", "jalur", "lapisan", "tes", "dimensi", "aturan", "status", "keparahan",
                            "gagal", "total", "nilai", "contoh", "tgl_awal", "tgl_akhir"])
    if gagal:
        log("error", "rekonsiliasi ETL vs ELT berbeda", contoh=contoh)
        print(f"REKONSILIASI GAGAL: {json.dumps(contoh, default=str)}", file=sys.stderr)
        sys.exit(1)
    log("info", "rekonsiliasi ETL vs ELT identik", tgl_awal=iso[0], tgl_akhir=iso[-1],
        baris=int(lake["n"].sum()) if lake.height else 0)


if __name__ == "__main__":
    main()
