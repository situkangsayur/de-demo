"""ETL silver -> gold (Polars + DuckDB), pola write-audit-publish.

Star schema (W3), grain fakta_transaksi = satu transaksi:
  fakta_transaksi, fakta_pembiayaan (grain satu akad cair)
  fitur_nasabah (untuk tim AI; grain satu nasabah per tanggal, point-in-time)
  dim_tanggal, dim_cabang, dim_kanal, dim_akad, dim_nasabah (SCD-2, W4)

Langkah:
  --tahap siapkan   : bangun gold ke namespace lake.staging.* (belum terlihat pengguna)
  (dq_gold memeriksa staging; bila gagal, publikasi tidak pernah jalan -> gold tidak berubah, W8)
  --tahap publikasi : salin staging ke lake.gold.* (dimensi ditimpa penuh, fakta per partisi tanggal)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

import duckdb
import polars as pl
from pyiceberg.expressions import In

from . import iceberg_util as ice
from .kolom import AKAD_VALID, KANAL_VALID, STATUS_CAIR
from .konfigurasi import log
from .silver import tgl_proses

AWAL_TGL, AKHIR_TGL = dt.date(2025, 1, 1), dt.date(2027, 12, 31)
# batas atas DateTime64 ClickHouse adalah 2299-12-31; dipakai juga di lake agar kedua jalur identik
SELAMANYA = dt.datetime(2299, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc)
AWAL_WAKTU = dt.datetime(1900, 1, 1, tzinfo=dt.timezone.utc)
HARI = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]

AKAD_INFO = {"MRB": ("Murabahah", "jual beli"), "MSY": ("Musyarakah", "bagi hasil"),
             "MDH": ("Mudharabah", "bagi hasil"), "IJR": ("Ijarah", "sewa")}
KANAL_INFO = {"atm": ("ATM", "fisik"), "mobile": ("Mobile banking", "digital"),
              "ecommerce": ("Pembayaran toko daring", "digital"),
              "teller": ("Teller cabang", "fisik"), "web": ("Internet banking", "digital")}


def dim_tanggal() -> pl.DataFrame:
    df = pl.DataFrame({"tanggal": pl.date_range(AWAL_TGL, AKHIR_TGL, "1d", eager=True)})
    return df.select(
        (pl.col("tanggal").dt.strftime("%Y%m%d").cast(pl.Int32)).alias("tanggal_key"), "tanggal",
        pl.col("tanggal").dt.year().cast(pl.Int32).alias("tahun"),
        pl.col("tanggal").dt.month().cast(pl.Int32).alias("bulan"),
        pl.col("tanggal").dt.day().cast(pl.Int32).alias("hari"),
        pl.col("tanggal").dt.week().cast(pl.Int32).alias("pekan_iso"),
        pl.col("tanggal").dt.weekday().map_elements(lambda w: HARI[w - 1], return_dtype=pl.Utf8).alias("nama_hari"),
        (pl.col("tanggal").dt.weekday() >= 6).alias("akhir_pekan"))


def dim_kanal() -> pl.DataFrame:
    return pl.DataFrame({"kanal_key": KANAL_VALID, "nama_kanal": [KANAL_INFO[k][0] for k in KANAL_VALID],
                         "jenis_kanal": [KANAL_INFO[k][1] for k in KANAL_VALID]})


def dim_akad() -> pl.DataFrame:
    return pl.DataFrame({"akad_key": AKAD_VALID, "nama_akad": [AKAD_INFO[k][0] for k in AKAD_VALID],
                         "jenis_akad": [AKAD_INFO[k][1] for k in AKAD_VALID]})


def dim_cabang() -> pl.DataFrame:
    s = ice.baca("silver.cabang")
    return s.select(pl.col("cabang_id").alias("cabang_key"), "kode", "nama", "kota", "wilayah").sort("cabang_key")


def dim_nasabah() -> pl.DataFrame:
    """SCD-2 dari seluruh riwayat versi silver.nasabah.

    Riwayat lengkap tersedia dari CDC, sehingga interval berlaku dihitung dengan window function
    (lead) dan hasilnya deterministik: dijalankan berapa kali pun, dalam urutan apa pun, sama.
    Versi pertama berlaku sejak 1900-01-01 agar transaksi yang tercatat sedikit lebih awal daripada
    event profil tetap menemukan versinya.
    """
    s = ice.baca("silver.nasabah").sort(["nasabah_id", "versi_ts"])
    s = s.with_columns(pl.int_range(1, pl.len() + 1).over("nasabah_id").cast(pl.Int32).alias("versi"),
                       pl.col("versi_ts").shift(-1).over("nasabah_id").alias("_berikut"))
    return s.select(
        pl.format("{}-{}", "nasabah_id", "versi").alias("nasabah_sk"), "nasabah_id", "versi", "cif",
        "nama_samaran", "nik_token", "no_hp_token", "email_token", "alamat_token", "tahun_lahir", "kota", "segmen", "cabang_id",
        "terdaftar_pada", pl.col("terdaftar_pada").dt.convert_time_zone("Asia/Jakarta").dt.date().alias("tgl_daftar"),
        pl.col("versi_ts").alias("diubah_pada"),
        pl.when(pl.col("versi") == 1).then(pl.lit(AWAL_WAKTU)).otherwise(pl.col("versi_ts")).alias("berlaku_dari"),
        pl.col("_berikut").fill_null(pl.lit(SELAMANYA)).alias("berlaku_sampai"),
        pl.col("_berikut").is_null().alias("is_current"))


def _lookup_nasabah(fakta: pl.DataFrame, waktu: str, dim: pl.DataFrame) -> pl.DataFrame:
    """Pasangkan setiap baris fakta dengan versi nasabah yang berlaku pada saat kejadian (DuckDB)."""
    con = duckdb.connect()
    con.register("f", fakta.to_arrow())
    con.register("d", dim.select("nasabah_sk", "nasabah_id", "berlaku_dari", "berlaku_sampai").to_arrow())
    hasil = con.execute(f"""
        select f.*, d.nasabah_sk from f left join d
          on f.nasabah_id = d.nasabah_id and f.{waktu} >= d.berlaku_dari and f.{waktu} < d.berlaku_sampai
    """).to_arrow_table()
    return pl.from_arrow(hasil)


def fakta_transaksi(tgls: list[dt.date], dim_n: pl.DataFrame) -> pl.DataFrame:
    s = ice.baca("silver.transaksi", In("tgl", [d.isoformat() for d in tgls]))
    akad = ice.baca("silver.akad_pembiayaan", kolom=("akad_id", "kode_akad"))
    s = s.join(akad, on="akad_id", how="left")
    s = _lookup_nasabah(s, "waktu", dim_n)
    return s.select(
        "trx_id", "tgl", pl.col("tgl").dt.strftime("%Y%m%d").cast(pl.Int32).alias("tanggal_key"), "waktu",
        pl.col("cabang_id").alias("cabang_key"), pl.col("kanal").alias("kanal_key"), "nasabah_sk", "nasabah_id",
        "rekening_id", pl.col("kode_akad").alias("akad_key"), "jenis", "ref_order_id", "nominal", "_source").sort(["tgl", "trx_id"])


def fakta_pembiayaan(tgls: list[dt.date], dim_n: pl.DataFrame) -> pl.DataFrame:
    s = ice.baca("silver.akad_pembiayaan").filter(pl.col("tgl_cair").is_in(tgls)
                                                  & pl.col("status").is_in(STATUS_CAIR))
    s = _lookup_nasabah(s, "tanggal_cair", dim_n)
    # akad di aplikasi bank tidak menyimpan cabang: pakai cabang nasabah yang berlaku saat cair
    s = s.join(dim_n.select("nasabah_sk", pl.col("cabang_id").alias("_cab_n")), on="nasabah_sk", how="left")
    return s.select(
        "akad_id", "tgl_cair", pl.col("tgl_cair").dt.strftime("%Y%m%d").cast(pl.Int32).alias("tanggal_key"),
        pl.coalesce("cabang_id", "_cab_n").alias("cabang_key"), "nasabah_sk", "nasabah_id",
        pl.col("kode_akad").alias("akad_key"), "plafon", "margin", "terbayar", "tenor_bulan", "status").sort(["tgl_cair", "akad_id"])



SQL_FITUR = """
-- Fitur per nasabah per tanggal untuk tim AI, point-in-time: fitur tgl_fitur = D hanya memakai
-- kejadian dengan tgl <= D (frame window berakhir di CURRENT ROW) dan versi profil yang berlaku
-- pada akhir hari D (ASOF JOIN ke SCD-2). Tidak ada data dari masa depan yang bocor ke training.
with harian as (
  select nasabah_id, tgl, count(*) as n, sum(nominal)::double as nom,
         count(*) filter (where kanal in ('mobile', 'web', 'ecommerce')) as n_digital,
         count(*) filter (where jenis = 'bayar_belanja') as n_belanja,
         coalesce(sum(nominal) filter (where jenis = 'bayar_belanja'), 0)::double as nom_belanja
  from trx where nasabah_id is not null group by nasabah_id, tgl),
cair as (
  select nasabah_id, tgl_cair as tgl, count(*) as n_akad, sum(plafon)::double as pokok
  from akad where tgl_cair is not null and status in ('cair', 'lunas') group by nasabah_id, tgl_cair),
batas as (select least((select min(tgl) from trx), coalesce((select min(tgl) from cair), date '2999-01-01')) as awal,
                 (select max(tgl) from trx) as akhir),
hari as (select unnest(generate_series(awal, akhir, interval 1 day))::date as tgl from batas),
-- awal riwayat nasabah: tanggal daftar, atau transaksi pertama bila lebih awal (data backfill/terlambat)
nas as (select nasabah_id, min(t) as tgl_daftar from (
          select nasabah_id, tgl_daftar as t from dim
          union all select nasabah_id, min(tgl) as t from harian group by nasabah_id) group by nasabah_id),
grid as (select n.nasabah_id, h.tgl, n.tgl_daftar from nas n join hari h on h.tgl >= n.tgl_daftar),
g as (
  select grid.nasabah_id, grid.tgl, grid.tgl_daftar, coalesce(h.n, 0) as n, coalesce(h.nom, 0) as nom,
         coalesce(h.n_digital, 0) as n_digital, coalesce(h.n_belanja, 0) as n_belanja,
         coalesce(h.nom_belanja, 0) as nom_belanja, coalesce(c.n_akad, 0) as n_akad, coalesce(c.pokok, 0) as pokok,
         ((grid.tgl + 1)::timestamp - interval 7 hour) as t_akhir
  from grid left join harian h on h.nasabah_id = grid.nasabah_id and h.tgl = grid.tgl
            left join cair c on c.nasabah_id = grid.nasabah_id and c.tgl = grid.tgl),
w as (
  select *,
    sum(n) over w7 as trx_7h, sum(nom) over w7 as nominal_7h,
    sum(n) over w30 as trx_30h, sum(nom) over w30 as nominal_30h, sum(n_digital) over w30 as digital_30h,
    sum(n_belanja) over w30 as belanja_30h, sum(nom_belanja) over w30 as nom_belanja_30h,
    max(case when n > 0 then tgl end) over wsemua as trx_terakhir,
    sum(n_akad) over wsemua as akad_kum, sum(pokok) over wsemua as pokok_kum
  from g
  window w7 as (partition by nasabah_id order by tgl rows between 6 preceding and current row),
         w30 as (partition by nasabah_id order by tgl rows between 29 preceding and current row),
         wsemua as (partition by nasabah_id order by tgl rows between unbounded preceding and current row))
select
  w.nasabah_id, w.tgl as tgl_fitur, d.nasabah_sk as nasabah_sk_saat_itu, d.segmen as segmen_saat_itu,
  d.kota as kota_saat_itu, date_diff('day', w.tgl_daftar, w.tgl)::bigint as umur_nasabah_hari,
  w.trx_7h::bigint as trx_7h, round(w.nominal_7h, 2) as nominal_7h,
  w.trx_30h::bigint as trx_30h, round(w.nominal_30h, 2) as nominal_30h,
  case when w.trx_terakhir is null then null else date_diff('day', w.trx_terakhir, w.tgl)::bigint end as hari_sejak_trx_terakhir,
  case when w.trx_30h = 0 then null else round(w.digital_30h / w.trx_30h, 4) end as rasio_digital_30h,
  w.belanja_30h::bigint as belanja_ecom_30h, round(w.nom_belanja_30h, 2) as nominal_belanja_ecom_30h,
  w.akad_kum::bigint as akad_cair_kumulatif, round(w.pokok_kum, 2) as pokok_cair_kumulatif
from w asof left join dimv d on w.nasabah_id = d.nasabah_id and w.t_akhir > d.berlaku_dari
"""


def fitur_nasabah(tgls: list[dt.date], dim_n: pl.DataFrame) -> pl.DataFrame:
    trx = ice.baca("silver.transaksi", kolom=("nasabah_id", "tgl", "nominal", "kanal", "jenis"))
    akad = ice.baca("silver.akad_pembiayaan", kolom=("nasabah_id", "tgl_cair", "plafon", "status"))
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("trx", trx.to_arrow())
    con.register("akad", akad.to_arrow())
    con.register("dim", dim_n.select("nasabah_id", "tgl_daftar").to_arrow())
    con.register("dimv", dim_n.select("nasabah_id", "nasabah_sk", "segmen", "kota",
                                      pl.col("berlaku_dari").dt.replace_time_zone(None)).to_arrow())
    hasil = pl.from_arrow(con.execute(SQL_FITUR).to_arrow_table())
    return hasil.filter(pl.col("tgl_fitur").is_in(tgls)).sort(["tgl_fitur", "nasabah_id"])


DIMENSI = ("dim_tanggal", "dim_kanal", "dim_akad", "dim_cabang", "dim_nasabah")
FAKTA = {"fakta_transaksi": "tgl", "fakta_pembiayaan": "tgl_cair", "fitur_nasabah": "tgl_fitur"}


def siapkan(tgls: list[dt.date]) -> dict:
    """Bangun seluruh gold ke lake.staging.* (staging selalu ditimpa penuh)."""
    dims = {"dim_tanggal": dim_tanggal(), "dim_kanal": dim_kanal(), "dim_akad": dim_akad(),
            "dim_cabang": dim_cabang(), "dim_nasabah": dim_nasabah()}
    for n, df in dims.items():
        ice.timpa(f"staging.{n}", df)
    ft = fakta_transaksi(tgls, dims["dim_nasabah"])
    fp = fakta_pembiayaan(tgls, dims["dim_nasabah"])
    ice.timpa("staging.fakta_transaksi", ft)
    ice.timpa("staging.fakta_pembiayaan", fp)
    fn = fitur_nasabah(tgls, dims["dim_nasabah"])
    ice.timpa("staging.fitur_nasabah", fn)
    meta = {"tgl": [d.isoformat() for d in tgls], "fakta_transaksi": ft.height, "fakta_pembiayaan": fp.height, "fitur_nasabah": fn.height}
    ice.timpa("staging._tgl_proses", pl.DataFrame({"tgl": tgls, "info": [json.dumps(meta)] * len(tgls)}))
    return meta


def publikasi() -> dict:
    tgls = ice.baca("staging._tgl_proses")["tgl"].to_list()
    hasil = {}
    for n in DIMENSI:
        hasil[n] = ice.timpa(f"gold.{n}", ice.baca(f"staging.{n}"))
    for n, part in FAKTA.items():
        hasil[n] = ice.timpa(f"gold.{n}", ice.baca(f"staging.{n}"), partisi=part, nilai_partisi=tgls)
    log("info", "gold dipublikasikan", tgl_awal=str(min(tgls)), tgl_akhir=str(max(tgls)), snapshot=hasil)
    return hasil


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tahap", choices=["siapkan", "publikasi"], required=True)
    ap.add_argument("--tgl")
    ap.add_argument("--penuh", action="store_true")
    a = ap.parse_args()
    if a.tahap == "siapkan":
        log("info", "staging gold siap", **siapkan(tgl_proses(a.tgl, a.penuh)))
    else:
        publikasi()


if __name__ == "__main__":
    main()
