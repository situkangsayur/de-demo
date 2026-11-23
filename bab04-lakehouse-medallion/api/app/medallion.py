"""Medallion: bronze (Parquet mentah) -> silver (Iceberg, Polars) -> gold (Iceberg, DuckDB SQL).

Aturan tiap lapisan mengikuti slide P04 "Aturan tiap layer" dan platform (pipeline/silver.py, gold.py):
- silver: tipe benar, dedup app vs cdc (_source = app / cdc / app+cdc), kanal dibakukan, PII ditoken,
  kode akad tidak valid dikarantina; ditulis overwrite per partisi hari (idempotent);
- gold: agregat siap pakai tanpa PII, dibangun ulang utuh dalam satu commit.
Gerbang data quality: gerbang 1 sebelum silver ditulis, gerbang 2 sebelum gold ditulis. Gerbang gagal
berarti baris pelanggar masuk tabel karantina dan lapisan berikutnya tidak berubah.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid

import duckdb
import polars as pl
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, And, GreaterThanOrEqual, LessThan, Or
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (BooleanType, DateType, IntegerType, LongType, NestedField, StringType,
                             TimestamptzType)

from . import bronze
from .konfig import TOKEN_PEPPER, Stopwatch, katalog, log

RIWAYAT = os.environ.get("STATE_DIR", "/data") + "/riwayat-run.json"
KANAL = {"mobile": "mobile", "mobile banking": "mobile", "m-banking": "mobile", "atm": "atm",
         "teller": "teller", "web": "web", "internet banking": "web", "ecommerce": "ecommerce",
         "e-commerce": "ecommerce"}
KANAL_VALID = sorted(set(KANAL.values()))
AKAD = {"MRB": "MRB", "MURABAHAH": "MRB", "MSY": "MSY", "MUSYARAKAH": "MSY", "MDH": "MDH",
        "MUDHARABAH": "MDH", "IJR": "IJR", "IJARAH": "IJR"}
AKAD_VALID = ["IJR", "MDH", "MRB", "MSY"]
PII = {"nik", "no_hp", "email", "nama", "no_rekening"}

T = {"s": StringType, "i": IntegerType, "l": LongType, "ts": TimestamptzType, "d": DateType, "b": BooleanType}
SKEMA = {
    "silver.transaksi": [("trx_id", "s"), ("rekening_id", "s"), ("nasabah_id", "s"), ("cabang_id", "i"),
                         ("kanal", "s"), ("kanal_asli", "s"), ("jenis", "s"), ("nominal", "l"),
                         ("waktu", "ts"), ("dicatat_pada", "ts"), ("idempotency_key", "s"),
                         ("_terlambat", "b"), ("_source", "s"), ("_batch_id", "s"), ("_ingested_at", "ts")],
    "silver.karantina_transaksi": [("trx_id", "s"), ("aturan", "s"), ("nilai", "s"), ("_file", "s"),
                                   ("run_id", "s"), ("dikarantina_pada", "ts")],
    "silver.nasabah": [("nasabah_id", "s"), ("cif", "s"), ("nama_samaran", "s"), ("nik_token", "s"),
                       ("no_hp_token", "s"), ("email_token", "s"), ("kota", "s"), ("segmen", "s"),
                       ("cabang_id", "i"), ("jumlah_versi", "i"), ("updated_at", "ts"), ("_source", "s")],
    "silver.rekening": [("rekening_id", "s"), ("nasabah_id", "s"), ("no_rekening_token", "s"), ("jenis", "s"),
                        ("status", "s"), ("cabang_id", "i"), ("dibuka_at", "ts"), ("_source", "s")],
    "silver.cabang": [("cabang_id", "i"), ("kode", "s"), ("nama", "s"), ("kota", "s"), ("wilayah", "s")],
    "silver.akad_pembiayaan": [("akad_id", "s"), ("nasabah_id", "s"), ("kode_akad", "s"), ("kode_akad_asli", "s"),
                               ("pokok", "l"), ("margin", "l"), ("tenor_bulan", "i"), ("status", "s"),
                               ("cair_at", "ts"), ("_source", "s")],
    "silver.akad_karantina": [("akad_id", "s"), ("kode_akad_asli", "s"), ("alasan", "s"), ("pokok", "l"),
                              ("dikarantina_pada", "ts")],
    "gold.fakta_trx_harian": [("tgl", "d"), ("cabang_kode", "s"), ("kanal", "s"), ("jml_trx", "l"),
                              ("total_nominal", "l")],
    "gold.pembiayaan_per_akad": [("kode_akad", "s"), ("jml_akad", "l"), ("total_pokok", "l"),
                                 ("total_margin", "l")],
}
PARTISI = {"silver.transaksi": ("waktu", DayTransform(), "waktu_day")}


def skema(nama: str) -> Schema:
    return Schema(*[NestedField(i + 1, n, T[t](), required=False) for i, (n, t) in enumerate(SKEMA[nama])])


def arrow_skema(nama: str) -> pa.Schema:
    m = {"s": pa.string(), "i": pa.int32(), "l": pa.int64(), "ts": pa.timestamp("us", tz="UTC"),
         "d": pa.date32(), "b": pa.bool_()}
    return pa.schema([pa.field(n, m[t]) for n, t in SKEMA[nama]])


def tabel(nama: str):
    cat = katalog()
    try:
        return cat.load_table(nama)
    except NoSuchTableError:
        cat.create_namespace_if_not_exists(nama.split(".")[0])
        spec = PartitionSpec()
        if nama in PARTISI:
            kol, tr, pn = PARTISI[nama]
            sid = skema(nama).find_field(kol).field_id
            spec = PartitionSpec(PartitionField(source_id=sid, field_id=1000, transform=tr, name=pn))
        t = cat.create_table(nama, schema=skema(nama), partition_spec=spec,
                             properties={"format-version": "2", "write.parquet.compression-codec": "zstd"})
        log("info", "tabel iceberg dibuat", tabel=nama)
        return t


def ke_arrow(df: pl.DataFrame, nama: str) -> pa.Table:
    s = arrow_skema(nama)
    return df.select(s.names).to_arrow().cast(s)


def timpa(nama: str, df: pl.DataFrame, filt=None, props: dict | None = None) -> int | None:
    t = tabel(nama)
    t.overwrite(ke_arrow(df, nama), overwrite_filter=filt if filt is not None else AlwaysTrue(),
                snapshot_properties=props or {})
    t = t.refresh()
    return t.current_snapshot().snapshot_id if t.current_snapshot() else None


def baca(nama: str) -> pl.DataFrame:
    return pl.from_arrow(tabel(nama).scan().to_arrow())


# ---------------------------------------------------------------- bantuan transformasi
def _ts(c: str) -> pl.Expr:
    s = pl.col(c).cast(pl.Utf8).str.strip_chars()
    s = s.str.replace(r"Z$", "+00:00")
    return pl.coalesce(
        s.str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%:z", strict=False, time_unit="us"),
        s.str.to_datetime("%Y-%m-%dT%H:%M:%S%:z", strict=False, time_unit="us"),
    ).dt.convert_time_zone("UTC")


def token(e: pl.Expr) -> pl.Expr:
    """tok_ + 20 hex SHA-256(pepper|nilai); sama dengan platform (pipeline/silver.py)."""
    return e.map_elements(lambda v: None if v is None or v == "" else
                          "tok_" + hashlib.sha256(f"{TOKEN_PEPPER}|{v}".encode()).hexdigest()[:20],
                          return_dtype=pl.Utf8)


def samaran(e: pl.Expr) -> pl.Expr:
    return e.str.split(" ").list.eval(pl.element().str.slice(0, 1) + pl.lit("***")).list.join(" ")


def terakhir(df: pl.DataFrame) -> pl.DataFrame:
    """Satu baris per id: versi terakhir menang; _sumber mencatat app, cdc, atau app+cdc."""
    sumber = df.group_by("id").agg(pl.col("_source").unique().sort().str.join("+").alias("_sumber"))
    akhir = (df.sort(["id", "_ingested_at", "_ts_ms", "_source"], nulls_last=False)
               .group_by("id", maintain_order=True).last())
    return akhir.join(sumber, on="id", how="left")


def _kol(df: pl.DataFrame, c: str) -> pl.Expr:
    return pl.col(c) if c in df.columns else pl.lit(None, dtype=pl.Utf8).alias(c)


def _rentang_hari(dts: list[dt.date]):
    exprs = []
    for d in dts:
        a = dt.datetime.combine(d, dt.time(0), dt.timezone.utc)
        exprs.append(And(GreaterThanOrEqual("waktu", a.isoformat()),
                         LessThan("waktu", (a + dt.timedelta(days=1)).isoformat())))
    out = exprs[0]
    for e in exprs[1:]:
        out = Or(out, e)
    return out


def _cek(aturan: str, dimensi: str, pelanggar: pl.DataFrame, kolom_nilai: str | None = None) -> dict:
    contoh = []
    if pelanggar.height:
        cols = ["trx_id"] + ([kolom_nilai] if kolom_nilai else [])
        contoh = [{k: (None if v is None else str(v)) for k, v in r.items()}
                  for r in pelanggar.select(cols).head(3).to_dicts()]
    return {"aturan": aturan, "dimensi": dimensi, "pelanggar": pelanggar.height,
            "lulus": pelanggar.height == 0, "contoh": contoh}


# ---------------------------------------------------------------- bronze -> silver
def silver_dimensi() -> dict:
    hasil = {}
    cab = terakhir(bronze.baca("cabang"))
    out = cab.select(pl.col("id").cast(pl.Int32).alias("cabang_id"), pl.col("kode").str.strip_chars(),
                     "nama", "kota", "wilayah").sort("cabang_id")
    timpa("silver.cabang", out)
    hasil["silver.cabang"] = out.height

    nas_raw = bronze.baca("nasabah")
    versi = nas_raw.group_by("id").agg(pl.len().cast(pl.Int32).alias("jumlah_versi"))
    nas = terakhir(nas_raw).join(versi, on="id")
    out = nas.select(
        pl.col("id").alias("nasabah_id"), "cif", samaran(pl.col("nama")).alias("nama_samaran"),
        token(pl.col("nik").str.replace_all(r"[^0-9]", "")).alias("nik_token"),
        token(pl.col("no_hp").str.replace_all(r"[^0-9]", "")).alias("no_hp_token"),
        token(pl.col("email").str.to_lowercase()).alias("email_token"),
        pl.col("kota").str.strip_chars().str.to_titlecase(), pl.col("segmen").str.to_lowercase(),
        pl.col("cabang_id").cast(pl.Int32, strict=False), "jumlah_versi",
        _ts("updated_at").alias("updated_at"), pl.col("_sumber").alias("_source"))
    timpa("silver.nasabah", out.sort("nasabah_id"))
    hasil["silver.nasabah"] = out.height

    rek = terakhir(bronze.baca("rekening"))
    out = rek.select(pl.col("id").alias("rekening_id"), "nasabah_id",
                     token(pl.col("no_rekening")).alias("no_rekening_token"), "jenis", "status",
                     pl.col("cabang_id").cast(pl.Int32, strict=False), _ts("dibuka_at").alias("dibuka_at"),
                     pl.col("_sumber").alias("_source"))
    timpa("silver.rekening", out.sort("rekening_id"))
    hasil["silver.rekening"] = out.height

    akd = terakhir(bronze.baca("akad_pembiayaan"))
    akd = akd.with_columns(pl.col("kode_akad").alias("kode_akad_asli"),
                           pl.col("kode_akad").str.strip_chars().str.to_uppercase()
                           .replace_strict(AKAD, default=None).alias("kode_akad"))
    valid = akd.filter(pl.col("kode_akad").is_not_null()).select(
        pl.col("id").alias("akad_id"), "nasabah_id", "kode_akad", "kode_akad_asli",
        pl.col("pokok").cast(pl.Int64, strict=False), pl.col("margin").cast(pl.Int64, strict=False),
        pl.col("tenor_bulan").cast(pl.Int32, strict=False), "status", _ts("cair_at").alias("cair_at"),
        pl.col("_sumber").alias("_source"))
    karantina = akd.filter(pl.col("kode_akad").is_null()).select(
        pl.col("id").alias("akad_id"), "kode_akad_asli",
        pl.format("kode akad '{}' di luar domain {}", pl.col("kode_akad_asli").fill_null("NULL"),
                  pl.lit("/".join(AKAD_VALID))).alias("alasan"),
        pl.col("pokok").cast(pl.Int64, strict=False),
        pl.lit(dt.datetime.now(dt.timezone.utc)).dt.cast_time_unit("us").alias("dikarantina_pada"))
    timpa("silver.akad_pembiayaan", valid.sort("akad_id"))
    timpa("silver.akad_karantina", karantina.sort("akad_id"))
    hasil["silver.akad_pembiayaan"] = valid.height
    hasil["silver.akad_karantina"] = karantina.height
    return hasil


def silver_transaksi(run_id: str, dts: list[dt.date]) -> dict:
    raw = bronze.baca("transaksi", dts)
    raw = raw.with_columns(pl.col("nominal").str.strip_chars().cast(pl.Int64, strict=False).alias("_nom"))
    # konflik nominal antar-sumber untuk id yang sama (consistency)
    nom = raw.group_by("id").agg(
        pl.col("_nom").filter(pl.col("_source") == "app").last().alias("app"),
        pl.col("_nom").filter(pl.col("_source") == "cdc").last().alias("cdc"))
    konflik = nom.filter(pl.col("app").is_not_null() & pl.col("cdc").is_not_null() & (pl.col("app") != pl.col("cdc")))
    akhir = terakhir(raw)
    rek = baca("silver.rekening").select("rekening_id", "nasabah_id")
    akhir = akhir.join(rek, on="rekening_id", how="left") if "rekening_id" in akhir.columns else akhir
    out = akhir.select(
        pl.col("id").alias("trx_id"), "rekening_id", "nasabah_id",
        pl.col("cabang_id").cast(pl.Int32, strict=False),
        pl.col("kanal").str.strip_chars().str.to_lowercase().replace(KANAL).alias("kanal"),
        pl.col("kanal").alias("kanal_asli"), pl.col("jenis").str.strip_chars().str.to_lowercase(),
        pl.col("_nom").alias("nominal"), pl.col("nominal").alias("_nominal_teks"),
        _ts("occurred_at").alias("waktu"), _ts("created_at").alias("dicatat_pada"),
        _kol(akhir, "idempotency_key"), pl.col("_sumber").alias("_source"), "_batch_id", "_ingested_at",
        "_file")
    out = out.with_columns(
        ((pl.col("dicatat_pada") - pl.col("waktu")).dt.total_seconds() > 60).fill_null(False).alias("_terlambat"))

    # gerbang 1: bronze -> silver
    batas = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)
    cek = [
        _cek("nominal angka dan > 0", "validity", out.filter(pl.col("nominal").is_null() | (pl.col("nominal") <= 0)),
             "_nominal_teks"),
        _cek(f"kanal dalam domain {'/'.join(KANAL_VALID)}", "validity",
             out.filter(~pl.col("kanal").is_in(KANAL_VALID).fill_null(False)), "kanal_asli"),
        _cek("rekening_id terisi", "completeness", out.filter(pl.col("rekening_id").is_null()), None),
        _cek("trx_id unik sesudah dedup", "uniqueness", out.filter(pl.col("trx_id").is_duplicated()), None),
        _cek("waktu terbaca dan tidak di masa depan", "validity",
             out.filter(pl.col("waktu").is_null() | (pl.col("waktu") > batas)), None),
        _cek("nominal app = cdc untuk transaksi yang sama", "consistency",
             out.filter(pl.col("trx_id").is_in(konflik["id"].implode())), "nominal"),
    ]
    statistik = {"baris_bronze": raw.height, "baris_unik": out.height,
                 "duplikat_dibuang": raw.height - out.height,
                 "sumber": {r["_source"]: r["len"] for r in out.group_by("_source").len().to_dicts()},
                 "terlambat": int(out["_terlambat"].sum()),
                 "kanal_dibakukan": int((out["kanal"] != out["kanal_asli"]).sum())}
    if not all(c["lulus"] for c in cek):
        rows = []
        now = dt.datetime.now(dt.timezone.utc)
        for c in cek:
            if not c["lulus"]:
                for x in c["contoh"]:
                    rows.append({"trx_id": x.get("trx_id"), "aturan": c["aturan"],
                                 "nilai": json.dumps(x, ensure_ascii=False), "_file": None,
                                 "run_id": run_id, "dikarantina_pada": now})
        t = tabel("silver.karantina_transaksi")
        t.append(ke_arrow(pl.DataFrame(rows, schema_overrides={"dikarantina_pada": pl.Datetime("us", "UTC")}),
                          "silver.karantina_transaksi"))
        return {"lulus": False, "cek": cek, "statistik": statistik, "snapshot_id": None}
    snap = timpa("silver.transaksi", out.sort(["waktu", "trx_id"]), _rentang_hari(dts),
                 {"run-id": run_id, "lapisan": "silver"})
    return {"lulus": True, "cek": cek, "statistik": statistik, "snapshot_id": snap,
            "hari": [str(d) for d in dts]}


# ---------------------------------------------------------------- silver -> gold (DuckDB)
SQL_GOLD = """
SELECT CAST(timezone('Asia/Jakarta', t.waktu) AS DATE) AS tgl,
       c.kode AS cabang_kode, t.kanal,
       COUNT(*)::BIGINT AS jml_trx, SUM(t.nominal)::BIGINT AS total_nominal
FROM silver_transaksi t
LEFT JOIN silver_cabang c USING (cabang_id)
GROUP BY ALL
ORDER BY tgl, cabang_kode, kanal
"""
SQL_AKAD = """
SELECT kode_akad, COUNT(*)::BIGINT AS jml_akad, SUM(pokok)::BIGINT AS total_pokok,
       SUM(margin)::BIGINT AS total_margin
FROM silver_akad
WHERE status IN ('cair', 'lunas')
GROUP BY kode_akad ORDER BY kode_akad
"""


def gold(run_id: str) -> dict:
    con = duckdb.connect()
    silver_transaksi_ = tabel("silver.transaksi").scan().to_arrow()  # noqa: F841 (dibaca DuckDB)
    con.register("silver_transaksi", silver_transaksi_)
    con.register("silver_cabang", tabel("silver.cabang").scan().to_arrow())
    con.register("silver_akad", tabel("silver.akad_pembiayaan").scan().to_arrow())
    fakta = pl.from_arrow(con.sql(SQL_GOLD).to_arrow_table())
    akad = pl.from_arrow(con.sql(SQL_AKAD).to_arrow_table())
    total_silver = con.execute("SELECT COALESCE(SUM(nominal),0)::BIGINT FROM silver_transaksi").fetchone()[0]
    tanpa_cabang = con.execute("""SELECT COUNT(*) FROM silver_transaksi t
                              WHERE cabang_id NOT IN (SELECT cabang_id FROM silver_cabang)""").fetchone()[0]
    total_gold = int(fakta["total_nominal"].sum() or 0)
    pii = sorted(PII & set(fakta.columns) | PII & set(akad.columns))
    cek = [
        {"aturan": "rekonsiliasi: total nominal silver = total gold", "dimensi": "consistency",
         "pelanggar": 0 if total_silver == total_gold else 1, "lulus": total_silver == total_gold,
         "contoh": [{"silver": total_silver, "gold": total_gold}]},
        {"aturan": "referential: setiap cabang_id transaksi ada di silver.cabang", "dimensi": "consistency",
         "pelanggar": tanpa_cabang, "lulus": tanpa_cabang == 0, "contoh": []},
        {"aturan": "gold tanpa kolom PII", "dimensi": "validity", "pelanggar": len(pii), "lulus": not pii,
         "contoh": [{"kolom": c} for c in pii]},
    ]
    if not all(c["lulus"] for c in cek):
        return {"lulus": False, "cek": cek}
    s1 = timpa("gold.fakta_trx_harian", fakta, props={"run-id": run_id, "lapisan": "gold"})
    s2 = timpa("gold.pembiayaan_per_akad", akad, props={"run-id": run_id, "lapisan": "gold"})
    return {"lulus": True, "cek": cek, "baris": {"gold.fakta_trx_harian": fakta.height,
                                                 "gold.pembiayaan_per_akad": akad.height},
            "snapshot_id": {"gold.fakta_trx_harian": s1, "gold.pembiayaan_per_akad": s2},
            "sql": SQL_GOLD.strip()}


def jalankan(hanya_terakhir: bool = False) -> dict:
    """Satu run pipeline: dimensi, transaksi (gerbang 1), gold (gerbang 2)."""
    run_id = f"run-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:4]}"
    dts = bronze.daftar_dt("transaksi")
    if not dts:
        raise ValueError("bronze transaksi masih kosong; tambahkan hari dulu")
    if hanya_terakhir:
        dts = dts[-1:]
    langkah = []
    with Stopwatch() as w:
        dim = silver_dimensi()
    langkah.append({"langkah": "silver dimensi (Polars)", "ms": w.ms, "baris": dim, "lulus": True})
    with Stopwatch() as w:
        trx = silver_transaksi(run_id, dts)
    langkah.append({"langkah": "gerbang 1 + silver.transaksi (Polars)", "ms": w.ms, **trx})
    if trx["lulus"]:
        with Stopwatch() as w:
            g = gold(run_id)
        langkah.append({"langkah": "gerbang 2 + gold (DuckDB SQL)", "ms": w.ms, **g})
    else:
        langkah.append({"langkah": "gerbang 2 + gold (DuckDB SQL)", "ms": 0, "lulus": False, "dilewati": True,
                        "cek": [], "pesan": "dilewati: gerbang 1 gagal, gold tidak berubah"})
    hasil = {"run_id": run_id, "waktu": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
             "hari": [str(d) for d in dts], "lulus": all(x.get("lulus") for x in langkah), "langkah": langkah}
    riwayat = muat_riwayat()
    riwayat.insert(0, hasil)
    with open(RIWAYAT, "w") as f:
        json.dump(riwayat[:20], f, default=str)
    log("info" if hasil["lulus"] else "warn", "run medallion selesai", run_id=run_id, lulus=hasil["lulus"])
    return hasil


def muat_riwayat() -> list:
    try:
        with open(RIWAYAT) as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def jumlah_lapisan() -> list[dict]:
    out = []
    for nama in SKEMA:
        try:
            t = katalog().load_table(nama)
        except NoSuchTableError:
            out.append({"tabel": nama, "ada": False})
            continue
        s = t.current_snapshot()
        ringkas = s.summary.additional_properties if s and s.summary else {}
        out.append({"tabel": nama, "ada": True, "baris": int(ringkas.get("total-records", 0)),
                    "berkas": int(ringkas.get("total-data-files", 0)),
                    "snapshot": len(t.snapshots()), "snapshot_id": s.snapshot_id if s else None})
    return out


def lacak(trx_id: str | None) -> dict:
    """Satu transaksi melewati tiga lapisan (slide P04)."""
    if not trx_id:
        st = baca("silver.transaksi").filter((pl.col("_source") == "app+cdc")
                                             & (pl.col("kanal") != pl.col("kanal_asli")))
        if st.is_empty():
            raise ValueError("silver.transaksi masih kosong; jalankan pipeline dulu")
        trx_id = st.sort("trx_id")["trx_id"][0]
    raw = bronze.baca("transaksi").filter(pl.col("id") == trx_id)
    sv = baca("silver.transaksi").filter(pl.col("trx_id") == trx_id)
    gd = None
    if sv.height:
        r = sv.to_dicts()[0]
        cab = baca("silver.cabang").filter(pl.col("cabang_id") == r["cabang_id"])
        tgl = r["waktu"].astimezone(dt.timezone(dt.timedelta(hours=7))).date()
        g = baca("gold.fakta_trx_harian").filter((pl.col("tgl") == tgl) & (pl.col("kanal") == r["kanal"])
                                                 & (pl.col("cabang_kode") == (cab["kode"][0] if cab.height else None)))
        gd = g.to_dicts()
    def baris(df):
        return [{k: (None if v is None else str(v)) for k, v in x.items()} for x in df.to_dicts()]
    return {"trx_id": trx_id, "bronze": baris(raw.select([c for c in raw.columns if c != "_before"])),
            "silver": baris(sv), "gold": [{k: str(v) for k, v in x.items()} for x in (gd or [])]}
