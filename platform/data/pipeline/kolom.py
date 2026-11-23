"""Pemetaan kolom bronze -> kolom kanonik silver, beserta normalisasi tipe.

Bronze ditulis penulis A (kontrak: platform/KONTRAK-DATA.md, skema: platform/ops/SKEMA.md).
Nama kolom data bisa sedikit berbeda antara event aplikasi dan CDC Debezium, dan tipe bisa
berbeda antar-berkas (string RFC3339 vs timestamp vs epoch mikrodetik dari Debezium). Semua
perbedaan itu diselesaikan di satu tempat ini, sehingga silver selalu bertipe benar.
"""
from __future__ import annotations

import polars as pl

UTC = "UTC"

# kanonik: (alias yang dicari berurutan, tipe kanonik, wajib?)
SKEMA: dict[str, dict[str, tuple[list[str], str, bool]]] = {
    "transaksi": {
        "id": (["id", "transaksi_id", "trx_id"], "str", True),
        "rekening_id": (["rekening_id", "account_id"], "str", True),
        "cabang_id": (["cabang_id", "branch_id"], "str", True),
        "nasabah_id": (["nasabah_id", "customer_id"], "str", False),
        "akad_id": (["akad_id", "akad_pembiayaan_id"], "str", False),
        "kanal": (["kanal", "channel"], "str", True),
        "jenis": (["jenis", "jenis_transaksi", "tipe"], "str", False),
        "ref_order_id": (["ref_order_id", "order_id"], "str", False),
        "transfer_id": (["transfer_id"], "str", False),
        "idempotency_key": (["idempotency_key"], "str", False),
        "saldo_setelah": (["saldo_setelah"], "num", False),
        "dicatat_pada": (["created_at"], "ts", False),
        "updated_at": (["updated_at"], "ts", False),
        "nominal": (["nominal", "jumlah", "amount"], "num", True),
        "waktu": (["occurred_at", "waktu", "waktu_transaksi", "_occurred_at", "created_at"], "ts", True),
    },
    "nasabah": {
        "id": (["id", "nasabah_id"], "str", True),
        "cif": (["cif", "no_cif"], "str", False),
        "nama": (["nama", "nama_lengkap"], "str", False),
        "nik": (["nik"], "str", False),
        "no_hp": (["no_hp", "nomor_hp", "telepon", "hp"], "str", False),
        "email": (["email", "surel"], "str", False),
        "tanggal_lahir": (["tanggal_lahir", "tgl_lahir"], "date", False),
        "alamat": (["alamat"], "str", False),
        "kota": (["kota", "alamat_kota"], "str", False),
        "segmen": (["segmen", "segment"], "str", False),
        "cabang_id": (["cabang_id"], "str", False),
        "terdaftar_pada": (["created_at", "terdaftar_pada", "dibuat_pada", "tanggal_daftar"], "ts", False),
        "updated_at": (["updated_at", "diperbarui_pada", "modified_at"], "ts", False),
    },
    "rekening": {
        "id": (["id", "rekening_id"], "str", True),
        "nasabah_id": (["nasabah_id"], "str", True),
        "cabang_id": (["cabang_id"], "str", False),
        "no_rekening": (["no_rekening", "nomor_rekening"], "str", False),
        "produk": (["jenis", "produk", "jenis_rekening"], "str", False),
        "status": (["status"], "str", False),
        "dibuka_pada": (["dibuka_at", "dibuka_pada", "created_at"], "ts", False),
        "updated_at": (["updated_at", "diperbarui_pada"], "ts", False),
    },
    "akad_pembiayaan": {
        "id": (["id", "akad_id"], "str", True),
        "nasabah_id": (["nasabah_id"], "str", True),
        "rekening_id": (["rekening_id"], "str", False),
        "cabang_id": (["cabang_id"], "str", False),
        "kode_akad": (["kode_akad", "jenis_akad", "akad"], "str", True),
        "plafon": (["pokok", "plafon", "nominal", "jumlah"], "num", True),
        "margin": (["margin"], "num", False),
        "terbayar": (["terbayar"], "num", False),
        "diajukan_at": (["diajukan_at"], "ts", False),
        "tenor_bulan": (["tenor_bulan", "tenor"], "int", False),
        "status": (["status"], "str", False),
        "tanggal_cair": (["cair_at", "tanggal_cair", "dicairkan_pada", "cair_pada", "disbursed_at"], "ts", False),
        "created_at": (["created_at", "dibuat_pada"], "ts", False),
        "updated_at": (["updated_at", "diperbarui_pada"], "ts", False),
    },
    "cabang": {
        "id": (["id", "cabang_id"], "str", True),
        "kode": (["kode", "kode_cabang"], "str", False),
        "nama": (["nama", "nama_cabang"], "str", False),
        "kota": (["kota"], "str", False),
        "wilayah": (["wilayah", "region", "provinsi"], "str", False),
        "updated_at": (["updated_at", "diperbarui_pada"], "ts", False),
    },
}

META = ["_ingested_at", "_source", "_batch_id", "_event_id", "_op", "_event_type", "_occurred_at", "_ts_ms"]

KANAL = {"mobile": "mobile", "mobile banking": "mobile", "m-banking": "mobile", "mbanking": "mobile",
         "app": "mobile", "atm": "atm", "teller": "teller", "counter": "teller", "cabang": "teller",
         "web": "web", "internet banking": "web", "ib": "web", "netbanking": "web",
         "ecommerce": "ecommerce", "e-commerce": "ecommerce", "toko": "ecommerce", "shop": "ecommerce"}
KANAL_VALID = ["atm", "ecommerce", "mobile", "teller", "web"]
STATUS_CAIR = ["cair", "lunas"]  # akad yang sudah dicairkan (lunas = pernah cair)

AKAD = {"MRB": "MRB", "MURABAHAH": "MRB", "MSY": "MSY", "MUSYARAKAH": "MSY", "MDH": "MDH",
        "MUDHARABAH": "MDH", "MUDARABAH": "MDH", "IJR": "IJR", "IJARAH": "IJR"}
AKAD_VALID = ["IJR", "MDH", "MRB", "MSY"]


def _ts(df: pl.DataFrame, c: str) -> pl.Expr:
    """Ubah kolom apa pun (string, timestamp, epoch, date) menjadi Datetime(us, UTC)."""
    t = df.schema[c]
    e = pl.col(c)
    if isinstance(t, pl.Datetime):
        e = e.dt.replace_time_zone(UTC) if t.time_zone is None else e.dt.convert_time_zone(UTC)
        return e.dt.cast_time_unit("us")
    if t == pl.Date:
        return e.cast(pl.Datetime("us")).dt.replace_time_zone(UTC)
    if t.is_integer() or t.is_float():
        x = e.cast(pl.Int64)
        # Debezium: MicroTimestamp (µs), Timestamp (ms), Date (hari sejak epoch)
        return (pl.when(x.abs() > 10**17).then(pl.from_epoch(x // 1000, "us"))
                .when(x.abs() > 10**14).then(pl.from_epoch(x, "us"))
                .when(x.abs() > 10**11).then(pl.from_epoch(x, "ms"))
                .when(x.abs() > 10**6).then(pl.from_epoch(x, "s"))
                .otherwise(pl.from_epoch(x * 86400, "s"))
                ).dt.cast_time_unit("us").dt.replace_time_zone(UTC)
    s = e.cast(pl.Utf8).str.strip_chars()
    # epoch dalam bentuk string (ingestor meratakan semua kolom CDC menjadi string;
    # Debezium time.precision.mode=connect: timestamp = epoch ms, date = hari sejak epoch)
    x = pl.when(s.str.contains(r"^-?\d+$")).then(s.cast(pl.Int64, strict=False))
    angka = (pl.when(x.abs() > 10**17).then(pl.from_epoch(x // 1000, "us"))
             .when(x.abs() > 10**14).then(pl.from_epoch(x, "us"))
             .when(x.abs() > 10**11).then(pl.from_epoch(x, "ms"))
             .when(x.abs() > 10**6).then(pl.from_epoch(x, "s"))
             .when(x.is_not_null()).then(pl.from_epoch(x * 86400, "s"))
             ).dt.cast_time_unit("us").dt.replace_time_zone(UTC)
    return pl.coalesce(
        angka,
        s.str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%:z", strict=False, time_unit="us").dt.convert_time_zone(UTC),
        s.str.to_datetime("%Y-%m-%dT%H:%M:%S%:z", strict=False, time_unit="us").dt.convert_time_zone(UTC),
        s.str.replace("Z$", "+00:00").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%:z", strict=False, time_unit="us").dt.convert_time_zone(UTC),
        s.str.replace("Z$", "+00:00").str.to_datetime("%Y-%m-%dT%H:%M:%S%:z", strict=False, time_unit="us").dt.convert_time_zone(UTC),
        s.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit="us").dt.replace_time_zone(UTC),
        s.str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False, time_unit="us").dt.replace_time_zone(UTC),
        s.str.to_date("%Y-%m-%d", strict=False).cast(pl.Datetime("us")).dt.replace_time_zone(UTC),
    )


def _konversi(df: pl.DataFrame, c: str, tipe: str) -> pl.Expr:
    t = df.schema[c]
    if tipe == "str":
        return pl.col(c).cast(pl.Utf8)
    if tipe == "num":
        if t == pl.Utf8:
            return pl.col(c).str.strip_chars().cast(pl.Float64, strict=False)
        return pl.col(c).cast(pl.Float64, strict=False)
    if tipe == "int":
        return pl.col(c).cast(pl.Float64, strict=False).cast(pl.Int32, strict=False)
    if tipe == "date":
        return _ts(df, c).dt.date()
    return _ts(df, c)


_KOSONG = {"str": pl.Utf8, "num": pl.Float64, "int": pl.Int32, "date": pl.Date,
           "ts": pl.Datetime("us", UTC)}


def normalisasi(df: pl.DataFrame, entitas: str, berkas: str) -> pl.DataFrame:
    """Satu berkas bronze -> kolom kanonik + metadata. Kolom tambahan dari sumber dibuang."""
    pilih = []
    for kan, (alias, tipe, _wajib) in SKEMA[entitas].items():
        asal = next((a for a in alias if a in df.columns), None)
        if asal is None:
            pilih.append(pl.lit(None, dtype=_KOSONG[tipe]).alias(kan))
        else:
            pilih.append(_konversi(df, asal, tipe).alias(kan))
    for m in META:
        if m not in df.columns:
            pilih.append(pl.lit(None, dtype=pl.Datetime("us", UTC) if m == "_ingested_at" else pl.Utf8).alias(m))
        elif m == "_ingested_at":
            pilih.append(_ts(df, m).alias(m))
        else:
            pilih.append(pl.col(m).cast(pl.Utf8).alias(m))
    pilih.append(pl.lit(berkas).alias("_file"))
    return df.select(pilih)
