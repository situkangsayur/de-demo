"""Bantuan tulis/baca tabel Iceberg lewat PyIceberg (katalog SQL di PostgreSQL)."""
from __future__ import annotations

import datetime as dt

import polars as pl
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, In

from .konfigurasi import katalog, log

# bronze tetap Parquet biasa milik ingestor (bukan tabel Iceberg), jadi tidak ada namespace bronze
NAMESPACE = ("silver", "gold", "staging")


def siapkan_namespace() -> None:
    cat = katalog()
    for ns in NAMESPACE:
        cat.create_namespace_if_not_exists(ns)


def _arrow(df: pl.DataFrame) -> pa.Table:
    t = df.to_arrow()
    # PyIceberg belum menerima string_view/large_string di semua jalur; samakan ke string biasa.
    skema = pa.schema([pa.field(f.name, pa.string() if pa.types.is_large_string(f.type)
                                or str(f.type) == "string_view" else f.type, nullable=True)
                       for f in t.schema])
    return t.cast(skema)


def muat(nama: str):
    return katalog().load_table(nama)


def ada(nama: str) -> bool:
    try:
        muat(nama)
        return True
    except NoSuchTableError:
        return False


def _pastikan(nama: str, data: pa.Table, partisi: str | None):
    cat = katalog()
    try:
        tbl = cat.load_table(nama)
        baru = [f for f in data.schema.names if f not in tbl.schema().column_names]
        if baru:  # schema evolution: kolom baru ditambahkan, kolom lama tetap
            with tbl.update_schema() as u:
                u.union_by_name(data.schema)
            tbl = cat.load_table(nama)
        return tbl
    except NoSuchTableError:
        tbl = cat.create_table(nama, schema=data.schema,
                               properties={"format-version": "2", "write.parquet.compression-codec": "zstd"})
        if partisi:
            with tbl.update_spec() as u:
                u.add_identity(partisi)
            tbl = cat.load_table(nama)
        log("info", "tabel iceberg dibuat", tabel=nama, partisi=partisi)
        return tbl


def timpa(nama: str, df: pl.DataFrame, partisi: str | None = None,
          nilai_partisi: list[dt.date] | None = None) -> int | None:
    """Overwrite idempotent.

    - nilai_partisi=None: seluruh isi tabel diganti (dimensi kecil).
    - nilai_partisi=[...]: hanya partisi itu yang diganti (fakta per tanggal); partisi lain utuh.
    Hapus + tulis terjadi dalam satu transaksi Iceberg, jadi pembaca tidak pernah melihat keadaan
    setengah jadi. Mengembalikan snapshot id baru.
    """
    data = _arrow(df)
    tbl = _pastikan(nama, data, partisi)
    data = data.select(tbl.schema().column_names) if set(tbl.schema().column_names) <= set(data.schema.names) \
        else data
    filt = AlwaysTrue() if nilai_partisi is None else In(partisi, [d.isoformat() for d in nilai_partisi])
    with tbl.transaction() as tx:
        tx.delete(filt)
        if data.num_rows:
            tx.append(data)
    tbl = muat(nama)
    snap = tbl.current_snapshot()
    log("info", "tabel ditulis", tabel=nama, baris=data.num_rows,
        partisi=[d.isoformat() for d in nilai_partisi] if nilai_partisi else "semua",
        snapshot_id=snap.snapshot_id if snap else None)
    return snap.snapshot_id if snap else None


def baca(nama: str, filt=None, kolom: tuple[str, ...] = ("*",)) -> pl.DataFrame:
    tbl = muat(nama)
    scan = tbl.scan(row_filter=filt or AlwaysTrue(), selected_fields=kolom)
    return pl.from_arrow(scan.to_arrow())
