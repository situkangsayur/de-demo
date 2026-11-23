"""data-init: menyiapkan sisi data sekali saat platform menyala (idempotent).

1. menunggu bucket lake (dibuat penulis A, atau RustFS dev);
2. namespace Iceberg bronze/silver/gold/staging di katalog SQL PostgreSQL;
3. database ClickHouse raw/stg/gold/meta beserta tabel meta;
4. lifecycle rule bucket (W9) dan tabel cost 3 tahun.
"""
from __future__ import annotations

import time

from . import cost, iceberg_util, tiering
from .konfigurasi import BUCKET, boto, clickhouse, log

DDL = [
    "CREATE DATABASE IF NOT EXISTS raw",
    "CREATE DATABASE IF NOT EXISTS stg",
    "CREATE DATABASE IF NOT EXISTS gold",
    "CREATE DATABASE IF NOT EXISTS meta",
    """CREATE TABLE IF NOT EXISTS meta.dq_hasil (
        waktu DateTime64(3, 'UTC'), run_id String, jalur LowCardinality(String), lapisan LowCardinality(String),
        tes String, dimensi LowCardinality(String), aturan String, status LowCardinality(String),
        keparahan LowCardinality(String), gagal UInt64, total UInt64, nilai Nullable(Float64), contoh String,
        tgl_awal Date, tgl_akhir Date
    ) ENGINE = MergeTree ORDER BY (waktu, jalur, tes) TTL toDateTime(waktu) + INTERVAL 90 DAY""",
    """CREATE TABLE IF NOT EXISTS meta.cost_bulanan (
        skenario LowCardinality(String), laju LowCardinality(String), bulan UInt8, warm_gb Float64, cold_gb Float64,
        archive_gb Float64, simpan_usd Float64, ambil_usd Float64, egress_usd Float64, total_usd Float64,
        kumulatif_usd Float64
    ) ENGINE = MergeTree ORDER BY (skenario, laju, bulan)""",
    """CREATE TABLE IF NOT EXISTS meta.cost_harga (
        kunci String, deskripsi String, usagetype_aws String, usd Float64, tanggal_akses Date
    ) ENGINE = MergeTree ORDER BY kunci""",
    """CREATE TABLE IF NOT EXISTS meta.tiering_status (
        waktu DateTime64(3, 'UTC'), mode String, pesan String
    ) ENGINE = MergeTree ORDER BY waktu""",
]


def tunggu_bucket(batas_detik: int = 300) -> None:
    s3 = boto()
    t0 = time.time()
    while True:
        try:
            s3.head_bucket(Bucket=BUCKET)
            return
        except Exception as e:
            if time.time() - t0 > batas_detik:
                raise SystemExit(f"bucket {BUCKET} tidak tersedia: {e}")
            log("info", "menunggu bucket", bucket=BUCKET)
            time.sleep(3)


def main() -> None:
    tunggu_bucket()
    iceberg_util.siapkan_namespace()
    ch = clickhouse()
    for q in DDL:
        ch.command(q)
    st = tiering.pasang()
    ch.insert("meta.tiering_status", [[__import__("datetime").datetime.now(__import__("datetime").timezone.utc).replace(tzinfo=None), st["mode"], st["pesan_transisi"] or ""]],
              column_names=["waktu", "mode", "pesan"])
    cost.simpan_clickhouse(cost.semua())
    log("info", "data-init selesai", lifecycle=st["mode"])


if __name__ == "__main__":
    main()
