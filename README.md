# de-demo

Demo kelas mata kuliah **Data Engineering** (Program Studi Teknik Informatika STMIK Tazkia,
pengampu Hendri Karisma). Setiap demo punya UI/visualisasi. Potongan kode buku ajar ada di repo
terpisah, [`sample-code-buku`](https://github.com/situkangsayur/sample-code-buku).

Inti repo ini adalah **platform end-to-end** yang juga menjadi contoh proyek UTS: aplikasi
operasional → event ke NATS (dari aplikasi dan dari CDC Debezium) → Airflow menjalankan pipeline ELT
dan ETL → data lake Iceberg di RustFS dan data warehouse ClickHouse → dashboard yang membaca keduanya.
Lab per pertemuan (P01–P07) memakai data dari platform yang sama.

Konvensi: [`docs/KONVENSI.md`](docs/KONVENSI.md). Tahap 1: sampai UTS.

## Isi

| Pertemuan | Folder | Isi | UI |
|---|---|---|---|
| UTS (P08) | [`platform/`](platform/README.md) | aplikasi bank + toko (repo `sister-hpc-demo` Bab 5) → NATS + Debezium → bronze RustFS → Airflow ETL (Iceberg) dan ELT (ClickHouse + dbt) → gerbang data quality → dashboard lake vs warehouse, `fitur_nasabah` | 52000, 52010, 52012 |
| P01 | [`bab01-data-engineering-101/`](bab01-data-engineering-101/README.md) | siklus hidup data: satu transaksi dari aplikasi → CDC → bronze → silver → gold → dashboard, latensi, data kotor, hot vs cold path | 52100 |
| P02 | [`bab02-pemodelan-data-dan-format/`](bab02-pemodelan-data-dan-format/README.md) | OLTP vs star vs OBT, SCD 1/2, CSV/JSON/Avro/Parquet, pruning dan pushdown, schema evolution | 52200 |
| P03 | [`bab03-storage-skala-besar-dan-tiering/`](bab03-storage-skala-besar-dan-tiering/README.md) | RustFS: object storage, partition, small files, kompresi, replication vs erasure coding, lifecycle, cost tiering | 52300 |
| P04 | [`bab04-lakehouse-medallion/`](bab04-lakehouse-medallion/README.md) | medallion + Iceberg: pohon metadata, time travel, schema/partition evolution, MERGE, compaction | 52400 |
| P05 | [`bab05-etl-satu-mesin/`](bab05-etl-satu-mesin/README.md) | benchmark pandas/Polars/DuckDB/DataFusion, query plan, zero-copy Arrow, batas memori dan spill, pipeline idempotent | 52500 |
| P06 | [`bab06-orkestrasi-dbt-data-quality/`](bab06-orkestrasi-dbt-data-quality/README.md) | Airflow 3 (sensor, retry/backoff, idempotency, backfill, SLA), dbt + test enam dimensi, write-audit-publish | 52600 |
| P07 | [`bab07-storage-engine-nosql-serving/`](bab07-storage-engine-nosql-serving/README.md) | PostgreSQL/ClickHouse/Redis/OpenSearch 2 node: pola akses, B-tree vs LSM, row vs columnar, serving fitur, search | 52700 |

Setiap lab berjalan sendiri (`cp .env.example .env && docker compose up -d --build`; uji `bash uji.sh`) dan dapat
tersambung ke platform bila platform menyala. Log: `shared/observability` (Grafana 52900).
