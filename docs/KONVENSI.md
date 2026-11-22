# Konvensi repo `de-demo`

Demo kelas untuk mata kuliah **Data Engineering** (STMIK Tazkia, Hendri Karisma). Berbeda dari
`sample-code-buku` dan dari proyek per bab di `~/stmik-codes/data-eng`: isinya demo yang dijalankan di
kelas dan **selalu punya UI/visualisasi**. Tahap 1: sampai UTS (P01–P07).

## Gagasan besar (permintaan pengampu, 23 Nov 2026)

Satu **platform end-to-end** yang juga menjadi **contoh proyek UTS** (slide P08):

1. **Area operasional:** aplikasi transaksi (Go + Next.js) dengan database OLTP PostgreSQL.
2. **Ingestion dua jalur, keduanya event-driven lewat NATS JetStream:**
   - aplikasi mem-*publish* event bisnis ke NATS;
   - log perubahan database (WAL) ditangkap **Debezium** (CDC) dan dikirim ke NATS (Debezium Server,
     sink NATS JetStream; tanpa Kafka).
3. **Orkestrasi utama: Apache Airflow** dengan dua pola pipeline:
   - **ELT:** data mentah dimuat dulu ke warehouse, lalu ditransformasi di warehouse (dbt);
   - **ETL:** data ditransformasi dulu (Polars/DuckDB/Arrow), lalu dimuat.
4. **Data lake:** Apache Iceberg di object storage **RustFS** (S3-compatible), lapisan medallion
   bronze/silver/gold.
5. **Data warehouse:** **ClickHouse** (model dimensional / star schema).
6. **Dashboard app** (Next.js + API Go) yang membaca dari lake (DuckDB atas Iceberg) dan dari warehouse.

Setiap lab pertemuan (P01–P07) memakai data dari platform yang sama.

## Stack

| Kebutuhan | Pilihan |
|---|---|
| Aplikasi operasional / API | Go 1.24 (`golang:1.24-alpine` → `alpine:3.20`) |
| UI | Next.js (App Router, TypeScript, `output: "standalone"`, `node:20-alpine`) |
| Message broker | NATS 2.10 + JetStream |
| CDC | Debezium Server (sink NATS JetStream), PostgreSQL logical replication |
| Orkestrasi | Apache Airflow (versi 2.x/3.x dipin; LocalExecutor, tanpa Celery bila cukup) |
| Transformasi | Python: Polars, DuckDB, PyArrow, PyIceberg; dbt (adapter ClickHouse dan/atau DuckDB) |
| Object storage | RustFS (S3-compatible) |
| Table format | Apache Iceberg (catalog REST atau SQL catalog di PostgreSQL) |
| Warehouse | ClickHouse |
| Database OLTP | PostgreSQL 16 |
| Logging | Grafana + Loki + Alloy di `shared/observability` (port 52900–52909) |
| Orkestrasi container | Docker Compose |

## Struktur

```
platform/          # platform end-to-end = contoh proyek UTS (port 52000–52099)
babNN-slug/        # lab per pertemuan, memakai platform (port 52000 + NN*100 + k)
shared/observability/
docs/KONVENSI.md
```

Slug lab mengikuti buku: `bab01-data-engineering-101`, `bab02-pemodelan-data-dan-format`,
`bab03-storage-skala-besar-dan-tiering`, `bab04-lakehouse-medallion`, `bab05-etl-satu-mesin`,
`bab06-orkestrasi-dbt-data-quality`, `bab07-storage-engine-nosql-serving`.

Setiap folder: `README.md` (bahasa Indonesia: tujuan, kaitan bab dan slide, diagram mermaid, cara
menjalankan, **skenario demo di kelas**, port, log, pembersihan), `compose.yaml` (`name: de-...`),
`.env.example`, `uji.sh` (up, cek, `down -v` dengan trap).

## Port

- Semua port host diikat ke `${BIND_ADDR}` (default `127.0.0.1`; server lab `10.100.21.22`).
- `platform`: 52000–52099. Lab bab NN: `52000 + NN*100 + k` (Bab 2 → 52200–52299). Observability:
  52900 (Grafana), 52901 (Loki), 52902 (Alloy).

## Disk, keamanan, gaya, uji

Sama dengan repo `sister-hpc-demo`:

- Sisa disk server lab terbatas (±30 GB). Cek `df -h /` sebelum build; tunggu bila < 8 GB. Image
  dipin, tanpa `latest`. Tanpa `node_modules` di host. Sesudah uji: `docker compose down -v`,
  `docker builder prune -f`. Dilarang `docker system/volume/image prune -a` atau menyentuh resource
  proyek lain.
- Tidak ada password/token asli di repo; nilai contoh di `.env.example`. Jangan membuka berkas
  kredensial di luar folder sendiri.
- Log service JSON satu baris (`time`, `level`, `msg`, `service`, `request_id`/`event_id`).
- README dan komentar bahasa Indonesia, istilah teknis bentuk asli.
- `uji.sh` lolos dari keadaan bersih.
