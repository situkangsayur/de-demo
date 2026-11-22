# Kontrak data platform (dipakai penulis A dan B)

Domain: **bank syariah digital (sintetis)**, mengikuti skenario jalur A slide P08 UTS.

## Pembagian berkas

- `platform/compose.yaml` (milik koordinator): `name: de-platform` dan `include:` dua berkas di bawah.
- `platform/compose.ops.yaml` + `platform/ops/` (penulis A): PostgreSQL OLTP, aplikasi operasional (Go +
  Next.js), simulator, NATS JetStream, Debezium Server, ingestor ke bronze, **RustFS** (beserta pembuatan
  bucket `lake`).
- `platform/compose.data.yaml` + `platform/data/` (penulis B): Airflow (+ PostgreSQL metadata sendiri),
  katalog Iceberg, ClickHouse, dbt, dashboard (Go + Next.js).
- Network bersama: `de-platform_default` (otomatis dari project yang sama). Nama service unik per penulis.

## Port (${BIND_ADDR})

| Port | Service | Pemilik |
|---|---|---|
| 52000 | UI aplikasi operasional (core banking + simulator + aliran event live) | A |
| 52001 | API aplikasi operasional | A |
| 52002 | NATS monitoring | A |
| 52003 | RustFS S3 API | A |
| 52004 | RustFS console | A |
| 52005 | PostgreSQL OLTP | A |
| 52010 | Airflow UI | B |
| 52011 | ClickHouse HTTP | B |
| 52012 | UI dashboard | B |
| 52013 | API dashboard | B |
| 52014 | katalog Iceberg (REST, bila dipakai) | B |

## Entitas OLTP (PostgreSQL, schema `core`)

`cabang`, `nasabah` (berisi PII: `nik`, `no_hp`; profil bisa berubah → bahan SCD-2), `rekening`,
`akad_pembiayaan` (kode akad: `MRB` murabahah, `MSY` musyarakah, `MDH` mudharabah, `IJR` ijarah; kode
tidak valid sengaja muncul sesekali), `transaksi` (grain: satu transaksi; kolom minimal `id`,
`rekening_id`, `cabang_id`, `kanal` (`mobile`, `atm`, `teller`, `web`), `jenis`, `nominal`, `waktu`).
Penulis A menetapkan kolom lengkap dan menuliskannya di `platform/ops/SKEMA.md`.

## Event di NATS JetStream

- Stream `CORE_EVT`, subject `evt.<entitas>.<aksi>` (mis. `evt.transaksi.dibuat`,
  `evt.nasabah.diperbarui`): event bisnis yang dipublikasikan **aplikasi** (outbox atau langsung).
- Stream `CORE_CDC`, subject `cdc.core.<tabel>`: perubahan dari WAL lewat **Debezium Server** (sink NATS
  JetStream), format JSON envelope Debezium (`before`, `after`, `op`, `ts_ms`, `source`).
- Payload event aplikasi: JSON dengan `event_id` (UUID), `event_type`, `occurred_at` (RFC3339), `data`.

## Bronze (RustFS, bucket `lake`)

- Path: `s3://lake/bronze/<sumber>/<entitas>/dt=YYYY-MM-DD/part-<batch_id>.parquet`,
  `<sumber>` = `app` (event aplikasi) atau `cdc` (Debezium).
- Kolom metadata wajib (W1): `_ingested_at` (timestamp UTC), `_source` (`app`/`cdc`), `_batch_id`,
  `_event_id`, `_op` (untuk cdc: `c`/`u`/`d`/`r`), lalu kolom data entitas yang sudah diratakan.
- Partisi `dt` = tanggal kejadian (`occurred_at`/`ts_ms`), sehingga data terlambat masuk ke partisi
  tanggal aslinya.
- Ingestor menulis batch tiap N detik atau M event (mana yang lebih dulu) dan idempotent terhadap
  `event_id` yang sama dalam satu batch.
- Kredensial RustFS dari `.env` (contoh di `.env.example`): `S3_ENDPOINT=http://rustfs:9000`,
  `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET=lake`.

## Lapisan berikutnya (penulis B)

- silver: Iceberg `lake.silver.<entitas>` (tipe benar, dedup, standardisasi kode, tokenization PII).
- gold: star schema `fakta_transaksi` + `dim_tanggal`, `dim_cabang`, `dim_kanal`, `dim_nasabah` (SCD-2),
  `dim_akad`; di lake (Iceberg `lake.gold.*`) dan di warehouse (ClickHouse database `gold`).
