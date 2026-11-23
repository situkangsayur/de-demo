# Kontrak data platform (revisi 23 Nov 2026)

Keputusan pengampu: **aplikasi operasional adalah aplikasi microservices mata kuliah Sistem Terdistribusi**
(`sister-hpc-demo/bab05-microservices-messaging`, toko daring), **diperluas dengan layanan bank**
(rekening, saldo, transfer, pembiayaan syariah) sehingga menjadi **bank + e-commerce**. Sampai UTS,
aplikasi ditangani di mata kuliah Sister; mata kuliah Data Engineering menangani **pipeline dan
analytics** untuk mendukung bisnis dan tim AI. Dua repo di-clone berdampingan:

```
<induk>/sister-hpc-demo/   # aplikasi (Sister)
<induk>/de-demo/           # pipeline + analytics (DE); SISTER_DIR=../sister-hpc-demo di .env
```

Domain analitik contoh UTS tetap **jalur A bank syariah digital** (slide P08), dengan data e-commerce
sebagai sumber tambahan.

## Pembagian kerja

| Bagian | Pemilik | Berkas |
|---|---|---|
| Layanan bank di aplikasi Sister + `wal_level=logical` + database `bank` | penulis S (repo sister-hpc-demo) | `sister-hpc-demo/bab05-microservices-messaging/` |
| Integrasi + ingestion DE | penulis A | `de-demo/platform/compose.ops.yaml`, `de-demo/platform/ops/` |
| Pipeline + serving DE | penulis B | `de-demo/platform/compose.data.yaml`, `de-demo/platform/data/` |
| `platform/compose.yaml`, kontrak ini | koordinator | |

`platform/compose.yaml` (`name: de-platform`) meng-include `${SISTER_DIR}/bab05-microservices-messaging/compose.yaml`
(lewat `compose.ops.yaml`), `compose.ops.yaml`, dan `compose.data.yaml`, sehingga semua service berada di
satu project dan satu network. Service Sister dipakai dengan nama aslinya (`postgres`, `nats`, `apisix`, ...).

## Layanan bank di aplikasi Sister (penulis S)

Service Go baru `bank` (satu image yang sama dengan service lain bab05), di belakang APISIX
(`/api/bank/*`, key-auth), database PostgreSQL `bank` (instance `postgres` yang sama), outbox + publish
ke NATS JetStream.

Tabel (schema `public` di database `bank`), kolom minimal:

- `cabang` (`id`, `kode`, `nama`, `kota`, `wilayah`)
- `nasabah` (`id`, `cif`, `nama`, `nik`, `no_hp`, `email`, `kota`, `segmen`, `cabang_id`, `created_at`,
  `updated_at`) — PII: `nik`, `no_hp`, `email`; `kota`/`segmen` dapat berubah (bahan SCD-2)
- `rekening` (`id`, `no_rekening`, `nasabah_id`, `jenis` (`tabungan_wadiah`, `giro`, `deposito_mudharabah`),
  `saldo`, `status`, `dibuka_at`)
- `transaksi` (grain satu transaksi: `id`, `rekening_id`, `cabang_id`, `kanal` (`mobile`, `atm`, `teller`,
  `web`, `ecommerce`), `jenis` (`setor`, `tarik`, `transfer_masuk`, `transfer_keluar`, `bayar_belanja`,
  `angsuran`), `nominal`, `occurred_at` (waktu kejadian di perangkat; boleh mundur = data terlambat),
  `created_at`, `ref_order_id` (null bila bukan belanja))
- `akad_pembiayaan` (`id`, `nasabah_id`, `kode_akad` (`MRB` murabahah, `MSY` musyarakah, `MDH` mudharabah,
  `IJR` ijarah; kode tidak valid boleh tersimpan apa adanya), `pokok`, `margin`, `tenor_bulan`, `status`
  (`diajukan`, `disetujui`, `cair`, `lunas`, `ditolak`), `cair_at`)
- `outbox` (pola sama dengan service lain bab05)

Integrasi e-commerce: checkout toko dapat dibayar dari rekening bank (payment service memanggil bank;
transaksi `bayar_belanja` dengan `ref_order_id`).

Event NATS JetStream: stream **`BANK`**, subject `bank.<entitas>.<aksi>` (mis. `bank.transaksi.dibuat`,
`bank.nasabah.diperbarui`, `bank.akad.dicairkan`), payload JSON:
`{"event_id": UUID, "event_type": "...", "occurred_at": RFC3339, "data": {...baris entitas...}}`,
header `Nats-Msg-Id = event_id`. Stream `SHOP` milik toko tetap seperti sekarang.

PostgreSQL bab05: `wal_level=logical`, `max_replication_slots`/`max_wal_senders` cukup untuk dua slot
Debezium (database `bank` dan `orders`), user replikasi atau hak `REPLICATION` untuk Debezium (password dari
`.env`). Perubahan ini tidak boleh mengganggu demo Sister (uji.sh bab05 tetap lolos).

## Ingestion DE (penulis A)

- **Debezium Server** (`quay.io/debezium/server:3.6.3.Final`, sink `nats-jetstream`), dua instance: database
  `bank` (tabel `nasabah`, `rekening`, `transaksi`, `akad_pembiayaan`, `cabang`) dan `orders`. Stream
  **`CDC`**, subject `cdc.<database>.<tabel>` (atur `topic.prefix` dan routing agar menjadi
  `cdc.bank.transaksi`, `cdc.orders.orders`, dst.).
- **Ingestor** membaca stream `BANK`, `SHOP`, dan `CDC` (durable consumer, ack sesudah tulis) dan menulis bronze.
- **Generator lalu lintas** (milik DE): memanggil API aplikasi lewat APISIX (nasabah baru, transaksi berbagai
  cabang dan kanal, belanja toko dibayar dari rekening, pengajuan dan pencairan akad, perubahan profil) dan
  menyisipkan "kotoran" sesuai slide P08: duplikat (request dikirim ulang tanpa idempotency key), nilai
  kosong, `occurred_at` mundur (data terlambat), kode akad tidak valid. Mode backfill: membangkitkan riwayat
  N hari (default 30) dengan `occurred_at` historis.
- **RustFS** (`rustfs/rustfs:1.0.4`) + bucket `lake`.
- UI "Aliran data" (Next.js) menampilkan aliran app → NATS (BANK/SHOP) dan DB → Debezium → NATS (CDC) →
  bronze secara live.

## Bronze (RustFS, bucket `lake`)

- Path: `s3://lake/bronze/<sumber>/<entitas>/dt=YYYY-MM-DD/part-<batch_id>.parquet`;
  `<sumber>` = `app` (event BANK/SHOP) atau `cdc` (Debezium). Entitas bank: `nasabah`, `rekening`, `transaksi`,
  `akad_pembiayaan`, `cabang`; entitas toko: `orders`, `order_items` (bila ada), `payment`, `product`.
- Kolom metadata (W1): `_ingested_at` (UTC), `_source` (`app`/`cdc`), `_batch_id`, `_event_id` (app: dari
  event; cdc: `<tabel>:<lsn>:<pk>`), `_event_type`, `_occurred_at`, `_ts_ms`, `_op` (cdc: `c`/`u`/`d`/`r`).
- Kolom data diratakan; boleh semua bertipe string (casting di silver, W2).
- `dt` = tanggal UTC dari waktu bisnis (`occurred_at`; untuk cdc pakai kolom waktu bisnis tabel bila ada,
  selain itu `ts_ms`), sehingga data terlambat dan backfill masuk ke partisi tanggal aslinya.

## Lapisan berikutnya (penulis B)

- silver Iceberg `lake.silver.<entitas>` (tipe benar, dedup termasuk app vs cdc, standardisasi kode,
  karantina kode akad tidak valid, tokenization PII).
- gold star schema `fakta_transaksi` + `dim_tanggal`, `dim_cabang`, `dim_kanal`, `dim_nasabah` (SCD-2),
  `dim_akad`; di lake (Iceberg `lake.gold.*`) dan di warehouse (ClickHouse `gold`).
- **Untuk tim AI**: `gold.fitur_nasabah` (fitur per nasabah per tanggal: frekuensi dan nominal transaksi 7/30
  hari, recency, rasio kanal, belanja e-commerce, status pembiayaan), dibuat point-in-time agar aman untuk
  training (tanpa data dari masa depan).
- Dashboard bisnis menjawab empat pertanyaan direksi (slide P08) dari lake dan dari warehouse.

## Port

| Port | Service | Pemilik |
|---|---|---|
| 50500–50506 | aplikasi Sister bab05 (UI toko, APISIX, nginx, NATS, OpenSearch, Postgres) | S |
| 52000 | UI "Aliran data" + generator lalu lintas | A |
| 52001 | API generator/aliran | A |
| 52003 / 52004 | RustFS API / console | A |
| 52010 | Airflow UI | B |
| 52011 | ClickHouse HTTP | B |
| 52012 / 52013 | UI / API dashboard | B |
| 52014 | katalog Iceberg (bila REST) | B |
| 52900 | Grafana (shared/observability) | koordinator |
