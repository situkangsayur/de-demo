# Platform DE · bagian ops: aplikasi → NATS → bronze

Bagian **ops** platform end-to-end Data Engineering (contoh proyek UTS, slide P08 jalur A bank syariah
digital). Isinya jalur **ingestion event-driven**: data dari area operasional mengalir lewat
**NATS JetStream** ke lapisan **bronze** (Parquet di RustFS), siap diolah pipeline silver/gold
(bagian data, `compose.data.yaml`).

Aplikasi operasionalnya adalah aplikasi microservices mata kuliah Sistem Terdistribusi
(`sister-hpc-demo/bab05-microservices-messaging`: toko daring + layanan bank). Aplikasi itu dipakai
apa adanya lewat `include`; Data Engineering hanya menambahkan ingestion dan analytics.

| Konsep (buku/slide DE) | Wujud di demo |
|---|---|
| Ingestion event-driven, publish-subscribe (Bab 1, Bab 4) | aplikasi menerbitkan event bisnis ke stream `BANK`/`SHOP` lewat **transactional outbox**; ingestor DE hanyalah satu consumer lagi |
| Change Data Capture dari log database (Bab 4) | **Debezium Server** membaca WAL PostgreSQL (logical decoding `pgoutput`) dan menerbitkan envelope `before/after/op` ke stream `CDC`, tanpa Kafka |
| Delivery semantics, idempotency | durable consumer, **ack sesudah Parquet tertulis** (at-least-once), dedup `_event_id` per batch |
| Bronze + metadata lineage (W1) | `s3://lake/bronze/<sumber>/<entitas>/dt=…/part-<batch_id>.parquet` dengan `_ingested_at`, `_source`, `_batch_id`, `_event_id`, … |
| Partisi berdasarkan waktu kejadian, data terlambat, backfill (W7) | `dt` dari `occurred_at`/kolom waktu bisnis; data terlambat masuk partisi aslinya; mode backfill N hari |
| "Kotoran" data (W2, W8) | generator menyisipkan duplikat, nilai kosong, data terlambat, kode akad tidak valid, perubahan profil (SCD-2, W4) |
| Object storage S3-compatible | RustFS 1.0.4, bucket `lake` |

Skema tabel sumber, event, dan bronze: [`ops/SKEMA.md`](ops/SKEMA.md). Kontrak bersama:
[`KONTRAK-DATA.md`](KONTRAK-DATA.md).

## Arsitektur

```mermaid
flowchart LR
  subgraph sister["Aplikasi Sister bab05 (include, nama service asli)"]
    UIshop["UI toko :50500"] --> GW["APISIX :50501"]
    GEN["generator DE<br/>(ops-api)"] -->|REST + apikey| GW
    GW --> BANK["bank"]
    GW --> ORD["order x2"]
    ORD -.saga.-> PAY["payment"] -->|debit rekening| BANK
    BANK --> PG[("PostgreSQL 16<br/>wal_level=logical<br/>db bank, orders, ...")]
    ORD --> PG
  end
  subgraph nats["NATS 2.10 JetStream (milik Sister)"]
    SB(["stream BANK<br/>bank.&lt;entitas&gt;.&lt;aksi&gt;"])
    SS(["stream SHOP<br/>shop.*"])
    SC(["stream CDC<br/>cdc.&lt;db&gt;.&lt;tabel&gt;"])
  end
  BANK -. "outbox relay" .-> SB
  ORD -. "outbox relay" .-> SS
  PG -- "WAL (slot dbz_bank)" --> D1["debezium-bank"]
  PG -- "WAL (slot dbz_orders)" --> D2["debezium-orders"]
  D1 & D2 -- "sink nats-jetstream" --> SC
  SB & SS & SC -- "durable pull consumer<br/>de_bronze_*" --> ING["ingestor<br/>(Python, PyArrow)"]
  ING -- "Parquet zstd, ack sesudah tulis" --> RFS[("RustFS :52003<br/>s3://lake/bronze/...")]
  RFS --> DATA["bagian data (penulis B):<br/>Airflow, Iceberg silver/gold, ClickHouse"]
  OPSAPI["ops-api :52001<br/>stats, SSE live, sampel"] -. subscribe bank.> shop.> cdc.> .-> nats
  OPSAPI -. /stats .-> ING
  OPSUI["UI Aliran data :52000"] --> OPSAPI
```

Alur satu transaksi tarik tunai:

```mermaid
sequenceDiagram
  participant G as generator / UI toko
  participant B as service bank
  participant P as PostgreSQL (bank)
  participant N as NATS JetStream
  participant D as debezium-bank
  participant I as ingestor
  participant S as RustFS (bronze)
  G->>B: POST /api/bank/transaksi (lewat APISIX)
  B->>P: BEGIN; UPDATE rekening saldo; INSERT transaksi; INSERT outbox; COMMIT
  B-->>N: relay outbox → bank.transaksi.dibuat (Nats-Msg-Id = event_id)
  P-->>D: WAL: UPDATE rekening, INSERT transaksi
  D-->>N: cdc.bank.rekening (op=u), cdc.bank.transaksi (op=c)
  N-->>I: fetch (de_bronze_bank, de_bronze_cdc)
  Note over I: tampung 10 detik / 2000 pesan
  I->>S: tulis part-<batch_id>.parquet per (sumber, entitas, dt)
  I-->>N: ack (baru sekarang)
```

## Service dan image

| Service | Image | Peran |
|---|---|---|
| *(include)* `postgres`, `nats`, `apisix`, `bank`, `order-1/2`, `payment`, … | milik Sister bab05 | aplikasi operasional (lihat README bab05) |
| `rustfs` | `rustfs/rustfs:1.0.4` | object storage S3-compatible (volume `ops_rustfs`) |
| `rustfs-init` | `de-ops-ingestor:1.0` | membuat bucket `lake` (sekali jalan) |
| `nats-init` | `de-ops-go:1.0` (`opsctl init-streams`) | membuat stream `CDC` (`cdc.>`, file, 7 hari, 2 GiB) |
| `debezium-bank`, `debezium-orders` | `quay.io/debezium/server:3.6.3.Final` | CDC database `bank` dan `orders` → stream `CDC` (konfigurasi `ops/debezium/*.properties`) |
| `ingestor` | `de-ops-ingestor:1.0` (Python 3.12, nats-py 2.9.0, PyArrow 18.1.0) | BANK/SHOP/CDC → bronze Parquet; `/stats` untuk UI |
| `ops-api` | `de-ops-go:1.0` (Go 1.24, `opsctl serve`) | API aliran data (stats, SSE, sampel pesan) + generator lalu lintas |
| `ops-ui` | `de-ops-ui:1.0` (Next.js 15 standalone) | UI "Aliran data" dan "Generator lalu lintas" |

## Port (`${BIND_ADDR}`, default 127.0.0.1)

| Port | Isi |
|---|---|
| 52000 | UI Aliran data + generator |
| 52001 | API ops (`/api/flow/stats`, `/api/flow/live` SSE, `/api/flow/sample?subject=`, `/api/gen/*`) |
| 52003 | RustFS S3 API |
| 52004 | RustFS console (login dengan `S3_ACCESS_KEY`/`S3_SECRET_KEY`) |
| 50500–50506 | aplikasi Sister (UI toko, APISIX, nginx, NATS monitoring 50503, NATS client 50504, OpenSearch, Postgres 50506) |

## Menjalankan bagian ops saja

Dua repo di-clone berdampingan:

```
<induk>/sister-hpc-demo/
<induk>/de-demo/
```

```bash
cd de-demo/platform
cp ops/.env.ops.example .env      # sekali; server lab: BIND_ADDR=10.100.21.22
# SISTER_DIR relatif terhadap folder platform/: ../../sister-hpc-demo (default)
docker compose -f compose.ops.yaml up -d --build --wait
# buka http://127.0.0.1:52000
```

Bila `.env` platform sudah dibuat koordinator (gabungan ops + data), langkah `cp` dilewati.
Start pertama 2–3 menit (build image Sister dan DE, OpenSearch membentuk klaster, Debezium snapshot
awal). Memori sekitar 4 GB (aplikasi Sister ±2,5 GB, dua Debezium ±0,6 GB masing-masing).

Platform lengkap (ops + data) dijalankan koordinator: `docker compose up -d --build` di folder
`platform/` (`compose.yaml` meng-include `compose.ops.yaml` dan `compose.data.yaml`).

Uji otomatis bagian ops (project terpisah `de-ops-uji`, diakhiri `down -v`):

```bash
bash ops/uji-ops.sh
```

## Skenario demo di kelas

Buka dua tab di proyektor: **UI Aliran data** (`:52000`) dan **UI toko Sister** (`:50500`).

1. **Satu transaksi, dua jalur (±5 menit).** Di UI toko, pilih produk, bayar dari rekening contoh
   (mis. milik Aisyah). Di UI Aliran data amati titik biru bergerak (event `shop.order.created`,
   `bank.transaksi.dibuat`) dan titik oranye (`cdc.bank.rekening` op `u`, `cdc.bank.transaksi` op
   `c`, `cdc.orders.orders`). Kaitan teori: dua cara mengambil data dari sistem operasional:
   event yang **sengaja dipublikasikan** aplikasi (punya makna bisnis, bentuk ditentukan pemilik
   aplikasi) dan **log perubahan database** (lengkap, tanpa mengubah aplikasi, tetapi berbentuk
   baris tabel).
2. **Envelope Debezium (±5 menit).** Klik baris `cdc.bank.nasabah` atau `cdc.bank.rekening`. Tunjukkan
   `op` (c/u/d/r), `before` vs `after`, `source.lsn`, `txId`, `ts_ms`. Ubah profil nasabah di UI toko
   (tab bank) lalu muat ulang contoh `cdc.bank.nasabah`: `op=u` dengan `before` = null, karena tabel
   memakai `REPLICA IDENTITY DEFAULT` (WAL hanya membawa nilai baru). Diskusikan: dengan `FULL` nilai
   lama ikut terkirim (kolom yang berubah ditandai di UI), dengan biaya WAL lebih besar. Snapshot awal
   (`op=r`) terlihat pada `cdc.bank.cabang`.
3. **Generator dan kotoran (±7 menit).** Buka halaman Generator, klik *Mulai* dengan laju 5/detik.
   Naikkan *Duplikat* ke 20% dan *Kode akad tidak valid* ke 30%. Kembali ke Aliran data: laju msg/s
   naik, feed bergerak. Di tabel partisi bronze, perhatikan `dt` hari ini dan beberapa `dt` kemarin
   (data terlambat, `occurred_at` mundur). Kaitan: kotoran ini yang harus dibersihkan silver (W2) dan
   ditangkap data quality test (W8).
4. **Lag dan ack sesudah tulis (±5 menit).** Hentikan ingestor: `docker compose stop ingestor`.
   Generator tetap jalan: jumlah pesan di stream naik, kolom *lag* consumer `de_bronze_*` naik, bronze
   berhenti bertambah, dan aplikasi **tidak terganggu** (decoupling). Nyalakan lagi:
   `docker compose start ingestor`; lag turun ke 0 dan file baru muncul. Tidak ada data yang hilang
   karena ack baru dikirim sesudah file tertulis.
5. **Debezium mati dan menyusul (±3 menit).** `docker compose stop debezium-bank`, lakukan beberapa
   transaksi, lalu `start` lagi. Debezium melanjutkan dari offset (LSN) terakhir; slot replikasi di
   Postgres menahan WAL selama Debezium mati (lihat `SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots;`).
6. **Backfill (±5 menit).** Di Generator isi *Hari* 30, *Aksi per hari* 100, klik *Jalankan backfill*.
   Partisi `dt` 30 hari ke belakang terisi di bronze. Lonjakan tidak wajar di satu cabang pada dua
   hari acak menjadi bahan pertanyaan direksi no. 4. Untuk uji W7, jalankan pipeline penulis B dua
   kali pada 7 partisi yang sama dan tunjukkan jumlah baris silver tidak berubah.
7. **Lihat file Parquet.** Console RustFS (`:52004`) → bucket `lake` → `bronze/…`. Atau baca langsung:

   ```bash
   docker compose exec ingestor python -c "
   import pyarrow.parquet as pq; from ingestor.common import s3fs
   t = pq.read_table('lake/bronze/cdc/nasabah', filesystem=s3fs()); print(t.schema); print(t.num_rows)"
   ```

## Generator lalu lintas

`ops-api` memanggil API aplikasi lewat APISIX (`/api/bank/*`, `/api/orders`, header `apikey`), seperti
klien sungguhan dari 8 cabang dan kanal mobile/atm/web/teller (+ ecommerce untuk belanja).

| Aksi | Bobot | Catatan |
|---|---|---|
| setor / tarik / transfer | 70 | nominal log-normal; tarik ATM kelipatan 50.000; sebagian besar di cabang asal nasabah |
| belanja toko dibayar dari rekening | 10 | `POST /api/orders` dengan `no_rekening` |
| nasabah baru + rekening + setoran awal | 4 | 20% nasabah baru sengaja tidak pernah bertransaksi (pertanyaan direksi no. 3) |
| ubah profil (kota/segmen/no_hp) | 2 + 60×p | bahan SCD-2 |
| ajukan akad / proses akad (setujui, tolak, cairkan, angsuran) | 3 / 4 | 15% pengajuan ditolak |

Kotoran (peluang dapat diatur dari UI): **duplikat** (request dikirim ulang tanpa `Idempotency-Key`),
**nilai kosong** (`cabang_id`, `email`, `no_hp`), **data terlambat** (`occurred_at` mundur 1–72 jam),
**kode akad tidak valid** (`mrb`, `MRBH`, `Murabahah`, `XYZ`, kosong, …), **perubahan profil**.
**Lonjakan** dapat dipicu manual per cabang (mode live) dan otomatis pada dua hari acak (backfill).

Backfill: `POST /api/gen/backfill {"days":30,"per_day":200,"with_shop":true}` (maksimal 800 hari).
Mulai kecil: 30 hari × 200 aksi ≈ 6 ribu aksi ≈ 25 ribu pesan NATS ≈ beberapa MB bronze. 13 bulan ×
500 aksi ≈ 200 ribu aksi; perhatikan retensi stream (BANK 7 hari/512 MB, CDC 7 hari/2 GiB) dan sisa disk.

## Log

Semua service menulis log JSON satu baris (`time`, `level`, `msg`, `service`, `request_id`/`batch_id`).
Debezium memakai `quarkus.log.console.json` (field `timestamp`, `level`, `message`, `loggerName`). Di Grafana (`:52900`, `shared/observability`) pilih
`compose_project=de-platform` lalu `compose_service` `ingestor`, `ops-api`, `debezium-bank`, ….
Tanpa Grafana: `docker compose logs -f ingestor ops-api`.

## Pembersihan

```bash
docker compose -f compose.ops.yaml down -v   # hapus container + volume (Postgres, NATS, RustFS, offset Debezium)
docker builder prune -f
```

## Hal yang perlu diputuskan (catatan penulis A)

- **REPLICA IDENTITY FULL** untuk `nasabah` (dan sebaiknya `rekening`, `akad_pembiayaan`) di database
  `bank`, supaya `before` terisi pada update (demo SCD-2 lebih jelas). Ini perubahan di aplikasi Sister.
- **occurred_at pada pembuatan nasabah/rekening/akad dan PATCH nasabah.** Saat ini hanya transaksi,
  transfer, dan aksi akad yang menerima `occurred_at`; backfill membuat nasabah/rekening/akad dengan
  waktu sekarang sehingga transaksi historis bisa lebih tua daripada nasabahnya.
- Order toko tidak menerima `occurred_at`, jadi belanja selalu bertanggal hari ini (backfill dapat
  mematikannya dengan `with_shop=false`).
