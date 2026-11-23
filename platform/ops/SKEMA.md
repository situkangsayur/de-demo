# Skema sumber, event, dan bronze (bagian ops)

Dokumen ini merangkum **apa yang masuk ke bronze** dan dari mana asalnya. Tabel OLTP dimiliki aplikasi
Sister bab05 (penulis S, `sister-hpc-demo/bab05-microservices-messaging/services/bank/store.go` dan
`services/order/main.go`); bila berbeda, kode itulah yang benar dan dokumen ini perlu diperbarui.
Kontrak bersama: [`../KONTRAK-DATA.md`](../KONTRAK-DATA.md).

## 1. Tabel OLTP (PostgreSQL 16 milik Sister, `wal_level=logical`)

### Database `bank`, schema `public` (publication `dbz_bank`, slot `dbz_bank`)

| Tabel | Kolom (tipe) | Catatan |
|---|---|---|
| `cabang` | `id` INT PK, `kode` TEXT UNIQUE, `nama` TEXT, `kota` TEXT, `wilayah` TEXT | seed 8 cabang (JKT-SDR … MKS-01) |
| `nasabah` | `id` UUID PK, `cif` TEXT UNIQUE, `nama` TEXT, `nik` TEXT null, `no_hp` TEXT null, `email` TEXT null, `kota` TEXT null, `segmen` TEXT (`ritel`/`prioritas`/`umkm`), `cabang_id` INT null FK, `created_at` TIMESTAMPTZ, `updated_at` TIMESTAMPTZ | PII: `nik`, `no_hp`, `email`. `kota`/`segmen`/`no_hp` berubah lewat PATCH → SCD-2 |
| `rekening` | `id` UUID PK, `no_rekening` TEXT UNIQUE, `nasabah_id` UUID FK, `jenis` TEXT (`tabungan_wadiah`/`giro`/`deposito_mudharabah`), `saldo` BIGINT ≥ 0, `status` TEXT (`aktif`/`diblokir`/`ditutup`), `cabang_id` INT null, `dibuka_at` TIMESTAMPTZ, `updated_at` TIMESTAMPTZ | setiap transaksi meng-UPDATE `saldo` → banyak event CDC `op=u` |
| `akad_pembiayaan` | `id` UUID PK, `nasabah_id` UUID FK, `rekening_id` UUID null, `kode_akad` TEXT (bebas), `pokok` BIGINT, `margin` BIGINT, `tenor_bulan` INT, `status` TEXT (`diajukan`/`disetujui`/`cair`/`lunas`/`ditolak`), `terbayar` BIGINT, `diajukan_at` TIMESTAMPTZ, `cair_at` TIMESTAMPTZ null, `updated_at` TIMESTAMPTZ | kode tidak valid (`mrb`, `MRBH`, `XYZ`, …) **disimpan apa adanya** |
| `transaksi` | `id` UUID PK, `rekening_id` UUID FK, `cabang_id` INT null, `kanal` TEXT (`mobile`/`atm`/`teller`/`web`/`ecommerce`), `jenis` TEXT (`setor`/`tarik`/`transfer_masuk`/`transfer_keluar`/`bayar_belanja`/`angsuran`), `nominal` BIGINT > 0, `saldo_setelah` BIGINT, `occurred_at` TIMESTAMPTZ, `created_at` TIMESTAMPTZ, `ref_order_id` TEXT null, `transfer_id` UUID null, `akad_id` UUID null, `idempotency_key` TEXT UNIQUE null, `keterangan` TEXT, `request_id` TEXT | grain fakta: satu baris. `occurred_at` boleh mundur (data terlambat). Transfer = dua baris dengan `transfer_id` sama |
| `outbox` | `id` UUID, `subject`, `payload` JSONB, `request_id`, `created_at`, `sent_at` | **tidak** di-CDC (sudah terwakili stream BANK) |

### Database `orders`, schema `public` (publication `dbz_orders`, slot `dbz_orders`)

| Tabel | Kolom | Catatan |
|---|---|---|
| `orders` | `id` TEXT PK (`ord-xxxxxxxx`), `idempotency_key` TEXT UNIQUE null, `customer` TEXT (generator mengisi CIF), `items` JSONB, `total` INT, `card` TEXT (`ok`/`declined`/`rekening`), `no_rekening` TEXT, `status` TEXT, `reason` TEXT, `request_id`, `served_by`, `precheck`, `created_at`, `updated_at` TIMESTAMPTZ | status berubah lewat saga (PENDING → CONFIRMED/CANCELLED) |
| `order_history` | `id` BIGSERIAL, `order_id`, `status`, `note`, `by_replica`, `at` TIMESTAMPTZ | jejak saga |

Database `catalog`, `inventory`, `payment`, `notification` tidak di-CDC. Produk diambil sebagai
snapshot harian dari `GET /api/catalog/products` (entitas bronze `app/product`).

## 2. Event di NATS JetStream

| Stream | Subject | Penerbit | Bentuk payload |
|---|---|---|---|
| `BANK` (retensi 7 hari, 512 MB) | `bank.cabang.dibuat`, `bank.nasabah.dibuat`, `bank.nasabah.diperbarui`, `bank.rekening.dibuka`, `bank.transaksi.dibuat`, `bank.akad.diajukan/disetujui/ditolak/dicairkan/diangsur/dilunasi` | service `bank` (outbox) | envelope kontrak `{event_id, event_type, occurred_at, data}`, header `Nats-Msg-Id = event_id` |
| `SHOP` (retensi 24 jam, milik bab05) | `shop.order.created/confirmed/cancelled`, `shop.inventory.reserved/rejected/released`, `shop.payment.succeeded/failed` | service toko (outbox) | `OrderEvent` bab05 **tanpa envelope** (`order_id`, `customer`, `items`, `total`, `card`, `no_rekening`, `reason`); `Nats-Msg-Id` = id outbox |
| `CDC` (retensi 7 hari, 2 GiB, dibuat `nats-init`) | `cdc.bank.<tabel>`, `cdc.orders.<tabel>` | Debezium Server (2 instance) | envelope Debezium JSON tanpa schema: `{before, after, source{db,schema,table,lsn,txId,ts_ms,...}, op, ts_ms, transaction}` |

Contoh `bank.transaksi.dibuat`:

```json
{"event_id":"6f1c…","event_type":"bank.transaksi.dibuat","occurred_at":"2026-11-21T02:14:09Z",
 "data":{"id":"a0b2…","rekening_id":"5d1e…","cabang_id":4,"kanal":"mobile","jenis":"tarik",
         "nominal":350000,"saldo_setelah":1650000,"occurred_at":"2026-11-21T02:14:09Z", "...":"..."}}
```

Contoh `cdc.bank.rekening` (UPDATE saldo):

```json
{"before":{"id":"5d1e…","no_rekening":"1100042","saldo":2000000,"...":"..."},
 "after":{"id":"5d1e…","no_rekening":"1100042","saldo":1650000,"...":"..."},
 "source":{"connector":"postgresql","db":"bank","schema":"public","table":"rekening","lsn":26843312,"txId":812},
 "op":"u","ts_ms":1795748049123}
```

`nasabah`, `rekening`, `akad_pembiayaan`, dan `orders` memakai `REPLICA IDENTITY FULL`, sehingga update dan
delete membawa `before` lengkap. Tabel lain (`transaksi`, `cabang`, `order_history`) memakai bawaan Postgres:
`before` null pada update (tabel itu praktis hanya di-INSERT).

## 3. Bronze (RustFS, bucket `lake`)

Path: `s3://lake/bronze/<sumber>/<entitas>/dt=YYYY-MM-DD/part-<batch_id>.parquet`, `batch_id` =
`YYYYMMDDTHHMMSSZ-<6 hex>` (sama untuk semua file dari satu flush). Kompresi zstd.

### Kolom metadata (W1), selalu 8 kolom pertama dengan urutan ini

| Kolom | Tipe Parquet | Isi |
|---|---|---|
| `_ingested_at` | timestamp[us, UTC] | waktu file ditulis ingestor |
| `_source` | string | `app` atau `cdc` |
| `_batch_id` | string | id batch flush |
| `_event_id` | string | app: `event_id` envelope (BANK) / `Nats-Msg-Id` (SHOP; `order_items`: `<msg_id>:<urutan>`); cdc: `<tabel>:<lsn>:<pk>`; produk: `product:<id>:<dt>` |
| `_event_type` | string | app: subject (mis. `bank.transaksi.dibuat`, `shop.order.created`); cdc: `<tabel>.<op>` (mis. `rekening.u`) |
| `_occurred_at` | string RFC3339 UTC | app: `occurred_at` envelope (SHOP: waktu publish); cdc: kolom waktu bisnis (lihat tabel di bawah), bila tidak ada `ts_ms` |
| `_ts_ms` | int64 | app: waktu pesan di stream (ms); cdc: `ts_ms` envelope Debezium (waktu Debezium memproses perubahan) |
| `_op` | string | cdc: `c` insert, `u` update, `d` delete, `r` snapshot awal; app: null |

Kolom tambahan khusus cdc (di luar kontrak, ditempatkan sebagai kolom data pertama):

| Kolom | Tipe | Isi |
|---|---|---|
| `_before` | string (JSON) | baris lama dari envelope (`before`) untuk `op` `u`/`d`; null untuk `c`/`r` atau bila tabel tidak `REPLICA IDENTITY FULL`. Bahan audit dan SCD-2 (mis. `json_extract_string(_before, '$.kota')` di DuckDB) |

### Kolom data

Semua **string** (null tetap null). Objek bersarang diratakan dengan `_` (`a.b` → `a_b`), list
disimpan sebagai JSON string (`items`). Casting ke tipe yang benar dilakukan di silver (W2).

- app: isi `data` envelope, ditambah kolom `aksi` (bagian terakhir subject: `dibuat`, `diperbarui`, `created`, …).
- cdc: isi `after` (untuk `op=d`: `before`). Angka (BIGINT) tertulis sebagai teks angka; TIMESTAMPTZ sebagai
  RFC3339 UTC (format `ZonedTimestamp` Debezium).

### Entitas dan partisi `dt` (tanggal UTC)

| Sumber/entitas | Asal | `dt` dari |
|---|---|---|
| `app/cabang`, `app/nasabah`, `app/rekening`, `app/transaksi`, `app/akad_pembiayaan` | stream BANK (`bank.akad.*` → `akad_pembiayaan`) | `occurred_at` envelope |
| `app/orders`, `app/payment`, `app/inventory` | stream SHOP (`shop.order.*`, `shop.payment.*`, `shop.inventory.*`) | waktu publish |
| `app/order_items` | dipecah dari `items` pada `shop.order.created` (kolom `order_id`, `line_no`, `product_id`, `qty`, `price`) | waktu publish |
| `app/product` | snapshot harian katalog (`_event_type=product.snapshot`) | tanggal snapshot |
| `cdc/transaksi` | `cdc.bank.transaksi` | `occurred_at` |
| `cdc/nasabah` | `cdc.bank.nasabah` | `updated_at`, lalu `created_at` |
| `cdc/rekening` | `cdc.bank.rekening` | `dibuka_at` (`updated_at` rekening = waktu proses, bukan waktu bisnis) |
| `cdc/akad_pembiayaan` | `cdc.bank.akad_pembiayaan` | `cair_at` bila status cair/lunas, lalu `updated_at`, `diajukan_at` |
| `cdc/cabang` | `cdc.bank.cabang` | `ts_ms` (tidak ada kolom waktu) |
| `cdc/orders`, `cdc/order_history` | `cdc.orders.*` | `updated_at`/`created_at`; `at` |

### Jaminan dan batasan

- **At-least-once:** ack dikirim sesudah Parquet tertulis. Bila ingestor mati sebelum ack, pesan dikirim
  ulang dan bisa tertulis dua kali di batch berbeda. Dalam satu batch `_event_id` yang sama hanya
  ditulis sekali. Dedup lintas batch (dan app vs cdc) tugas silver.
- **Duplikat bisnis** (request dikirim ulang tanpa Idempotency-Key) punya `_event_id` berbeda: dua
  transaksi sungguhan di OLTP, terdeteksi di silver dari kombinasi `rekening_id`, `nominal`, `kanal`,
  `occurred_at` yang identik dalam selang pendek.
- Schema kolom data dapat bertambah antar file (kolom baru dari aplikasi). Pembaca sebaiknya memakai
  `union_by_name` (DuckDB) atau schema evolution (PyIceberg).
