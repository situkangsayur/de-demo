-- Skema awal warehouse lab Bab 6. Dijalankan sekali oleh entrypoint ClickHouse pada volume baru.
--   raw   : bronze dimuat apa adanya (semua kolom data String, seperti kontrak bronze platform)
--   audit : target dbt (write): model dibangun dan diuji di sini
--   gold  : hasil yang dipublikasikan (publish) sesudah dbt test lulus
--   meta  : hasil data quality, log publikasi, catatan SLA (deadline) terlewat
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS audit;
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS raw.transaksi (
  dt Date,
  _ingested_at String, _source String, _batch_id String, _event_id String,
  _event_type String, _occurred_at String, _ts_ms String, _op Nullable(String),
  id Nullable(String), rekening_id Nullable(String), cabang_id Nullable(String),
  kanal Nullable(String), jenis Nullable(String), nominal Nullable(String),
  occurred_at Nullable(String), created_at Nullable(String),
  keterangan Nullable(String), akad_id Nullable(String)
) ENGINE = MergeTree PARTITION BY dt ORDER BY (dt, _event_id);

CREATE TABLE IF NOT EXISTS raw.akad_pembiayaan (
  dt Date,
  _ingested_at String, _source String, _batch_id String, _event_id String,
  _event_type String, _occurred_at String, _ts_ms String, _op Nullable(String),
  id Nullable(String), nasabah_id Nullable(String), rekening_id Nullable(String),
  kode_akad Nullable(String), pokok Nullable(String), margin Nullable(String),
  tenor_bulan Nullable(String), status Nullable(String), cair_at Nullable(String)
) ENGINE = MergeTree PARTITION BY dt ORDER BY (dt, _event_id);

-- Tabel dimensi kecil: full load (TRUNCATE + INSERT) setiap run.
CREATE TABLE IF NOT EXISTS raw.cabang (
  id String, kode String, nama String, kota String, wilayah String, _ingested_at String
) ENGINE = MergeTree ORDER BY id;

CREATE TABLE IF NOT EXISTS raw.rekening (
  id String, no_rekening String, nasabah_id String, jenis String, cabang_id String,
  status String, dibuka_at String, _ingested_at String
) ENGINE = MergeTree ORDER BY id;

-- Total kontrol harian dari laporan core banking (dipakai uji accuracy).
CREATE TABLE IF NOT EXISTS raw.kontrol (
  dt Date, jumlah_trx UInt64, total_nominal Int64, sumber String, diambil_at DateTime
) ENGINE = MergeTree PARTITION BY dt ORDER BY dt;

-- Pembanding: pipeline naif tanpa gerbang mutu dan tanpa timpa partisi (INSERT biasa).
CREATE TABLE IF NOT EXISTS gold.harian_cabang_naif (
  tgl Date, cabang_id UInt16, jumlah_trx UInt64, total_nominal Int64, dimuat_at DateTime, run_id String
) ENGINE = MergeTree ORDER BY (tgl, cabang_id);

CREATE TABLE IF NOT EXISTS meta.dq_hasil (
  run_id String, tgl Date, waktu DateTime64(3), unique_id String, nama String, model String,
  kolom String, dimensi String, keparahan String, status String, failures UInt64, pesan String
) ENGINE = MergeTree ORDER BY (tgl, waktu, unique_id);

CREATE TABLE IF NOT EXISTS meta.publikasi (
  run_id String, tgl Date, tabel String, waktu DateTime64(3), baris UInt64, total_nominal Int64
) ENGINE = MergeTree ORDER BY (tgl, waktu);

CREATE TABLE IF NOT EXISTS meta.sla_terlewat (
  run_id String, tgl String, batas String, dicatat DateTime64(3)
) ENGINE = MergeTree ORDER BY dicatat;
