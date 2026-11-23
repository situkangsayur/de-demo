-- Skema OLTP lab Bab 7 (nama kolom mengikuti kontrak platform; id integer agar kunci acak mudah dibangkitkan).
CREATE EXTENSION IF NOT EXISTS pageinspect;   -- melihat isi page B-tree (bt_metap)

CREATE TABLE cabang (id smallint PRIMARY KEY, kode text, nama text, kota text, wilayah text);
CREATE TABLE nasabah (id int PRIMARY KEY, cif text UNIQUE, nama text, kota text, segmen text, cabang_id smallint);
CREATE TABLE rekening (id bigint PRIMARY KEY, no_rekening text UNIQUE, nasabah_id int, jenis text,
                       saldo bigint, status text, cabang_id smallint);
CREATE TABLE transaksi (id bigint PRIMARY KEY, rekening_id bigint, nasabah_id int, cabang_id smallint,
                        kanal text, jenis text, nominal bigint, occurred_at timestamptz, keterangan text);
-- Indeks dibuat sesudah COPY (lebih cepat): trx_nasabah_waktu (range scan) dan trx_ket_fts (GIN full-text).

CREATE TABLE fitur_nasabah (nasabah_id int PRIMARY KEY, tgl_fitur date, trx_7h int, nominal_7h bigint,
  trx_30h int, nominal_30h bigint, hari_sejak_trx_terakhir int, rasio_digital_30h double precision,
  belanja_ecom_30h int, nominal_belanja_ecom_30h bigint, segmen_saat_itu text, kota_saat_itu text);

-- Eksperimen tulis satu baris per transaksi (amplifikasi tulis B-tree + WAL).
CREATE TABLE tulis_uji (id bigserial PRIMARY KEY, rekening_id bigint, nominal bigint,
                        waktu timestamptz DEFAULT now(), keterangan text);
CREATE INDEX tulis_uji_rek ON tulis_uji (rekening_id);

-- Jurnal akad dengan hash chain (praktikum mini immutable ledger).
CREATE TABLE jurnal_akad (id int PRIMARY KEY, akad text, nasabah text, nilai bigint, waktu timestamptz,
                          hash_prev text, hash text);

CREATE TABLE lab_meta (kunci text PRIMARY KEY, nilai text);
