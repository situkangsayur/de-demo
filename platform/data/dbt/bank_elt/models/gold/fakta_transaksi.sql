{{ config(engine='MergeTree()', order_by='(tgl, trx_id)', partition_by='toYYYYMM(tgl)') }}
-- Grain: satu transaksi. Versi nasabah dicari dengan ASOF JOIN: versi dengan berlaku_dari terdekat
-- yang <= waktu transaksi, yaitu versi SCD-2 yang berlaku saat transaksi terjadi.
select
  t.trx_id as trx_id, t.tgl as tgl, toInt32(toYYYYMMDD(t.tgl)) as tanggal_key, t.waktu as waktu,
  t.cabang_id as cabang_key, t.kanal as kanal_key, d.nasabah_sk as nasabah_sk, t.nasabah_id as nasabah_id,
  t.rekening_id as rekening_id, a.kode_akad as akad_key, t.jenis as jenis, t.ref_order_id as ref_order_id,
  t.nominal as nominal,
  t._source as _source
from {{ ref('stg_transaksi') }} t
asof left join (select nasabah_id, berlaku_dari, nasabah_sk from {{ ref('dim_nasabah') }}) d
  on t.nasabah_id = d.nasabah_id and t.waktu >= d.berlaku_dari
left join (select akad_id, kode_akad from {{ ref('stg_akad') }} where kode_valid) a on a.akad_id = t.akad_id
