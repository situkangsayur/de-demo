{{ config(order_by='(tgl, cabang_id)') }}
-- Fakta grain satu transaksi untuk tanggal var('tgl'). Dibangun di database audit; dipublikasikan ke gold
-- oleh Airflow (REPLACE PARTITION) hanya bila dbt test lulus.
select
    t.trx_id                       as trx_id,
    t.dt                           as tgl,
    t.waktu                        as waktu,
    t.rekening_id                  as rekening_id,
    r.nasabah_id                   as nasabah_id,
    coalesce(t.cabang_id, 0)       as cabang_id,
    t.kanal                        as kanal,
    t.jenis                        as jenis,
    coalesce(t.nominal, 0)         as nominal
from {{ ref('stg_transaksi') }} t
left join {{ ref('stg_rekening') }} r on r.rekening_id = t.rekening_id
