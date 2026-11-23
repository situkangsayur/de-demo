{{ config(meta={'dimensi': 'validity'}) }}
-- Fitur tidak boleh memakai data dari masa depan: recency >= 0, umur >= 0, trx_7h <= trx_30h.
select nasabah_id, tgl_fitur from {{ ref('fitur_nasabah') }}
where hari_sejak_trx_terakhir < 0 or umur_nasabah_hari < 0 or trx_7h > trx_30h
