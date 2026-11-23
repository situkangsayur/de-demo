{{ config(meta={'dimensi': 'completeness'}) }}
-- Setiap cabang aktif harus punya minimal satu transaksi pada tanggal ini.
select c.cabang_id, c.kode
from {{ ref('stg_cabang') }} c
where c.cabang_id not in (select cabang_id from {{ ref('stg_transaksi') }} where cabang_id is not null)
