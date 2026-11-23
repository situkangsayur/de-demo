{{ config(engine='MergeTree()', order_by='(tgl_cair, akad_id)') }}
-- Grain: satu akad pembiayaan yang cair. Akad dengan kode tidak valid ada di akad_karantina.
select
  a.akad_id as akad_id, a.tgl_cair as tgl_cair, toInt32(toYYYYMMDD(a.tgl_cair)) as tanggal_key,
  coalesce(a.cabang_id, d.cabang_id) as cabang_key, d.nasabah_sk as nasabah_sk, a.nasabah_id as nasabah_id,
  a.kode_akad as akad_key, a.plafon as plafon, a.margin as margin, a.terbayar as terbayar, a.tenor_bulan as tenor_bulan, a.status as status
from (select * from {{ ref('stg_akad') }} where kode_valid and tanggal_cair is not null and status in ('cair', 'lunas')) a
asof left join (select nasabah_id, berlaku_dari, nasabah_sk, cabang_id from {{ ref('dim_nasabah') }}) d
  on a.nasabah_id = d.nasabah_id and a.tanggal_cair >= d.berlaku_dari
