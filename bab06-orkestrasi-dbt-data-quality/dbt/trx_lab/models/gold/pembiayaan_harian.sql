{{ config(order_by='(tgl, kode_akad)') }}
-- Pembiayaan cair per jenis akad per hari.
select
    dt                             as tgl,
    coalesce(kode_akad, '')        as kode_akad,
    toUInt64(count())              as jumlah_akad,
    toInt64(sum(coalesce(pokok, 0))) as total_pokok
from {{ ref('stg_akad') }}
where status in ('cair', 'lunas')
group by tgl, kode_akad
