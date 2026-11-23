{{ config(meta={'dimensi': 'consistency'}) }}
-- Agregat gold harus konsisten dengan fakta di bawahnya.
with h as (select toInt64(sum(total_nominal)) as t, toUInt64(sum(jumlah_trx)) as n from {{ ref('harian_cabang') }}),
     f as (select toInt64(sum(nominal)) as t, toUInt64(count()) as n from {{ ref('fakta_transaksi') }})
select h.t as total_harian, f.t as total_fakta, h.n as n_harian, f.n as n_fakta
from h cross join f
where h.t != f.t or h.n != f.n
