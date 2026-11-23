{{ config(meta={'dimensi': 'consistency'}) }}
-- Jumlah baris dan total nominal per tgl sama di staging dan fakta.
select coalesce(s.tgl, f.tgl) as tgl, s.n as n_stg, f.n as n_fakta, s.t as t_stg, f.t as t_fakta
from (select tgl, count() as n, sum(nominal) as t from {{ ref('stg_transaksi') }} group by tgl) s
full outer join (select tgl, count() as n, sum(nominal) as t from {{ ref('fakta_transaksi') }} group by tgl) f
  on s.tgl = f.tgl
where s.n is null or f.n is null or s.n != f.n or s.t != f.t
