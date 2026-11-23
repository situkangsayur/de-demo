-- Versi lake (DuckDB atas Iceberg), dijalankan lewat lake-engine:
--   curl -s localhost:52013/api/uji/q1?dari=2026-11-15&sampai=2026-11-22   (dashboard menampilkan keduanya)
select tgl, count(*) as baris, sum(nominal) as total, count(distinct trx_id) as trx_unik
from gold.fakta_transaksi
where tgl between date '2026-11-15' and date '2026-11-22'
group by tgl
order by tgl;
