-- Q1 dan Q3 (slide P08): jalankan sebelum dan sesudah run ulang / backfill; kedua hasil harus identik.
-- baris = trx_unik membuktikan tidak ada duplikat. Ganti tanggal sesuai demo.
-- ClickHouse (warehouse):  curl -u de:... 'http://127.0.0.1:52011/' --data-binary @q1_idempoten.sql
select tgl,
       count()              as baris,
       sum(nominal)         as total,
       uniqExact(trx_id)    as trx_unik
from gold.fakta_transaksi
where tgl between toDate('2026-11-15') and toDate('2026-11-22')
group by tgl
order by tgl;
