-- Fitur tim AI point-in-time: nilai fitur pada tgl_fitur D hanya dari kejadian <= D.
-- Hitung ulang trx_30h langsung dari fakta (jendela D-29..D) harus sama dengan nilai fitur.
with d as (select toDate('2026-11-22') as tgl)
select f.nasabah_id, f.tgl_fitur, f.trx_30h, coalesce(h.n, 0) as hitung_ulang, f.trx_30h = coalesce(h.n, 0) as sama
from gold.fitur_nasabah f
left join (select nasabah_id, count() as n from gold.fakta_transaksi
           where tgl between (select tgl from d) - 29 and (select tgl from d) group by nasabah_id) h
  on h.nasabah_id = f.nasabah_id
where f.tgl_fitur = (select tgl from d)
order by sama, f.nasabah_id limit 20;
