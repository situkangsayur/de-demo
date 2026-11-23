-- Q2: catat sebelum dan sesudah run dengan data rusak. Waktu modifikasi dan jumlah baris part gold
-- tidak boleh berubah (dbt melewati model gold karena test staging gagal).
select table, max(modification_time) as diubah, sum(rows) as baris
from system.parts where database = 'gold' and active group by table order by table;

-- test yang gagal beserta contoh baris pelanggarnya
select waktu, jalur, tes, dimensi, aturan, gagal, contoh
from meta.dq_hasil where status = 'gagal' order by waktu desc limit 20;
