-- "Tunjukkan baris nasabah yang berubah alamat/kota di dimensi SCD-2" (pertanyaan demo P08).
select nasabah_sk, nasabah_id, versi, kota, segmen, no_hp_token, berlaku_dari, berlaku_sampai, is_current
from gold.dim_nasabah
where nasabah_id in (select nasabah_id from gold.dim_nasabah group by nasabah_id having count() > 1 limit 3)
order by nasabah_id, versi;
