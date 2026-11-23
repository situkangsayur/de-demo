{{ config(meta={'dimensi': 'timeliness'}, severity='warn') }}
-- p95 jarak waktu kejadian -> masuk bronze tidak boleh lebih dari 60 menit (peringatan, tidak menghentikan).
select quantile(0.95)(dateDiff('minute', waktu, diingest)) as p95_menit
from {{ ref('stg_transaksi') }}
having p95_menit > 60
