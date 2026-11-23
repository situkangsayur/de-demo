{{ config(meta={'dimensi': 'accuracy'}) }}
-- Rekonsiliasi dengan laporan resmi core banking: jumlah dan total nominal fakta harus sama dengan kontrol.
with f as (
    select toUInt64(count()) as jumlah, toInt64(sum(nominal)) as total from {{ ref('fakta_transaksi') }}
), k as (
    select toUInt64(sum(jumlah_trx)) as jumlah, toInt64(sum(total_nominal)) as total, toUInt64(count()) as ada
    from {{ ref('stg_kontrol') }}
)
select f.jumlah as jumlah_fakta, k.jumlah as jumlah_kontrol, f.total as total_fakta, k.total as total_kontrol
from f cross join k
where k.ada = 0 or f.jumlah != k.jumlah or f.total != k.total
