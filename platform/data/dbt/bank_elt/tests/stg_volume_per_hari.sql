{{ config(meta={'dimensi': 'completeness'}) }}
-- Tidak ada hari tanpa transaksi di antara tanggal pertama dan terakhir yang sudah dimuat.
with r as (select min(tgl) as awal, max(tgl) as akhir from {{ ref('stg_transaksi') }}),
hari as (select addDays(awal, number) as tgl from r array join range(toUInt32(dateDiff('day', awal, akhir) + 1)) as number)
select h.tgl from hari h
left join (select tgl, count() as n from {{ ref('stg_transaksi') }} group by tgl) s on s.tgl = h.tgl
where coalesce(s.n, 0) = 0
