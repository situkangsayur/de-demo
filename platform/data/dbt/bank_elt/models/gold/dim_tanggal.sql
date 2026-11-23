{{ config(engine='MergeTree()', order_by='tanggal_key') }}
select
  toInt32(toYYYYMMDD(tanggal)) as tanggal_key,
  tanggal,
  toInt32(toYear(tanggal)) as tahun,
  toInt32(toMonth(tanggal)) as bulan,
  toInt32(toDayOfMonth(tanggal)) as hari,
  toInt32(toISOWeek(tanggal)) as pekan_iso,
  arrayElement(['Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu', 'Minggu'], toDayOfWeek(tanggal)) as nama_hari,
  toDayOfWeek(tanggal) >= 6 as akhir_pekan
from (select toDate('{{ var("awal_tanggal") }}') + number as tanggal from numbers({{ var("hari_dim_tanggal") }}))
