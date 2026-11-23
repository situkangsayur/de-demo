{{ config(engine='MergeTree()', order_by='(nasabah_id, versi)') }}
-- SCD tipe 2 dari riwayat lengkap (CDC): interval berlaku dihitung dengan window function.
-- Versi pertama berlaku sejak 1900-01-01; versi aktif berlaku sampai 2299-12-31 (batas DateTime64).
with v as (
  select *,
    toInt32(row_number() over (partition by nasabah_id order by versi_ts)) as versi,
    leadInFrame(toNullable(versi_ts)) over (partition by nasabah_id order by versi_ts
                                            rows between unbounded preceding and unbounded following) as berikut
  from {{ ref('stg_nasabah_versi') }}
)
select
  concat(nasabah_id, '-', toString(versi)) as nasabah_sk,
  nasabah_id, versi, cif, nama_samaran, nik_token, no_hp_token, email_token, alamat_token, tahun_lahir, kota, segmen, cabang_id,
  terdaftar_pada,
  toDate(toTimezone(terdaftar_pada, 'Asia/Jakarta')) as tgl_daftar,
  versi_ts as diubah_pada,
  if(versi = 1, toDateTime64('1900-01-01 00:00:00', 6, 'UTC'), versi_ts) as berlaku_dari,
  coalesce(berikut, toDateTime64('2299-12-31 23:59:59', 6, 'UTC')) as berlaku_sampai,
  berikut is null as is_current
from v
