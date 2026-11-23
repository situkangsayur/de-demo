-- Semua versi profil nasabah, PII sudah di-tokenize (NIK, no HP, alamat) dan nama disamarkan.
-- Perubahan yang tidak mengubah atribut yang dilacak SCD-2 tidak membuat versi baru.
with dasar as (
  select
    id as nasabah_id, cif, nama, nik, no_hp, email, alamat, kota, segmen, cabang_id, tanggal_lahir, _op,
    {{ ke_waktu('coalesce(created_at, terdaftar_pada)') }} as terdaftar_ts,
    coalesce({{ ke_waktu('updated_at') }}, {{ ke_waktu('coalesce(created_at, terdaftar_pada)') }}, {{ ke_waktu('_ingested_at') }}) as versi_ts,
    row_number() over (partition by id, versi_ts order by {{ urut_terakhir() }}) as rn
  from {{ source('raw', 'nasabah') }}
  where coalesce(_op, '') != 'd'
),
satu as (
  select
    nasabah_id, cif,
    {{ samaran('nama') }} as nama_samaran,
    {{ token("replaceRegexpAll(nik, '[^0-9]', '')") }} as nik_token,
    {{ token(norm_hp('no_hp')) }} as no_hp_token,
    {{ token('lowerUTF8(trimBoth(email))') }} as email_token,
    {{ token('lowerUTF8(trimBoth(alamat))') }} as alamat_token,
    toInt32OrNull(toString(toYear({{ ke_waktu('tanggal_lahir') }}))) as tahun_lahir,
    initcapUTF8(trimBoth(kota)) as kota,
    lowerUTF8(trimBoth(segmen)) as segmen,
    cabang_id,
    coalesce(terdaftar_ts, versi_ts) as terdaftar_awal,
    versi_ts
  from dasar where rn = 1
),
sidik as (
  select *,
    concat(coalesce(kota, '∅'), '|', coalesce(segmen, '∅'), '|', coalesce(cabang_id, '∅'), '|',
           coalesce(no_hp_token, '∅'), '|', coalesce(alamat_token, '∅')) as _sidik,
    lagInFrame(_sidik, 1, '') over (partition by nasabah_id order by versi_ts
                                    rows between unbounded preceding and unbounded following) as _sidik_sebelum,
    row_number() over (partition by nasabah_id order by versi_ts) as _urut
  from satu
)
select
  nasabah_id, cif, nama_samaran, nik_token, no_hp_token, email_token, alamat_token, tahun_lahir, kota, segmen, cabang_id,
  min(terdaftar_awal) over (partition by nasabah_id) as terdaftar_pada,
  versi_ts
from sidik
where _urut = 1 or _sidik != _sidik_sebelum
