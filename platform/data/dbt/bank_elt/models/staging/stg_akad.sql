-- Versi terakhir setiap akad; kode distandardkan. kode_valid = 0 berarti dikarantina.
-- Alias di subquery sengaja tidak sama dengan nama kolom raw: ClickHouse mendahulukan alias
-- daripada kolom, sehingga `... as tanggal_cair` akan "menimpa" kolom tanggal_cair di ekspresi lain.
select
  akad_id, nasabah_id, rekening_id, cabang_id, kode_std as kode_akad, kode_akad_asli, plafon_dec as plafon, margin_dec as margin, terbayar_dec as terbayar,
  tenor_int as tenor_bulan, status_std as status, t_cair as tanggal_cair,
  toDate(toTimezone(t_cair, 'Asia/Jakarta')) as tgl_cair,
  kode_std in ('IJR', 'MDH', 'MRB', 'MSY') as kode_valid
from (
  select
    id as akad_id, nasabah_id, rekening_id, cabang_id,
    {{ akad_std('kode_akad') }} as kode_std,
    kode_akad as kode_akad_asli,
    {{ ke_angka('coalesce(pokok, plafon)') }} as plafon_dec,
    {{ ke_angka('margin') }} as margin_dec,
    {{ ke_angka('terbayar') }} as terbayar_dec,
    toInt32OrNull(splitByChar('.', coalesce(tenor_bulan, ''))[1]) as tenor_int,
    lowerUTF8(trimBoth(status)) as status_std,
    {{ ke_waktu('coalesce(cair_at, tanggal_cair)') }} as t_cair,
    _op,
    row_number() over (partition by id order by {{ urut_terakhir() }}) as rn
  from {{ source('raw', 'akad_pembiayaan') }}
)
where rn = 1 and coalesce(_op, '') != 'd'
