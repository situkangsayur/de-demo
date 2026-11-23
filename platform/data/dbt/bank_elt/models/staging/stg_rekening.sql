select rekening_id, nasabah_id, cabang_id, no_rekening, produk, status
from (
  select
    id as rekening_id, nasabah_id, cabang_id, no_rekening,
    lowerUTF8(trimBoth(coalesce(jenis, produk))) as produk,
    lowerUTF8(trimBoth(status)) as status,
    _op,
    row_number() over (partition by id order by {{ urut_terakhir() }}) as rn
  from {{ source('raw', 'rekening') }}
)
where rn = 1 and coalesce(_op, '') != 'd'
