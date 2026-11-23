-- Versi terakhir setiap cabang.
select cabang_id, kode, nama, kota, wilayah
from (
  select
    id as cabang_id,
    upperUTF8(trimBoth(kode)) as kode,
    trimBoth(nama) as nama,
    initcapUTF8(trimBoth(kota)) as kota,
    trimBoth(wilayah) as wilayah,
    _op,
    row_number() over (partition by id order by {{ urut_terakhir() }}) as rn
  from {{ source('raw', 'cabang') }}
)
where rn = 1 and coalesce(_op, '') != 'd'
