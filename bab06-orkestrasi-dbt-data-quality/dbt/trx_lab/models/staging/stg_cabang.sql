select toUInt16(id) as cabang_id, kode, nama, kota, wilayah
from {{ source('raw', 'cabang') }}
