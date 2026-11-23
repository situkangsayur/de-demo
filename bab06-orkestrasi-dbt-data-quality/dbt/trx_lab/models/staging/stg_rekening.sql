select id as rekening_id, no_rekening, nasabah_id, jenis, toUInt16OrNull(cabang_id) as cabang_id, status
from {{ source('raw', 'rekening') }}
