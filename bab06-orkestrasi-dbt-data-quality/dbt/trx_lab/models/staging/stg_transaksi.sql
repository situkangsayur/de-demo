-- Silver transaksi untuk tanggal var('tgl'): cast tipe, standardisasi kanal, dedup teknis per _event_id
-- (pengiriman at-least-once). Duplikat bisnis (trx_id sama, _event_id beda) TIDAK dibuang di sini:
-- itu tugas uji uniqueness untuk menangkapnya.
select
    id                                                         as trx_id,
    rekening_id,
    toUInt16OrNull(cabang_id)                                  as cabang_id,
    multiIf(lower(trim(kanal)) in ('mobile', 'mobile banking', 'm-banking'), 'mobile',
            lower(trim(kanal)) = 'atm', 'atm',
            lower(trim(kanal)) = 'teller', 'teller',
            lower(trim(kanal)) in ('web', 'internet banking'), 'web',
            lower(trim(kanal)) in ('ecommerce', 'e-commerce'), 'ecommerce',
            lower(trim(kanal)))                                as kanal,
    jenis,
    toInt64OrNull(nominal)                                     as nominal,
    parseDateTime64BestEffortOrNull(occurred_at, 3, 'UTC')     as waktu,
    parseDateTime64BestEffortOrNull(created_at, 3, 'UTC')      as dibuat,
    parseDateTime64BestEffortOrNull(_ingested_at, 3, 'UTC')    as diingest,
    keterangan,
    _event_id,
    dt
from {{ source('raw', 'transaksi') }}
where dt = toDate('{{ var("tgl") }}')
limit 1 by _event_id
