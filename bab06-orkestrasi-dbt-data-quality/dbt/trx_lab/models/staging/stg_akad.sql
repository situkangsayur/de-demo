-- Silver akad pembiayaan tanggal var('tgl'). Kode akad distandardkan (peka huruf: 'mrb' tidak valid); kode tidak dikenal dibiarkan apa adanya
-- agar tertangkap uji validity.
select
    id                                                         as akad_id,
    nasabah_id,
    multiIf(trim(kode_akad) in ('MRB', 'MURABAHAH'), 'MRB',
            trim(kode_akad) in ('MSY', 'MUSYARAKAH'), 'MSY',
            trim(kode_akad) in ('MDH', 'MUDHARABAH'), 'MDH',
            trim(kode_akad) in ('IJR', 'IJARAH'), 'IJR',
            trim(kode_akad))                            as kode_akad,
    toInt64OrNull(pokok)                                       as pokok,
    toInt64OrNull(margin)                                      as margin,
    toUInt16OrNull(tenor_bulan)                                as tenor_bulan,
    status,
    parseDateTime64BestEffortOrNull(cair_at, 3, 'UTC')         as cair_at,
    dt
from {{ source('raw', 'akad_pembiayaan') }}
where dt = toDate('{{ var("tgl") }}')
limit 1 by _event_id
