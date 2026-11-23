-- Dedup app vs cdc: satu baris per transaksi (versi terakhir), _source = app / cdc / app+cdc.
with r as (
  select
    id as trx_id, rekening_id, cabang_id, nasabah_id, akad_id, ref_order_id, kanal, jenis, nominal, _source, _event_id, _op,
    _ingested_at, _ts_ms, updated_at,
    {{ ke_waktu('coalesce(occurred_at, waktu, _occurred_at)') }} as waktu_utc,
    toDate(toTimezone({{ ke_waktu('coalesce(occurred_at, waktu, _occurred_at)') }}, 'Asia/Jakarta')) as tgl
  from {{ source('raw', 'transaksi') }}
),
sumber as (
  select trx_id,
    arrayStringConcat(arraySort(groupUniqArray(_source)), '+') as sumber_gabung,
    -- konflik: nominal versi terakhir app berbeda dengan versi terakhir cdc
    countIf(_source = 'app') > 0 and countIf(_source = 'cdc') > 0
      and abs(toFloat64OrZero(argMaxIf(nominal, {{ ke_waktu('_ingested_at') }}, _source = 'app'))
              - toFloat64OrZero(argMaxIf(nominal, {{ ke_waktu('_ingested_at') }}, _source = 'cdc'))) > 0.005 as konflik_nominal
  from r group by trx_id
),
akhir as (
  select *, row_number() over (partition by trx_id order by {{ urut_terakhir() }}) as rn from r
)
select
  a.trx_id as trx_id, a.rekening_id as rekening_id,
  coalesce(a.nasabah_id, k.nasabah_id) as nasabah_id,
  a.cabang_id as cabang_id, a.akad_id as akad_id, a.ref_order_id as ref_order_id,
  {{ kanal_std('a.kanal') }} as kanal,
  a.kanal as kanal_asli,
  lowerUTF8(trimBoth(a.jenis)) as jenis,
  {{ ke_angka('a.nominal') }} as nominal,
  a.waktu_utc as waktu, a.tgl as tgl,
  s.sumber_gabung as _source,
  {{ ke_waktu('a._ingested_at') }} as _ingested_at,
  s.konflik_nominal as _konflik_nominal
from akhir a
left join sumber s on s.trx_id = a.trx_id
left join {{ ref('stg_rekening') }} k on k.rekening_id = a.rekening_id
where a.rn = 1 and coalesce(a._op, '') != 'd'
