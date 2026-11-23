{{ config(order_by='(tgl, cabang_id)') }}
-- Agregat siap dashboard: transaksi per cabang per hari.
select
    f.tgl                          as tgl,
    f.cabang_id                    as cabang_id,
    any(c.kode)                    as kode_cabang,
    any(c.nama)                    as nama_cabang,
    toUInt64(count())              as jumlah_trx,
    toInt64(sum(f.nominal))        as total_nominal,
    toUInt64(uniqExact(f.rekening_id)) as jumlah_rekening
from {{ ref('fakta_transaksi') }} f
left join {{ ref('stg_cabang') }} c on c.cabang_id = f.cabang_id
group by f.tgl, f.cabang_id
