{{ config(severity='warn', meta={'dimensi': 'uniqueness'}) }}
-- Indikasi request dikirim ulang tanpa idempotency key: isi sama, trx_id beda, selang <= 60 detik.
select trx_id from (
  select trx_id, waktu,
    lagInFrame(waktu) over (partition by rekening_id, nominal, jenis, kanal order by waktu
                            rows between unbounded preceding and unbounded following) as sebelum,
    row_number() over (partition by rekening_id, nominal, jenis, kanal order by waktu) as urut
  from {{ ref('stg_transaksi') }}
) where urut > 1 and dateDiff('second', sebelum, waktu) <= 60
