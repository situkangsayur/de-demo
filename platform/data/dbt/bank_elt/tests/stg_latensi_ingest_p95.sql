{{ config(severity=env_var('DQ_LATENSI_KEPARAHAN', 'warn'), meta={'dimensi': 'timeliness'}) }}
-- Bawaan peringatan: data backfill historis wajar berlatensi besar (lihat pipeline/dq.py).
-- p95 jeda antara kejadian dan masuk bronze tidak boleh melebihi ambang (menit).
select p95_menit from (
  select quantileExact(0.95)(dateDiff('second', waktu, _ingested_at) / 60) as p95_menit
  from {{ ref('stg_transaksi') }} where _ingested_at is not null
) where p95_menit > {{ var('latensi_maks_menit') }}
