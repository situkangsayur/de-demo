{{ config(meta={'dimensi': 'uniqueness'}) }}
select nasabah_id, tgl_fitur, count() as n from {{ ref('fitur_nasabah') }} group by nasabah_id, tgl_fitur having n > 1
