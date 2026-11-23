{{ config(meta={'dimensi': 'uniqueness'}) }}
select nasabah_id, countIf(is_current) as n from {{ ref('dim_nasabah') }} group by nasabah_id having n != 1
