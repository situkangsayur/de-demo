{{ config(severity='warn', meta={'dimensi': 'validity'}) }}
-- Kode akad tidak valid dikarantina; peringatan bila rasionya melebihi ambang.
select persen from (
  select 100 * countIf(not kode_valid) / count() as persen from {{ ref('stg_akad') }}
) where persen > {{ var('karantina_maks_persen') }}
