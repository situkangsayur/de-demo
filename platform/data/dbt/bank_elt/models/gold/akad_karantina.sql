{{ config(engine='MergeTree()', order_by='akad_id') }}
-- Akad dengan kode di luar MRB/MSY/MDH/IJR: tidak masuk fakta, disimpan untuk ditindaklanjuti sumber.
select akad_id, nasabah_id, cabang_id, kode_akad_asli, plafon, tanggal_cair,
  concat('kode akad ''', coalesce(kode_akad_asli, 'NULL'), ''' di luar domain IJR/MDH/MRB/MSY') as alasan
from {{ ref('stg_akad') }} where not kode_valid
