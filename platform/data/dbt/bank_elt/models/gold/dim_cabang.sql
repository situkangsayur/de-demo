{{ config(engine='MergeTree()', order_by='cabang_key') }}
select cabang_id as cabang_key, kode, nama, kota, wilayah from {{ ref('stg_cabang') }}
