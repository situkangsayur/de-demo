{{ config(meta={'dimensi': 'consistency'}) }}
-- Transaksi yang tercatat di event aplikasi dan di CDC harus bernominal sama.
select trx_id, nominal, _source from {{ ref('stg_transaksi') }} where _konflik_nominal
