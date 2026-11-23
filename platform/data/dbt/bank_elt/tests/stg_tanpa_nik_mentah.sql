{{ config(meta={'dimensi': 'validity'}) }}
-- PII hanya boleh berupa token: tidak ada 16 digit berurutan (pola NIK) di kolom non-token.
select nasabah_id from {{ ref('stg_nasabah_versi') }}
where match(concat(coalesce(nasabah_id, ''), ' ', coalesce(cif, ''), ' ', coalesce(nama_samaran, ''), ' ',
                   coalesce(kota, ''), ' ', coalesce(segmen, '')), '(^|[^0-9])[0-9]{16}([^0-9]|$)')
