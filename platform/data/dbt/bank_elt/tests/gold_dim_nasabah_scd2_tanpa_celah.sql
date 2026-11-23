{{ config(meta={'dimensi': 'validity'}) }}
-- berlaku_sampai versi n harus sama dengan berlaku_dari versi n+1 (tanpa celah, tanpa tumpang tindih).
select nasabah_sk from (
  select nasabah_sk, berlaku_dari, berlaku_sampai,
    leadInFrame(toNullable(berlaku_dari)) over (partition by nasabah_id order by versi
                                                rows between unbounded preceding and unbounded following) as dari_berikut
  from {{ ref('dim_nasabah') }}
) where berlaku_dari >= berlaku_sampai or (dari_berikut is not null and dari_berikut != berlaku_sampai)
