{{ config(engine='MergeTree()', order_by='(tgl_fitur, nasabah_id)', partition_by='toYYYYMM(tgl_fitur)') }}
-- Fitur per nasabah per tanggal untuk tim AI, point-in-time: fitur tgl_fitur = D hanya memakai
-- kejadian dengan tgl <= D (frame window berakhir di CURRENT ROW) dan versi profil yang berlaku
-- pada akhir hari D (ASOF JOIN ke SCD-2). Logika sama persis dengan pipeline/gold.py (SQL_FITUR).
with harian as (
  select nasabah_id, tgl, count() as n, toFloat64(sum(nominal)) as nom,
         countIf(kanal in ('mobile', 'web', 'ecommerce')) as n_digital,
         countIf(jenis = 'bayar_belanja') as n_belanja,
         toFloat64(sumIf(nominal, jenis = 'bayar_belanja')) as nom_belanja
  from {{ ref('stg_transaksi') }} where nasabah_id is not null group by nasabah_id, tgl),
cair as (
  select nasabah_id, tgl_cair as tgl, count() as n_akad, toFloat64(sum(plafon)) as pokok
  from {{ ref('stg_akad') }}
  where kode_valid and tgl_cair is not null and status in ('cair', 'lunas') group by nasabah_id, tgl_cair),
batas as (
  select least((select min(tgl) from {{ ref('stg_transaksi') }}),
               coalesce((select min(tgl) from cair), toDate('2149-01-01'))) as awal,
         (select max(tgl) from {{ ref('stg_transaksi') }}) as akhir),
hari as (select addDays(awal, number) as tgl from batas array join range(toUInt32(dateDiff('day', awal, akhir) + 1)) as number),
-- awal riwayat nasabah: tanggal daftar, atau transaksi pertama bila lebih awal (data backfill/terlambat)
nas as (select nasabah_id, min(t) as tgl_daftar from (
          select nasabah_id, tgl_daftar as t from {{ ref('dim_nasabah') }}
          union all select nasabah_id, min(tgl) as t from harian group by nasabah_id) group by nasabah_id),
grid as (select n.nasabah_id as nasabah_id, h.tgl as tgl, n.tgl_daftar as tgl_daftar from nas n cross join hari h where h.tgl >= n.tgl_daftar),
g as (
  select grid.nasabah_id as nasabah_id, grid.tgl as tgl, grid.tgl_daftar as tgl_daftar,
         coalesce(h.n, 0) as n, coalesce(h.nom, 0) as nom, coalesce(h.n_digital, 0) as n_digital,
         coalesce(h.n_belanja, 0) as n_belanja, coalesce(h.nom_belanja, 0) as nom_belanja,
         coalesce(c.n_akad, 0) as n_akad, coalesce(c.pokok, 0) as pokok,
         toDateTime64(toString(grid.tgl + 1), 6, 'Asia/Jakarta') as t_akhir
  from grid
  left join harian h on h.nasabah_id = grid.nasabah_id and h.tgl = grid.tgl
  left join cair c on c.nasabah_id = grid.nasabah_id and c.tgl = grid.tgl),
w as (
  select *,
    sum(n) over w7 as trx_7h, sum(nom) over w7 as nominal_7h,
    sum(n) over w30 as trx_30h, sum(nom) over w30 as nominal_30h, sum(n_digital) over w30 as digital_30h,
    sum(n_belanja) over w30 as belanja_30h, sum(nom_belanja) over w30 as nom_belanja_30h,
    max(if(n > 0, tgl, NULL)) over wsemua as trx_terakhir,
    sum(n_akad) over wsemua as akad_kum, sum(pokok) over wsemua as pokok_kum
  from g
  window w7 as (partition by nasabah_id order by tgl rows between 6 preceding and current row),
         w30 as (partition by nasabah_id order by tgl rows between 29 preceding and current row),
         wsemua as (partition by nasabah_id order by tgl rows between unbounded preceding and current row))
select
  w.nasabah_id as nasabah_id, w.tgl as tgl_fitur, d.nasabah_sk as nasabah_sk_saat_itu, d.segmen as segmen_saat_itu,
  d.kota as kota_saat_itu, toInt64(dateDiff('day', w.tgl_daftar, w.tgl)) as umur_nasabah_hari,
  toInt64(w.trx_7h) as trx_7h, round(w.nominal_7h, 2) as nominal_7h,
  toInt64(w.trx_30h) as trx_30h, round(w.nominal_30h, 2) as nominal_30h,
  if(w.trx_terakhir is null, NULL, toInt64(dateDiff('day', w.trx_terakhir, w.tgl))) as hari_sejak_trx_terakhir,
  if(w.trx_30h = 0, NULL, round(w.digital_30h / w.trx_30h, 4)) as rasio_digital_30h,
  toInt64(w.belanja_30h) as belanja_ecom_30h, round(w.nom_belanja_30h, 2) as nominal_belanja_ecom_30h,
  toInt64(w.akad_kum) as akad_cair_kumulatif, round(w.pokok_kum, 2) as pokok_cair_kumulatif
from w
asof left join (select nasabah_id, berlaku_dari, nasabah_sk, segmen, kota from {{ ref('dim_nasabah') }}) d
  on w.nasabah_id = d.nasabah_id and w.t_akhir > d.berlaku_dari
