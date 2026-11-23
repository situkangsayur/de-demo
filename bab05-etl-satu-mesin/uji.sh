#!/usr/bin/env bash
# Uji otomatis lab Bab 5 (ETL satu mesin) dari keadaan bersih:
#   1) build dan nyalakan api (benchmark, mem_limit) + ui,
#   2) dataset 200 ribu (otomatis) dan 1 juta baris; benchmark enam engine, hasil harus identik,
#   3) query plan (pushdown), metadata Parquet, zero-copy, batas memori + spill DuckDB,
#   4) pipeline bronze -> silver -> gold dua kali (idempotent), token Python = token DuckDB,
#   5) halaman UI melayani HTTP 200, lalu down -v (selalu, lewat trap).
# UJI_BARIS=10000000 bash uji.sh untuk menguji dataset 10 juta (lebih lama, ±220 MB di volume).
set -euo pipefail
cd "$(dirname "$0")"

[ -f .env ] || cp .env.example .env
set -a; . ./.env; set +a
BIND_ADDR=${BIND_ADDR:-127.0.0.1}
UI=http://$BIND_ADDR:52500
API=$UI/api
N=${UJI_BARIS:-1000000}

lolos=0; gagal=0
ok()   { echo "  [LOLOS] $*"; lolos=$((lolos+1)); }
salah(){ echo "  [GAGAL] $*"; gagal=$((gagal+1)); }
cek()  { if [ "$1" = "1" ]; then ok "$2"; else salah "$2"; fi; }
bersih() { echo "== Membersihkan (down -v)"; docker compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap bersih EXIT

py() { python3 -c "import json,sys; d=json.loads(sys.stdin.read(), strict=False); print($1)"; }
get()  { curl -sf "$API/$1"; }
post() { curl -s -X POST "$API/$1"; }
tunggu_job() { # menunggu pekerjaan latar selesai, mencetak JSON job
  local j
  for i in $(seq 1 400); do
    j=$(get job); echo "$j" | grep -q '"status":"berjalan"' || break; sleep 2
  done
  echo "$j"
}

echo "== Keadaan bersih dan build"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build >/dev/null
for i in $(seq 1 120); do curl -sf -o /dev/null "$UI" && break; sleep 2; done
curl -sf -o /dev/null "$UI" && ok "UI menyala di $UI" || { salah "UI tidak menyala"; exit 1; }

echo "== Dataset"
I=$(get info)
cek "$(echo "$I" | py "int(d['dataset'][0]['ada'] and d['dataset'][0]['berkas'] == 30)")" "dataset 200 ribu baris tersedia otomatis (30 berkas)"
cek "$(echo "$I" | py "int(d['batas_memori_mb'] is not None and d['batas_memori_mb'] <= 4096)")" "container benchmark dibatasi mem_limit ($(echo "$I" | py "d['batas_memori_mb']") MB)"
post "dataset?baris=$N" >/dev/null
J=$(tunggu_job)
cek "$(echo "$J" | py "int(d['status'] == 'selesai' and d['hasil']['ada'])")" "dataset $N baris dibangkitkan"

echo "== Benchmark enam engine"
post "benchmark?baris=$N&ulang=3" >/dev/null
J=$(tunggu_job)
echo "$J" | py "'\n'.join(f\"     {h['engine']:<17} {h['median_ms']} ms  {h['puncak_mb']} MB\" for h in d['hasil']['hasil'])"
cek "$(echo "$J" | py "int(d['status'] == 'selesai' and all(h['ok'] for h in d['hasil']['hasil']))")" "keenam engine selesai"
cek "$(echo "$J" | py "int(d['hasil']['hasil_sama'])")" "hasil keenam engine identik (checksum total nominal)"
cek "$(echo "$J" | py "int({h['engine']: h['puncak_mb'] for h in d['hasil']['hasil']}['polars_lazy'] < {h['engine']: h['puncak_mb'] for h in d['hasil']['hasil']}['polars_eager'])")" "Polars lazy memakai memori lebih kecil daripada eager"
cek "$(get benchmark/riwayat | py "int(len(d['tabel']['baris']) == 6)")" "tabel praktikum terisi"

echo "== Query plan dan Parquet"
R=$(get "rencana?baris=$N")
cek "$(echo "$R" | py "int(any('SELECTION' in b['teks'] for b in d['polars_optimasi']) and not any('SELECTION' in b['teks'] for b in d['polars_tanpa_optimasi']))")" "Polars optimizer mendorong filter ke scan (predicate pushdown)"
cek "$(echo "$R" | py "int(any('3/13 COLUMNS' in b['teks'] for b in d['polars_optimasi']))")" "Polars hanya membaca 3 dari 13 kolom (projection pushdown)"
cek "$(echo "$R" | py "int(any('Filters' in b['teks'] for b in d['duckdb']))")" "DuckDB EXPLAIN: filter di READ_PARQUET"
cek "$(echo "$R" | py "int(any('pruning_predicate' in b['teks'] for b in d['datafusion']))")" "DataFusion EXPLAIN: pruning_predicate"
cek "$(echo "$R" | py "int(d['parquet']['rg_dibaca'] < d['parquet']['rg_total'] and d['parquet']['byte_dipakai'] < d['parquet']['byte_total'] / 3)")" "row group dilewati dan byte kolom terpakai < 1/3"

echo "== Zero-copy"
post "zerocopy?baris=$N" >/dev/null
J=$(tunggu_job)
cek "$(echo "$J" | py "int([l['alamat_sama'] for l in d['hasil']['langkah'] if l['alamat_sama'] is not None] == [True, False, True, False])")" "alamat buffer: from_arrow(rechunk=False) dan ArrowDtype sama; rechunk dan NumPy disalin"

echo "== Batas memori"
post "memori?baris=$N&anggaran_mb=256" >/dev/null
J=$(tunggu_job)
cek "$(echo "$J" | py "int(d['status'] == 'selesai' and len(d['hasil']['hasil']) == 4)")" "query berat dijalankan empat engine"
cek "$(echo "$J" | py "int({r['engine']: r for r in d['hasil']['hasil']}['polars_streaming']['puncak_mb'] < {r['engine']: r for r in d['hasil']['hasil']}['polars_eager']['puncak_mb'])")" "Polars streaming lebih hemat memori daripada eager"
cek "$(echo "$J" | py "int({r['engine']: r for r in d['hasil']['hasil']}['duckdb']['ok'])")" "DuckDB selesai dengan memory_limit 256MB"

echo "== Pipeline bronze -> silver -> gold"
cek "$(post "pipeline/bronze?baris=300000" | py "int(d['baris_app'] > 0 and d['baris_cdc'] > 0)")" "bronze app + cdc dibangkitkan"
P1=$(post pipeline/jalankan)
cek "$(echo "$P1" | py "int(d['lulus'] and d['dedup']['transaksi_unik'] == 300000 and d['dedup']['baris_bronze'] > 300000)")" "dedup app vs cdc: 300.000 transaksi unik"
cek "$(echo "$P1" | py "int(set(d['dedup']['sumber']) == {'app', 'cdc', 'app+cdc'})")" "_source mencatat app, cdc, app+cdc"
cek "$(echo "$P1" | py "int(d['token']['identik'])")" "token UDF Python = token DuckDB sha256"
P2=$(post pipeline/jalankan)
cek "$(python3 -c "import json,sys; a=json.loads(sys.argv[1]); b=json.loads(sys.argv[2]); print(int(a['lapisan']==b['lapisan'] and a['lapisan']['silver']==300000))" "$P1" "$P2")" "jalankan ulang: silver dan gold per tanggal tidak berubah (idempotent)"
cek "$(get pipeline/status | py "int(len(d['gold']['baris']) > 0)")" "gold fakta_trx_harian terbaca"

echo "== Halaman UI"
for p in / /rencana /zero-copy /memori /pipeline; do
  kode=$(curl -s -o /dev/null -w '%{http_code}' "$UI$p")
  cek "$([ "$kode" = 200 ] && echo 1 || echo 0)" "GET $p -> $kode"
done
cek "$(docker compose logs api 2>/dev/null | grep -c '"service": "bab05-api"' | awk '{print ($1>0)}')" "log JSON satu baris dari api"

echo "== Ringkasan: $lolos lolos, $gagal gagal"
[ "$gagal" = 0 ]
