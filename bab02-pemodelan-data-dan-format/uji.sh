#!/usr/bin/env bash
# Uji lab Bab 2 dari keadaan bersih: build, up, cek model/SCD/format/Parquet/evolusi, lalu down -v.
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] || cp .env.example .env
set -a; . ./.env; set +a
B="${BIND_ADDR:-127.0.0.1}"
API="http://$B:52201"; UI="http://$B:52200"
LULUS=0; GAGAL=0
cek() { if eval "$2" >/dev/null 2>&1; then echo "  ok   $1"; LULUS=$((LULUS+1)); else echo "  GAGAL $1"; GAGAL=$((GAGAL+1)); fi; }
bersih() { docker compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap bersih EXIT
py() { python3 -c "import json,sys; d=json.load(sys.stdin); $1"; }

echo "== build dan up"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build --wait --wait-timeout 240

echo "== data dan model"
cek "healthz" "curl -fs $API/healthz | grep -q ok"
cek "tabel OLTP, star, OBT terbentuk" "curl -fs $API/api/ringkasan | py 't=d[\"tabel\"]; assert t[\"transaksi\"][\"baris\"]==t[\"fakta_transaksi\"][\"baris\"]==t[\"obt_transaksi\"][\"baris\"]>0'"
cek "dim_nasabah SCD2 punya lebih banyak baris dari nasabah" "curl -fs $API/api/ringkasan | py 't=d[\"tabel\"]; assert t[\"dim_nasabah\"][\"baris\"]>t[\"nasabah\"][\"baris\"]'"
cek "q1: tiga model hasilnya sama" "curl -fs '$API/api/model/banding?q=q1' | py 'm=d[\"model\"]; assert m[\"oltp\"][\"beda_dengan_star\"]==0 and m[\"obt\"][\"beda_dengan_star\"]==0'"
cek "q2: OLTP berbeda dari star (SCD)" "curl -fs '$API/api/model/banding?q=q2' | py 'm=d[\"model\"]; assert m[\"oltp\"][\"beda_dengan_star\"]>0 and m[\"obt\"][\"beda_dengan_star\"]==0'"
cek "nasabah 7712: Bogor lalu Bandung (SCD2)" "curl -fs $API/api/scd | py 'k=[r[2] for r in d[\"scd2\"]]; assert k==[\"bogor\",\"bandung\"]; assert d[\"scd1\"][0][2]==\"bandung\"'"
cek "pindah kota menambah versi SCD2" "curl -fs -X POST $API/api/scd/pindah -H 'Content-Type: application/json' -d '{\"nasabah_id\":7712,\"kota\":\"surabaya\",\"tanggal\":\"2026-10-01\"}' | py 'assert len(d[\"scd2\"])==3 and d[\"scd1\"][0][2]==\"surabaya\" and d[\"fakta_menunjuk_versi_baru\"]>=0'"
cek "kota tidak sah ditolak 400" "[ \"\$(curl -s -o /dev/null -w '%{http_code}' -X POST $API/api/scd/pindah -H 'Content-Type: application/json' -d '{\"nasabah_id\":7712,\"kota\":\"atlantis\",\"tanggal\":\"2026-11-01\"}')\" = 400 ]"
cek "dampak SCD per kota" "curl -fs $API/api/scd/dampak | py 'assert len(d[\"kota\"])==8 and any(abs(k[\"selisih\"])>0 for k in d[\"kota\"])'"

echo "== format dan Parquet"
curl -fs -X POST "$API/api/format/jalankan" -H 'Content-Type: application/json' -d '{"n":100000}' > /tmp/labA-b02-fmt-$$.json
cek "lima format ditulis" "py 'assert len(d[\"hasil\"])==5' < /tmp/labA-b02-fmt-$$.json"
cek "Parquet zstd lebih kecil dari CSV dan JSON" "py 'h={x[\"format\"]:x for x in d[\"hasil\"]}; assert h[\"Parquet (zstd)\"][\"ukuran_byte\"] < h[\"CSV\"][\"ukuran_byte\"] < h[\"JSON Lines\"][\"ukuran_byte\"]' < /tmp/labA-b02-fmt-$$.json"
cek "agregat Parquet lebih cepat dari CSV" "py 'h={x[\"format\"]:x for x in d[\"hasil\"]}; assert h[\"Parquet (snappy)\"][\"baca_agregat_ms\"] < h[\"CSV\"][\"baca_agregat_ms\"]' < /tmp/labA-b02-fmt-$$.json"
cek "pushdown melewati row group bila urut" "curl -fs '$API/api/parquet/anatomi?urut=true' | py 'assert d[\"row_group_dilewati\"]>=d[\"jumlah_row_group\"]//2 and d[\"byte_dibaca\"]<d[\"byte_total\"]*0.2'"
cek "pushdown tidak melewati apa pun bila acak" "curl -fs '$API/api/parquet/anatomi?urut=false' | py 'assert d[\"row_group_dilewati\"]==0'"
cek "evolusi: aman terbaca, merusak gagal" "curl -fs $API/api/evolusi | py 'r={k[\"kasus\"]:k[\"berhasil\"] for k in d[\"avro\"]}; assert r[\"Tambah kolom opsional (dengan default)\"] and not r[\"Ganti nama kolom kanal -> saluran\"] and r[\"Ganti nama dengan alias (aliases: [kanal])\"] and not r[\"Ganti tipe nominal int -> string\"] and r[\"Perlebar tipe nominal int -> long\"]'"
cek "evolusi Parquet union_by_name" "curl -fs $API/api/evolusi | py 'assert \"catatan\" in d[\"parquet\"][\"kolom\"]'"
cek "platform: mandiri bila endpoint kosong" "curl -fs $API/api/platform | grep -q aktif"

echo "== UI"
for p in / /scd /format /parquet /evolusi; do cek "UI $p 200" "curl -fs -o /dev/null $UI$p"; done
cek "proxy UI ke API" "curl -fs $UI/api/ringkasan | grep -q fakta_transaksi"
cek "log JSON satu baris dengan service" "docker compose logs api --no-log-prefix | grep -m1 '\"service\": \"bab02-api\"' | python3 -c 'import json,sys; json.loads(sys.stdin.readline())'"
rm -f /tmp/labA-b02-fmt-$$.json
echo "== hasil: $LULUS lulus, $GAGAL gagal"
[ "$GAGAL" -eq 0 ]
