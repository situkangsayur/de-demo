#!/usr/bin/env bash
# Uji lab Bab 1 dari keadaan bersih: build, up, cek endpoint dan alur kotoran, lalu down -v.
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] || cp .env.example .env
# percepat batch selama uji
export FLUSH_DETIK=1 BATCH_DETIK=3
set -a; . ./.env; set +a
export FLUSH_DETIK=1 BATCH_DETIK=3
B="${BIND_ADDR:-127.0.0.1}"
API="http://$B:52101"; UI="http://$B:52100"
LULUS=0; GAGAL=0
cek() { if eval "$2" >/dev/null 2>&1; then echo "  ok   $1"; LULUS=$((LULUS+1)); else echo "  GAGAL $1"; GAGAL=$((GAGAL+1)); fi; }
bersih() { docker compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap bersih EXIT

echo "== build dan up"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build --wait --wait-timeout 180

kirim() { curl -s -o /tmp/labA-b01-$$.json -w '%{http_code}' -X POST "$API/api/transaksi" -H 'Content-Type: application/json' -d "$1"; }
trx() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["trx_id"][-1])' /tmp/labA-b01-$$.json; }

echo "== endpoint dasar"
cek "healthz" "curl -fs $API/healthz | grep -q ok"
cek "status memuat 7 tahap" "curl -fs $API/api/status | python3 -c 'import json,sys; d=json.load(sys.stdin); assert len(d[\"hitung\"])==7'"
cek "backfill 7 hari ada di gold" "curl -fs $API/api/dashboard | python3 -c 'import json,sys; d=json.load(sys.stdin); assert len(d[\"per_tanggal\"])>=7'"
cek "UI halaman utama 200" "curl -fs -o /dev/null $UI/"
cek "UI dashboard 200" "curl -fs -o /dev/null $UI/dashboard"
cek "UI peran 200" "curl -fs -o /dev/null $UI/peran"
cek "proxy UI ke API" "curl -fs $UI/api/status | grep -q hitung"

echo "== satu transaksi bersih sampai dashboard"
cek "kirim transaksi 201" "[ \"\$(kirim '{\"rekening_id\":1,\"kanal\":\"Mobile Banking\",\"jenis\":\"setor\",\"nominal\":\"150000\"}')\" = 201 ]"
T1=$(trx)
sleep 5; curl -fs "$API/api/dashboard" >/dev/null; sleep 1
cek "jejak $T1 selesai sampai dashboard" "curl -fs $API/api/jejak/$T1 | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d[\"jejak\"][\"status\"]==\"selesai\", d[\"jejak\"]; assert \"dashboard\" in d[\"latensi_ms\"]'"
cek "bronze punya 8 kolom metadata dan dua sumber" "curl -fs $API/api/jejak/$T1 | python3 -c 'import json,sys; d=json.load(sys.stdin); b=d[\"bronze\"]; assert {x[\"_source\"] for x in b}=={\"app\",\"cdc\"}; assert all(k in b[0] for k in [\"_ingested_at\",\"_source\",\"_batch_id\",\"_event_id\",\"_event_type\",\"_occurred_at\",\"_ts_ms\",\"_op\"])'"
cek "silver menstandarkan kanal" "curl -fs $API/api/jejak/$T1 | grep -q '\"kanal\":\"mobile\"'"

echo "== kesalahan tertangkap di tahap yang tepat"
cek "masa depan ditolak 400" "[ \"\$(kirim '{\"kanal\":\"mobile\",\"nominal\":\"1000\",\"geser_menit\":30}')\" = 400 ]"
cek "saldo kurang ditolak 409" "[ \"\$(kirim '{\"rekening_id\":2,\"kanal\":\"atm\",\"jenis\":\"tarik\",\"nominal\":\"999999999999\"}')\" = 409 ]"
kirim '{"rekening_id":3,"kanal":"atm","jenis":"setor","nominal":""}' >/dev/null; T2=$(trx)
kirim '{"rekening_id":4,"kanal":"qris","jenis":"setor","nominal":"5000"}' >/dev/null; T3=$(trx)
kirim '{"rekening_id":5,"kanal":"web","jenis":"setor","nominal":"50000","kirim_ulang":"tanpa_kunci"}' >/dev/null; T4=$(trx)
kirim '{"rekening_id":6,"kanal":"mobile","jenis":"setor","nominal":"70000","ack_hilang":true,"ulang_publish":true}' >/dev/null
kirim '{"rekening_id":7,"kanal":"teller","jenis":"setor","nominal":"250000","geser_menit":-1560}' >/dev/null; T5=$(trx)
sleep 5
cek "nominal kosong dikarantina" "curl -fs $API/api/jejak/$T2 | grep -q '\"status\":\"karantina\"'"
cek "kanal tidak dikenal dikarantina" "curl -fs $API/api/jejak/$T3 | grep -q 'kanal tidak dikenal'"
cek "request ulang tanpa kunci ditandai duplikat bisnis" "curl -fs $API/api/jejak/$T4 | grep -q 'dugaan duplikat bisnis'"
cek "data terlambat diberi flag" "curl -fs $API/api/jejak/$T5 | grep -q 'data terlambat'"
cek "redelivery dan publish ulang terhitung" "curl -fs $API/api/status | python3 -c 'import json,sys; h=json.load(sys.stdin)[\"hitung\"]; assert h[\"silver\"][\"tertangkap\"][\"duplikat event_id (redelivery)\"]>=1; assert h[\"event_app\"][\"tertangkap\"][\"publish ulang, Nats-Msg-Id sama (dibuang JetStream)\"]>=1'"
cek "rekonsiliasi silver vs gold cocok" "curl -fs $API/api/dashboard | grep -q '\"cocok\":true'"
cek "generator dapat dinyalakan" "curl -fs -X POST $API/api/konfig -H 'Content-Type: application/json' -d '{\"generator_aktif\":true,\"generator_laju\":10,\"generator_persen_kotor\":20,\"flush_detik\":1,\"batch_detik\":3}' | grep -q '\"generator_aktif\":true'"
sleep 4
cek "generator menghasilkan transaksi" "curl -fs $API/api/status | python3 -c 'import json,sys; assert json.load(sys.stdin)[\"hitung\"][\"aplikasi\"][\"masuk\"]>15'"
cek "endpoint platform menjawab (mandiri)" "curl -fs $API/api/platform | grep -q aktif"
cek "log JSON satu baris dengan service" "docker compose logs api --no-log-prefix | grep -m1 '\"service\":\"bab01-api\"' | python3 -c 'import json,sys; json.loads(sys.stdin.readline())'"
rm -f /tmp/labA-b01-$$.json
echo "== hasil: $LULUS lulus, $GAGAL gagal"
[ "$GAGAL" -eq 0 ]
