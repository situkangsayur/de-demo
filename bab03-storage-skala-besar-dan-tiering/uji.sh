#!/usr/bin/env bash
# Uji lab Bab 3 dari keadaan bersih: build, up, cek object storage, partition, small files, kompresi,
# kelas penyimpanan, lifecycle, simulasi tier, UI, lalu down -v.
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] || cp .env.example .env
set -a; . ./.env; set +a
B="${BIND_ADDR:-127.0.0.1}"
API="http://$B:52301"; UI="http://$B:52300"
LULUS=0; GAGAL=0
cek() { if eval "$2" >/dev/null 2>&1; then echo "  ok   $1"; LULUS=$((LULUS+1)); else echo "  GAGAL $1"; GAGAL=$((GAGAL+1)); fi; }
bersih() { docker compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap bersih EXIT
py() { python3 -c "import json,sys; d=json.load(sys.stdin); $1"; }
post() { local b='{}'; [ -n "${2:-}" ] && b="$2"; curl -fs -X POST "$API$1" -H 'Content-Type: application/json' -d "$b"; }

echo "== build dan up"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build --wait --wait-timeout 300

echo "== object storage"
cek "healthz" "curl -fs $API/healthz | grep -q ok"
cek "RustFS console menjawab" "curl -s -o /dev/null -w '%{http_code}' http://$B:52304/rustfs/console/ | grep -q 200"
cek "30 partisi dt= sebagai CommonPrefix" "curl -fs '$API/api/objek/daftar?prefix=bronze/app/transaksi/' | py 'assert len(d[\"folder\"])==30 and all(\"dt=\" in f for f in d[\"folder\"])'"
cek "PUT, HEAD, GET range, LIST, 404" "post /api/objek/dasar | py 'l=d[\"langkah\"]; assert [x[\"ok\"] for x in l]==[True,True,True,True,False]; assert l[2][\"hasil\"][\"ContentRange\"].startswith(\"bytes 0-63/\")'"
cek "multipart: ETag berakhiran -5" "post /api/objek/multipart '{\"mb\":24,\"part_mb\":5}' | py 'assert d[\"etag\"].strip(chr(34)).endswith(\"-5\") and len(d[\"part\"])==5'"
cek "ganti nama = salin + hapus per objek" "post /api/objek/ganti-nama '{\"jumlah\":50}' | py 'assert d[\"objek\"]==50 and d[\"request\"]==101'"

echo "== partition"
cek "satu partisi: 1 berkas dibuka" "curl -fs $API/api/partisi/query | py 'assert d[\"berkas_dibuka\"]==1 and d[\"baris\"]>0'"
cek "semua partisi: 30 berkas dibuka" "curl -fs '$API/api/partisi/query?semua=true' | py 'assert d[\"berkas_dibuka\"]==30'"

echo "== small files dan kompresi"
post /api/kecil/jalankan '{"jumlah":[1,10,100]}' > /tmp/labA-b03-kecil-$$.json
cek "jumlah berkas sesuai skenario" "py 'assert [h[\"berkas\"] for h in d[\"hasil\"]]==[1,10,100]' < /tmp/labA-b03-kecil-$$.json"
cek "100 berkas lebih lambat dibaca dan lebih mahal dari 1 berkas" "py 'h=d[\"hasil\"]; assert h[2][\"baca_ms\"]>h[0][\"baca_ms\"] and h[2][\"biaya_request_usd_per_bulan\"]>h[0][\"biaya_request_usd_per_bulan\"]' < /tmp/labA-b03-kecil-$$.json"
cek "compaction 100 berkas menjadi 1" "post /api/kecil/compaction | py 'assert d[\"berkas_sebelum\"]==100 and d[\"berkas_sesudah\"]==1'"
cek "kompresi: zstd lebih kecil dari CSV dan Parquet tanpa kompresi" "post /api/kompresi | py 'h={x[\"skenario\"]:x[\"byte\"] for x in d[\"hasil\"]}; assert h[\"Parquet zstd (level 3)\"] < h[\"Parquet tanpa kompresi\"] < h[\"CSV\"]'"

echo "== kelas, lifecycle, tier"
cek "kelas: STANDARD diterima, GLACIER ditolak" "post /api/kelas/coba | py 'k={x[\"kelas\"]:x[\"diterima\"] for x in d[\"hasil\"]}; assert k[\"STANDARD\"] and not k[\"GLACIER\"]'"
cek "lifecycle: Transition ditolak, Expiration terpasang" "post /api/lifecycle/pasang | py 'l=d[\"langkah\"]; assert not l[0][\"diterima\"] and l[-1][\"diterima\"]; assert any(\"Expiration\" in r for r in d[\"terpasang\"])'"
cek "simulasi: hari ini hampir semua hot (umur 1-30 hari)" "post '/api/tier/simulasi?geser_hari=0' | py 't=[o[\"tier\"] for o in d[\"objek\"]]; assert set(t)<={\"hot\",\"warm\"} and t.count(\"hot\")>=29'"
cek "simulasi: +400 hari menjadi cold dan diberi tag" "post '/api/tier/simulasi?geser_hari=400&tandai=true' | py 'assert {o[\"tier\"] for o in d[\"objek\"]}<={\"warm\",\"cold\"} and d[\"tag_contoh\"][0][\"Key\"]==\"tier-simulasi\"'"
cek "simulasi: +3700 hari dihapus" "post '/api/tier/simulasi?geser_hari=3700' | py 'assert {o[\"tier\"] for o in d[\"objek\"]}=={\"dihapus\"}'"
cek "platform: mandiri bila endpoint kosong" "curl -fs $API/api/platform/bronze | grep -q aktif"

echo "== UI"
for p in / /partisi /file-kecil /kompresi /durabilitas /tiering; do cek "UI $p 200" "curl -fs -o /dev/null $UI$p"; done
cek "proxy UI ke API" "curl -fs '$UI/api/objek/daftar?prefix=' | grep -q bronze"
cek "log JSON satu baris dengan service" "docker compose logs api --no-log-prefix | grep -m1 '\"service\": \"bab03-api\"' | python3 -c 'import json,sys; json.loads(sys.stdin.readline())'"
rm -f /tmp/labA-b03-kecil-$$.json
echo "== hasil: $LULUS lulus, $GAGAL gagal"
[ "$GAGAL" -eq 0 ]
