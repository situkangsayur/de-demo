#!/usr/bin/env bash
# Uji lab P07 dari keadaan bersih: up -> muat data ke 4 store -> benchmark pola akses -> eksperimen tulis
# -> row vs columnar -> serving fitur -> OpenSearch 2 node -> hash chain -> UI -> down -v (trap).
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] || cp .env.example .env
set -a; . ./.env; set +a
B=${BIND_ADDR:-127.0.0.1}
API="http://$B:52701/api"
UI="http://$B:52700"
LULUS=0; GAGAL=0

bersihkan() {
  [ -n "${LOG_UJI:-}" ] && docker compose logs --no-color > "$LOG_UJI" 2>&1
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap bersihkan EXIT
ok()    { echo "  LULUS  $*"; LULUS=$((LULUS+1)); }
salah() { echo "  GAGAL  $*"; GAGAL=$((GAGAL+1)); }
cek()   { if eval "$2"; then ok "$1"; else salah "$1"; fi; }
py()    { python3 -c "import json,sys; d=json.load(sys.stdin); $1"; }
post()  { curl -sS -XPOST "$API/$1" -H 'Content-Type: application/json' -d "$2"; }
lt()    { python3 -c "import sys; sys.exit(0 if float('$1') < float('$2') else 1)"; }

echo "== df -h /"; df -h / | tail -1
echo "== build dan up (bersih)"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build --wait

echo "== 1. muat data ke PostgreSQL, ClickHouse, Redis, OpenSearch"
for _ in $(seq 1 120); do
  s=$(curl -fsS "$API/status" | py "print('siap' if d['siap'] else ('galat' if d['muat'].get('galat') else 'muat'))")
  [ "$s" != muat ] && break; sleep 5
done
cek "data siap ($(curl -fsS "$API/status" | py "print(d['muat']['jumlah'])"))" '[ "$s" = siap ]'
cek "throughput bulk tercatat untuk 4 store" '[ "$(curl -fsS "$API/status" | py "print(len(d[\"muat\"][\"throughput_bulk\"]))")" = 4 ]'

echo "== 2. benchmark semua pola (n=100)"
BJ=$(mktemp); post bench '{"pola":"semua","n":100}' > "$BJ"
p50() { py "print([h['p50_ms'] for h in d['hasil'] if h['pola']=='$1' and h['store']=='$2' and h['engine'].startswith('$3')][0])" < "$BJ"; }
baca() { py "print([h.get('baca','') for h in d['hasil'] if h['pola']=='$1' and h['engine'].startswith('$2')][0].split()[0])" < "$BJ"; }
cek "tidak ada galat benchmark" '[ "$(py "print(sum(1 for h in d[\"hasil\"] if h.get(\"galat\")))" < "$BJ")" = 0 ]'
cek "point: Redis ($(p50 point Redis "")) < ClickHouse ($(p50 point ClickHouse ""))" 'lt "$(p50 point Redis "")" "$(p50 point ClickHouse "")"'
cek "point: PostgreSQL B-tree ($(p50 point PostgreSQL "")) < ClickHouse" 'lt "$(p50 point PostgreSQL "")" "$(p50 point ClickHouse "")"'
cek "range: ClickHouse ORDER BY cocok membaca lebih sedikit baris ($(baca range "ORDER BY (nasabah") < $(baca range "ORDER BY (cabang"))" 'lt "$(baca range "ORDER BY (nasabah")" "$(baca range "ORDER BY (cabang")"'
cek "agregasi: ClickHouse ($(p50 agregasi ClickHouse "")) < PostgreSQL ($(p50 agregasi PostgreSQL ""))" 'lt "$(p50 agregasi ClickHouse "")" "$(p50 agregasi PostgreSQL "")"'
cek "teks: OpenSearch ($(p50 teks OpenSearch "")) < PostgreSQL ILIKE ($(p50 teks PostgreSQL ILIKE))" 'lt "$(p50 teks OpenSearch "")" "$(p50 teks PostgreSQL ILIKE)"'
cek "teks: PostgreSQL GIN ($(p50 teks PostgreSQL GIN)) < ILIKE" 'lt "$(p50 teks PostgreSQL GIN)" "$(p50 teks PostgreSQL ILIKE)"'
cek "fitur: Redis ($(p50 fitur Redis "")) < ClickHouse hitung langsung ($(p50 fitur ClickHouse hitung))" 'lt "$(p50 fitur Redis "")" "$(p50 fitur ClickHouse hitung)"'

echo "== 3. eksperimen tulis satu baris per operasi (n=200)"
for st in postgres clickhouse redis opensearch; do
  r=$(post engine/tulis "{\"store\":\"$st\",\"n\":200}")
  a=$(echo "$r" | py "print(round(d.get('amplifikasi_tulis',0),2))")
  cek "$st: amplifikasi tulis ${a}x, $(echo "$r" | py "print(round(d.get('ops_per_detik',0)))") ops/detik" 'lt 0 "$a"'
  [ "$st" = clickhouse ] && cek "clickhouse: 200 INSERT = $(echo "$r" | py "print(int(d['metrik']['part_baru']))") part baru, merge $(echo "$r" | py "print(int(d['metrik']['merge']))")" '[ "$(echo "$r" | py "print(int(d[\"metrik\"][\"part_baru\"]))")" = 200 ]'
done
cek "B-tree transaksi_pkey: $(curl -fsS "$API/engine/btree" | py "print([b['kedalaman'] for b in d if b['indeks']=='transaksi_pkey'][0])") level" '[ "$(curl -fsS "$API/engine/btree" | py "print([b[\"kedalaman\"] for b in d if b[\"indeks\"]==\"transaksi_pkey\"][0])")" -ge 2 ]'
cek "reset tabel uji" '[ "$(post engine/reset "{}" | py "print(d.get(\"status\"))")" = ok ]'

echo "== 4. row vs columnar"
cek "ukuran per kolom ClickHouse terbaca" '[ "$(curl -fsS "$API/kolom/ukuran" | py "print(sum(1 for c in d[\"clickhouse\"] if c[\"terkompresi\"]>0))")" -ge 9 ]'
r=$(post kolom/banding '{"jenis":"agregasi"}')
pg=$(echo "$r" | py "print([h['byte_dibaca'] for h in d['hasil'] if h['store']=='PostgreSQL'][0])")
ch=$(echo "$r" | py "print([h['byte_dibaca'] for h in d['hasil'] if h['store']=='ClickHouse'][0])")
cek "agregasi: byte dibaca ClickHouse ($ch) < PostgreSQL ($pg)" 'lt "$ch" "$pg"'

echo "== 5. serving fitur_nasabah"
cek "sinkron gold -> Redis + PostgreSQL ($(post fitur/sinkron '{}' | py "print(d['baris'])") nasabah)" '[ "$(post fitur/sinkron "{}" | py "print(d[\"baris\"])")" -gt 0 ]'
cek "lookup Redis fitur:7 berisi trx_30h" '[ "$(curl -fsS "$API/fitur/7" | py "print([h for h in d[\"hasil\"] if h[\"store\"]==\"Redis\"][0][\"data\"].get(\"trx_30h\",\"\") != \"\")")" = True ]'

echo "== 6. OpenSearch 2 node"
C=$(curl -fsS "$API/search/cluster")
cek "cluster green, 2 node" '[ "$(echo "$C" | py "print(d[\"health\"][\"status\"], d[\"health\"][\"number_of_nodes\"])")" = "green 2" ]'
cek "shard transaksi tersebar di 2 node (primary + replica)" '[ "$(echo "$C" | py "print(len({s[\"node\"] for s in d[\"shards\"] if s[\"index\"]==\"transaksi\"}), len([s for s in d[\"shards\"] if s[\"index\"]==\"transaksi\"]))")" = "2 4" ]'
cek "disk threshold dimatikan" 'echo "$C" | grep -q "\"threshold_enabled\": *\"false\""'
cek "cari 'zakat wakaf' menemukan dokumen" '[ "$(curl -fsS "$API/search/cari?q=zakat%20wakaf" | py "print(d[\"opensearch\"][\"total\"])")" -gt 0 ]'

echo "== 7. hash chain"
post ledger/reset '{}' >/dev/null
cek "rantai awal utuh dan sama dengan jangkar" '[ "$(curl -fsS "$API/ledger" | py "print(d[\"rantai_utuh\"], d[\"cocok_jangkar\"])")" = "True True" ]'
cek "UPDATE langsung entri 2 terdeteksi" '[ "$(post ledger/sunting "{\"id\":2,\"nilai\":1000000}" | py "print(d[\"rantai_utuh\"], [e[\"id\"] for e in d[\"entri\"] if not e[\"isi_cocok\"]])")" = "False [2]" ]'
cek "rantai dihitung ulang: tampak utuh, tetapi tidak cocok jangkar" '[ "$(post ledger/tutupi "{}" | py "print(d[\"rantai_utuh\"], d[\"cocok_jangkar\"])")" = "True False" ]'
post ledger/reset '{}' >/dev/null

echo "== 8. UI"
for p in / /engine /kolom /fitur /search /ledger; do
  c=$(curl -s -o /dev/null -w '%{http_code}' "$UI$p")
  cek "UI $p HTTP $c" '[ "$c" = 200 ]'
done
cek "API lewat proxy UI (/api/bench)" '[ "$(curl -fsS "$UI/api/bench" | py "print(len(d[\"hasil\"]))")" -ge 15 ]'

echo "== ringkasan: $LULUS lulus, $GAGAL gagal"
[ "$GAGAL" -eq 0 ]
