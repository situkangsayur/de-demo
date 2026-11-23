#!/usr/bin/env bash
# Uji bagian data platform (penulis B), dari keadaan bersih sampai `down -v`.
#
#   UJI_MODE=dev  : compose.data.yaml + data/dev/compose.dev.yaml (RustFS + bronze sintetis 45 hari)
#   UJI_MODE=e2e  : seluruh platform (compose.yaml: Sister + ops + data); bronze dari ingestor penulis A
#   UJI_MODE=auto : e2e bila compose.ops.yaml dan aplikasi Sister tersedia, selain itu dev (bawaan)
#
# Langkah: build -> up -> muat awal (run penuh) -> cek silver/gold lake & warehouse -> Q1 (run dua kali)
# -> Q3 (backfill 7 hari dua kali) -> Q2 (data rusak: DQ gagal, gold tetap) -> time travel -> dashboard
# (HTTP 200, empat pertanyaan dari dua sumber identik, fitur AI) -> tiering -> down -v + builder prune.
set -euo pipefail
# Seluruh isi dibungkus satu blok agar bash membaca skrip utuh sebelum menjalankannya
# (aman bila berkas diubah saat uji berjalan).
{
cd "$(dirname "$0")/.."            # folder platform/
PLATFORM=$(pwd)

MODE=${UJI_MODE:-auto}
SISTER=${SISTER_DIR:-../../sister-hpc-demo}
if [ "$MODE" = auto ]; then
  if [ -f compose.ops.yaml ] && [ -f "$SISTER/bab05-microservices-messaging/compose.yaml" ]; then
    MODE=e2e; else MODE=dev; fi
fi

WIB_HARI() { TZ=Asia/Jakarta date -d "$1 day" +%F; }
H1=$(WIB_HARI -1); H2=$(WIB_HARI -2); H8=$(WIB_HARI -8)
LULUS=0; GAGAL=0
ok()   { echo "  [LULUS] $*"; LULUS=$((LULUS + 1)); }
salah(){ echo "  [GAGAL] $*"; GAGAL=$((GAGAL + 1)); }
cek()  { if eval "$2"; then ok "$1"; else salah "$1"; fi; }
judul(){ echo; echo "=== $* ==="; }

# ---------------------------------------------------------------- disk
SISA=$(df -BG --output=avail / | tail -1 | tr -dc 0-9)
while [ "$SISA" -lt 8 ]; do echo "sisa disk ${SISA} GB < 8 GB, menunggu..."; sleep 60; SISA=$(df -BG --output=avail / | tail -1 | tr -dc 0-9); done
echo "sisa disk: ${SISA} GB, mode: $MODE"

# ---------------------------------------------------------------- env sementara (secret acak)
ENVF=$(mktemp)
acak() { openssl rand -hex 16; }
data_env() {   # contoh bagian data dengan secret acak
  sed -e "s/^S3_ACCESS_KEY=.*/S3_ACCESS_KEY=uji$(openssl rand -hex 6)/" -e "s/^S3_SECRET_KEY=.*/S3_SECRET_KEY=$(acak)/" \
      -e "s/^DATA_PG_PASSWORD=.*/DATA_PG_PASSWORD=$(acak)/" -e "s/^AIRFLOW_ADMIN_PASSWORD=.*/AIRFLOW_ADMIN_PASSWORD=$(acak)/" \
      -e "s/^AIRFLOW_JWT_SECRET=.*/AIRFLOW_JWT_SECRET=$(acak)$(acak)/" -e "s/^AIRFLOW_API_SECRET_KEY=.*/AIRFLOW_API_SECRET_KEY=$(acak)/" \
      -e "s/^CLICKHOUSE_PASSWORD=.*/CLICKHOUSE_PASSWORD=$(acak)/" -e "s/^TOKEN_PEPPER=.*/TOKEN_PEPPER=$(acak)/" \
      -e "s/^MUAT_AWAL=.*/MUAT_AWAL=0/" data/.env.data.example
}
if [ "$MODE" = e2e ]; then
  # jangan menyentuh stack lain yang memakai port yang sama (aplikasi Sister, platform yang sedang demo)
  if docker ps --format '{{.Names}}' | grep -qE '^(sister-bab05-|de-platform-[a-z]+-[0-9]+$)'; then
    echo "ada container sister-bab05-* / de-platform-* yang sedang jalan; hentikan dulu (port bentrok)"; exit 1
  fi
  if [ -f .env ]; then
    cp .env "$ENVF"            # platform/.env milik pengguna, hanya diteruskan ke compose
  else                         # gabungan contoh ops + data (nilai lab)
    { cat ops/.env.ops.example; data_env | grep -vE '^(BIND_ADDR|S3_[A-Z_]+)='; } > "$ENVF"
  fi
  echo "MUAT_AWAL=0" >> "$ENVF"
  PROJ=(-p de-platform)
  FILES=(-f compose.yaml)
else
  data_env > "$ENVF"
  PROJ=(-p de-platform-b-uji)
  FILES=(-f compose.data.yaml -f data/dev/compose.dev.yaml)
fi
dc() { docker compose "${PROJ[@]}" "${FILES[@]}" --env-file "$ENVF" "$@"; }
bersih() {
  echo; echo "--- pembersihan ---"
  if [ "${UJI_SIMPAN:-0}" = 1 ]; then echo "UJI_SIMPAN=1: stack dibiarkan menyala (env: $ENVF)"; return; fi
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  docker builder prune -f >/dev/null 2>&1 || true
  rm -f "$ENVF"
}
trap bersih EXIT
uji() { dc exec -T airflow-scheduler bash -c "cd /opt/data && \$PY_DATA -m pipeline.uji_cli $*" | tail -1; }
jq_() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
BIND=$(grep -E '^BIND_ADDR=' "$ENVF" | cut -d= -f2); BIND=${BIND:-127.0.0.1}

# ---------------------------------------------------------------- build + up
judul "build dan up"
dc build 2>&1 | grep -E "Built|ERROR" || true
dc up -d 2>&1 | tail -3
for i in $(seq 1 120); do
  s=$(dc ps --format '{{.Service}} {{.Health}}' 2>/dev/null | grep -E '^(dash-api|airflow-apiserver|lake-engine) ' | grep -c healthy || true)
  [ "$s" -ge 3 ] && break; sleep 5
done
cek "dash-api, airflow-apiserver, lake-engine sehat" '[ "$s" -ge 3 ]'
cek "data-init selesai (namespace, database, lifecycle, cost)" '[ "$(dc ps -a --format "{{.Service}} {{.State}} {{.Status}}" | grep "^data-init " | grep -c "Exited (0)")" = 1 ]'
for i in $(seq 1 60); do dc exec -T airflow-scheduler airflow dags list 2>/dev/null | grep -q bank_harian && break; sleep 5; done

if [ "$MODE" = e2e ]; then
  judul "generator lalu lintas (penulis A): backfill 30 hari lewat aplikasi Sister"
  # nilai kosong pada kolom wajib (kanal) membuat gerbang DQ menghentikan gold setiap hari; untuk uji
  # dimatikan, kotoran lain (duplikat, terlambat, akad tidak valid, profil) tetap aktif
  curl -s -X POST "http://$BIND:52001/api/gen/config" -H 'Content-Type: application/json' \
    -d "$(curl -s "http://$BIND:52001/api/gen/status" | python3 -c "import json,sys; c=json.load(sys.stdin)['config']; c['dirt']['kosong']=0; print(json.dumps(c))")" >/dev/null
  curl -s -X POST "http://$BIND:52001/api/gen/backfill" -H 'Content-Type: application/json' -d '{"days":30,"per_day":150,"with_shop":true}'; echo
  for i in $(seq 1 180); do
    m=$(curl -s "http://$BIND:52001/api/gen/status" | python3 -c "import json,sys; print(json.load(sys.stdin).get('mode') or '')" 2>/dev/null || echo backfill)
    [ "$m" != backfill ] && break; sleep 10
  done
  echo "  status generator: $(curl -s "http://$BIND:52001/api/gen/status" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('mode'), d.get('counts'), d.get('backfill'))" | cut -c1-400)"
  sleep 30   # ingestor flush (10 detik) + CDC
  judul "menunggu bronze dari ingestor (penulis A) untuk $H1"
  for i in $(seq 1 120); do
    n=$(dc exec -T airflow-scheduler bash -c "cd /opt/data && \$PY_DATA -c 'from pipeline import bronze; import datetime as d; print(len(bronze.daftar_berkas(\"transaksi\", [d.date.fromisoformat(\"$H1\")])))'" | tail -1)
    [ "${n:-0}" -gt 0 ] && break; sleep 10
  done
  cek "bronze transaksi dt=$H1 ada (jalankan backfill generator di UI ops bila belum)" '[ "${n:-0}" -gt 0 ]'
fi

# DAG diaktifkan sesudah bronze ada: scheduler langsung membuat satu run terjadwal untuk kemarin
# (max_active_runs=1 mengurutkannya dengan run uji)
uji aktifkan

# ---------------------------------------------------------------- muat awal
judul "muat awal: run penuh s.d. $H1"
R=$(uji trigger "$H1" penuh); echo "$R" | cut -c1-600
cek "run muat awal sukses" '[ "$(echo "$R" | jq_ "d[\"state\"]")" = success ]'
cek "grup etl dan elt sama-sama berjalan" '[ "$(echo "$R" | jq_ "d[\"tasks\"].get(\"etl.gold_publikasi\")+d[\"tasks\"].get(\"elt.dbt_build_gold\")")" = successsuccess ]'
A=$(uji ringkas "$H8" "$H1")
cek "silver dan gold lake terisi 8 hari" '[ "$(echo "$A" | jq_ "len(d[\"lake\"])")" = 8 ] && [ "$(echo "$A" | jq_ "len(d[\"silver\"])")" = 8 ]'
cek "gold warehouse terisi 8 hari" '[ "$(echo "$A" | jq_ "len(d[\"warehouse\"])")" = 8 ]'
cek "gold lake = gold warehouse (baris, total, trx_unik per tgl)" '[ "$(echo "$A" | jq_ "d[\"lake\"]==d[\"warehouse\"]")" = True ]'

# ---------------------------------------------------------------- Q1
judul "Q1: DAG untuk $H1 dijalankan dua kali"
Q1A=$(uji ringkas "$H1" "$H1")
R1=$(uji trigger "$H1"); R2=$(uji trigger "$H1")
Q1B=$(uji ringkas "$H1" "$H1")
echo "  sebelum: $(echo "$Q1A" | jq_ 'd["lake"]')  sesudah: $(echo "$Q1B" | jq_ 'd["lake"]')"
cek "kedua run sukses" '[ "$(echo "$R1" | jq_ "d[\"state\"]")$(echo "$R2" | jq_ "d[\"state\"]")" = successsuccess ]'
cek "baris, total, trx_unik identik di lake dan warehouse" '[ "$(echo "$Q1A" | jq_ "(d[\"lake\"],d[\"warehouse\"],d[\"silver\"])")" = "$(echo "$Q1B" | jq_ "(d[\"lake\"],d[\"warehouse\"],d[\"silver\"])")" ]'
cek "gold mendapat snapshot baru (run benar-benar menulis)" '[ "$(echo "$Q1A" | jq_ "d[\"snapshot_gold\"][\"fakta_transaksi\"]")" != "$(echo "$Q1B" | jq_ "d[\"snapshot_gold\"][\"fakta_transaksi\"]")" ]'

# ---------------------------------------------------------------- Q3
judul "Q3: backfill 7 hari $H8 s.d. $H2, lalu diulang"
B1=$(uji backfill "$H8" "$H2"); echo "  backfill 1: $(echo "$B1" | cut -c1-400)"
Q3A=$(uji ringkas "$H8" "$H2")
B2=$(uji backfill "$H8" "$H2"); echo "  backfill 2: $(echo "$B2" | cut -c1-400)"
Q3B=$(uji ringkas "$H8" "$H2")
cek "backfill 1: 7 run sukses" '[ "$(echo "$B1" | jq_ "sum(1 for r in d[\"runs\"] if r[1]==\"success\")")" = 7 ]'
cek "backfill 2: 7 run sukses" '[ "$(echo "$B2" | jq_ "sum(1 for r in d[\"runs\"] if r[1]==\"success\")")" = 7 ]'
cek "hasil identik setelah backfill ulang (lake, warehouse, silver)" '[ "$(echo "$Q3A" | jq_ "(d[\"lake\"],d[\"warehouse\"],d[\"silver\"])")" = "$(echo "$Q3B" | jq_ "(d[\"lake\"],d[\"warehouse\"],d[\"silver\"])")" ]'
cek "7 partisi, tidak ada yang hilang atau ganda (baris = trx_unik)" '[ "$(echo "$Q3B" | jq_ "len(d[\"lake\"])==7 and all(r[1]==r[3] for r in d[\"lake\"]+d[\"warehouse\"])")" = True ]'
cek "hasil backfill sama dengan muat awal" '[ "$(echo "$Q3B" | jq_ "d[\"lake\"]")" = "$(echo "$A" | jq_ "d[\"lake\"][:7]")" ]'
echo "  waktu: backfill 1 $(echo "$B1" | jq_ 'd["detik"]') s, backfill 2 $(echo "$B2" | jq_ 'd["detik"]') s"

# ---------------------------------------------------------------- Q2
judul "Q2: data rusak disisipkan ke bronze $H1"
G0=$(uji ringkas "$H1" "$H1")
uji sabotase sisipkan "$H1" >/dev/null
RS=$(uji trigger "$H1"); echo "  $(echo "$RS" | jq_ '{k: v for k, v in d["tasks"].items() if v != "success"}')"
G1=$(uji ringkas "$H1" "$H1")
cek "run gagal" '[ "$(echo "$RS" | jq_ "d[\"state\"]")" = failed ]'
cek "dq_silver gagal, gold_siapkan/dq_gold/publikasi tidak jalan (upstream_failed)" '[ "$(echo "$RS" | jq_ "(d[\"tasks\"][\"etl.dq_silver\"], d[\"tasks\"][\"etl.gold_publikasi\"])")" = "('"'"'failed'"'"', '"'"'upstream_failed'"'"')" ]'
cek "dbt_build_gold gagal (test staging), model gold di-SKIP" '[ "$(echo "$RS" | jq_ "d[\"tasks\"][\"elt.dbt_build_gold\"]")" = failed ]'
cek "gold lake tidak berubah (snapshot sama)" '[ "$(echo "$G0" | jq_ "d[\"snapshot_gold\"]")" = "$(echo "$G1" | jq_ "d[\"snapshot_gold\"]")" ]'
cek "gold warehouse tidak berubah (part dan baris sama)" '[ "$(echo "$G0" | jq_ "(d[\"warehouse\"],d[\"part_gold_clickhouse\"])")" = "$(echo "$G1" | jq_ "(d[\"warehouse\"],d[\"part_gold_clickhouse\"])")" ]'
DQ=$(uji dq_gagal); echo "  test gagal: $(echo "$DQ" | jq_ 'sorted({(r[0], r[1]) for r in d})')"
cek "pesan menunjuk aturan dan baris (kanal 'satelit', trx SAB..., konflik app/cdc)" \
  'echo "$DQ" | grep -q satelit && echo "$DQ" | grep -q SAB && echo "$DQ" | grep -q app_cdc'
uji sabotase pulihkan "$H1" >/dev/null
RP=$(uji trigger "$H1"); G2=$(uji ringkas "$H1" "$H1")
cek "setelah dipulihkan run sukses dan angka kembali seperti semula" '[ "$(echo "$RP" | jq_ "d[\"state\"]")" = success ] && [ "$(echo "$G0" | jq_ "(d[\"lake\"],d[\"warehouse\"])")" = "$(echo "$G2" | jq_ "(d[\"lake\"],d[\"warehouse\"])")" ]'

# ---------------------------------------------------------------- time travel
judul "time travel Iceberg"
TT=$(curl -s "http://$BIND:52013/api/time-travel?tabel=gold.fakta_transaksi")
NS=$(echo "$TT" | jq_ 'len(d["snapshot"])')
cek "gold.fakta_transaksi punya >= 3 snapshot ($NS)" '[ "$NS" -ge 3 ]'
TERTUA=$(echo "$TT" | jq_ 'd["snapshot"][-1]["waktu"]')
AO=$(curl -s "http://$BIND:52013/api/time-travel/asof?waktu=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$TERTUA")")
echo "  $(echo "$AO" | jq_ 'd["sql"]')"
cek "query AS OF snapshot tertua menjawab" '[ "$(echo "$AO" | jq_ "len(d[\"baris\"]) > 0")" = True ]'

# ---------------------------------------------------------------- dashboard
judul "dashboard"
for p in / /lineage /airflow /dq /time-travel /tiering /fitur /uji; do
  c=$(curl -s -o /dev/null -w '%{http_code}' "http://$BIND:52012$p")
  cek "UI $p HTTP $c" '[ "$c" = 200 ]'
done
for n in 1 2 3 4; do
  J=$(curl -s "http://$BIND:52012/api/bandingkan/$n?tgl=$H1")
  echo "  Q$n: warehouse $(echo "$J" | jq_ 'len(d["warehouse"]["baris"])') baris $(echo "$J" | jq_ 'round(d["warehouse"]["latency_ms"],1)') ms | lake $(echo "$J" | jq_ 'len(d["lake"]["baris"])') baris $(echo "$J" | jq_ 'round(d["lake"]["latency_ms"],1)') ms"
  # e2e: generator membuat nasabah dengan waktu sekarang, jadi Q3 (nasabah baru s.d. kemarin) boleh kosong
  MIN=1; [ "$MODE" = e2e ] && [ "$n" = 3 ] && MIN=0
  cek "pertanyaan $n dijawab kedua sumber dan identik" '[ "$(echo "$J" | jq_ "d[\"identik\"] and len(d[\"warehouse\"][\"baris\"]) >= $MIN")" = True ]'
done
F=$(curl -s "http://$BIND:52013/api/fitur?tgl=$H1&nasabah_id=1")
cek "fitur_nasabah (tim AI) identik di lake dan warehouse" '[ "$(echo "$F" | jq_ "d[\"identik\"] and len(d[\"warehouse_sampel\"][\"baris\"]) > 0")" = True ]'
D=$(curl -s "http://$BIND:52013/api/dq")
cek "hasil DQ tersimpan, 5 dimensi" '[ "$(echo "$D" | jq_ "len({r[0] for r in d[\"dimensi\"][\"baris\"]})")" = 5 ]'
AF=$(curl -s "http://$BIND:52013/api/airflow/runs")
cek "status run Airflow lewat REST API" '[ "$(echo "$AF" | jq_ "len(d[\"runs\"][\"dag_runs\"]) > 0")" = True ]'

# ---------------------------------------------------------------- tiering
judul "tiering dan cost"
T=$(curl -s "http://$BIND:52013/api/tiering?geser_hari=760")
cek "lifecycle rule terpasang di bucket" '[ "$(echo "$T" | jq_ "any(r.get(\"ID\",\"\").startswith(\"bronze\") for r in d[\"lake\"][\"aturan\"])")" = True ]'
cek "simulasi 760 hari: bronze masuk archive" '[ "$(echo "$T" | jq_ "any(r[\"lapisan\"]==\"bronze\" and r[\"tier\"]==\"archive\" for r in d[\"lake\"][\"simulasi\"][\"ringkasan\"])")" = True ]'
cek "tabel cost 3 tahun (3 skenario x 2 laju x 36 bulan)" '[ "$(echo "$T" | jq_ "len(d[\"cost\"][\"baris\"])")" = 216 ]'

judul "ukuran image"
docker images --format '{{.Repository}}:{{.Tag}} {{.Size}}' | grep -E '^de-platform-(data|dash)' || true
df -h / | tail -1

echo; echo "HASIL: $LULUS lulus, $GAGAL gagal (mode $MODE)"
[ "$GAGAL" = 0 ]
exit
}
