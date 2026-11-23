#!/usr/bin/env bash
# Uji lab P06 dari keadaan bersih: up -> run terjadwal -> idempotency -> retry/backoff -> gerbang DQ
# (sabotase) -> backfill -> sensor + Deadline Alert (SLA) -> UI dan lineage -> down -v (trap).
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] || cp .env.example .env
set -a; . ./.env; set +a
export SLA_MENIT=2                     # SLA pendek agar Deadline Alert teruji dalam beberapa menit
B=${BIND_ADDR:-127.0.0.1}
API="http://$B:52601/api"
UI="http://$B:52600"
LULUS=0; GAGAL=0

bersihkan() {
  [ -n "${LOG_UJI:-}" ] && docker compose logs --no-color > "$LOG_UJI" 2>&1
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap bersihkan EXIT

ok()    { echo "  LULUS  $*"; LULUS=$((LULUS+1)); }
salah() { echo "  GAGAL  $*"; GAGAL=$((GAGAL+1)); }
cek()   { if eval "$2"; then ok "$1"; else salah "$1"; fi; }
j()     { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
post()  {  # galat API dicetak tetapi tidak menghentikan skrip; pemeriksaan sesudahnya yang menentukan
  local out; out=$(curl -sS -XPOST "$API/$1" -H 'Content-Type: application/json' -d "$2" -w ' HTTP%{http_code}')
  case "$out" in *HTTP2??) echo "$out" ;; *) echo "  (POST $1 -> $out)" >&2; echo "$out" ;; esac
}

tgl() { date -u -d "+7 hours -$1 day" +%F; }     # tanggal WIB, N hari lalu
D1=$(tgl 1); D2=$(tgl 2); D3=$(tgl 3); D4=$(tgl 4); D5=$(tgl 5); D6=$(tgl 6); D7=$(tgl 7)

# info run terbaru untuk tanggal: "<state> <end_date> <run_id>"
run_info() {
  curl -fsS "$API/runs?limit=30" | python3 -c "
import json,sys
rs=[r for r in json.load(sys.stdin)['runs'] if r['tgl']=='$1']
r=rs[0] if rs else None
print(r['state'], r['end_date'], r['dag_run_id']) if r else print('- - -')"
}
# tunggu run tanggal $1 selesai (success/failed) dengan end_date berbeda dari $2
tunggu_run() {
  local t=$1 lama=${2:--} batas=${3:-300} i=0
  while [ $i -lt "$batas" ]; do
    read -r st end id < <(run_info "$t")
    if { [ "$st" = success ] || [ "$st" = failed ]; } && [ "$end" != "$lama" ]; then echo "$st $id"; return 0; fi
    sleep 5; i=$((i+5))
  done
  echo "timeout -"; return 0
}
ti() {  # state/try task $2 pada run tanggal $1
  curl -fsS "$API/runs?limit=30" | python3 -c "
import json,sys
r=[r for r in json.load(sys.stdin)['runs'] if r['tgl']=='$1'][0]['tasks'].get('$2',{})
print(r.get('state'), r.get('try_number'))"
}
gold() { curl -fsS "$API/gold" | python3 -c "
import json,sys
r=[x for x in json.load(sys.stdin)['per_tgl'] if x['tgl']=='$1']
r=r[0] if r else {}
print(r.get('$2','-'))"; }

echo "== df -h /"; df -h / | tail -1
echo "== build dan up (bersih)"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build --wait

echo "== 1. run terjadwal (catchup=False -> interval kemarin $D1)"
read -r st id < <(tunggu_run "$D1" - 300)
cek "run terjadwal $D1 sukses ($id)" '[ "$st" = success ]'
cek "gold $D1 = kontrol core banking" '[ "$(gold "$D1" gold_trx)" = "$(gold "$D1" kontrol_trx)" ]'
SIDIK=$(gold "$D1" gold_sidik)

echo "== 2. idempotency: jalankan ulang $D1 (clear)"
read -r _ lama _ < <(run_info "$D1")
post jalankan "{\"tgl\":\"$D1\"}" >/dev/null
read -r st _ < <(tunggu_run "$D1" "$lama")
cek "run ulang sukses" '[ "$st" = success ]'
cek "sidik gold tetap ($SIDIK)" '[ "$(gold "$D1" gold_sidik)" = "$SIDIK" ]'
cek "publikasi tercatat 2 kali" '[ "$(gold "$D1" publikasi)" = 2 ]'
cek "gold_naif menggandakan baris (2 INSERT)" '[ "$(gold "$D1" naif_insert)" = 2 ]'

echo "== 3. retry + exponential backoff: core banking flaky, run $D2"
post kendali '{"aksi":"flaky_on"}' >/dev/null
post jalankan "{\"tgl\":\"$D2\"}" >/dev/null
read -r st _ < <(tunggu_run "$D2" -)
post kendali '{"aksi":"flaky_off"}' >/dev/null
read -r s tr < <(ti "$D2" ambil_kontrol_core_banking)
cek "run $D2 sukses walau sumber 503 dua kali" '[ "$st" = success ]'
cek "ambil_kontrol_core_banking sukses pada percobaan ke-$tr (>=3)" '[ "$s" = success ] && [ "$tr" -ge 3 ]'

echo "== 4. gerbang data quality: sisipkan data rusak $D3"
post kendali "{\"aksi\":\"sabotase\",\"tgl\":\"$D3\"}" >/dev/null
post jalankan "{\"tgl\":\"$D3\"}" >/dev/null
read -r st _ < <(tunggu_run "$D3" -)
cek "run $D3 gagal" '[ "$st" = failed ]'
cek "dbt_test failed" '[ "$(ti "$D3" dbt_test | cut -d" " -f1)" = failed ]'
cek "publikasi_gold upstream_failed" '[ "$(ti "$D3" publikasi_gold | cut -d" " -f1)" = upstream_failed ]'
cek "gold $D3 tidak dipublikasikan" '[ "$(gold "$D3" gold_trx)" = - ]'
DIM=$(curl -fsS "$API/dq" | j "len({h['dimensi'] for h in d['hasil'] if h['status'] in ('fail','error')})")
cek "test gagal mencakup $DIM dimensi (>=5)" '[ "$DIM" -ge 5 ]'
cek "gold_naif $D3 tercemar ($(gold "$D3" naif_trx) > $(gold "$D3" kontrol_trx))" '[ "$(gold "$D3" naif_trx)" -gt "$(gold "$D3" kontrol_trx)" ]'
post kendali "{\"aksi\":\"pulihkan\",\"tgl\":\"$D3\"}" >/dev/null
read -r _ lama _ < <(run_info "$D3")
post jalankan "{\"tgl\":\"$D3\"}" >/dev/null
read -r st _ < <(tunggu_run "$D3" "$lama")
cek "sesudah dipulihkan: run sukses dan gold = kontrol" '[ "$st" = success ] && [ "$(gold "$D3" gold_trx)" = "$(gold "$D3" kontrol_trx)" ]'

echo "== 5. backfill $D6 .. $D4"
post backfill "{\"dari\":\"$D6\",\"sampai\":\"$D4\"}" >/dev/null
for t in "$D6" "$D5" "$D4"; do
  read -r st id < <(tunggu_run "$t" - 300)
  cek "backfill $t sukses ($id)" '[ "$st" = success ] && [[ "$id" == backfill__* ]] && [ "$(gold "$t" gold_trx)" = "$(gold "$t" kontrol_trx)" ]'
done

echo "== 6. sensor + Deadline Alert (SLA $SLA_MENIT menit): tahan data $D7"
post kendali "{\"aksi\":\"tahan\",\"tgl\":\"$D7\"}" >/dev/null
post jalankan "{\"tgl\":\"$D7\"}" >/dev/null
sleep 40
cek "sensor menunggu (up_for_reschedule/running)" '[[ "$(ti "$D7" tunggu_bronze | cut -d" " -f1)" =~ ^(up_for_reschedule|running|queued|scheduled)$ ]]'
read -r _ _ RID7 < <(run_info "$D7")
n=0; for _ in $(seq 1 24); do
  n=$(curl -fsS "$API/sla" | j "len([s for s in d['terlewat'] if s['run_id']=='$RID7'])")
  [ "$n" -gt 0 ] && break; sleep 10
done
cek "Deadline Alert run $RID7 tercatat di meta.sla_terlewat" '[ "$n" -gt 0 ]'
post kendali "{\"aksi\":\"kirim\",\"tgl\":\"$D7\"}" >/dev/null
read -r st _ < <(tunggu_run "$D7" -)
cek "data dikirim: sensor lolos, run $D7 sukses" '[ "$st" = success ]'

echo "== 7. UI dan lineage"
for p in / /idempotensi /lineage /mutu /dbt-docs/index.html; do
  c=$(curl -s -o /dev/null -w '%{http_code}' "$UI$p")
  cek "UI $p HTTP $c" '[ "$c" = 200 ]'
done
cek "API lewat proxy UI (/api/runs)" '[ "$(curl -fsS "$UI/api/runs?limit=3" | j "len(d[\"runs\"])")" -ge 1 ]'
NS=$(curl -fsS "$API/lineage" | j "len(d['simpul'])")
cek "lineage dbt: $NS simpul (source+model+test)" '[ "$NS" -ge 25 ]'
cek "graph DAG: 10 task" '[ "$(curl -fsS "$API/dag" | j "len(d[\"tasks\"])")" = 10 ]'

echo "== ringkasan: $LULUS lulus, $GAGAL gagal"
[ "$GAGAL" -eq 0 ]
