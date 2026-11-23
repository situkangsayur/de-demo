#!/usr/bin/env bash
# Uji bagian ops platform (penulis A): aplikasi Sister + NATS + Debezium + ingestor + RustFS + UI.
# Menyalakan HANYA compose.ops.yaml (project terpisah "de-ops-uji"), menjalankan generator sebentar,
# memeriksa event di stream BANK/SHOP/CDC, CDC insert+update, bronze Parquet + kolom metadata
# (dibaca balik dengan PyArrow di container), backfill kecil, UI, lalu "down -v".
#
# Pakai (dari folder mana pun):  bash platform/ops/uji-ops.sh
# Opsi lingkungan: UJI_KEEP=1 (jangan down di akhir), UJI_LIVE_SECONDS (default 45).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM="$(cd "$HERE/.." && pwd)"
cd "$PLATFORM"

PROJECT=de-ops-uji
ENVFILE="$HERE/.env"
[ -f "$ENVFILE" ] || cp "$HERE/.env.ops.example" "$ENVFILE"
DC=(docker compose -p "$PROJECT" --env-file "$ENVFILE" -f compose.ops.yaml)
BIND=$(grep -E '^BIND_ADDR=' "$ENVFILE" | cut -d= -f2 || true)
BIND=${BIND:-127.0.0.1}
API="http://$BIND:52001"
UI="http://$BIND:52000"
LIVE_SECONDS=${UJI_LIVE_SECONDS:-45}

pass=0
fail=0
ok() { echo "  [OK]   $*"; pass=$((pass + 1)); }
bad() { echo "  [GAGAL] $*"; fail=$((fail + 1)); }
step() { echo; echo "== $*"; }

cleanup() {
  if [ "${UJI_KEEP:-0}" = "1" ]; then
    echo "UJI_KEEP=1: stack $PROJECT dibiarkan menyala"
    return
  fi
  step "Pembersihan"
  "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  docker builder prune -f >/dev/null 2>&1 || true
  df -h / | tail -1
}

step "Prasyarat"
SISTER=$(grep -E '^SISTER_DIR=' "$ENVFILE" | cut -d= -f2 || true)
SISTER=${SISTER:-../../sister-hpc-demo}
[ -f "$SISTER/bab05-microservices-messaging/compose.yaml" ] || { echo "repo Sister tidak ditemukan di $SISTER (SISTER_DIR relatif terhadap platform/)"; exit 1; }
grep -q "bank:" "$SISTER/bab05-microservices-messaging/compose.yaml" || { echo "bab05 belum punya service bank"; exit 1; }
if [ -n "$(docker ps -q --filter name=sister-bab05-)" ]; then
  echo "Stack sister-bab05 sedang menyala (uji penulis S?). Port 50500-50506 dipakai; tunggu sampai selesai."
  exit 1
fi
for p in 50500 50501 50502 50503 50504 50505 50506 52000 52001 52003 52004; do
  if (exec 3<>"/dev/tcp/$BIND/$p") 2>/dev/null; then
    echo "port $BIND:$p sudah dipakai proses lain; hentikan dulu"
    exit 1
  fi
done
avail=$(df -BG --output=avail / | tail -1 | tr -dc 0-9)
echo "sisa disk: ${avail} GB"
if [ "$avail" -lt 8 ]; then
  echo "sisa disk < 8 GB, uji ditunda"
  exit 1
fi
trap cleanup EXIT

step "Build dan start (compose.ops.yaml saja)"
"${DC[@]}" up -d --build --wait --wait-timeout 600 2>&1 | grep -E "Built|Started|Healthy|Exited|Error|error" | tail -40 || true
"${DC[@]}" ps --format '{{.Service}}\t{{.Status}}' | sort

jget() { curl -sf --max-time 10 "$1"; }
py() { python3 -c "$1"; }

step "Komponen sehat"
for s in ops-api ops-ui ingestor rustfs debezium-bank debezium-orders bank nats postgres; do
  st=$("${DC[@]}" ps --format '{{.Service}} {{.Health}}' | awk -v s="$s" '$1==s{print $2}')
  [ "$st" = "healthy" ] && ok "$s healthy" || bad "$s status '$st'"
done
curl -sf "http://$BIND:50501/gw/ping" >/dev/null && ok "apisix menjawab /gw/ping" || bad "apisix tidak menjawab"
for s in rustfs-init nats-init; do
  code=$("${DC[@]}" ps -a --format '{{.Service}} {{.ExitCode}}' | awk -v s="$s" '$1==s{print $2}')
  [ "$code" = "0" ] && ok "$s selesai (exit 0)" || bad "$s exit '$code'"
done

step "Generator live ${LIVE_SECONDS} detik"
curl -sf -X POST "$API/api/gen/config" -H 'content-type: application/json' \
  -d '{"rate":8,"workers":4,"spike":"","spike_mul":5,"dirt":{"duplikat":0.1,"kosong":0.1,"terlambat":0.15,"akad_invalid":0.3,"profil":0.15}}' >/dev/null && ok "konfigurasi generator" || bad "konfigurasi generator"
curl -sf -X POST "$API/api/gen/start" >/dev/null && ok "generator mulai" || bad "generator mulai"
sleep "$LIVE_SECONDS"
curl -sf -X POST "$API/api/gen/stop" >/dev/null || true
STATUS=$(jget "$API/api/gen/status")
echo "$STATUS" | py "import json,sys; s=json.load(sys.stdin); [print('   ',k,v) for k,v in sorted(s['counts'].items())]; [print('    gagal:',k,v) for k,v in sorted(s['errors'].items())]"
echo "$STATUS" | py "import json,sys; c=json.load(sys.stdin)['counts']; sys.exit(0 if sum(v for k,v in c.items() if k.startswith('transaksi') or k=='transfer')>=20 else 1)" \
  && ok "generator membuat >= 20 transaksi" || bad "generator membuat terlalu sedikit transaksi"
echo "$STATUS" | py "import json,sys; c=json.load(sys.stdin)['counts']; sys.exit(0 if any(k.startswith('kotor') for k in c) else 1)" \
  && ok "kotoran disengaja tersisip" || bad "tidak ada kotoran"

step "Event di NATS JetStream"
sleep 5
STATS=$(jget "$API/api/flow/stats")
for s in BANK SHOP CDC; do
  n=$(echo "$STATS" | py "import json,sys; d=json.load(sys.stdin); print(next((x['messages'] for x in d['streams'] if x['name']=='$s'),0))")
  [ "${n:-0}" -gt 0 ] && ok "stream $s berisi $n pesan" || bad "stream $s kosong"
done
for subj in bank.transaksi.dibuat bank.nasabah.dibuat cdc.bank.transaksi cdc.bank.rekening cdc.orders.orders shop.order.created; do
  echo "$STATS" | py "import json,sys; d=json.load(sys.stdin); sys.exit(0 if any('$subj' in x['subjects'] for x in d['streams']) else 1)" \
    && ok "subject $subj ada" || bad "subject $subj tidak ada"
done

step "Debezium menangkap insert dan update"
op=$(jget "$API/api/flow/sample?subject=cdc.bank.transaksi" | py "import json,sys; print(json.load(sys.stdin)['data']['op'])" || true)
[ "$op" = "c" ] && ok "cdc.bank.transaksi op=c (INSERT)" || bad "cdc.bank.transaksi op='$op'"
op=$(jget "$API/api/flow/sample?subject=cdc.bank.rekening" | py "import json,sys; print(json.load(sys.stdin)['data']['op'])" || true)
[ "$op" = "u" ] && ok "cdc.bank.rekening op=u (UPDATE saldo)" || bad "cdc.bank.rekening op='$op'"
jget "$API/api/flow/sample?subject=cdc.bank.nasabah" | py "
import json,sys
d=json.load(sys.stdin)['data']
assert set(['before','after','op','ts_ms','source']) <= set(d), d.keys()
print('    contoh envelope cdc.bank.nasabah: op=%s source=%s.%s lsn=%s' % (d['op'], d['source']['db'], d['source']['table'], d['source'].get('lsn')))
" && ok "envelope Debezium punya before/after/op/ts_ms/source" || bad "envelope Debezium tidak lengkap"

step "Bronze Parquet di RustFS"
sleep 15 # tunggu flush ingestor (INGEST_FLUSH_SECONDS)
BRONZE=$(jget "$API/api/flow/stats")
for p in app/transaksi app/nasabah app/rekening app/orders app/order_items cdc/transaksi cdc/rekening cdc/nasabah cdc/orders cdc/cabang; do
  echo "$BRONZE" | py "import json,sys; d=json.load(sys.stdin)['bronze']; sys.exit(0 if any(x['source']+'/'+x['entity']=='$p' and x['rows']>0 for x in d['partitions']) else 1)" \
    && ok "partisi bronze/$p terisi" || bad "partisi bronze/$p kosong"
done
lag=$(echo "$BRONZE" | py "import json,sys; d=json.load(sys.stdin); print(sum(c['num_pending'] for s in d['streams'] for c in s['consumers'] if c['name'].startswith('de_bronze')))")
echo "    lag consumer de_bronze_*: $lag"

step "Baca balik Parquet (PyArrow di container ingestor)"
"${DC[@]}" exec -T ingestor python - <<'PY' && ok "kolom metadata W1 sesuai kontrak" || bad "kolom metadata tidak sesuai"
import pyarrow.parquet as pq
from pyarrow import fs
from ingestor.common import s3fs
from ingestor.mapping import META_COLS
s3 = s3fs()
def files(prefix):
    sel = fs.FileSelector(prefix, recursive=True, allow_not_found=True)
    return sorted(i.path for i in s3.get_file_info(sel) if i.path.endswith(".parquet"))
ok = True
for ent in ("app/transaksi", "cdc/transaksi", "cdc/nasabah"):
    fl = files(f"lake/bronze/{ent}")
    t = pq.read_table(fl[-1], filesystem=s3)
    names = t.column_names
    print(f"    s3://{fl[-1]}  baris={t.num_rows}")
    print(f"      kolom: {', '.join(names[:16])}{' ...' if len(names) > 16 else ''}")
    print(f"      tipe : _ingested_at={t.schema.field('_ingested_at').type} _ts_ms={t.schema.field('_ts_ms').type}")
    if names[:len(META_COLS)] != META_COLS:
        print("      kolom metadata salah:", names[:len(META_COLS)])
        ok = False
    r = t.slice(0, 1).to_pylist()[0]
    print("      contoh:", {k: r[k] for k in names[:10]})
ops = set()
for f in files("lake/bronze/cdc/rekening") + files("lake/bronze/cdc/transaksi"):
    ops |= set(pq.read_table(f, filesystem=s3, columns=["_op"]).column("_op").to_pylist())
print("    _op di cdc:", sorted(o for o in ops if o))
ok = ok and {"c", "u"} <= ops
raise SystemExit(0 if ok else 1)
PY

step "Backfill kecil (4 hari x 30 aksi, dengan belanja) -> partisi dt historis berurutan"
curl -sf -X POST "$API/api/gen/backfill" -H 'content-type: application/json' -d '{"days":4,"per_day":30,"with_shop":true}' >/dev/null && ok "backfill dimulai" || bad "backfill gagal dimulai"
for _ in $(seq 1 90); do
  mode=$(jget "$API/api/gen/status" | py "import json,sys; print(json.load(sys.stdin)['mode'])")
  [ "$mode" = "idle" ] && break
  sleep 2
done
sleep 15
TODAY=$(date -u +%F)
jget "$API/api/flow/stats" | py "
import json,sys
d=json.load(sys.stdin)['bronze']['partitions']
bad=0
for ent in ('app/transaksi','app/nasabah','app/rekening','app/orders','cdc/transaksi','cdc/rekening','cdc/nasabah','cdc/orders'):
    dts=sorted({p['dt'] for p in d if p['source']+'/'+p['entity']==ent})
    hist=[x for x in dts if x < '$TODAY']
    print('    %-15s dt historis: %s' % (ent, hist))
    bad += 0 if hist else 1
sys.exit(bad)
" && ok "backfill menulis partisi dt historis (nasabah, rekening, transaksi, orders; app dan cdc)" || bad "partisi historis kurang"
q3=$(jget "$API/api/gen/status" | py "import json,sys; print(json.load(sys.stdin)['counts'].get('nasabah baru tanpa transaksi (Q3)',0))")
[ "${q3:-0}" -ge 1 ] && ok "ada $q3 nasabah baru yang sengaja tidak bertransaksi (Q3)" || bad "tidak ada nasabah Q3"

step "Urutan waktu dan before (CDC, PyArrow di container ingestor)"
"${DC[@]}" exec -T ingestor python - <<'PY' && ok "urutan waktu konsisten dan _before terisi pada update" || bad "urutan waktu atau _before bermasalah"
import json
import pyarrow.parquet as pq
from pyarrow import fs
from ingestor.common import s3fs
s3 = s3fs()
def rows(ent, cols):
    sel = fs.FileSelector(f"lake/bronze/{ent}", recursive=True, allow_not_found=True)
    out = []
    for i in s3.get_file_info(sel):
        if i.path.endswith(".parquet"):
            t = pq.read_table(i.path, filesystem=s3)
            out += t.select([c for c in cols if c in t.column_names]).to_pylist()
    return out
from datetime import datetime
ts = lambda x: datetime.fromisoformat(x.replace("Z", "+00:00"))
nas = {r["id"]: ts(r["created_at"]) for r in rows("cdc/nasabah", ["id", "created_at"])}
rek = {r["id"]: (ts(r["dibuka_at"]), r["nasabah_id"]) for r in rows("cdc/rekening", ["id", "dibuka_at", "nasabah_id"])}
trx = rows("cdc/transaksi", ["id", "rekening_id", "occurred_at"])
viol_r = [k for k, (d, n) in rek.items() if n in nas and d < nas[n]]
viol_t = [t["id"] for t in trx if t["rekening_id"] in rek and ts(t["occurred_at"]) < rek[t["rekening_id"]][0]]
print(f"    rekening dibuka sebelum nasabah terdaftar: {len(viol_r)} dari {len(rek)}")
print(f"    transaksi sebelum rekening dibuka       : {len(viol_t)} dari {len(trx)}")
up = [r for r in rows("cdc/nasabah", ["_op", "_before", "kota"]) if r["_op"] == "u"]
filled = [r for r in up if r.get("_before")]
print(f"    cdc/nasabah op=u: {len(up)}, _before terisi: {len(filled)}")
if filled:
    b = json.loads(filled[-1]["_before"])
    print(f"    contoh _before.kota={b.get('kota')!r} -> after.kota={filled[-1]['kota']!r}")
raise SystemExit(0 if not viol_r and not viol_t and up and len(filled) == len(up) else 1)
PY

step "UI dan API untuk UI"
for path in / /generator; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$UI$path")
  [ "$code" = "200" ] && ok "UI $path HTTP 200" || bad "UI $path HTTP $code"
done
curl -sf "$UI/api/ops/flow/stats" | py "import json,sys; d=json.load(sys.stdin); sys.exit(0 if d['streams'] else 1)" && ok "UI proxy /api/ops/flow/stats berisi data" || bad "UI proxy stats"
n=$(timeout 5 curl -sN "$UI/api/ops/flow/live" | grep -c '^:' || true)
[ "${n:-0}" -ge 1 ] && ok "SSE /api/ops/flow/live terhubung" || bad "SSE tidak terhubung"
code=$(curl -s -o /dev/null -w '%{http_code}' "http://$BIND:52004/")
[ "$code" -lt 500 ] && ok "RustFS console HTTP $code" || bad "RustFS console HTTP $code"

step "Log JSON"
"${DC[@]}" logs --no-log-prefix --tail 2 ops-api ingestor 2>/dev/null || true

echo
echo "Hasil: $pass lolos, $fail gagal"
[ "$fail" -eq 0 ]
