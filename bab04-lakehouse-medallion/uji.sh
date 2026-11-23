#!/usr/bin/env bash
# Uji otomatis lab Bab 4 (lakehouse medallion + Iceberg) dari keadaan bersih:
#   1) build dan nyalakan RustFS + api + ui,
#   2) pipeline medallion lulus; data kotor ditolak gerbang 1 dan gold tidak berubah; pulih lagi,
#   3) operasi Iceberg (append, compaction, MERGE, DELETE, schema/partition evolution, time travel,
#      pruning, DuckDB iceberg_scan, rollback, expire),
#   4) halaman UI melayani HTTP 200,
#   5) down -v (selalu, lewat trap).
set -euo pipefail
cd "$(dirname "$0")"

[ -f .env ] || cp .env.example .env
set -a; . ./.env; set +a
BIND_ADDR=${BIND_ADDR:-127.0.0.1}
UI=http://$BIND_ADDR:52400
API=$UI/api

lolos=0; gagal=0
ok()   { echo "  [LOLOS] $*"; lolos=$((lolos+1)); }
salah(){ echo "  [GAGAL] $*"; gagal=$((gagal+1)); }
cek()  { if [ "$1" = "1" ]; then ok "$2"; else salah "$2"; fi; }
bersih() { echo "== Membersihkan (down -v)"; docker compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap bersih EXIT

# py 'ekspresi atas d' < json  -> mencetak hasil ekspresi Python (d = JSON masukan)
py() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
get()  { curl -sf "$API/$1"; }
post() { curl -s -X POST "$API/$1"; }

echo "== Keadaan bersih dan build"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build >/dev/null
for i in $(seq 1 120); do curl -sf -o /dev/null "$UI" && break; sleep 2; done
curl -sf -o /dev/null "$UI" && ok "UI menyala di $UI" || { salah "UI tidak menyala"; exit 1; }

echo "== Bronze"
B=$(get bronze)
cek "$(echo "$B" | py "int(d['hari'] == 3)")" "bronze 3 hari dibangkitkan otomatis"
cek "$(echo "$B" | py "int(all(e['berkas'] > 0 for e in d['entitas']))")" "lima entitas bronze ada (cabang, nasabah, rekening, transaksi, akad)"
C=$(get "bronze/contoh?entitas=transaksi")
cek "$(echo "$C" | py "int(d['kolom'][:8] == ['_ingested_at','_source','_batch_id','_event_id','_event_type','_occurred_at','_ts_ms','_op'])")" "8 kolom metadata platform di depan"
cek "$(echo "$C" | py "int(d['skema']['nominal'] == 'String')")" "kolom data bronze bertipe String"

echo "== Medallion"
R=$(post medallion/jalankan)
cek "$(echo "$R" | py "int(d['lulus'])")" "run pipeline lulus kedua gerbang"
cek "$(echo "$R" | py "int(d['langkah'][1]['statistik']['duplikat_dibuang'] > 0 and 'app+cdc' in d['langkah'][1]['statistik']['sumber'])")" "dedup app vs cdc membuang duplikat"
L=$(get medallion/lapisan)
GOLD1=$(echo "$L" | py "[t for t in d['tabel'] if t['tabel']=='gold.fakta_trx_harian'][0]['snapshot_id']")
SILVER_N=$(echo "$L" | py "[t for t in d['tabel'] if t['tabel']=='silver.transaksi'][0]['baris']")
cek "$([ "$SILVER_N" -gt 1000 ] && echo 1 || echo 0)" "silver.transaksi berisi $SILVER_N baris"
T=$(get medallion/lacak)
cek "$(echo "$T" | py "int(len(d['bronze']) == 2 and len(d['silver']) == 1 and len(d['gold']) == 1)")" "satu transaksi: 2 baris bronze, 1 silver, 1 gold"
cek "$(echo "$T" | py "int('nik' not in d['silver'][0] and 'rekening_id' not in d['gold'][0])")" "silver/gold tanpa PII mentah"

echo "== Gerbang data quality"
post bronze/kotor >/dev/null
R=$(post medallion/jalankan)
cek "$(echo "$R" | py "int(not d['lulus'] and not d['langkah'][1]['lulus'])")" "data kotor menggagalkan gerbang 1"
cek "$(echo "$R" | py "int(sum(1 for c in d['langkah'][1]['cek'] if not c['lulus']) >= 4)")" "empat aturan dilanggar (nominal, kanal, rekening, konflik app/cdc)"
GOLD2=$(get medallion/lapisan | py "[t for t in d['tabel'] if t['tabel']=='gold.fakta_trx_harian'][0]['snapshot_id']")
cek "$([ "$GOLD1" = "$GOLD2" ] && echo 1 || echo 0)" "snapshot gold tidak berubah saat gerbang gagal"
cek "$(get medallion/karantina | py "int(d['silver.karantina_transaksi']['total'] >= 4)")" "baris pelanggar masuk silver.karantina_transaksi"
post bronze/pulihkan >/dev/null
cek "$(post medallion/jalankan | py "int(d['lulus'])")" "sesudah dipulihkan pipeline lulus lagi"

echo "== Iceberg"
op() { post "iceberg/operasi/$1"; }
cek "$(op reset | py "int('pesan' in d)")" "lab.transaksi dibuat dari silver"
SNAP0=$(get iceberg/pohon | py "d['snapshots'][0]['snapshot_id']")
op append >/dev/null
P=$(get iceberg/pohon)
cek "$(echo "$P" | py "int(len(d['snapshots']) == 6)")" "lima commit append kecil = lima snapshot baru"
cek "$(echo "$P" | py "int(d['katalog']['metadata_location'].endswith(d['metadata']['path']))")" "katalog menunjuk metadata.json terbaru"
cek "$(echo "$P" | py "int(any(m['baru'] for m in d['manifest']) and d['total_berkas_manifest'] > 4)")" "pohon metadata: manifest baru dan data file terbaca"
cek "$(op compaction | py "int(d['sebelum']['maks'] > 1 and d['sesudah']['maks'] == 1)")" "compaction: satu berkas per partisi"
cek "$(op merge | py "int('MERGE' in d['pesan'])")" "MERGE/upsert"
cek "$(op delete | py "int('DELETE' in d['pesan'])")" "DELETE copy-on-write"
op skema-tambah >/dev/null; op skema-ganti_nama >/dev/null; op skema-lebarkan >/dev/null
P=$(get iceberg/pohon)
cek "$(echo "$P" | py "int({'kategori_merchant','jenis_transaksi'} <= {f['nama'] for f in d['metadata']['skema']} and d['metadata']['jumlah_skema'] == 4)")" "schema evolution: 3 perubahan skema, kolom baru dan rename"
cek "$(op partisi | py "int('bucket' in d['pesan'])")" "partition evolution bucket(4, rekening_id)"
cek "$(get iceberg/pohon | py "int(d['metadata']['jumlah_spec'] == 2 and {f['spec_id'] for m in d['manifest'] for f in m['berkas']} == {0, 1})")" "berkas spec lama dan spec baru hidup berdampingan"
TT=$(get "iceberg/time-travel?snapshot_id=$SNAP0")
cek "$(echo "$TT" | py "int(d['baris'] == $SILVER_N and 'jenis' in d['skema'])")" "time travel ke snapshot pertama: baris dan skema lama"
DARI=$(get bronze | py "[e for e in d['entitas'] if e['entitas']=='transaksi'][0]['dt'][-1]")
cek "$(get "iceberg/pruning?dari=${DARI}T00:00:00%2B00:00" | py "int(0 < d['berkas_dibaca'] < d['berkas_total'])")" "hidden partitioning memangkas berkas"
S=$(curl -s -X POST "$API/iceberg/sql" -H 'Content-Type: application/json' -d '{"sql":"SELECT kanal, SUM(total_nominal) AS t FROM gold.fakta_trx_harian GROUP BY kanal"}')
cek "$(echo "$S" | py "int(len(d['baris']) == 5 and 'iceberg_scan' in d['sql_dijalankan'])")" "DuckDB iceberg_scan atas gold"
cek "$(op "rollback?snapshot_id=$SNAP0" | py "int(str(d['snapshot_id']) == '$SNAP0')")" "rollback ke snapshot pertama"
E=$(op expire)
cek "$(get iceberg/pohon | py "int(len(d['snapshots']) == 1 and d['objek']['data'] == d['objek']['data_dirujuk_snapshot_mana_pun'])")" "expire snapshots + hapus berkas yatim"

echo "== Halaman UI"
for p in / /lacak /iceberg /time-travel /sql; do
  kode=$(curl -s -o /dev/null -w '%{http_code}' "$UI$p")
  cek "$([ "$kode" = 200 ] && echo 1 || echo 0)" "GET $p -> $kode"
done
cek "$(docker compose logs api 2>/dev/null | grep -c '"service": "bab04-api"' | awk '{print ($1>0)}')" "log JSON satu baris dari api"

echo "== Ringkasan: $lolos lolos, $gagal gagal"
[ "$gagal" = 0 ]
