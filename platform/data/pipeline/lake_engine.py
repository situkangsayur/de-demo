"""lake-engine: DuckDB atas tabel Iceberg gold, dibuka lewat HTTP untuk API dashboard (Go).

Go tidak punya driver DuckDB tanpa cgo, jadi query lake dijalankan di proses Python kecil ini.
Setiap tabel lake.gold.* dipasang sebagai view DuckDB `gold.<tabel>` di atas
iceberg_scan('<metadata.json terbaru>'). Lokasi metadata dibaca dari katalog SQL (PostgreSQL)
setiap kali ada query, sehingga hasil selalu mengikuti snapshot terbaru.

Endpoint (JSON):
  GET  /health
  POST /query            {"sql": "..."}                         -> kolom, baris, ms, snapshot
  GET  /snapshots?tabel=gold.fakta_transaksi                    -> riwayat snapshot + isi per snapshot
  GET  /asof?tabel=gold.fakta_transaksi&waktu=2026-11-23T01:00:00Z  -> query AS OF waktu
  GET  /lapisan                                                 -> ringkasan bronze/silver/gold
  GET  /tiering?geser_hari=0                                    -> lifecycle terpasang + simulasi tier
  POST /sabotase         {"aksi": "sisipkan"|"pulihkan", "tgl": "YYYY-MM-DD"}
"""
from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from sqlalchemy import text

from . import bronze, sabotase, tiering
from .konfigurasi import duckdb_lake, katalog, log

KUNCI = threading.Lock()
_con = None
_terpasang: dict[str, str] = {}


def _lokasi() -> dict[str, str]:
    with katalog().engine.connect() as c:
        rows = c.execute(text("select table_namespace, table_name, metadata_location from iceberg_tables")).fetchall()
    return {f"{ns}.{t}": loc for ns, t, loc in rows}


def _segarkan() -> dict[str, str]:
    """Pasang ulang view yang metadata-nya berubah. Dipanggil di dalam KUNCI."""
    global _con
    if _con is None:
        _con = duckdb_lake()
        _con.execute("create schema if not exists gold; create schema if not exists silver;")
    lok = _lokasi()
    for nama, loc in lok.items():
        if not nama.startswith(("gold.", "silver.")) or _terpasang.get(nama) == loc:
            continue
        _con.execute(f"create or replace view {nama} as select * from iceberg_scan('{loc}')")
        _terpasang[nama] = loc
    return {n: l for n, l in lok.items() if n.startswith(("gold.", "silver."))}


def _json(v):
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()
    return v


def query(sql: str) -> dict:
    with KUNCI:
        _segarkan()
        t0 = time.perf_counter()
        cur = _con.execute(sql)
        baris = cur.fetchall()
        ms = round((time.perf_counter() - t0) * 1000, 2)
        kolom = [d[0] for d in cur.description]
    return {"kolom": kolom, "baris": [[_json(v) for v in r] for r in baris], "ms": ms, "mesin": "duckdb+iceberg"}


def snapshots(tabel: str) -> dict:
    tbl = katalog().load_table(tabel)
    loc = tbl.metadata_location
    hasil = []
    with KUNCI:
        _segarkan()
        for s in sorted(tbl.snapshots(), key=lambda s: s.timestamp_ms, reverse=True)[:15]:
            info = _con.execute(f"""select count(*), coalesce(sum(nominal), 0), count(distinct tgl), min(tgl), max(tgl)
                                    from iceberg_scan('{loc}', snapshot_from_id => {s.snapshot_id})""").fetchone() \
                if tabel.endswith("fakta_transaksi") else \
                _con.execute(f"select count(*) from iceberg_scan('{loc}', snapshot_from_id => {s.snapshot_id})").fetchone()
            hasil.append({"snapshot_id": s.snapshot_id, "parent_id": s.parent_snapshot_id,
                          "waktu": dt.datetime.fromtimestamp(s.timestamp_ms / 1000, dt.timezone.utc).isoformat(),
                          "operasi": s.summary.operation.value if s.summary else None,
                          "ringkasan": {k: v for k, v in (s.summary.additional_properties if s.summary else {}).items()
                                        if k in ("added-records", "deleted-records", "total-records", "added-data-files",
                                                 "deleted-data-files", "total-data-files")},
                          "isi": [_json(v) for v in info]})
    cur = tbl.current_snapshot()
    return {"tabel": tabel, "metadata_location": loc, "snapshot_sekarang": cur.snapshot_id if cur else None,
            "kolom_isi": ["baris", "total_nominal", "jumlah_tgl", "tgl_min", "tgl_max"]
            if tabel.endswith("fakta_transaksi") else ["baris"], "snapshot": hasil}


def asof(tabel: str, waktu: str) -> dict:
    tbl = katalog().load_table(tabel)
    w = dt.datetime.fromisoformat(waktu.replace("Z", "+00:00"))
    ms = int(w.timestamp() * 1000)
    snap = None
    for s in sorted(tbl.snapshots(), key=lambda s: s.timestamp_ms):
        if s.timestamp_ms <= ms:
            snap = s
    if snap is None:
        return {"tabel": tabel, "waktu": waktu, "snapshot_id": None, "pesan": "tabel belum ada pada waktu itu"}
    sql = (f"select tgl, count(*) as baris, sum(nominal) as total from iceberg_scan('{tbl.metadata_location}', "
           f"snapshot_from_id => {snap.snapshot_id}) group by tgl order by tgl desc limit 10")
    with KUNCI:
        _segarkan()
        rows = _con.execute(sql).fetchall()
    return {"tabel": tabel, "waktu": waktu, "snapshot_id": snap.snapshot_id,
            "snapshot_waktu": dt.datetime.fromtimestamp(snap.timestamp_ms / 1000, dt.timezone.utc).isoformat(),
            "sql": f"SELECT tgl, count(*), sum(nominal) FROM {tabel} FOR SYSTEM_TIME AS OF '{waktu}' GROUP BY tgl"
                   f"  -- DuckDB: iceberg_scan(..., snapshot_from_id => {snap.snapshot_id})",
            "kolom": ["tgl", "baris", "total"], "baris": [[_json(v) for v in r] for r in rows]}


def lapisan() -> dict:
    f = defaultdict(lambda: {"berkas": 0, "byte": 0, "dt": set()})
    for ent in ("transaksi", "nasabah", "rekening", "akad_pembiayaan", "cabang"):
        for path, d in bronze.daftar_berkas(ent):
            sumber = path.split("/")[2]
            k = f[(sumber, ent)]
            k["berkas"] += 1
            k["dt"].add(d)
    br = [{"sumber": s, "entitas": e, "berkas": v["berkas"], "partisi_dt": len(v["dt"]),
           "dt_min": min(v["dt"]).isoformat() if v["dt"] else None,
           "dt_max": max(v["dt"]).isoformat() if v["dt"] else None} for (s, e), v in sorted(f.items())]
    tabel = []
    cat = katalog()
    for ns in ("silver", "staging", "gold"):
        for _, t in sorted(cat.list_tables(ns)):
            tb = cat.load_table(f"{ns}.{t}")
            s = tb.current_snapshot()
            ap = s.summary.additional_properties if s and s.summary else {}
            tabel.append({"tabel": f"{ns}.{t}", "baris": int(ap.get("total-records", 0)),
                          "berkas": int(ap.get("total-data-files", 0)), "byte": int(ap.get("total-files-size", 0)),
                          "snapshot_id": s.snapshot_id if s else None, "jumlah_snapshot": len(tb.snapshots()),
                          "diperbarui": dt.datetime.fromtimestamp(s.timestamp_ms / 1000, dt.timezone.utc).isoformat() if s else None,
                          "partisi": str(tb.spec())})
    return {"bronze": br, "iceberg": tabel, "sabotase_aktif": sabotase.ada_sabotase()}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _kirim(self, kode: int, isi) -> None:
        b = json.dumps(isi, default=str, ensure_ascii=False).encode()
        self.send_response(kode)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _jalankan(self, fn):
        t0 = time.perf_counter()
        rid = self.headers.get("X-Request-ID", "-")
        try:
            self._kirim(200, fn())
            log("info", "permintaan", path=self.path[:120], status=200, ms=round((time.perf_counter() - t0) * 1000, 1),
                request_id=rid)
        except Exception as e:
            log("error", "permintaan gagal", path=self.path[:120], err=str(e)[:500], request_id=rid)
            self._kirim(500, {"error": str(e)[:1000]})

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        rute = {
            "/health": lambda: {"status": "ok"},
            "/snapshots": lambda: snapshots(q.get("tabel", "gold.fakta_transaksi")),
            "/asof": lambda: asof(q.get("tabel", "gold.fakta_transaksi"), q["waktu"]),
            "/lapisan": lapisan,
            "/tiering": lambda: {"aturan": tiering.aturan_terpasang(),
                                 "simulasi": tiering.simulasi(int(q.get("geser_hari", "0")))},
        }
        if u.path not in rute:
            return self._kirim(404, {"error": "tidak ada"})
        self._jalankan(rute[u.path])

    def do_POST(self):
        n = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(n) or b"{}")
        u = urlparse(self.path).path
        if u == "/query":
            return self._jalankan(lambda: query(body["sql"]))
        if u == "/sabotase":
            d = dt.date.fromisoformat(body["tgl"])
            return self._jalankan(lambda: sabotase.sisipkan(d) if body.get("aksi") == "sisipkan" else sabotase.pulihkan(d))
        self._kirim(404, {"error": "tidak ada"})


def main() -> None:
    port = int(os.environ.get("LAKE_ENGINE_PORT", "8090"))
    log("info", "lake-engine siap", port=port)
    ThreadingHTTPServer(("0.0.0.0", port), H).serve_forever()


if __name__ == "__main__":
    main()
