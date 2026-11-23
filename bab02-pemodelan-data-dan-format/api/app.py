"""API lab Bab 2: pemodelan data (OLTP vs star schema vs OBT, SCD tipe 1 vs 2) dan format file."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import statistics
import sys
import threading
import time
import uuid

import duckdb
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import data
import formats


class LogJSON(logging.Formatter):
    def format(self, r: logging.LogRecord) -> str:
        d = {"time": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"), "level": r.levelname.lower(),
             "msg": r.getMessage(), "service": "bab02-api"}
        d.update(getattr(r, "extra", {}))
        return json.dumps(d, default=str)


h = logging.StreamHandler(sys.stdout)
h.setFormatter(LogJSON())
log = logging.getLogger("bab02")
log.addHandler(h)
log.setLevel(logging.INFO)


def catat(msg: str, level: int = logging.INFO, **kw) -> None:
    log.log(level, msg, extra={"extra": kw})


KUNCI = threading.Lock()
con = duckdb.connect()
con.execute("SET threads TO 2")
N_AWAL = int(os.environ.get("N_TRANSAKSI", "300000"))
STATE: dict = {"n": 0, "siap": False}


def siapkan(n: int) -> dict:
    t = time.perf_counter()
    r = data.bangkitkan(con, n)
    STATE.update(n=n, siap=True, dibangkitkan_ms=round((time.perf_counter() - t) * 1000))
    catat("data dibangkitkan", n_transaksi=n, durasi_ms=STATE["dibangkitkan_ms"])
    return r


app = FastAPI(title="Lab DE Bab 2")


@app.on_event("startup")
def mulai() -> None:
    os.makedirs(formats.DIR, exist_ok=True)
    with KUNCI:
        siapkan(N_AWAL)


@app.middleware("http")
async def mw(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
    t = time.perf_counter()
    try:
        resp = await call_next(request)
    except Exception as e:  # galat tak terduga tetap dicatat dengan request_id
        catat("galat", logging.ERROR, request_id=rid, path=request.url.path, err=str(e))
        return JSONResponse({"error": str(e)}, status_code=500)
    if request.url.path != "/healthz" and not (request.method == "GET" and resp.status_code < 400 and request.url.path == "/api/ringkasan"):
        catat("http", request_id=rid, method=request.method, path=request.url.path, kode=resp.status_code,
              durasi_ms=round((time.perf_counter() - t) * 1000, 1))
    return resp


@app.get("/healthz")
def healthz():
    if not STATE["siap"]:
        raise HTTPException(503, "data belum siap")
    return {"status": "ok"}


@app.get("/api/ringkasan")
def ringkasan():
    with KUNCI:  # satu koneksi DuckDB dipakai bergiliran (DuckDB menolak dua panggilan bersamaan)
        r = data.ringkasan(con)
        avro = formats.avro_duckdb_tersedia(con)
    return {"n_transaksi": STATE["n"], "dibangkitkan_ms": STATE.get("dibangkitkan_ms"), "tabel": r,
            "avro_duckdb": avro, "platform": bool(os.environ.get("PLATFORM_S3_ENDPOINT"))}


class Bangkit(BaseModel):
    n: int = 300000


@app.post("/api/bangkitkan")
def bangkitkan(b: Bangkit):
    if not 10_000 <= b.n <= 3_000_000:
        raise HTTPException(400, "n harus 10.000 sampai 3.000.000")
    with KUNCI:
        return {"n_transaksi": b.n, "tabel": siapkan(b.n)}


def _jalankan(sql: str, ulang: int = 5) -> tuple[float, list[float], list, list[str]]:
    waktu = []
    rows, kol = [], []
    for _ in range(ulang):
        t = time.perf_counter()
        cur = con.execute(sql)
        rows = cur.fetchall()
        waktu.append((time.perf_counter() - t) * 1000)
        kol = [d[0] for d in cur.description]
    return statistics.median(waktu), [round(w, 2) for w in waktu], rows, kol


@app.get("/api/model/banding")
def banding(q: str = "q1"):
    if q not in data.PERTANYAAN:
        raise HTTPException(400, "q harus q1 atau q2")
    p = data.PERTANYAAN[q]
    hasil = {}
    with KUNCI:
        for model in ("oltp", "star", "obt"):
            med, semua, rows, kol = _jalankan(p[model])
            j, kedalaman = data.JOIN_INFO[(q, model)]
            hasil[model] = {"sql": p[model], "join": j, "kedalaman_join": kedalaman, "median_ms": round(med, 2), "semua_ms": semua,
                            "baris_hasil": len(rows), "kolom": kol,
                            "contoh": [[float(v) if hasattr(v, "as_integer_ratio") or type(v).__name__ == "Decimal" else v for v in r] for r in rows[:8]],
                            "_set": {tuple(str(v) for v in r) for r in rows}}
        acuan = hasil["star"].pop("_set")
        for model in ("oltp", "obt"):
            s = hasil[model].pop("_set")
            hasil[model]["beda_dengan_star"] = len(s ^ acuan)
        hasil["star"]["beda_dengan_star"] = 0
        ukuran = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ["transaksi", "fakta_transaksi", "obt_transaksi"]}
    return {"q": q, "judul": p["judul"], "model": hasil, "baris": ukuran}


@app.get("/api/scd")
def scd(nasabah_id: int = data.NASABAH_CONTOH):
    with KUNCI:
        oltp = con.execute("SELECT id, cif, nama, kota, segmen, cabang_id FROM nasabah WHERE id = ?", [nasabah_id]).fetchall()
        if not oltp:
            raise HTTPException(404, "nasabah tidak ada")
        scd1 = con.execute("SELECT sk_nasabah, id_nasabah, kota, segmen FROM dim_nasabah_scd1 WHERE id_nasabah = ?", [nasabah_id]).fetchall()
        scd2 = con.execute("""SELECT sk_nasabah, id_nasabah, kota, segmen, berlaku_dari::VARCHAR, berlaku_sampai::VARCHAR, aktif
                              FROM dim_nasabah WHERE id_nasabah = ? ORDER BY berlaku_dari""", [nasabah_id]).fetchall()
        per_bulan = con.execute("""
            SELECT d.tahun_bulan, n2.kota AS kota_scd2, n1.kota AS kota_scd1, SUM(f.nominal)::DOUBLE, COUNT(*)
            FROM fakta_transaksi f JOIN dim_tanggal d USING (sk_tanggal)
            JOIN dim_nasabah n2 ON n2.sk_nasabah = f.sk_nasabah
            JOIN dim_nasabah_scd1 n1 ON n1.id_nasabah = n2.id_nasabah
            WHERE n2.id_nasabah = ? GROUP BY ALL ORDER BY 1""", [nasabah_id]).fetchall()
    return {"nasabah_id": nasabah_id, "oltp": oltp[0], "oltp_kolom": ["id", "cif", "nama", "kota", "segmen", "cabang_id"],
            "scd1": scd1, "scd2": scd2,
            "per_bulan": [{"bulan": b, "kota_scd2": k2, "kota_scd1": k1, "nominal": v, "jumlah": n} for b, k2, k1, v, n in per_bulan]}


class Pindah(BaseModel):
    nasabah_id: int
    kota: str
    tanggal: str


@app.post("/api/scd/pindah")
def pindah(p: Pindah):
    """Nasabah pindah kota: OLTP dan SCD 1 menimpa; SCD 2 menutup baris lama dan menambah baris baru."""
    try:
        tgl = dt.date.fromisoformat(p.tanggal)
    except ValueError:
        raise HTTPException(400, "tanggal harus YYYY-MM-DD")
    kota_sah = [c[3] for c in data.CABANG]
    if p.kota not in kota_sah:
        raise HTTPException(400, f"kota harus salah satu dari {kota_sah}")
    with KUNCI:
        aktif = con.execute("SELECT sk_nasabah, kota, segmen, berlaku_dari FROM dim_nasabah WHERE id_nasabah = ? AND aktif",
                            [p.nasabah_id]).fetchall()
        if not aktif:
            raise HTTPException(404, "nasabah tidak ada")
        sk_lama, kota_lama, segmen, dari = aktif[0]
        if p.kota == kota_lama:
            raise HTTPException(400, "kota baru sama dengan kota sekarang")
        if tgl <= dari:
            raise HTTPException(400, f"tanggal harus sesudah berlaku_dari versi aktif ({dari})")
        con.execute("UPDATE nasabah SET kota = ? WHERE id = ?", [p.kota, p.nasabah_id])               # OLTP: timpa
        con.execute("UPDATE dim_nasabah_scd1 SET kota = ? WHERE id_nasabah = ?", [p.kota, p.nasabah_id])  # SCD 1: timpa
        sk_baru = con.execute("SELECT max(sk_nasabah) + 1 FROM dim_nasabah").fetchone()[0]
        con.execute("UPDATE dim_nasabah SET berlaku_sampai = ?, aktif = FALSE WHERE sk_nasabah = ?", [tgl - dt.timedelta(days=1), sk_lama])
        con.execute("INSERT INTO dim_nasabah VALUES (?, ?, ?, ?, ?, DATE '9999-12-31', TRUE)", [sk_baru, p.nasabah_id, segmen, p.kota, tgl])
        # fakta sesudah tanggal pindah menunjuk versi baru (di produksi: late-arriving dimension)
        n = con.execute("""UPDATE fakta_transaksi SET sk_nasabah = ? WHERE sk_nasabah = ? AND sk_tanggal >= ?""",
                        [sk_baru, sk_lama, int(tgl.strftime("%Y%m%d"))]).fetchone()[0]
        con.execute("DROP TABLE IF EXISTS obt_transaksi")
        con.execute("""CREATE TABLE obt_transaksi AS
            SELECT f.id_transaksi, d.tanggal, d.tahun_bulan, c.nama AS nama_cabang, c.wilayah, k.kanal, k.kelompok,
                   p.jenis_rekening, p.akad, n.id_nasabah, n.kota AS kota_nasabah, n.segmen, f.nominal
            FROM fakta_transaksi f JOIN dim_tanggal d USING (sk_tanggal) JOIN dim_cabang c USING (sk_cabang)
            JOIN dim_kanal k USING (sk_kanal) JOIN dim_produk p USING (sk_produk) JOIN dim_nasabah n USING (sk_nasabah)""")
    catat("nasabah pindah kota", nasabah_id=p.nasabah_id, kota_lama=kota_lama, kota_baru=p.kota, fakta_dipindah=n)
    return {"sk_lama": sk_lama, "sk_baru": sk_baru, "fakta_menunjuk_versi_baru": n, **scd(p.nasabah_id)}


@app.get("/api/scd/dampak")
def dampak():
    """Total nominal setahun per kota nasabah: SCD 1 (kota sekarang) vs SCD 2 (kota saat transaksi)."""
    with KUNCI:
        rows = con.execute("""
            WITH a AS (SELECT n1.kota AS kota, SUM(f.nominal)::DOUBLE v FROM fakta_transaksi f
                       JOIN dim_nasabah_scd1 n1 ON n1.id_nasabah = f.id_nasabah GROUP BY 1),
                 b AS (SELECT n2.kota AS kota, SUM(f.nominal)::DOUBLE v FROM fakta_transaksi f
                       JOIN dim_nasabah n2 USING (sk_nasabah) GROUP BY 1)
            SELECT coalesce(a.kota, b.kota), coalesce(a.v, 0), coalesce(b.v, 0) FROM a FULL JOIN b ON a.kota = b.kota ORDER BY 3 DESC""").fetchall()
        versi = con.execute("SELECT count(*), count(DISTINCT id_nasabah) FROM dim_nasabah").fetchone()
    return {"kota": [{"kota": k, "scd1": a, "scd2": b, "selisih": a - b} for k, a, b in rows],
            "baris_dim_scd2": versi[0], "nasabah": versi[1]}


class Format(BaseModel):
    n: int = 200000
    sumber: str = "lab"


@app.post("/api/format/jalankan")
def format_jalankan(f: Format):
    if not 1_000 <= f.n <= 3_000_000:
        raise HTTPException(400, "n harus 1.000 sampai 3.000.000")
    with KUNCI:
        tabel = "transaksi"
        if f.sumber == "platform":
            try:
                t = formats.baca_platform()
            except Exception as e:
                raise HTTPException(502, f"bronze platform tidak terbaca: {e}")
            if t is None:
                raise HTTPException(400, "PLATFORM_S3_ENDPOINT kosong")
            con.register("bronze_platform_arrow", t)
            con.execute("CREATE OR REPLACE TABLE bronze_platform AS SELECT * FROM bronze_platform_arrow")
            con.unregister("bronze_platform_arrow")
            tabel = "bronze_platform"
        return formats.bandingkan_format(con, tabel, f.n)


@app.get("/api/parquet/anatomi")
def anatomi(urut: bool = True, kolom: str = "kanal,nominal", dari: str = "", row_group: int = 0):
    with KUNCI:
        n = con.execute("SELECT count(*) FROM transaksi").fetchone()[0]
        rg = row_group if row_group > 0 else max(1000, n // 12)
        if not dari:
            dari = (data.hari_ini() - dt.timedelta(days=30)).isoformat()
        return formats.anatomi_parquet(con, urut, [k.strip() for k in kolom.split(",") if k.strip()], dari, rg)


@app.get("/api/evolusi")
def evolusi():
    with KUNCI:
        return formats.uji_evolusi(con)


@app.get("/api/platform")
def platform():
    ep = os.environ.get("PLATFORM_S3_ENDPOINT", "")
    if not ep:
        return {"aktif": False, "pesan": "PLATFORM_S3_ENDPOINT kosong: lab memakai generator sendiri"}
    try:
        t = formats.baca_platform()
        return {"aktif": True, "endpoint": ep, "baris_bronze_app_transaksi": t.num_rows}
    except Exception as e:
        return {"aktif": False, "pesan": f"bronze platform tidak terbaca: {str(e)[:300]}"}
