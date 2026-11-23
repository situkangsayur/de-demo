"""API lab Bab 4 (FastAPI). Semua endpoint di bawah /api; UI Next.js meneruskan /api/* ke sini."""
from __future__ import annotations

import threading
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import bronze, iceberg_lab, medallion
from .konfig import PLATFORM_S3_ENDPOINT, buat_bucket, log

app = FastAPI(title="de-bab04 lakehouse medallion")
KUNCI = threading.Lock()  # satu penulis pada satu waktu: commit Iceberg lab dibuat berurutan


@app.on_event("startup")
def mulai() -> None:
    buat_bucket()
    if not bronze.daftar_dt("transaksi"):
        with KUNCI:
            bronze.tambah_hari(3)
    log("info", "api siap")


@app.middleware("http")
async def catat(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
    t0 = time.perf_counter()
    try:
        resp = await call_next(request)
    except Exception as e:  # noqa: BLE001
        log("error", "permintaan gagal", request_id=rid, path=request.url.path, error=str(e))
        return JSONResponse({"error": str(e)}, status_code=500)
    if request.url.path != "/api/sehat":
        log("info", "permintaan", request_id=rid, method=request.method, path=request.url.path,
        status=resp.status_code, ms=round((time.perf_counter() - t0) * 1000, 1))
    return resp


def aman(fn, *a, **kw):
    try:
        with KUNCI:
            return fn(*a, **kw)
    except (ValueError, KeyError, RuntimeError) as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.get("/api/sehat")
def sehat():
    return {"status": "ok"}


# ---------------------------------------------------------------- bronze
@app.get("/api/bronze")
def bronze_ringkas():
    return {**bronze.ringkasan(), "platform_tersedia": bool(PLATFORM_S3_ENDPOINT)}


@app.get("/api/bronze/contoh")
def bronze_contoh(entitas: str = "transaksi"):
    return bronze.contoh(entitas)


@app.post("/api/bronze/tambah-hari")
def bronze_tambah(hari: int = 1):
    return aman(bronze.tambah_hari, max(1, min(hari, 5)))


@app.post("/api/bronze/kotor")
def bronze_kotor():
    return aman(bronze.sisipkan_kotor)


@app.post("/api/bronze/pulihkan")
def bronze_pulihkan():
    return aman(bronze.pulihkan_kotor)


@app.post("/api/bronze/dari-platform")
def bronze_platform(hari: int = 2):
    return aman(bronze.ambil_dari_platform, hari)


# ---------------------------------------------------------------- medallion
@app.post("/api/medallion/jalankan")
def medallion_jalan(terakhir: bool = False):
    return aman(medallion.jalankan, terakhir)


@app.get("/api/medallion/lapisan")
def medallion_lapisan():
    return {"tabel": medallion.jumlah_lapisan(), "riwayat": medallion.muat_riwayat()[:8]}


@app.get("/api/medallion/lacak")
def medallion_lacak(trx_id: str = ""):
    return aman(medallion.lacak, trx_id or None)


@app.get("/api/medallion/karantina")
def medallion_karantina():
    out = {}
    for nama in ("silver.karantina_transaksi", "silver.akad_karantina"):
        df = medallion.baca(nama)
        out[nama] = {"kolom": df.columns, "baris": [[None if v is None else str(v) for v in r]
                                                    for r in df.tail(30).rows()], "total": df.height}
    return out


@app.get("/api/medallion/gold")
def medallion_gold():
    df = medallion.baca("gold.fakta_trx_harian")
    return {"kolom": df.columns, "baris": [[str(v) if k == "tgl" else v for k, v in zip(df.columns, r)]
                                           for r in df.rows()]}


# ---------------------------------------------------------------- iceberg
@app.get("/api/iceberg/tabel")
def ice_tabel():
    return {"tabel": iceberg_lab.daftar_tabel()}


@app.get("/api/iceberg/pohon")
def ice_pohon(tabel: str = iceberg_lab.LAB, snapshot_id: int | None = None):
    return aman(iceberg_lab.pohon, tabel, snapshot_id)


@app.post("/api/iceberg/operasi/{op}")
def ice_operasi(op: str, snapshot_id: int | None = None):
    return aman(iceberg_lab.jalankan, op, snapshot_id=snapshot_id)


@app.get("/api/iceberg/time-travel")
def ice_time_travel(tabel: str = iceberg_lab.LAB, snapshot_id: int | None = None):
    return aman(iceberg_lab.query_snapshot, tabel, snapshot_id)


@app.get("/api/iceberg/pruning")
def ice_pruning(tabel: str = iceberg_lab.LAB, dari: str = ""):
    return aman(iceberg_lab.pruning, tabel, dari)


@app.post("/api/iceberg/sql")
async def ice_sql(request: Request):
    body = await request.json()
    try:
        return iceberg_lab.sql_duckdb(body.get("sql", ""))
    except Exception as e:  # noqa: BLE001  (kesalahan SQL mahasiswa ditampilkan apa adanya)
        return JSONResponse({"error": str(e)}, status_code=400)
