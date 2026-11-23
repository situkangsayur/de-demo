"""API lab Bab 5 (FastAPI). Semua endpoint di bawah /api; UI Next.js meneruskan /api/* ke sini."""
from __future__ import annotations

import os
import threading
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import bench, data, pipeline, rencana
from .konfig import DATA, UKURAN, batas_memori_mb, cpu, log

app = FastAPI(title="de-bab05 ETL satu mesin")
KUNCI = threading.Lock()


@app.on_event("startup")
def mulai() -> None:
    os.makedirs(DATA, exist_ok=True)
    if not data.info(UKURAN[0])["ada"]:
        data.bangkitkan(UKURAN[0])      # dataset terkecil langsung tersedia untuk demo
    log("info", "api siap", batas_memori_mb=batas_memori_mb(), cpu=cpu())


@app.middleware("http")
async def catat(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
    t0 = time.perf_counter()
    try:
        resp = await call_next(request)
    except Exception as e:  # noqa: BLE001
        log("error", "permintaan gagal", request_id=rid, path=request.url.path, error=str(e))
        return JSONResponse({"error": str(e)}, status_code=500)
    if request.url.path not in ("/api/sehat", "/api/job"):
        log("info", "permintaan", request_id=rid, method=request.method, path=request.url.path,
            status=resp.status_code, ms=round((time.perf_counter() - t0) * 1000, 1))
    return resp


def aman(fn, *a, kunci: bool = False, **kw):
    try:
        if kunci:
            with KUNCI:
                return fn(*a, **kw)
        return fn(*a, **kw)
    except (ValueError, KeyError, RuntimeError, FileNotFoundError) as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.get("/api/sehat")
def sehat():
    return {"status": "ok"}


@app.get("/api/info")
def info():
    return {"dataset": data.daftar(), "ukuran": UKURAN, "batas_memori_mb": batas_memori_mb(), "cpu": cpu(),
            "bronze": data.ringkas_bronze(), "engine": bench.ENGINE}


@app.get("/api/job")
def job():
    return bench.status()


@app.post("/api/dataset")
def dataset(baris: int):
    if baris not in UKURAN:
        return JSONResponse({"error": f"ukuran harus salah satu dari {UKURAN}"}, status_code=400)
    return aman(bench.mulai_dataset, baris)


@app.post("/api/dataset/hapus")
def dataset_hapus(baris: int):
    return aman(data.hapus, baris)


@app.post("/api/benchmark")
def benchmark(baris: int, engine: str = "", ulang: int = 3):
    return aman(bench.mulai_benchmark, baris, [e for e in engine.split(",") if e], ulang)


@app.get("/api/benchmark/riwayat")
def benchmark_riwayat():
    return {"riwayat": bench.muat_riwayat()[:10], "tabel": bench.tabel_ringkas()}


@app.get("/api/rencana")
def rencana_(baris: int):
    if not data.info(baris)["ada"]:
        return JSONResponse({"error": f"dataset {baris} baris belum dibangkitkan"}, status_code=400)
    return aman(rencana.rencana, data.folder(baris), f"{DATA}/nasabah.parquet")


@app.post("/api/zerocopy")
def zerocopy(baris: int):
    return aman(bench.mulai_zerocopy, baris)


@app.post("/api/memori")
def memori(baris: int, anggaran_mb: int = 512):
    return aman(bench.mulai_memori, baris, max(128, min(anggaran_mb, 4096)))


@app.post("/api/pipeline/bronze")
def pipeline_bronze(baris: int = 300_000):
    return aman(data.bangkitkan_bronze, max(30_000, min(baris, 1_500_000)), kunci=True)


@app.post("/api/pipeline/jalankan")
def pipeline_jalankan():
    return aman(pipeline.jalankan, kunci=True)


@app.get("/api/pipeline/status")
def pipeline_status():
    return {"bronze": data.ringkas_bronze(), "lapisan": pipeline.hitung_lapisan(),
            "riwayat": pipeline.muat_riwayat(), "gold": pipeline.gold_tabel(),
            "terakhir": pipeline.terakhir()}
