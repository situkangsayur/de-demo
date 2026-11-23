"""Pengatur pekerjaan panjang (benchmark, batas memori, zero-copy) di satu thread latar.

Satu pekerjaan pada satu waktu: benchmark yang berjalan bersamaan saling mengganggu dan hasilnya
tidak bisa dibandingkan. Setiap pengukuran adalah proses Python baru (app.kerja).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import statistics
import subprocess
import sys
import threading
import uuid

from . import data
from .konfig import DATA, log

RIWAYAT = f"{DATA}/riwayat-benchmark.json"
ENGINE = ["pandas", "polars_eager", "polars_lazy", "polars_streaming", "duckdb", "datafusion"]
ENGINE_BERAT = ["pandas", "polars_eager", "polars_streaming", "duckdb"]
_job: dict = {}
_kunci = threading.Lock()


def status() -> dict:
    return dict(_job) if _job else {"status": "kosong"}


def _mulai(jenis: str, fn, **kw) -> dict:
    global _job
    with _kunci:
        if _job.get("status") == "berjalan":
            raise ValueError(f"masih ada pekerjaan berjalan: {_job['jenis']}")
        _job = {"id": uuid.uuid4().hex[:8], "jenis": jenis, "status": "berjalan", "progres": [], "hasil": None,
                "mulai": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "param": kw}

    def kerja():
        try:
            _job["hasil"] = fn(**kw)
            _job["status"] = "selesai"
        except Exception as e:  # noqa: BLE001
            _job["status"] = "gagal"
            _job["error"] = str(e)
            log("error", "pekerjaan gagal", jenis=jenis, error=str(e))
    threading.Thread(target=kerja, daemon=True).start()
    return status()


def _anak(arg: dict, modul: str = "app.kerja", batas_detik: int = 600) -> dict:
    try:
        r = subprocess.run([sys.executable, "-m", modul, json.dumps(arg) if modul == "app.kerja" else arg["folder"]],
                           capture_output=True, text=True, timeout=batas_detik, cwd="/app")
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"melewati batas waktu {batas_detik} detik", "engine": arg.get("engine")}
    if r.returncode != 0:
        alasan = "dibunuh OOM killer (melewati mem_limit container)" if r.returncode in (-9, 137) \
            else (r.stderr.strip().splitlines() or [f"exit {r.returncode}"])[-1][:300]
        return {"ok": False, "error": alasan, "engine": arg.get("engine"), "kode_keluar": r.returncode}
    return json.loads(r.stdout.strip().splitlines()[-1])


def _progres(teks: str) -> None:
    _job["progres"].append(teks)
    log("info", "progres", jenis=_job.get("jenis"), teks=teks)


# ---------------------------------------------------------------- benchmark
def mulai_benchmark(baris: int, engine: list[str], ulang: int = 3) -> dict:
    if not data.info(baris)["ada"]:
        raise ValueError(f"dataset {baris} baris belum dibangkitkan")
    engine = [e for e in engine if e in ENGINE] or ENGINE
    return _mulai("benchmark", _benchmark, baris=baris, engine=engine, ulang=max(1, min(ulang, 5)))


def _benchmark(baris: int, engine: list[str], ulang: int) -> dict:
    folder, nas = data.folder(baris), f"{DATA}/nasabah.parquet"
    hasil = []
    for e in engine:
        runs = []
        for i in range(ulang):
            _progres(f"{e} percobaan {i + 1}/{ulang}")
            runs.append(_anak({"engine": e, "folder": folder, "nasabah": nas}))
            if not runs[-1].get("ok"):
                break
        ok = [r for r in runs if r.get("ok")]
        hasil.append({
            "engine": e, "ok": len(ok) == len(runs),
            "median_ms": round(statistics.median(r["ms"] for r in ok), 1) if ok else None,
            "semua_ms": [r["ms"] for r in ok],
            "puncak_mb": max((r["puncak_mb"] for r in ok), default=None),
            "baris_hasil": ok[0]["baris"] if ok else None, "checksum_total": ok[0]["checksum_total"] if ok else None,
            "error": next((r["error"] for r in runs if not r.get("ok")), None)})
    cek = {h["checksum_total"] for h in hasil if h["ok"]}
    rekam = {"waktu": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "baris": baris,
             "ulang": ulang, "hasil": hasil, "hasil_sama": len(cek) == 1, "cpu": os.cpu_count()}
    riwayat = muat_riwayat()
    riwayat.insert(0, rekam)
    with open(RIWAYAT, "w") as f:
        json.dump(riwayat[:50], f)
    return rekam


def muat_riwayat() -> list:
    try:
        with open(RIWAYAT) as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def tabel_ringkas() -> dict:
    """Tabel seperti slide praktikum: median terbaru per engine per ukuran, plus memori puncak."""
    sel: dict[str, dict[int, dict]] = {}
    for r in reversed(muat_riwayat()):
        for h in r["hasil"]:
            if h["ok"]:
                sel.setdefault(h["engine"], {})[r["baris"]] = {"ms": h["median_ms"], "mb": h["puncak_mb"]}
    ukuran = sorted({b for v in sel.values() for b in v})
    return {"ukuran": ukuran, "baris": [{"engine": e, "sel": {str(b): sel[e].get(b) for b in ukuran}}
                                        for e in ENGINE if e in sel]}


# ---------------------------------------------------------------- batas memori
def mulai_memori(baris: int, anggaran_mb: int) -> dict:
    if not data.info(baris)["ada"]:
        raise ValueError(f"dataset {baris} baris belum dibangkitkan")
    return _mulai("memori", _memori, baris=baris, anggaran_mb=anggaran_mb)


def _memori(baris: int, anggaran_mb: int) -> dict:
    hasil = []
    tmp = f"{DATA}/tmp-spill"
    for e in ENGINE_BERAT:
        _progres(f"query berat: {e}")
        shutil.rmtree(tmp, ignore_errors=True)
        r = _anak({"jenis": "berat", "engine": e, "folder": data.folder(baris), "anggaran_mb": anggaran_mb,
                   "tmp": tmp})
        r["melewati_anggaran"] = bool(r.get("puncak_mb") and r["puncak_mb"] > anggaran_mb)
        hasil.append(r)
    shutil.rmtree(tmp, ignore_errors=True)
    return {"baris": baris, "anggaran_mb": anggaran_mb, "hasil": hasil}


# ---------------------------------------------------------------- zero-copy
def mulai_zerocopy(baris: int) -> dict:
    if not data.info(baris)["ada"]:
        raise ValueError(f"dataset {baris} baris belum dibangkitkan")
    return _mulai("zerocopy", lambda baris: _anak({"folder": data.folder(baris)}, modul="app.zerocopy"), baris=baris)


def mulai_dataset(baris: int) -> dict:
    return _mulai("dataset", data.bangkitkan, n=baris)
