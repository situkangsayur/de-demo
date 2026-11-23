"""Tiering (W9): pasang lifecycle rule di bucket RustFS dan simulasikan tier setiap objek.

Kebijakan (README-DATA.md): warm 0-13 bulan, cold 13-24 bulan, archive 24 bulan-10 tahun, lalu dihapus.
RustFS hanya bisa memindahkan objek ke tier lain bila tier remote sudah didaftarkan admin
(mis. bucket S3 lain sebagai COLD). Di lab tidak ada tier remote, jadi siapkan() mencoba aturan
transisi lebih dulu; bila ditolak, yang dipasang adalah aturan expiration per prefix, sedangkan
transisi ditulis di tiering/lifecycle-produksi-s3.json dan disimulasikan oleh simulasi().
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
from collections import defaultdict

from .konfigurasi import BUCKET, boto, log

DIR = os.path.join(os.environ.get("DATA_DIR", "/opt/data"), "tiering")
WARM_HARI, COLD_HARI, ARCHIVE_HARI, HAPUS_HARI = 0, 395, 730, 3650
_DT = re.compile(r"(?:dt|tgl|tgl_cair)=(\d{4}-\d{2}-\d{2})")


def _baca(nama: str) -> dict:
    with open(os.path.join(DIR, nama)) as f:
        return json.load(f)


def pasang() -> dict:
    s3 = boto()
    dasar = _baca("lifecycle-rustfs.json")
    transisi = _baca("lifecycle-rustfs-transisi.json")
    status = {"mode": None, "pesan_transisi": None}
    try:
        aturan = {"Rules": transisi["Rules"] + [r for r in dasar["Rules"] if r["ID"] != "bronze-retensi-10-tahun"]}
        s3.put_bucket_lifecycle_configuration(Bucket=BUCKET, LifecycleConfiguration=aturan)
        status["mode"] = "transisi"
    except Exception as e:  # tier COLD/ARCHIVE belum terdaftar di RustFS
        status["pesan_transisi"] = str(e)[:300]
        s3.put_bucket_lifecycle_configuration(Bucket=BUCKET, LifecycleConfiguration=dasar)
        status["mode"] = "expiration"
    log("info", "lifecycle dipasang", bucket=BUCKET, **status)
    return status


def aturan_terpasang() -> list:
    try:
        return boto().get_bucket_lifecycle_configuration(Bucket=BUCKET).get("Rules", [])
    except Exception as e:
        return [{"error": str(e)[:300]}]


def tier_untuk(umur_hari: int, prefix: str) -> str:
    if prefix.startswith("staging"):
        return "warm"  # staging ditimpa setiap run; dirawat dengan expire_snapshots, bukan lifecycle
    if prefix.startswith("gold"):
        return "warm"  # gold dibangun ulang dari silver; selalu warm selama dipakai dashboard
    if umur_hari > HAPUS_HARI and prefix.startswith("bronze"):
        return "dihapus"
    if umur_hari > ARCHIVE_HARI and prefix.startswith("bronze"):
        return "archive"
    if umur_hari > COLD_HARI:
        return "cold"
    return "warm"


def simulasi(geser_hari: int = 0) -> dict:
    """Kelompokkan objek bucket menurut tier yang berlaku bila 'hari ini' digeser geser_hari ke depan.

    Umur objek bronze dihitung dari partisi dt= (tanggal kejadian), objek lain dari LastModified.
    """
    s3 = boto()
    hari_ini = dt.date.today() + dt.timedelta(days=geser_hari)
    ringkas = defaultdict(lambda: {"objek": 0, "byte": 0})
    per_partisi = defaultdict(lambda: {"objek": 0, "byte": 0, "tier": ""})
    for hal in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for o in hal.get("Contents", []):
            k = o["Key"]
            lapisan = k.split("/")[0]
            m = _DT.search(k)
            tgl = dt.date.fromisoformat(m.group(1)) if m else o["LastModified"].date()
            tier = tier_untuk((hari_ini - tgl).days, lapisan)
            ringkas[(lapisan, tier)]["objek"] += 1
            ringkas[(lapisan, tier)]["byte"] += o["Size"]
            if lapisan == "bronze" and m:
                p = per_partisi[tgl.isoformat()]
                p["objek"] += 1
                p["byte"] += o["Size"]
                p["tier"] = tier
    return {
        "hari_acuan": hari_ini.isoformat(), "geser_hari": geser_hari,
        "ringkasan": [{"lapisan": l, "tier": t, **v} for (l, t), v in sorted(ringkas.items())],
        "bronze_per_tgl": [{"tgl": k, **v} for k, v in sorted(per_partisi.items())],
        "batas_hari": {"cold": COLD_HARI, "archive": ARCHIVE_HARI, "hapus": HAPUS_HARI},
    }
