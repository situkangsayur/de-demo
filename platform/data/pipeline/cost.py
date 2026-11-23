"""Perhitungan cost penyimpanan 3 tahun (W9), model slide P08:

    B_m = sum_t V_{t,m} * h_t  +  R_m * h_ambil  +  E_m * h_egress

V_{t,m} dihitung per kohort bulanan: data yang masuk pada bulan c berumur (m - c) bulan pada bulan m
dan berada di tier sesuai kebijakan. Tiga skenario: semua_hot, kebijakan (warm 13 bulan -> cold
sampai 24 bulan -> archive), dan paling_hemat. Sensitivitas: laju data dua kali lipat.

Harga: AWS S3 region Asia Pacific (Jakarta) ap-southeast-3, Price List API (offer AmazonS3,
publicationDate 2026-11-21; offer AWSDataTransfer, publicationDate 2026-11-19), diakses 23 Nov 2026.
Glacier Deep Archive tidak tersedia di region Jakarta pada daftar harga tersebut.
Pemakaian: python -m pipeline.cost [--markdown]
"""
from __future__ import annotations

import argparse
import datetime as dt
import os

HARGA = {  # USD per GB-bulan (simpan) / per GB (ambil, egress)
    "standard": 0.025, "glacier_ir": 0.005, "glacier_flexible": 0.0045,
    "ambil_glacier_ir": 0.03, "ambil_flexible_standard": 0.012, "egress_internet": 0.132,
}
SUMBER_HARGA = [
    ("standard", "S3 Standard, 50 TB pertama", "APS4-TimedStorage-ByteHrs"),
    ("glacier_ir", "S3 Glacier Instant Retrieval", "APS4-TimedStorage-GIR-ByteHrs"),
    ("glacier_flexible", "S3 Glacier Flexible Retrieval", "APS4-TimedStorage-GlacierByteHrs"),
    ("ambil_glacier_ir", "Retrieval Glacier IR per GB", "APS4-Retrieval-GIR"),
    ("ambil_flexible_standard", "Retrieval Glacier Flexible (standard, 3-5 jam) per GB", "APS4-Standard-Retrieval-Bytes"),
    ("egress_internet", "Data transfer out ke internet, 10 TB pertama (100 GB/bulan gratis global)", "APS4-DataTransfer-Out-Bytes"),
]
TANGGAL_AKSES = "2026-11-23"
KURS = float(os.environ.get("COST_KURS_USD_IDR", "16500"))  # asumsi kurs, bukan kurs resmi

# Asumsi volume (bisa diubah lewat environment)
BRONZE_GB_HARI = float(os.environ.get("COST_BRONZE_GB_HARI", "1.0"))
TUMBUH = float(os.environ.get("COST_TUMBUH_PER_BULAN", "0.05"))
RASIO_SILVER = 0.5   # silver: kolumnar, dedup, tanpa kolom mentah
RASIO_GOLD = 0.2
EGRESS_GB = float(os.environ.get("COST_EGRESS_GB_BULAN", "150"))
BULAN = 36

# tier per (lapisan, umur bulan) untuk setiap skenario
def tier(skenario: str, lapisan: str, umur: int) -> str:
    if skenario == "semua_hot" or lapisan == "gold":
        return "standard"
    if skenario == "kebijakan":
        if umur < 13:
            return "standard"
        if lapisan == "silver" or umur < 24:
            return "glacier_ir"       # tetap bisa di-query seketika: analisis 2 tahun
        return "glacier_flexible"     # bronze > 24 bulan: archive, ambil 3-5 jam
    # paling_hemat: bronze langsung archive setelah 1 bulan, silver cold setelah 1 bulan
    if umur < 1:
        return "standard"
    return "glacier_flexible" if lapisan == "bronze" else "glacier_ir"


def hitung(skenario: str, laju: float) -> list[dict]:
    masuk = [laju * 30 * (1 + TUMBUH) ** c for c in range(BULAN)]  # GB bronze per bulan
    rasio = {"bronze": 1.0, "silver": RASIO_SILVER, "gold": RASIO_GOLD}
    hasil, kum = [], 0.0
    for m in range(BULAN):
        vol = {"standard": 0.0, "glacier_ir": 0.0, "glacier_flexible": 0.0}
        for c in range(m + 1):
            for lap, r in rasio.items():
                vol[tier(skenario, lap, m - c)] += masuk[c] * r
        simpan = sum(v * HARGA[t] for t, v in vol.items())
        # R_m: analisis tahunan (bulan 12, 24, 36) membaca seluruh silver 24 bulan terakhir
        ambil = 0.0
        if (m + 1) % 12 == 0:
            for c in range(max(0, m - 23), m + 1):
                t = tier(skenario, "silver", m - c)
                gb = masuk[c] * RASIO_SILVER
                ambil += gb * (HARGA["ambil_glacier_ir"] if t == "glacier_ir" else
                               HARGA["ambil_flexible_standard"] if t == "glacier_flexible" else 0)
        egress = max(0.0, EGRESS_GB - 100) * HARGA["egress_internet"]
        total = simpan + ambil + egress
        kum += total
        hasil.append({"bulan": m + 1, "warm_gb": round(vol["standard"], 1), "cold_gb": round(vol["glacier_ir"], 1),
                      "archive_gb": round(vol["glacier_flexible"], 1), "simpan_usd": round(simpan, 2),
                      "ambil_usd": round(ambil, 2), "egress_usd": round(egress, 2), "total_usd": round(total, 2),
                      "kumulatif_usd": round(kum, 2)})
    return hasil


def semua() -> dict:
    out = {"harga": HARGA, "tanggal_akses": TANGGAL_AKSES, "kurs_usd_idr": KURS,
           "asumsi": {"bronze_gb_hari_awal": BRONZE_GB_HARI, "tumbuh_per_bulan": TUMBUH, "rasio_silver": RASIO_SILVER,
                      "rasio_gold": RASIO_GOLD, "egress_gb_bulan": EGRESS_GB},
           "skenario": {}}
    for k in ("1x", "2x"):
        laju = BRONZE_GB_HARI * (1 if k == "1x" else 2)
        for s in ("semua_hot", "kebijakan", "paling_hemat"):
            out["skenario"][f"{s}@{k}"] = hitung(s, laju)
    hot, keb = out["skenario"]["semua_hot@1x"], out["skenario"]["kebijakan@1x"]
    out["bulan_mulai_untung"] = next((b["bulan"] for b, h in zip(keb, hot) if b["total_usd"] < h["total_usd"]), None)
    return out


def simpan_clickhouse(hasil: dict) -> None:
    from .konfigurasi import clickhouse
    ch = clickhouse()
    ch.command("TRUNCATE TABLE IF EXISTS meta.cost_bulanan")
    ch.command("TRUNCATE TABLE IF EXISTS meta.cost_harga")
    rows = []
    for nama, baris in hasil["skenario"].items():
        s, laju = nama.split("@")
        for b in baris:
            rows.append([s, laju, b["bulan"], b["warm_gb"], b["cold_gb"], b["archive_gb"], b["simpan_usd"],
                         b["ambil_usd"], b["egress_usd"], b["total_usd"], b["kumulatif_usd"]])
    ch.insert("meta.cost_bulanan", rows, column_names=["skenario", "laju", "bulan", "warm_gb", "cold_gb", "archive_gb",
                                                       "simpan_usd", "ambil_usd", "egress_usd", "total_usd", "kumulatif_usd"])
    ch.insert("meta.cost_harga", [[k, d, u, HARGA[k], dt.date.fromisoformat(TANGGAL_AKSES)] for k, d, u in SUMBER_HARGA],
              column_names=["kunci", "deskripsi", "usagetype_aws", "usd", "tanggal_akses"])


def markdown(hasil: dict) -> str:
    baris = ["| Skenario | Laju | Total 3 tahun (USD) | ≈ Rp (kurs asumsi) | Bulan 36 (USD/bulan) |", "|---|---|---:|---:|---:|"]
    for nama, b in hasil["skenario"].items():
        s, laju = nama.split("@")
        baris.append(f"| {s} | {laju} | {b[-1]['kumulatif_usd']:,.2f} | {b[-1]['kumulatif_usd'] * KURS:,.0f} | "
                     f"{b[-1]['total_usd']:,.2f} |")
    baris.append(f"\nKebijakan mulai lebih murah daripada semua_hot pada bulan ke-{hasil['bulan_mulai_untung']}.")
    return "\n".join(baris)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--markdown", action="store_true")
    a = ap.parse_args()
    h = semua()
    if a.markdown:
        print(markdown(h))
    else:
        simpan_clickhouse(h)
        print(markdown(h))


if __name__ == "__main__":
    main()
