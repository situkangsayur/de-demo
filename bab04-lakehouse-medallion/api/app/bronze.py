"""Bronze sintetis berskema platform (platform/ops/SKEMA.md), ditulis ke RustFS lab.

Path: s3://lake/bronze/<app|cdc>/<entitas>/dt=YYYY-MM-DD/part-<batch_id>.parquet
Kolom: 8 kolom metadata (_ingested_at, _source, _batch_id, _event_id, _event_type, _occurred_at,
_ts_ms, _op), lalu _before (khusus cdc), lalu kolom data yang semuanya string.

Kerumitan sumber nyata yang sengaja ditiru (agar aturan silver terlihat bekerja):
- 75% transaksi datang dari app dan cdc sekaligus, 15% hanya cdc, 10% hanya app;
- kode kanal tidak seragam ('ATM', 'Mobile Banking', 'teller ', 'WEB');
- ±1% duplikat bisnis (request ulang tanpa idempotency key) dan ±2% data terlambat;
- ±5% kode akad tidak valid, perubahan profil nasabah (op=u dengan _before).
Deterministik per hari: hari ke-i selalu menghasilkan isi yang sama.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import random
import re
import uuid

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs

from .konfig import BUCKET, log, s3fs, s3fs_platform, PLATFORM_S3_BUCKET

WIB = dt.timezone(dt.timedelta(hours=7))
UTC = dt.timezone.utc
STATE = os.environ.get("STATE_DIR", "/data") + "/bronze-state.json"
ENTITAS = ("cabang", "nasabah", "rekening", "transaksi", "akad_pembiayaan")
META = ["_ingested_at", "_source", "_batch_id", "_event_id", "_event_type", "_occurred_at", "_ts_ms", "_op"]

CABANG = [(1, "JKT-SDR", "KC Jakarta Sudirman", "Jakarta", "Jabodetabek"),
          (2, "JKT-01", "KC Jakarta Selatan", "Jakarta", "Jabodetabek"),
          (3, "BGR-01", "KC Bogor", "Bogor", "Jabodetabek"),
          (4, "BDG-01", "KC Bandung", "Bandung", "Jawa Barat"),
          (5, "SBY-01", "KC Surabaya", "Surabaya", "Jawa Timur"),
          (6, "MDN-01", "KC Medan", "Medan", "Sumatra"),
          (7, "YGY-01", "KC Yogyakarta", "Yogyakarta", "Jawa Tengah-DIY"),
          (8, "MKS-01", "KC Makassar", "Makassar", "Sulawesi")]
NAMA_D = ["Ahmad", "Siti", "Muhammad", "Nur", "Rizki", "Dewi", "Fajar", "Aisyah", "Budi", "Fatimah"]
NAMA_B = ["Pratama", "Hidayat", "Saputra", "Lestari", "Nugroho", "Siregar", "Wibowo", "Maulana"]
KANAL_RAW = {"mobile": ["mobile", "Mobile Banking", "m-banking"], "atm": ["ATM", "atm"],
             "teller": ["teller", "teller "], "web": ["web", "WEB"], "ecommerce": ["ecommerce", "e-commerce"]}
BOBOT_KANAL = {"mobile": 45, "atm": 18, "teller": 9, "web": 13, "ecommerce": 15}
JENIS = {"mobile": ["transfer_keluar", "transfer_masuk", "angsuran"], "atm": ["tarik", "setor"],
         "teller": ["setor", "tarik", "angsuran"], "web": ["transfer_keluar", "transfer_masuk"],
         "ecommerce": ["bayar_belanja"]}
AKAD = ["MRB", "MRB", "MSY", "MDH", "IJR", "murabahah", "mrb"]
AKAD_RUSAK = ["MRBH", "XYZ", ""]
N_NASABAH = 240
TRX_PER_HARI = 900


def _state() -> dict:
    try:
        with open(STATE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def _simpan_state(s: dict) -> None:
    with open(STATE, "w") as f:
        json.dump(s, f)


def _iso(t: dt.datetime) -> str:
    return t.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Penampung:
    """Mengumpulkan baris per (sumber, entitas, dt UTC) lalu menulis satu Parquet per kelompok."""

    def __init__(self, r: random.Random, tag: str):
        self.r, self.tag, self.isi = r, tag, {}
        self.lsn = r.randint(10**7, 10**8)

    def tambah(self, sumber: str, entitas: str, waktu: dt.datetime, data: dict, op: str,
               aksi: str = "dibuat", before: dict | None = None, telat: dt.timedelta | None = None) -> None:
        masuk = waktu + (telat or dt.timedelta(seconds=self.r.randint(1, 90)))
        self.lsn += self.r.randint(1, 400)
        ent_subj = "akad" if entitas == "akad_pembiayaan" else entitas
        meta = {"_ingested_at": masuk.astimezone(UTC), "_source": sumber, "_batch_id": "",
                "_event_id": str(uuid.UUID(int=self.r.getrandbits(128))) if sumber == "app"
                else f"{entitas}:{self.lsn}:{data['id']}",
                "_event_type": f"bank.{ent_subj}.{aksi}" if sumber == "app" else f"{entitas}.{op}",
                "_occurred_at": _iso(waktu), "_ts_ms": int(masuk.timestamp() * 1000),
                "_op": op if sumber == "cdc" else None}
        isi = {k: (None if v is None else _iso(v) if isinstance(v, dt.datetime) else str(v)) for k, v in data.items()}
        if sumber == "cdc":
            isi = {"_before": json.dumps(before) if before else None, **isi}
        else:
            isi["aksi"] = aksi
        self.isi.setdefault((sumber, entitas, waktu.astimezone(UTC).date()), []).append({**meta, **isi})

    def tulis(self, fs=None, bucket: str = BUCKET) -> list[str]:
        fs = fs or s3fs()
        out = []
        for (sumber, entitas, d), rows in sorted(self.isi.items()):
            batch = f"{d:%Y%m%d}T{self.tag}-{sumber[:1]}{entitas[:3]}"
            for x in rows:
                x["_batch_id"] = batch
            out.append(tulis_parquet(rows, f"{bucket}/bronze/{sumber}/{entitas}/dt={d}/part-{batch}.parquet", fs))
        return out


def tulis_parquet(rows: list[dict], path: str, fs=None) -> str:
    kolom = META + [k for k in dict.fromkeys(k for x in rows for k in x) if k not in META]
    arr = {}
    for k in kolom:
        vals = [x.get(k) for x in rows]
        if k == "_ingested_at":
            arr[k] = pa.array(vals, pa.timestamp("us", tz="UTC"))
        elif k == "_ts_ms":
            arr[k] = pa.array(vals, pa.int64())
        else:
            arr[k] = pa.array(vals, pa.string())
    pq.write_table(pa.table(arr), path, filesystem=fs or s3fs(), compression="zstd")
    return path


def _waktu(r: random.Random, d: dt.date) -> dt.datetime:
    jam = r.choices(range(24), weights=[1, 1, 1, 1, 1, 2, 4, 6, 8, 9, 9, 9, 10, 9, 9, 9, 8, 8, 8, 7, 6, 5, 3, 2])[0]
    return dt.datetime(d.year, d.month, d.day, jam, r.randrange(60), r.randrange(60),
                       r.randrange(1000) * 1000, tzinfo=WIB)


def _nasabah_awal(awal: dt.date) -> list[dict]:
    r = random.Random(20261123)
    t0 = dt.datetime.combine(awal - dt.timedelta(days=30), dt.time(8, 0), WIB)
    out = []
    for i in range(1, N_NASABAH + 1):
        cab = r.choice(CABANG)
        nid = str(uuid.UUID(int=r.getrandbits(128)))
        out.append({"id": nid, "cif": f"CIF{i:07d}", "nama": f"{r.choice(NAMA_D)} {r.choice(NAMA_B)}",
                    "nik": f"3{r.randint(100, 999)}{r.randint(10**11, 10**12 - 1)}",
                    "no_hp": f"08{r.randint(11, 59)}{r.randint(10**7, 10**8 - 1)}",
                    "email": f"nasabah{i}@contoh.id", "kota": cab[3],
                    "segmen": r.choice(["ritel", "ritel", "prioritas", "umkm"]), "cabang_id": cab[0],
                    "created_at": t0 + dt.timedelta(minutes=i), "updated_at": t0 + dt.timedelta(minutes=i),
                    "_rek": {"id": str(uuid.UUID(int=r.getrandbits(128))), "no_rekening": f"71{i:08d}",
                             "nasabah_id": nid, "jenis": r.choice(["tabungan_wadiah", "giro", "deposito_mudharabah"]),
                             "saldo": r.randint(1, 500) * 100_000, "status": "aktif", "cabang_id": cab[0],
                             "dibuka_at": t0 + dt.timedelta(minutes=i), "updated_at": t0 + dt.timedelta(minutes=i)}})
    return out


def _awal() -> dt.date:
    s = _state()
    if "awal" not in s:
        s["awal"] = (dt.datetime.now(WIB).date() - dt.timedelta(days=10)).isoformat()
        s["hari"] = 0
        _simpan_state(s)
    return dt.date.fromisoformat(s["awal"])


def _snapshot_awal(awal: dt.date) -> None:
    """Snapshot awal Debezium (op=r) untuk cabang, nasabah, rekening."""
    p = Penampung(random.Random(1), "000000-init")
    t = dt.datetime.combine(awal - dt.timedelta(days=1), dt.time(5, 0), WIB)
    for cid, kode, nama, kota, wil in CABANG:
        p.tambah("cdc", "cabang", t, {"id": cid, "kode": kode, "nama": nama, "kota": kota, "wilayah": wil}, "r")
    for n in _nasabah_awal(awal):
        data = {k: v for k, v in n.items() if not k.startswith("_")}
        p.tambah("cdc", "nasabah", t, data, "r")
        p.tambah("cdc", "rekening", t, n["_rek"], "r")
    p.tulis()


def tambah_hari(jumlah: int = 1) -> dict:
    """Menambah bronze untuk hari berikutnya (incremental), mirip ingestor yang berjalan tiap hari."""
    awal = _awal()
    s = _state()
    if s.get("hari", 0) == 0:
        _snapshot_awal(awal)
    nasabah = _nasabah_awal(awal)
    berkas = []
    for _ in range(jumlah):
        i = s.get("hari", 0)
        d = awal + dt.timedelta(days=i)
        r = random.Random(1000 + i)
        p = Penampung(r, f"{i:02d}0000-{r.randrange(16**6):06x}")
        # perubahan profil (bahan SCD-2): cdc op=u membawa _before karena REPLICA IDENTITY FULL
        for n in r.sample(nasabah, k=4):
            lama = {k: v for k, v in n.items() if not k.startswith("_")}
            baru = dict(lama, kota=r.choice([c[3] for c in CABANG if c[3] != lama["kota"]]),
                        updated_at=_waktu(r, d))
            before = {k: (_iso(v) if isinstance(v, dt.datetime) else v) for k, v in lama.items()}
            p.tambah("app", "nasabah", baru["updated_at"], baru, "u", "diperbarui")
            p.tambah("cdc", "nasabah", baru["updated_at"], baru, "u", before=before)
        # akad pembiayaan
        for k in range(r.randint(4, 8)):
            n = r.choice(nasabah)
            t = _waktu(r, d)
            kode = r.choice(AKAD) if r.random() > 0.08 else r.choice(AKAD_RUSAK)
            pokok = r.choice([5, 10, 25, 50, 100, 250]) * 1_000_000
            a = {"id": f"AKD{d:%Y%m%d}{k:03d}", "nasabah_id": n["id"], "rekening_id": n["_rek"]["id"],
                 "kode_akad": kode, "pokok": pokok, "margin": int(pokok * r.choice([0.08, 0.1, 0.12])),
                 "tenor_bulan": r.choice([12, 24, 36]), "status": "cair", "terbayar": 0,
                 "diajukan_at": t - dt.timedelta(hours=2), "cair_at": t, "updated_at": t}
            p.tambah("app", "akad_pembiayaan", t, a, "u", "dicairkan")
            p.tambah("cdc", "akad_pembiayaan", t, a, "u")
        # transaksi
        nomor = 0
        for _ in range(int(TRX_PER_HARI * (0.8 if d.weekday() >= 5 else 1.0))):
            n = r.choice(nasabah)
            kanal = r.choices(list(BOBOT_KANAL), weights=list(BOBOT_KANAL.values()))[0]
            jenis = r.choice(JENIS[kanal])
            t = _waktu(r, d)
            nominal = r.choice([5, 10, 20, 25, 50, 75, 100, 150, 250, 500, 1000]) * 10_000
            ulang = 2 if r.random() < 0.01 else 1  # request ulang tanpa idempotency key
            for u in range(ulang):
                nomor += 1
                tk = t + dt.timedelta(seconds=3 * u)
                telat = dt.timedelta(hours=r.randint(2, 20)) if r.random() < 0.02 else None
                data = {"id": f"TRX{d:%Y%m%d}{nomor:05d}", "rekening_id": n["_rek"]["id"],
                        "cabang_id": n["cabang_id"], "kanal": r.choice(KANAL_RAW[kanal]), "jenis": jenis,
                        "nominal": nominal, "saldo_setelah": r.randint(1, 900) * 100_000, "occurred_at": tk,
                        "created_at": tk + (telat or dt.timedelta(milliseconds=1)),
                        "ref_order_id": f"ord-{r.randrange(16**8):08x}" if jenis == "bayar_belanja" else None,
                        "transfer_id": None, "akad_id": None,
                        "idempotency_key": None if ulang == 2 else f"idk-{d:%Y%m%d}-{nomor:05d}",
                        "keterangan": None, "request_id": f"req-{r.randrange(16**8):08x}"}
                x = r.random()
                if x < 0.90:
                    p.tambah("cdc", "transaksi", tk, data, "c", telat=telat)
                if x < 0.75 or x >= 0.90:
                    p.tambah("app", "transaksi", tk, data, "c", "dibuat", telat=telat)
        berkas += p.tulis()
        s["hari"] = i + 1
        _simpan_state(s)
        log("info", "bronze hari ditambah", tanggal=str(d), berkas=len(p.isi))
    return {"hari": s["hari"], "berkas_baru": len(berkas)}


# ---------------------------------------------------------------- data kotor untuk gerbang DQ
def path_kotor() -> str:
    d = max(daftar_dt("transaksi"))
    return f"{BUCKET}/bronze/app/transaksi/dt={d}/part-{d:%Y%m%d}T999999-kotor.parquet"


def sisipkan_kotor() -> dict:
    """Satu berkas app berisi baris rusak: kanal di luar domain, nominal teks/negatif, rekening kosong,
    dan satu transaksi yang nominalnya berbeda antara app dan cdc."""
    d = max(daftar_dt("transaksi"))
    contoh = pl.from_arrow(pq.read_table(daftar_berkas("transaksi", [d], sumber=("cdc",))[0][0],
                                         filesystem=s3fs())).head(1).to_dicts()[0]
    t = dt.datetime.combine(d, dt.time(10, 0), UTC)
    rusak = []
    for i, (kanal, nominal, rek) in enumerate([("satelit", "250000", contoh["rekening_id"]),
                                               ("mobile", "dua ratus ribu", contoh["rekening_id"]),
                                               ("atm", "-50000", contoh["rekening_id"]),
                                               ("teller", "100000", None)]):
        rusak.append({**contoh, "_source": "app", "_op": None, "_event_type": "bank.transaksi.dibuat",
                      "_event_id": f"kotor-{i}", "_batch_id": "kotor", "_ingested_at": t,
                      "_ts_ms": int(t.timestamp() * 1000), "id": f"KOTOR{i}", "kanal": kanal,
                      "nominal": nominal, "rekening_id": rek, "aksi": "dibuat"})
    # konflik: id yang sama dengan baris cdc, nominal berbeda
    rusak.append({**contoh, "_source": "app", "_op": None, "_event_type": "bank.transaksi.dibuat",
                  "_event_id": "kotor-konflik", "_batch_id": "kotor", "_ingested_at": t,
                  "_ts_ms": int(t.timestamp() * 1000), "nominal": str(int(contoh["nominal"]) + 1_000_000),
                  "aksi": "dibuat"})
    for x in rusak:
        x.pop("_before", None)
    tulis_parquet(rusak, path_kotor())
    return {"berkas": path_kotor(), "baris": len(rusak), "tanggal": str(d)}


def pulihkan_kotor() -> dict:
    hapus = [p for p, _, _ in daftar_berkas("transaksi") if p.endswith("-kotor.parquet")]
    for p in hapus:
        s3fs().delete_file(p)
    return {"dihapus": hapus}


# ---------------------------------------------------------------- membaca bronze
_DT = re.compile(r"dt=(\d{4}-\d{2}-\d{2})")


def daftar_berkas(entitas: str, dts=None, sumber=("app", "cdc"), fs=None, bucket: str = BUCKET):
    fs = fs or s3fs()
    hasil = []
    for s in sumber:
        infos = fs.get_file_info(pafs.FileSelector(f"{bucket}/bronze/{s}/{entitas}", recursive=True,
                                                   allow_not_found=True))
        for i in infos:
            m = _DT.search(i.path)
            if i.type == pafs.FileType.File and i.path.endswith(".parquet") and m:
                d = dt.date.fromisoformat(m.group(1))
                if dts is None or d in dts:
                    hasil.append((i.path, d, i.size))
    return sorted(hasil)


def daftar_dt(entitas: str) -> list[dt.date]:
    return sorted({d for _, d, _ in daftar_berkas(entitas)})


def baca(entitas: str, dts=None) -> pl.DataFrame:
    """Gabungan semua berkas app + cdc (skema boleh berbeda antar-berkas: diagonal_relaxed)."""
    bagian = []
    for path, d, _ in daftar_berkas(entitas, dts):
        df = pl.from_arrow(pq.read_table(path, filesystem=s3fs()))
        bagian.append(df.with_columns(pl.lit(path.rsplit("/", 1)[1]).alias("_file")))
    if not bagian:
        return pl.DataFrame()
    return pl.concat(bagian, how="diagonal_relaxed")


def ringkasan() -> dict:
    out = []
    for e in ENTITAS:
        bs = daftar_berkas(e)
        out.append({"entitas": e, "berkas": len(bs), "byte": sum(b[2] for b in bs),
                    "dt": sorted({str(b[1]) for b in bs})[-3:], "app": sum(1 for b in bs if "/app/" in b[0]),
                    "cdc": sum(1 for b in bs if "/cdc/" in b[0])})
    return {"entitas": out, "hari": _state().get("hari", 0),
            "kotor": any(p.endswith("-kotor.parquet") for p, _, _ in daftar_berkas("transaksi"))}


def contoh(entitas: str, n: int = 8) -> dict:
    df = baca(entitas, dts=daftar_dt(entitas)[-1:] if daftar_dt(entitas) else None)
    if df.is_empty():
        return {"kolom": [], "baris": []}
    df = df.sort("_ingested_at").head(n)
    return {"kolom": df.columns, "baris": [[None if v is None else str(v) for v in r] for r in df.rows()],
            "skema": {c: str(t) for c, t in df.schema.items()}}


# ---------------------------------------------------------------- opsional: bronze dari platform
def ambil_dari_platform(hari: int = 2) -> dict:
    """Menyalin partisi dt terakhir bronze platform (read-only) ke bucket lab.
    Skema identik, jadi aturan silver/gold lab langsung berlaku."""
    pf = s3fs_platform()
    salin = 0
    for e in ENTITAS:
        bs = daftar_berkas(e, fs=pf, bucket=PLATFORM_S3_BUCKET)
        dts = sorted({b[1] for b in bs})
        pilih = dts if e in ("cabang", "nasabah", "rekening") else dts[-hari:]
        for path, d, _ in bs:
            if d in pilih:
                tbl = pq.read_table(path, filesystem=pf)
                tujuan = f"{BUCKET}/" + path.split("/", 1)[1]
                pq.write_table(tbl, tujuan, filesystem=s3fs(), compression="zstd")
                salin += 1
    log("info", "bronze platform disalin", berkas=salin)
    return {"berkas_disalin": salin}
