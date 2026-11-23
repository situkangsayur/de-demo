"""Bronze sintetis sesuai platform/KONTRAK-DATA.md (revisi 23 Nov), dipakai selama bagian ops belum ada.

Menulis s3://lake/bronze/<app|cdc>/<entitas>/dt=YYYY-MM-DD/part-<batch_id>.parquet dengan kolom
metadata _ingested_at, _source, _batch_id, _event_id, _event_type, _occurred_at, _ts_ms, _op dan kolom
entitas bank (cabang, nasabah, rekening, transaksi, akad_pembiayaan) yang sudah diratakan.
Sengaja meniru kerumitan sumber nyata agar silver benar-benar bekerja:
- 75% transaksi tercatat di app dan cdc (duplikat lintas sumber), 15% hanya cdc, 10% hanya app;
- tipe berbeda antar-sumber: kolom data app semuanya string (waktu RFC3339 +07:00), di cdc bertipe
  gaya Debezium (epoch mikrodetik, nominal string decimal.handling.mode=string);
- kanal ecommerce dengan jenis bayar_belanja dan ref_order_id; angsuran; transfer masuk/keluar;
- duplikat bisnis (request dikirim ulang tanpa idempotency key) dan occurred_at mundur (data terlambat);
- akad melalui status diajukan -> disetujui -> cair (cair_at), sebagian ditolak;
- kode kanal tidak seragam ('ATM', 'Mobile Banking', 'm-banking', 'teller ', 'WEB');
- ±3% kode akad tidak valid (dikarantina) dan alias akad ('murabahah');
- profil nasabah berubah (kota/segmen/no HP/email) -> bahan SCD-2;
- nasabah baru 10 hari terakhir yang belum bertransaksi (pertanyaan 3);
- satu cabang melonjak pada hari kemarin (pertanyaan 4).
Deterministik (seed tetap): dijalankan ulang menulis berkas yang sama persis.
Pemakaian: python dev/bangkitkan_bronze.py   (env BRONZE_HARI, default 45)
"""
from __future__ import annotations

import datetime as dt
import os
import random
import sys
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pipeline.konfigurasi import BUCKET, boto, log, s3fs  # noqa: E402

WIB = dt.timezone(dt.timedelta(hours=7))
UTC = dt.timezone.utc
R = random.Random(20261123)
HARI = int(os.environ.get("BRONZE_HARI", "45"))
HARI_INI = dt.datetime.now(WIB).date()
AWAL = HARI_INI - dt.timedelta(days=HARI)
KEMARIN = HARI_INI - dt.timedelta(days=1)

CABANG = [(1, "JKT01", "KC Jakarta Thamrin", "jakarta pusat", "Jabodetabek"),
          (2, "JKT02", "KC Jakarta Selatan", "jakarta selatan", "Jabodetabek"),
          (3, "BGR01", "KC Bogor", "bogor", "Jabodetabek"),
          (4, "BDG01", "KC Bandung", "bandung", "Jawa Barat"),
          (5, "SBY01", "KC Surabaya", "surabaya", "Jawa Timur"),
          (6, "MDN01", "KC Medan", "medan", "Sumatra"),
          (7, "MKS01", "KC Makassar", "makassar", "Sulawesi"),
          (8, "YGY01", "KC Yogyakarta", "yogyakarta", "Jawa Tengah-DIY")]
BOBOT_CABANG = [18, 14, 9, 14, 13, 11, 9, 12]
KOTA = [c[3] for c in CABANG]
NAMA_D = ["Ahmad", "Siti", "Muhammad", "Nur", "Rizki", "Dewi", "Fajar", "Aisyah", "Budi", "Fatimah", "Hendra",
          "Laila", "Yusuf", "Zahra", "Ilham", "Putri", "Hasan", "Rahma", "Umar", "Salma"]
NAMA_B = ["Pratama", "Hidayat", "Saputra", "Lestari", "Nugroho", "Rahmawati", "Siregar", "Wibowo", "Hasibuan",
          "Kurniawan", "Syahputra", "Maulana", "Permata", "Fauzi", "Ramadhan"]
SEGMEN = ["ritel", "ritel", "prioritas", "umkm"]
KANAL_RAW = {"mobile": ["mobile", "Mobile Banking", "m-banking"], "atm": ["ATM", "atm"],
             "teller": ["teller", "teller "], "web": ["web", "WEB", "internet banking"],
             "ecommerce": ["ecommerce", "e-commerce"]}
BOBOT_KANAL = {"mobile": 45, "atm": 18, "teller": 9, "web": 13, "ecommerce": 15}
JENIS = {"mobile": ["transfer_keluar", "transfer_masuk", "angsuran", "transfer_keluar"], "atm": ["tarik", "tarik", "setor"],
         "teller": ["setor", "tarik", "angsuran"], "web": ["transfer_keluar", "transfer_masuk", "angsuran"],
         "ecommerce": ["bayar_belanja"]}
AKAD = ["MRB", "MRB", "MRB", "MSY", "MDH", "IJR", "IJR", "murabahah"]



def eid() -> str:
    return str(uuid.UUID(int=R.getrandbits(128)))


def iso(t: dt.datetime) -> str:
    return t.astimezone(WIB).isoformat(timespec="milliseconds")


def mikro(t: dt.datetime) -> int:
    return int(t.timestamp() * 1_000_000)


def acak_waktu(d: dt.date) -> dt.datetime:
    # pola harian: lebih ramai 08.00-21.00 WIB
    jam = R.choices(range(24), weights=[1, 1, 1, 1, 1, 2, 4, 6, 8, 9, 9, 9, 10, 9, 9, 9, 8, 8, 8, 7, 6, 5, 3, 2])[0]
    return dt.datetime(d.year, d.month, d.day, jam, R.randrange(60), R.randrange(60), R.randrange(1000) * 1000,
                       tzinfo=WIB)


def sebagai_string(data: dict) -> dict:
    """Event aplikasi: semua kolom data string (kontrak: casting di silver)."""
    out = {}
    for k, v in data.items():
        if v is None:
            out[k] = None
        elif isinstance(v, dt.datetime):
            out[k] = iso(v)
        else:
            out[k] = str(v)
    return out


def gaya_debezium(data: dict) -> dict:
    """CDC: timestamp -> epoch mikrodetik, date -> hari sejak epoch, numeric -> string."""
    out = {}
    for k, v in data.items():
        if isinstance(v, dt.datetime):
            out[k] = mikro(v)
        elif isinstance(v, dt.date):
            out[k] = (v - dt.date(1970, 1, 1)).days
        elif isinstance(v, float):
            out[k] = f"{v:.2f}"
        else:
            out[k] = v
    return out


_LSN = [1_000_000]


class Penampung:
    """Mengumpulkan baris per (sumber, entitas, dt UTC) lalu menulis Parquet."""

    def __init__(self):
        self.isi: dict[tuple[str, str, dt.date], list[dict]] = {}

    def tambah(self, sumber: str, entitas: str, waktu: dt.datetime, data: dict, op: str,
               aksi: str = "", terlambat: dt.timedelta | None = None) -> None:
        masuk = waktu + (terlambat or dt.timedelta(seconds=R.randint(1, 180)))
        _LSN[0] += R.randint(1, 500)
        aksi = aksi or ("dibuat" if op in ("c", "r") else "diperbarui")
        meta = {"_ingested_at": masuk.astimezone(UTC), "_source": sumber, "_batch_id": "",
                "_event_type": f"bank.{entitas.replace('akad_pembiayaan', 'akad')}.{aksi}" if sumber == "app"
                else f"cdc.bank.{entitas}",
                "_occurred_at": iso(waktu), "_ts_ms": str(int(masuk.timestamp() * 1000)),
                "_op": op if sumber == "cdc" else None,
                "_event_id": eid() if sumber == "app" else f"{entitas}:{_LSN[0]}:{data['id']}"}
        isi = sebagai_string(data) if sumber == "app" else gaya_debezium(data)
        self.isi.setdefault((sumber, entitas, waktu.astimezone(UTC).date()), []).append({**meta, **isi})

    def tulis(self) -> int:
        f = s3fs()
        n = 0
        for (sumber, entitas, d), rows in sorted(self.isi.items()):
            # dua batch per partisi, seperti ingestor yang menulis tiap N detik/M event
            rows.sort(key=lambda r: r["_ingested_at"])
            for i, bagian in enumerate((rows[: len(rows) // 2], rows[len(rows) // 2:])):
                if not bagian:
                    continue
                batch = f"{d:%Y%m%d}-{sumber}-{entitas[:3]}-{i}"
                for r in bagian:
                    r["_batch_id"] = batch
                kolom = sorted({k for r in bagian for k in r})
                tbl = pa.Table.from_pylist([{k: r.get(k) for k in kolom} for r in bagian])
                for c in tbl.column_names:  # kolom yang seluruhnya kosong tetap bertipe string
                    if pa.types.is_null(tbl[c].type):
                        tbl = tbl.set_column(tbl.column_names.index(c), c, tbl[c].cast(pa.string()))
                path = f"{BUCKET}/bronze/{sumber}/{entitas}/dt={d.isoformat()}/part-{batch}.parquet"
                pq.write_table(tbl, path, filesystem=f, compression="zstd")
                n += 1
        return n


def buat_bucket() -> None:
    s3 = boto()
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        log("info", "bucket dibuat", bucket=BUCKET)


def kedua(p: Penampung, entitas: str, t: dt.datetime, data: dict, op: str, aksi: str = "") -> None:
    p.tambah("app", entitas, t, data, op, aksi)
    p.tambah("cdc", entitas, t, data, op, aksi)


def main() -> None:
    buat_bucket()
    p = Penampung()
    awal_waktu = dt.datetime.combine(AWAL - dt.timedelta(days=1), dt.time(6, 0), WIB)
    for cid, kode, nama, kota, wil in CABANG:
        p.tambah("cdc", "cabang", awal_waktu, {"id": cid, "kode": kode, "nama": nama, "kota": kota, "wilayah": wil}, "r")

    nasabah: list[dict] = []
    rekening: list[dict] = []

    def nasabah_baru(t: dt.datetime, op: str) -> dict:
        nid = len(nasabah) + 1
        cab = R.choices(CABANG, weights=BOBOT_CABANG)[0]
        n = {"id": nid, "cif": f"CIF{nid:07d}", "nama": f"{R.choice(NAMA_D)} {R.choice(NAMA_B)}",
             "nik": f"{R.choice(['3171', '3273', '3578', '1271', '7371', '3471'])}{R.randint(10**11, 10**12 - 1)}",
             "no_hp": f"08{R.randint(11, 59)}{R.randint(10**7, 10**8 - 1)}",
             "email": f"nasabah{nid}@contoh.id", "kota": cab[3], "segmen": R.choice(SEGMEN), "cabang_id": cab[0],
             "created_at": t, "updated_at": t}
        nasabah.append(n)
        kedua(p, "nasabah", t, dict(n), op)
        r = {"id": nid, "no_rekening": f"71{nid:08d}", "nasabah_id": nid, "cabang_id": cab[0],
             "jenis": R.choice(["tabungan_wadiah", "tabungan_wadiah", "giro", "deposito_mudharabah"]),
             "saldo": float(R.randint(1, 500) * 100_000), "status": "aktif", "dibuka_at": t, "updated_at": t}
        rekening.append(r)
        kedua(p, "rekening", t, dict(r), op)
        return n

    for i in range(500):
        nasabah_baru(awal_waktu + dt.timedelta(minutes=i), "r")

    akad_id, tidak_aktif = 0, set()
    for d in [AWAL + dt.timedelta(days=i) for i in range(HARI + 1)]:
        # nasabah baru; sebagian (10 hari terakhir) tidak pernah bertransaksi
        for _ in range(R.randint(3, 7)):
            n = nasabah_baru(acak_waktu(d), "c")
            if (HARI_INI - d).days <= 10 and R.random() < 0.5:
                tidak_aktif.add(n["id"])
        # perubahan profil (SCD-2): pindah kota, naik segmen, ganti nomor HP atau email
        for n in R.sample(nasabah, k=max(1, len(nasabah) // 60)):
            t = max(acak_waktu(d), n["updated_at"] + dt.timedelta(minutes=1))
            ubah = R.choice(["kota", "segmen", "no_hp", "email"])
            if ubah == "kota":
                n["kota"] = R.choice([k for k in KOTA if k != n["kota"]])
            elif ubah == "segmen":
                n["segmen"] = R.choice([s for s in SEGMEN if s != n["segmen"]])
            elif ubah == "no_hp":
                n["no_hp"] = f"+62 8{R.randint(11, 59)}-{R.randint(1000, 9999)}-{R.randint(1000, 9999)}"
            else:
                n["email"] = f"{n['cif'].lower()}@surel.contoh.id"
            n["updated_at"] = t
            kedua(p, "nasabah", t, dict(n), "u")
        # pengajuan dan pencairan akad
        for _ in range(R.randint(3, 7)):
            akad_id += 1
            n = R.choice(nasabah[:-10])
            t = acak_waktu(d)
            kode = R.choice(AKAD) if R.random() > 0.03 else R.choice(["MRBX", "XYZ", "", "QRD"])
            pokok = float(R.choice([5, 10, 25, 50, 75, 100, 150, 250]) * 1_000_000)
            a = {"id": akad_id, "nasabah_id": n["id"], "rekening_id": n["id"], "kode_akad": kode, "pokok": pokok,
                 "terbayar": 0.0, "diajukan_at": t, "updated_at": t,
                 "margin": round(pokok * R.choice([0.08, 0.1, 0.12]), 2), "tenor_bulan": R.choice([12, 24, 36, 60]),
                 "status": "diajukan", "cair_at": None}
            kedua(p, "akad_pembiayaan", t, dict(a), "c", "diajukan")
            if R.random() < 0.12:
                a["status"], a["updated_at"] = "ditolak", t + dt.timedelta(minutes=30)
                kedua(p, "akad_pembiayaan", t + dt.timedelta(minutes=30), dict(a), "u", "ditolak")
                continue
            tc = t + dt.timedelta(minutes=R.randint(20, 120))
            if tc.date() != d or (d == HARI_INI and tc > dt.datetime.now(WIB)):
                continue  # masih "disetujui", belum cair
            a["status"], a["cair_at"], a["updated_at"] = "cair", tc, tc
            kedua(p, "akad_pembiayaan", tc, dict(a), "u", "dicairkan")
            # pencairan dicatat juga sebagai transaksi transfer_masuk dengan akad_id terisi
            tambah_trx(p, d, n, n["cabang_id"], "teller", waktu=tc, jenis="transfer_masuk", nominal=pokok, akad_id=akad_id)
        # transaksi
        aktif = [n for n in nasabah if n["id"] not in tidak_aktif and n["created_at"].date() <= d]
        jumlah = int(R.gauss(300, 30) * (0.75 if d.weekday() >= 5 else 1.0))
        for _ in range(jumlah):
            n = R.choice(aktif)
            cab = n["cabang_id"] if R.random() < 0.8 else R.choices(CABANG, weights=BOBOT_CABANG)[0][0]
            kanal = R.choices(list(BOBOT_KANAL), weights=list(BOBOT_KANAL.values()))[0]
            tambah_trx(p, d, n, cab, kanal)
        if d == KEMARIN:  # lonjakan tidak wajar di KC Surabaya
            for _ in range(120):
                tambah_trx(p, d, R.choice(aktif), 5, "mobile")
    n = p.tulis()
    log("info", "bronze sintetis ditulis", berkas=n, hari=HARI, awal=str(AWAL), akhir=str(HARI_INI))


_TRX = [0]


def tambah_trx(p: Penampung, d: dt.date, n: dict, cab: int, kanal: str, waktu: dt.datetime | None = None,
               jenis: str | None = None, nominal: float | None = None, akad_id: int | None = None) -> None:
    t = waktu or acak_waktu(d)
    if d == HARI_INI and t > dt.datetime.now(WIB):
        return
    jenis = jenis or R.choice(JENIS[kanal])
    nominal = nominal or float(R.choice([5, 10, 20, 25, 50, 75, 100, 150, 200, 250, 500, 1000, 2500]) * 10_000)
    # duplikat bisnis: request dikirim ulang tanpa header Idempotency-Key -> dua transaksi
    ulang = 2 if akad_id is None and R.random() < 0.01 else 1
    for k in range(ulang):
        _TRX[0] += 1
        tk = t + dt.timedelta(seconds=3 * k)
        # ±2% occurred_at mundur: perangkat offline, transaksi baru tercatat 2-20 jam kemudian
        telat = dt.timedelta(hours=R.randint(2, 20)) if akad_id is None and R.random() < 0.02 else None
        data = {"id": f"TRX{d:%Y%m%d}{_TRX[0]:07d}", "rekening_id": n["id"], "cabang_id": cab,
                "kanal": R.choice(KANAL_RAW[kanal]), "jenis": jenis, "nominal": nominal,
                "occurred_at": tk, "created_at": tk + (telat or dt.timedelta(milliseconds=-1)),
                "ref_order_id": f"ORD-{R.randint(100000, 999999)}" if jenis == "bayar_belanja" else None,
                "akad_id": akad_id, "saldo_setelah": float(R.randint(1, 900) * 100_000),
                "transfer_id": f"TRF{_TRX[0]:08d}" if jenis.startswith("transfer") else None,
                "idempotency_key": None if ulang == 2 else f"idk-{_TRX[0]:09d}",
                "keterangan": "pencairan akad" if akad_id else None, "request_id": eid()}
        r = R.random()
        if r < 0.85:
            p.tambah("cdc", "transaksi", tk, dict(data), "c", terlambat=telat)
        if r < 0.75 or r >= 0.85:
            p.tambah("app", "transaksi", tk, dict(data), "c", terlambat=telat)


if __name__ == "__main__":
    main()
