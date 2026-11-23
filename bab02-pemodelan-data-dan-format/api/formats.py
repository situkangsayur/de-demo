"""Perbandingan format file (CSV, JSON Lines, Avro, Parquet), anatomi Parquet, dan schema evolution."""
from __future__ import annotations

import datetime as dt
import io
import os
import shutil
import statistics
import time

import duckdb
import fastavro
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

DIR = os.environ.get("DATA_DIR", "/tmp/lab02")
KOLOM_FORMAT = ["id", "rekening_id", "cabang_id", "kanal", "jenis", "nominal", "occurred_at", "created_at"]

SKEMA_AVRO = {
    "type": "record", "name": "Transaksi", "namespace": "bank",
    "fields": [{"name": "id", "type": "string"}, {"name": "rekening_id", "type": "long"},
               {"name": "cabang_id", "type": "int"}, {"name": "kanal", "type": "string"},
               {"name": "jenis", "type": "string"}, {"name": "nominal", "type": "double"},
               {"name": "occurred_at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
               {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-micros"}}]}


def _median_ms(fn, ulang: int = 5) -> tuple[float, list[float]]:
    waktu = []
    for _ in range(ulang):
        t = time.perf_counter()
        fn()
        waktu.append((time.perf_counter() - t) * 1000)
    return statistics.median(waktu), [round(w, 2) for w in waktu]


def avro_duckdb_tersedia(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        con.execute("LOAD avro")
        return True
    except Exception:
        return False


def bandingkan_format(con: duckdb.DuckDBPyConnection, tabel: str, n: int) -> dict:
    """Tulis n baris ke tiap format, ukur ukuran, waktu tulis, waktu baca satu kolom dan semua kolom."""
    folder = os.path.join(DIR, "format")
    shutil.rmtree(folder, ignore_errors=True)
    os.makedirs(folder, exist_ok=True)
    arrow = con.execute(f"SELECT {', '.join(KOLOM_FORMAT)} FROM {tabel} LIMIT {n}").to_arrow_table()
    arrow = arrow.cast(pa.schema([(f.name, pa.float64() if pa.types.is_decimal(f.type) else f.type) for f in arrow.schema]))
    con.register("sumber_format", arrow)
    berkas = {
        "CSV": os.path.join(folder, "trx.csv"),
        "JSON Lines": os.path.join(folder, "trx.jsonl"),
        "Avro (deflate)": os.path.join(folder, "trx.avro"),
        "Parquet (snappy)": os.path.join(folder, "trx-snappy.parquet"),
        "Parquet (zstd)": os.path.join(folder, "trx-zstd.parquet"),
    }
    tulis = {
        "CSV": lambda p: con.execute(f"COPY sumber_format TO '{p}' (FORMAT CSV, HEADER)"),
        "JSON Lines": lambda p: con.execute(f"COPY sumber_format TO '{p}' (FORMAT JSON)"),
        "Avro (deflate)": lambda p: _tulis_avro(arrow, p),
        "Parquet (snappy)": lambda p: con.execute(f"COPY sumber_format TO '{p}' (FORMAT PARQUET, COMPRESSION SNAPPY)"),
        "Parquet (zstd)": lambda p: con.execute(f"COPY sumber_format TO '{p}' (FORMAT PARQUET, COMPRESSION ZSTD)"),
    }
    pakai_avro_duckdb = avro_duckdb_tersedia(con)
    hasil = []
    for nama, path in berkas.items():
        t = time.perf_counter()
        tulis[nama](path)
        t_tulis = (time.perf_counter() - t) * 1000
        sumber = _sumber_sql(nama, path, pakai_avro_duckdb)
        if sumber is None:  # Avro tanpa ekstensi DuckDB: dibaca fastavro
            satu, _ = _median_ms(lambda: _agregat_avro(path), 3)
            semua = satu
            mesin = "fastavro (Python)"
        else:
            satu, _ = _median_ms(lambda: con.execute(f"SELECT kanal, SUM(nominal) FROM {sumber} GROUP BY kanal").fetchall())
            semua, _ = _median_ms(lambda: con.execute(f"SELECT * FROM {sumber}").to_arrow_table())
            mesin = "tulis fastavro, baca DuckDB" if "Avro" in nama else "DuckDB"
        hasil.append({"format": nama, "ukuran_byte": os.path.getsize(path), "tulis_ms": round(t_tulis, 1),
                      "baca_agregat_ms": round(satu, 1), "baca_semua_ms": round(semua, 1), "mesin": mesin,
                      "layout": "kolom" if "Parquet" in nama else "baris",
                      "skema": "wajib, di footer" if "Parquet" in nama else "wajib, di header" if "Avro" in nama
                      else "tersirat" if "JSON" in nama else "tidak ada"})
    con.unregister("sumber_format")
    csv = next(h for h in hasil if h["format"] == "CSV")
    for h in hasil:
        h["rasio_ukuran_vs_csv"] = round(h["ukuran_byte"] / csv["ukuran_byte"], 3)
    contoh = {}
    with open(berkas["CSV"]) as f:
        contoh["CSV"] = "".join([next(f) for _ in range(3)])
    with open(berkas["JSON Lines"]) as f:
        contoh["JSON Lines"] = "".join([next(f) for _ in range(2)])
    with open(berkas["Parquet (zstd)"], "rb") as f:
        contoh["Parquet (zstd)"] = repr(f.read(64)) + " ... (biner, diawali dan diakhiri 'PAR1')"
    return {"n": arrow.num_rows, "tabel": tabel, "hasil": hasil, "contoh": contoh,
            "query_agregat": "SELECT kanal, SUM(nominal) FROM <berkas> GROUP BY kanal", "ulang": 5}


def _sumber_sql(nama: str, path: str, avro_duckdb: bool) -> str | None:
    if nama == "CSV":
        return f"read_csv('{path}')"
    if nama == "JSON Lines":
        return f"read_json('{path}')"
    if "Parquet" in nama:
        return f"read_parquet('{path}')"
    return f"read_avro('{path}')" if avro_duckdb else None


def _tulis_avro(tabel: pa.Table, path: str) -> None:
    epoch = dt.datetime(1970, 1, 1)
    def baris():
        for r in tabel.to_pylist():
            for k in ("occurred_at", "created_at"):
                v = r[k]
                r[k] = int((v.replace(tzinfo=None) - epoch).total_seconds() * 1_000_000) if isinstance(v, dt.datetime) else v
            yield r
    with open(path, "wb") as f:
        fastavro.writer(f, fastavro.parse_schema(SKEMA_AVRO), baris(), codec="deflate")


def _agregat_avro(path: str) -> dict:
    tot: dict[str, float] = {}
    with open(path, "rb") as f:
        for r in fastavro.reader(f):
            tot[r["kanal"]] = tot.get(r["kanal"], 0.0) + r["nominal"]
    return tot


# ---------------- anatomi Parquet: row group, column chunk, statistik, pruning, pushdown ----------------

def anatomi_parquet(con: duckdb.DuckDBPyConnection, urut: bool, kolom: list[str], dari: str, row_group: int) -> dict:
    folder = os.path.join(DIR, "anatomi")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"trx-{'urut' if urut else 'acak'}.parquet")
    urutan = "occurred_at" if urut else "hash(id)"
    t = con.execute(f"SELECT {', '.join(KOLOM_FORMAT)} FROM transaksi ORDER BY {urutan}").to_arrow_table()
    t = t.cast(pa.schema([(f.name, pa.float64() if pa.types.is_decimal(f.type) else f.type) for f in t.schema]))
    pq.write_table(t, path, row_group_size=row_group, compression="zstd")
    meta = pq.ParquetFile(path).metadata
    batas = dt.datetime.fromisoformat(dari)
    kolom = [k for k in kolom if k in KOLOM_FORMAT] or ["kanal", "nominal"]
    kolom_query = sorted(set(kolom) | {"occurred_at"})
    rgs, total, dibaca = [], 0, 0
    for i in range(meta.num_row_groups):
        rg = meta.row_group(i)
        kol = []
        st_occ = None
        for j in range(rg.num_columns):
            c = rg.column(j)
            nama = c.path_in_schema
            if nama == "occurred_at" and c.statistics is not None and c.statistics.has_min_max:
                st_occ = (c.statistics.min, c.statistics.max)
            kol.append({"kolom": nama, "byte": c.total_compressed_size, "byte_mentah": c.total_uncompressed_size,
                        "encoding": [str(e) for e in c.encodings],
                        "min": str(c.statistics.min) if c.statistics is not None and c.statistics.has_min_max else None,
                        "max": str(c.statistics.max) if c.statistics is not None and c.statistics.has_min_max else None})
        lewati = st_occ is not None and st_occ[1].replace(tzinfo=None) < batas
        for k in kol:
            total += k["byte"]
            k["dibaca"] = (not lewati) and k["kolom"] in kolom_query
            dibaca += k["byte"] if k["dibaca"] else 0
        rgs.append({"indeks": i, "baris": rg.num_rows, "dilewati": lewati, "kolom": kol,
                    "occurred_min": str(st_occ[0]) if st_occ else None, "occurred_max": str(st_occ[1]) if st_occ else None})
    sql = f"SELECT {', '.join(kolom)} FROM read_parquet('{path}') WHERE occurred_at >= TIMESTAMP '{dari}'"
    q_ms, _ = _median_ms(lambda: con.execute(sql).to_arrow_table())
    semua_ms, _ = _median_ms(lambda: con.execute(f"SELECT * FROM read_parquet('{path}')").to_arrow_table())
    baris_hasil = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]
    return {"path": os.path.basename(path), "urut": urut, "ukuran_byte": os.path.getsize(path), "row_group_size": row_group,
            "jumlah_row_group": meta.num_row_groups, "baris": meta.num_rows, "kolom_query": kolom_query, "dari": dari,
            "sql": sql, "byte_total": total, "byte_dibaca": dibaca, "row_group": rgs, "query_ms": round(q_ms, 1),
            "scan_penuh_ms": round(semua_ms, 1), "baris_hasil": baris_hasil,
            "row_group_dilewati": sum(1 for r in rgs if r["dilewati"])}


# ---------------- schema evolution ----------------

V1 = {"type": "record", "name": "Transaksi", "fields": [
    {"name": "id", "type": "string"}, {"name": "nominal", "type": "int"}, {"name": "kanal", "type": "string"}]}


def _skema(*fields, nama="Transaksi") -> dict:
    return {"type": "record", "name": nama, "fields": list(fields)}


F_ID = {"name": "id", "type": "string"}
F_NOM = {"name": "nominal", "type": "int"}
F_KANAL = {"name": "kanal", "type": "string"}

KASUS = [
    ("Tambah kolom opsional (dengan default)", "aman",
     V1, _skema(F_ID, F_NOM, F_KANAL, {"name": "catatan", "type": ["null", "string"], "default": None}), "backward"),
    ("Tambah kolom wajib tanpa default", "merusak",
     V1, _skema(F_ID, F_NOM, F_KANAL, {"name": "akad", "type": "string"}), "backward"),
    ("Hapus kolom (pembaca baru tidak memakai kanal)", "aman bagi pembaca baru",
     V1, _skema(F_ID, F_NOM), "backward"),
    ("Ganti nama kolom kanal -> saluran", "merusak",
     V1, _skema(F_ID, F_NOM, {"name": "saluran", "type": "string"}), "backward"),
    ("Ganti nama dengan alias (aliases: [kanal])", "aman dengan alias",
     V1, _skema(F_ID, F_NOM, {"name": "saluran", "type": "string", "aliases": ["kanal"]}), "backward"),
    ("Ganti tipe nominal int -> string", "merusak",
     V1, _skema(F_ID, {"name": "nominal", "type": "string"}, F_KANAL), "backward"),
    ("Perlebar tipe nominal int -> long", "biasanya aman",
     V1, _skema(F_ID, {"name": "nominal", "type": "long"}, F_KANAL), "backward"),
    ("Pembaca lama membaca data baru yang punya kolom tambahan", "aman (forward)",
     _skema(F_ID, F_NOM, F_KANAL, {"name": "catatan", "type": ["null", "string"], "default": None}), V1, "forward"),
]


def uji_evolusi(con: duckdb.DuckDBPyConnection) -> dict:
    data = [{"id": "TRX0001", "nominal": 250000, "kanal": "mobile"}, {"id": "TRX0002", "nominal": 50000, "kanal": "atm"}]
    hasil = []
    for judul, harapan, penulis, pembaca, arah in KASUS:
        buf = io.BytesIO()
        isi = [dict(r, catatan="uji") if any(f["name"] == "catatan" for f in penulis["fields"]) else r for r in data]
        fastavro.writer(buf, fastavro.parse_schema(penulis), isi)
        buf.seek(0)
        try:
            baris = list(fastavro.reader(buf, reader_schema=fastavro.parse_schema(pembaca)))
            hasil.append({"kasus": judul, "slide": harapan, "arah": arah, "berhasil": True, "contoh": baris[0], "galat": None,
                          "penulis": [f["name"] + ":" + str(f["type"]) for f in penulis["fields"]],
                          "pembaca": [f["name"] + ":" + str(f["type"]) for f in pembaca["fields"]]})
        except Exception as e:  # fastavro melempar SchemaResolutionError
            hasil.append({"kasus": judul, "slide": harapan, "arah": arah, "berhasil": False, "contoh": None,
                          "galat": f"{type(e).__name__}: {str(e)[:220]}",
                          "penulis": [f["name"] + ":" + str(f["type"]) for f in penulis["fields"]],
                          "pembaca": [f["name"] + ":" + str(f["type"]) for f in pembaca["fields"]]})
    # Parquet: dua berkas dengan skema berbeda dibaca bersama (union_by_name)
    folder = os.path.join(DIR, "evolusi")
    os.makedirs(folder, exist_ok=True)
    pq.write_table(pa.table({"id": ["TRX0001", "TRX0002"], "nominal": pa.array([250000, 50000], pa.int32()),
                             "kanal": ["mobile", "atm"]}), os.path.join(folder, "v1.parquet"))
    pq.write_table(pa.table({"id": ["TRX0003"], "nominal": pa.array([75000], pa.int64()), "kanal": ["web"],
                             "catatan": ["kolom baru"]}), os.path.join(folder, "v2.parquet"))
    baris = con.execute(f"SELECT * FROM read_parquet('{folder}/v*.parquet', union_by_name = true) ORDER BY id").fetchall()
    kol = [d[0] for d in con.description]
    try:
        cur = con.execute(f"SELECT * FROM read_parquet('{folder}/v*.parquet') ORDER BY id")
        n = len(cur.fetchall())
        tanpa = f"{n} baris terbaca, kolom: {[d[0] for d in cur.description]} (skema mengikuti berkas pertama)"
    except Exception as e:
        tanpa = f"{type(e).__name__}: {str(e)[:200]}"
    return {"avro": hasil, "parquet": {"kolom": kol, "baris": [list(map(lambda v: v if v is None or isinstance(v, (int, float)) else str(v), b)) for b in baris],
                                        "tanpa_union_by_name": tanpa}}


def baca_platform() -> pa.Table | None:
    """Bila PLATFORM_S3_ENDPOINT diisi, baca bronze transaksi dari RustFS platform (hanya baca)."""
    ep = os.environ.get("PLATFORM_S3_ENDPOINT", "").strip()
    if not ep:
        return None
    from pyarrow import fs
    import pyarrow.dataset as ds
    s3 = fs.S3FileSystem(endpoint_override=ep, access_key=os.environ.get("PLATFORM_S3_ACCESS_KEY", ""),
                         secret_key=os.environ.get("PLATFORM_S3_SECRET_KEY", ""), scheme="http" if ep.startswith("http://") else "https",
                         region="us-east-1")
    bucket = os.environ.get("PLATFORM_S3_BUCKET", "lake")
    base = f"{bucket}/bronze/app/transaksi"
    d = ds.dataset(base, filesystem=s3, format="parquet", partitioning="hive")
    t = d.to_table(columns=["id", "rekening_id", "cabang_id", "kanal", "jenis", "nominal", "occurred_at", "created_at"])
    # bronze bertipe string: casting seperti di silver (string kosong dianggap null)
    def kosong_null(a):
        return pc.if_else(pc.equal(a, ""), pa.scalar(None, pa.string()), a)
    t = pa.table({k: kosong_null(t[k]) for k in t.column_names})
    t = pa.table({
        "id": t["id"], "rekening_id": pc.cast(t["rekening_id"], pa.int64()), "cabang_id": pc.cast(t["cabang_id"], pa.int32()),
        "kanal": t["kanal"], "jenis": t["jenis"], "nominal": pc.cast(t["nominal"], pa.float64()),
        "occurred_at": pc.strptime(pc.utf8_slice_codeunits(t["occurred_at"], 0, 19), format="%Y-%m-%dT%H:%M:%S", unit="us"),
        "created_at": pc.strptime(pc.utf8_slice_codeunits(t["created_at"], 0, 19), format="%Y-%m-%dT%H:%M:%S", unit="us"),
    })
    return t
