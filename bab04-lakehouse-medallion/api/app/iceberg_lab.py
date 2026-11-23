"""Laboratorium Iceberg di atas tabel lab.transaksi (salinan silver.transaksi).

Setiap operasi menghasilkan satu commit (snapshot baru): append kecil, MERGE/upsert, DELETE,
schema evolution, partition evolution, compaction, expire snapshots + hapus berkas yatim, rollback.
Fungsi pohon() membaca rantai metadata yang sebenarnya dari RustFS:
katalog (SQLite) -> metadata.json -> manifest list -> manifest -> data file.
"""
from __future__ import annotations

import datetime as dt
import json
import random
import re
import sqlite3

import duckdb
import polars as pl
import pyarrow as pa
from pyarrow import fs as pafs
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, In, LessThan
from pyiceberg.transforms import BucketTransform, DayTransform
from pyiceberg.types import LongType, StringType, TimestampType, TimestamptzType

from .konfig import BUCKET, KATALOG_URI, Stopwatch, duckdb_lake, katalog, log, s3fs
from .medallion import tabel as tabel_medallion

LAB = "lab.transaksi"


def muat(nama: str = LAB):
    return katalog().load_table(nama)


def daftar_tabel() -> list[str]:
    cat = katalog()
    out = []
    for ns in cat.list_namespaces():
        for t in cat.list_tables(ns):
            out.append(".".join(t))
    return sorted(out)


# ---------------------------------------------------------------- penyelarasan data dengan skema terkini
def _selaraskan(t, data: pa.Table) -> pa.Table:
    """Data dibangkitkan dengan nama kolom awal; petakan lewat field id ke nama/tipe terkini
    (kolom yang di-rename tetap terisi, kolom baru diisi null bila tidak ada)."""
    awal = t.schemas()[0]
    kini = t.schema()
    kolom = {}
    for f in kini.fields:
        nama_awal = None
        try:
            nama_awal = awal.find_field(f.field_id).name
        except ValueError:
            pass
        if f.name in data.column_names:
            kolom[f.name] = data[f.name]
        elif nama_awal and nama_awal in data.column_names:
            kolom[f.name] = data[nama_awal]
        else:
            kolom[f.name] = pa.nulls(data.num_rows)
    arrow_kini = kini.as_arrow()
    return pa.table(kolom).cast(pa.schema([pa.field(f.name, f.type) for f in arrow_kini]))


def _batch_baru(t, n: int, hari: dt.date | None = None, seed: int | None = None) -> pa.Table:
    r = random.Random(seed)
    kini = pl.from_arrow(t.scan(limit=500).to_arrow())
    hari = hari or dt.datetime.now(dt.timezone.utc).date()
    sufiks = f"{r.randrange(16**6):06x}"
    rows = []
    for i in range(n):
        dasar = kini.row(r.randrange(kini.height), named=True) if kini.height else {}
        w = dt.datetime.combine(hari, dt.time(r.randrange(24), r.randrange(60)), dt.timezone.utc)
        rows.append({**dasar, "trx_id": f"STR{hari:%Y%m%d}{sufiks}{i:03d}",
                     "nominal": r.choice([10, 25, 50, 100]) * 10_000, "waktu": w, "dicatat_pada": w,
                     "_source": "app", "_batch_id": f"stream-{sufiks}",
                     "_ingested_at": dt.datetime.now(dt.timezone.utc)})
    df = pl.DataFrame(rows, schema=kini.schema)
    return _selaraskan(t, df.to_arrow())


# ---------------------------------------------------------------- operasi (masing-masing satu commit)
def reset() -> dict:
    cat = katalog()
    cat.create_namespace_if_not_exists("lab")
    try:
        cat.drop_table(LAB)
    except NoSuchTableError:
        pass
    sumber = tabel_medallion("silver.transaksi")
    data = sumber.scan().to_arrow()
    if data.num_rows == 0:
        raise ValueError("silver.transaksi masih kosong; jalankan pipeline medallion dulu")
    t = cat.create_table(LAB, schema=sumber.schema(), partition_spec=sumber.spec(),
                         location=f"s3://{BUCKET}/lab/transaksi-{dt.datetime.now():%H%M%S}",
                         properties={"format-version": "2", "write.parquet.compression-codec": "zstd"})
    t.append(data, snapshot_properties={"operasi-lab": "salin dari silver.transaksi"})
    return {"pesan": f"lab.transaksi dibuat ulang dari silver.transaksi ({data.num_rows} baris)"}


def append_kecil(kali: int = 5, baris: int = 40) -> dict:
    """Meniru ingestion streaming: banyak commit kecil -> banyak berkas kecil."""
    t = muat()
    for k in range(kali):
        t.append(_batch_baru(t, baris, seed=k + random.randrange(10**6)),
                 snapshot_properties={"operasi-lab": f"append kecil {k + 1}/{kali}"})
    return {"pesan": f"{kali} commit append, masing-masing {baris} baris"}


def merge() -> dict:
    """MERGE/upsert: koreksi nominal 15 transaksi + 5 transaksi baru; 3 transaksi BATAL dihapus."""
    t = muat()
    kini = t.scan(limit=400).to_arrow()
    r = random.Random()
    idx = sorted(r.sample(range(kini.num_rows), 18))
    koreksi = pl.from_arrow(kini.take(idx[:15]))
    nama_nominal = _nama_kini(t, "nominal")
    koreksi = koreksi.with_columns((pl.col(nama_nominal) + 1_000).alias(nama_nominal))
    baru = pl.from_arrow(_batch_baru(t, 5, seed=r.randrange(10**6)))
    sumber = pa.concat_tables([_selaraskan(t, koreksi.to_arrow()), baru.to_arrow().cast(t.schema().as_arrow())])
    batal = [kini[_nama_kini(t, "trx_id")][i].as_py() for i in idx[15:]]
    with t.transaction() as tx:
        tx.delete(In(_nama_kini(t, "trx_id"), batal))
    t = t.refresh()
    hasil = t.upsert(sumber, join_cols=[_nama_kini(t, "trx_id")],
                     snapshot_properties={"operasi-lab": "MERGE koreksi_harian"})
    return {"pesan": f"MERGE: {hasil.rows_updated} baris diperbarui, {hasil.rows_inserted} baris baru, "
                     f"{len(batal)} baris BATAL dihapus (commit DELETE terpisah)",
            "contoh_koreksi": koreksi.select(_nama_kini(t, "trx_id"), nama_nominal).head(5).to_dicts()}


def hapus() -> dict:
    """DELETE kanal atm pada hari pertama: copy-on-write menulis ulang berkas yang terdampak."""
    t = muat()
    df = pl.from_arrow(t.scan(selected_fields=(_nama_kini(t, "waktu"),)).to_arrow())
    hari = df[_nama_kini(t, "waktu")].min().date()
    a = dt.datetime.combine(hari, dt.time(0), dt.timezone.utc)
    filt = And(EqualTo(_nama_kini(t, "kanal"), "atm"),
               And(GreaterThanOrEqual(_nama_kini(t, "waktu"), a.isoformat()),
                   LessThan(_nama_kini(t, "waktu"), (a + dt.timedelta(days=1)).isoformat())))
    t.delete(filt, snapshot_properties={"operasi-lab": f"DELETE kanal=atm tanggal {hari}"})
    return {"pesan": f"DELETE WHERE kanal = 'atm' AND hari(waktu) = {hari}"}


def _nama_kini(t, nama_awal: str) -> str:
    fid = t.schemas()[0].find_field(nama_awal).field_id
    return t.schema().find_field(fid).name


def skema(aksi: str) -> dict:
    t = muat()
    with Stopwatch() as w:
        if aksi == "tambah":
            if "kategori_merchant" in t.schema().column_names:
                raise ValueError("kolom kategori_merchant sudah ada")
            with t.update_schema() as u:
                u.add_column("kategori_merchant", StringType(), doc="kategori merchant untuk belanja")
            pesan = "ADD COLUMN kategori_merchant STRING"
        elif aksi == "ganti_nama":
            if "jenis" not in t.schema().column_names:
                raise ValueError("kolom jenis sudah di-rename")
            with t.update_schema() as u:
                u.rename_column("jenis", "jenis_transaksi")
            pesan = "RENAME COLUMN jenis TO jenis_transaksi"
        elif aksi == "lebarkan":
            nama = _nama_kini(t, "cabang_id")
            with t.update_schema() as u:
                u.update_column(nama, field_type=LongType())
            pesan = f"ALTER COLUMN {nama} TYPE BIGINT (int -> long)"
        else:
            raise ValueError("aksi skema: tambah | ganti_nama | lebarkan")
    t = t.refresh()
    if aksi == "tambah":
        # baris baru sudah membawa kolom baru; baris lama membaca null tanpa ditulis ulang
        b = _batch_baru(t, 30, seed=7)
        b = b.set_column(b.column_names.index("kategori_merchant"), "kategori_merchant",
                         pa.array([random.choice(["fashion", "elektronik", "makanan"]) for _ in range(b.num_rows)]))
        t.append(b, snapshot_properties={"operasi-lab": "append dengan kolom baru"})
    return {"pesan": pesan + f" (perubahan metadata {w.ms} ms, tidak ada berkas data yang ditulis ulang)"}


def partisi() -> dict:
    t = muat()
    if any(isinstance(f.transform, BucketTransform) for f in t.spec().fields):
        raise ValueError("partisi bucket sudah ditambahkan")
    with t.update_spec() as u:
        u.add_field(_nama_kini(t, "rekening_id"), BucketTransform(4), "rekening_bucket")
    t = t.refresh()
    t.append(_batch_baru(t, 120, seed=11), snapshot_properties={"operasi-lab": "append sesudah partition evolution"})
    return {"pesan": "ADD PARTITION FIELD bucket(4, rekening_id); berkas lama tetap dengan spec lama, "
                     "120 baris baru ditulis dengan spec baru"}


def compaction() -> dict:
    """Menyatukan berkas kecil: baca isi snapshot terkini lalu tulis ulang dalam satu commit replace.
    (Spark: CALL system.rewrite_data_files; PyIceberg belum punya prosedur khusus.)"""
    t = muat()
    sebelum = _per_partisi(t)
    data = t.scan().to_arrow()
    t.overwrite(data, snapshot_properties={"operasi-lab": "compaction (rewrite data files)"})
    sesudah = _per_partisi(t.refresh())
    return {"pesan": f"compaction: {sebelum['berkas']} berkas di {sebelum['partisi']} partisi "
                     f"(maks {sebelum['maks']} berkas per partisi, rata-rata {sebelum['rata_kb']} KB) -> "
                     f"{sesudah['berkas']} berkas di {sesudah['partisi']} partisi (maks {sesudah['maks']}, "
                     f"rata-rata {sesudah['rata_kb']} KB); {data.num_rows} baris tetap",
            "sebelum": sebelum, "sesudah": sesudah}


def _per_partisi(t) -> dict:
    hit: dict[str, int] = {}
    byte = 0
    snap = t.current_snapshot()
    for m in snap.manifests(t.io):
        for e in m.fetch_manifest_entry(t.io):
            k = json.dumps(_partisi(t, e.data_file), sort_keys=True)
            hit[k] = hit.get(k, 0) + 1
            byte += e.data_file.file_size_in_bytes
    n = sum(hit.values())
    return {"berkas": n, "partisi": len(hit), "maks": max(hit.values(), default=0),
            "rata_kb": round(byte / max(n, 1) / 1024, 1)}


def expire(sisakan: int = 1) -> dict:
    """Expire snapshot lama (metadata), lalu hapus berkas data/manifest yang tidak dirujuk lagi."""
    t = muat()
    kini = t.current_snapshot().snapshot_id
    # snapshot kini tidak pernah dibuang (mis. sesudah rollback ke snapshot lama)
    lain = [s for s in sorted(t.snapshots(), key=lambda s: s.timestamp_ms) if s.snapshot_id != kini]
    buang = [s.snapshot_id for s in lain[:max(0, len(lain) - (sisakan - 1))]]
    if buang:
        t.maintenance.expire_snapshots().by_ids(buang).commit()
    t = t.refresh()
    dihapus = hapus_yatim(t)
    return {"pesan": f"{len(buang)} snapshot dikedaluwarsakan; {dihapus['berkas']} berkas yatim dihapus "
                     f"({dihapus['byte']} byte). Time travel ke snapshot itu tidak mungkin lagi."}


def _dirujuk(t) -> set[str]:
    io = t.io
    ref = set()
    for s in t.snapshots():
        ref.add(s.manifest_list)
        for m in s.manifests(io):
            ref.add(m.manifest_path)
            for e in m.fetch_manifest_entry(io, discard_deleted=True):
                ref.add(e.data_file.file_path)
    return ref


def objek(t) -> list[pafs.FileInfo]:
    lokasi = t.location().removeprefix("s3://")
    return [i for i in s3fs().get_file_info(pafs.FileSelector(lokasi, recursive=True, allow_not_found=True))
            if i.type == pafs.FileType.File]


def hapus_yatim(t) -> dict:
    ref = {p.removeprefix("s3://") for p in _dirujuk(t)}
    n = b = 0
    for i in objek(t):
        if "/data/" in i.path and i.path not in ref or (i.path.endswith(".avro") and i.path not in ref):
            s3fs().delete_file(i.path)
            n += 1
            b += i.size
    return {"berkas": n, "byte": b}


def rollback(snapshot_id: int) -> dict:
    t = muat()
    t.manage_snapshots().rollback_to_snapshot(snapshot_id).commit()
    return {"pesan": f"rollback ke snapshot {snapshot_id}; snapshot sesudahnya tetap ada di riwayat"}


OPERASI = {"reset": reset, "append": append_kecil, "merge": merge, "delete": hapus, "partisi": partisi,
           "compaction": compaction, "expire": expire}


def jalankan(op: str, **kw) -> dict:
    with Stopwatch() as w:
        if op.startswith("skema-"):
            hasil = skema(op.split("-", 1)[1])
        elif op == "rollback":
            hasil = rollback(int(kw["snapshot_id"]))
        else:
            hasil = OPERASI[op]()
    t = muat()
    hasil.update({"operasi": op, "ms": w.ms, "snapshot_id": t.current_snapshot().snapshot_id,
                  "metadata": t.metadata_location.rsplit("/", 1)[1]})
    log("info", "operasi iceberg", operasi=op, ms=w.ms, snapshot_id=hasil["snapshot_id"])
    return hasil


# ---------------------------------------------------------------- pembacaan pohon metadata
def _baca_json(path: str) -> dict:
    with s3fs().open_input_stream(path.removeprefix("s3://")) as f:
        return json.loads(f.read())


def _katalog_baris(nama: str) -> dict:
    ns, tb = nama.split(".")
    path = KATALOG_URI.removeprefix("sqlite:///")
    with sqlite3.connect(path) as c:
        r = c.execute("SELECT catalog_name, table_namespace, table_name, metadata_location, "
                      "previous_metadata_location FROM iceberg_tables WHERE table_namespace=? AND table_name=?",
                      (ns, tb)).fetchone()
    return dict(zip(["catalog_name", "table_namespace", "table_name", "metadata_location",
                     "previous_metadata_location"], r)) if r else {}


def _pendek(p: str | None) -> str:
    return p.rsplit("/", 1)[1] if p else ""


def _nilai(v, tipe=None):
    if isinstance(tipe, (TimestamptzType, TimestampType)) and isinstance(v, int):
        return dt.datetime.fromtimestamp(v / 1e6, dt.timezone.utc).isoformat(timespec="seconds")
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    return v


def _partisi(t, f) -> dict:
    """Nilai partisi berkas sesuai spec tempat berkas itu ditulis (day -> tanggal, bucket -> nomor)."""
    spec = t.specs()[f.spec_id]
    out = {}
    for i, pf in enumerate(spec.fields):
        try:
            v = f.partition[i]
        except (IndexError, TypeError):
            v = None
        if isinstance(pf.transform, DayTransform) and isinstance(v, int):
            v = (dt.date(1970, 1, 1) + dt.timedelta(days=v)).isoformat()
        out[pf.name] = _nilai(v)
    return out


def pohon(nama: str = LAB, snapshot_id: int | None = None, maks_berkas: int = 80) -> dict:
    t = muat(nama)
    io = t.io
    meta_path = t.metadata_location
    meta = _baca_json(meta_path)
    snaps = sorted(t.snapshots(), key=lambda s: s.timestamp_ms)
    snap = t.snapshot_by_id(snapshot_id) if snapshot_id else t.current_snapshot()
    sebelumnya = None
    if snap and snap.parent_snapshot_id:
        try:
            sebelumnya = t.snapshot_by_id(snap.parent_snapshot_id)
        except Exception:
            sebelumnya = None
    file_lama = set()
    if sebelumnya:
        for m in sebelumnya.manifests(io):
            for e in m.fetch_manifest_entry(io):
                file_lama.add(e.data_file.file_path)
    manifest = []
    total_berkas = 0
    if snap:
        skema_snap = t.schemas()[snap.schema_id] if snap.schema_id is not None else t.schema()
        for m in snap.manifests(io):
            entri = []
            for e in m.fetch_manifest_entry(io, discard_deleted=False):
                total_berkas += 1
                f = e.data_file
                batas = {}
                for kol in ("nominal", "waktu"):
                    try:
                        fid = t.schemas()[0].find_field(kol).field_id
                    except ValueError:
                        continue
                    lo, hi = (f.lower_bounds or {}).get(fid), (f.upper_bounds or {}).get(fid)
                    if lo is not None and hi is not None:
                        from pyiceberg.conversions import from_bytes
                        tipe = skema_snap.find_field(fid).field_type
                        batas[kol] = [_nilai(from_bytes(tipe, lo), tipe), _nilai(from_bytes(tipe, hi), tipe)]
                if len(entri) < maks_berkas:
                    entri.append({"path": _pendek(f.file_path), "status": e.status.name, "baris": f.record_count,
                                  "byte": f.file_size_in_bytes, "spec_id": f.spec_id,
                                  "partisi": _partisi(t, f),
                                  "baru": f.file_path not in file_lama and e.status.name == "ADDED",
                                  "statistik": batas, "snapshot_id": e.snapshot_id})
            manifest.append({"path": _pendek(m.manifest_path), "isi": m.content.name, "spec_id": m.partition_spec_id,
                             "added": m.added_files_count, "existing": m.existing_files_count,
                             "deleted": m.deleted_files_count, "added_snapshot_id": m.added_snapshot_id,
                             "baru": m.added_snapshot_id == snap.snapshot_id, "berkas": entri})
    obj = objek(t)
    ref = {p.removeprefix("s3://") for p in _dirujuk(t)}
    data_obj = [o for o in obj if "/data/" in o.path]
    return {
        "tabel": nama,
        "katalog": _katalog_baris(nama),
        "metadata": {"path": _pendek(meta_path), "format_version": meta.get("format-version"),
                     "current_snapshot_id": meta.get("current-snapshot-id"),
                     "current_schema_id": meta.get("current-schema-id"), "default_spec_id": meta.get("default-spec-id"),
                     "jumlah_skema": len(meta.get("schemas", [])), "jumlah_spec": len(meta.get("partition-specs", [])),
                     "jumlah_snapshot": len(meta.get("snapshots", [])),
                     "metadata_log": [_pendek(x["metadata-file"]) for x in meta.get("metadata-log", [])][-6:],
                     "skema": [{"id": f.field_id, "nama": f.name, "tipe": str(f.field_type)} for f in t.schema().fields],
                     "spec": [{"spec_id": s.spec_id, "field": [f"{f.transform}({t.schema().find_field(f.source_id).name})"
                                                               for f in s.fields]} for s in t.specs().values()]},
        "snapshot": None if not snap else {
            "snapshot_id": snap.snapshot_id, "parent_id": snap.parent_snapshot_id,
            "manifest_list": _pendek(snap.manifest_list), "operasi": snap.summary.operation.value if snap.summary else "",
            "ringkasan": snap.summary.additional_properties if snap.summary else {},
            "schema_id": snap.schema_id},
        "manifest": manifest,
        "total_berkas_manifest": total_berkas,
        "objek": {"total": len(obj), "byte": sum(o.size for o in obj), "data": len(data_obj),
                  "data_dirujuk_snapshot_mana_pun": sum(1 for o in data_obj if o.path in ref),
                  "metadata_json": sum(1 for o in obj if o.path.endswith(".metadata.json"))},
        "snapshots": [{"snapshot_id": s.snapshot_id, "parent_id": s.parent_snapshot_id,
                       "waktu": dt.datetime.fromtimestamp(s.timestamp_ms / 1000, dt.timezone.utc).isoformat(
                           timespec="seconds"),
                       "operasi": s.summary.operation.value if s.summary else "",
                       "keterangan": (s.summary.additional_properties or {}).get("operasi-lab")
                       or (s.summary.additional_properties or {}).get("run-id", "") if s.summary else "",
                       "baris": int((s.summary.additional_properties or {}).get("total-records", 0)) if s.summary else 0,
                       "berkas": int((s.summary.additional_properties or {}).get("total-data-files", 0)) if s.summary else 0,
                       "tambah_berkas": int((s.summary.additional_properties or {}).get("added-data-files", 0)) if s.summary else 0,
                       "hapus_berkas": int((s.summary.additional_properties or {}).get("deleted-data-files", 0)) if s.summary else 0,
                       "schema_id": s.schema_id, "kini": s.snapshot_id == meta.get("current-snapshot-id")}
                      for s in snaps],
        "riwayat_ref": [{"snapshot_id": h.snapshot_id,
                         "waktu": dt.datetime.fromtimestamp(h.timestamp_ms / 1000, dt.timezone.utc).isoformat(
                             timespec="seconds")} for h in t.history()],
    }


# ---------------------------------------------------------------- time travel dan pruning
def query_snapshot(nama: str, snapshot_id: int | None) -> dict:
    t = muat(nama)
    snap = t.snapshot_by_id(snapshot_id) if snapshot_id else t.current_snapshot()
    data = t.scan(snapshot_id=snap.snapshot_id).to_arrow()
    con = duckdb.connect()
    con.register("t", data)
    kanal = "kanal" if "kanal" in data.column_names else None
    sql = (f"SELECT {kanal} AS kanal, COUNT(*) AS jml, SUM(nominal)::BIGINT AS total FROM t GROUP BY 1 ORDER BY 1"
           if kanal and "nominal" in data.column_names else "SELECT COUNT(*) AS jml FROM t")
    hasil = con.execute(sql)
    kolom = [d[0] for d in hasil.description]
    skema_snap = t.schemas()[snap.schema_id] if snap.schema_id is not None else t.schema()
    return {"snapshot_id": snap.snapshot_id, "baris": data.num_rows, "kolom": kolom,
            "hasil": [list(r) for r in hasil.fetchall()],
            "skema": [f.name for f in skema_snap.fields], "sql_spark": f"SELECT ... FROM {nama} VERSION AS OF {snap.snapshot_id}"}


def pruning(nama: str, dari: str) -> dict:
    """Hidden partitioning: filter pada kolom asli 'waktu' memangkas berkas lewat partisi + statistik manifest."""
    t = muat(nama)
    kol = _nama_kini(t, "waktu") if nama == LAB else "waktu"
    semua = list(t.scan().plan_files())
    terpilih = list(t.scan(row_filter=GreaterThanOrEqual(kol, dari)).plan_files())
    return {"filter": f"WHERE {kol} >= '{dari}'", "berkas_total": len(semua), "berkas_dibaca": len(terpilih),
            "byte_total": sum(x.file.file_size_in_bytes for x in semua),
            "byte_dibaca": sum(x.file.file_size_in_bytes for x in terpilih)}


AMAN = re.compile(r"^\s*(select|with|describe|summarize)\b", re.I)


def sql_duckdb(sql: str) -> dict:
    """DuckDB sebagai query engine di atas lakehouse: iceberg_scan('<metadata.json>')."""
    if not AMAN.match(sql) or ";" in sql.strip().rstrip(";"):
        raise ValueError("hanya satu pernyataan SELECT/WITH/DESCRIBE yang diizinkan")
    con = duckdb_lake()
    ganti = {}
    for nama in daftar_tabel():
        ganti[nama] = f"iceberg_scan('{muat(nama).metadata_location}')"
    teks = sql
    for nama, fn in sorted(ganti.items(), key=lambda x: -len(x[0])):
        teks = re.sub(rf"\b{re.escape(nama)}\b", fn, teks)
    with Stopwatch() as w:
        tbl = con.sql(f"SELECT * FROM ({teks.rstrip().rstrip(';')}) LIMIT 500").to_arrow_table()
        kolom = tbl.column_names
        baris = [list(x.values()) for x in tbl.to_pylist()]
    return {"kolom": kolom, "baris": [[_nilai(v) for v in x] for x in baris], "ms": w.ms, "sql_dijalankan": teks}
