"""API lab Bab 3: object storage (RustFS), partition, small files, kompresi, kelas penyimpanan, lifecycle.

Semua operasi dijalankan sungguhan ke RustFS lewat protokol S3 (boto3 dan PyArrow S3FileSystem).
Data transaksi memakai skema bronze platform (8 kolom metadata + kolom transaksi), path
bronze/app/transaksi/dt=YYYY-MM-DD/part-*.parquet seperti platform/KONTRAK-DATA.md.
"""
from __future__ import annotations

import concurrent.futures as cf
import datetime as dt
import gzip
import io
import json
import logging
import os
import re
import sys
import threading
import time
import uuid

import boto3
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pyarrow import fs as pafs
from pydantic import BaseModel


class LogJSON(logging.Formatter):
    def format(self, r: logging.LogRecord) -> str:
        d = {"time": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"), "level": r.levelname.lower(),
             "msg": r.getMessage(), "service": "bab03-api"}
        d.update(getattr(r, "extra", {}))
        return json.dumps(d, default=str)


_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(LogJSON())
log = logging.getLogger("bab03")
log.addHandler(_h)
log.setLevel(logging.INFO)


def catat(msg: str, level: int = logging.INFO, **kw) -> None:
    log.log(level, msg, extra={"extra": kw})


ENDPOINT = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
AK = os.environ.get("S3_ACCESS_KEY", "")
SK = os.environ.get("S3_SECRET_KEY", "")
BUCKET = os.environ.get("S3_BUCKET", "lab3")
BARIS_HARIAN = int(os.environ.get("BARIS_HARIAN", "20000"))
KUNCI = threading.Lock()


def klien(endpoint: str = ENDPOINT, ak: str = AK, sk: str = SK):
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=ak, aws_secret_access_key=sk, region_name="us-east-1",
                        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}, max_pool_connections=32))


def fs_s3(endpoint: str = ENDPOINT, ak: str = AK, sk: str = SK) -> pafs.S3FileSystem:
    return pafs.S3FileSystem(endpoint_override=endpoint, access_key=ak, secret_key=sk, region="us-east-1",
                             scheme="https" if endpoint.startswith("https") else "http")


s3 = klien()
con = duckdb.connect()
con.execute("CREATE OR REPLACE MACRO h64(a, b) AS CAST(hash(a, b) % 1000000007 AS BIGINT)")


def transaksi_hari(tgl: dt.date, n: int = BARIS_HARIAN) -> pa.Table:
    """Satu hari transaksi bronze (sumber app) dengan 8 kolom metadata, deterministik per tanggal."""
    seed = int(tgl.strftime("%Y%m%d"))
    c = con.cursor()  # satu cursor per panggilan: aman dipakai dari beberapa thread
    t = c.sql(f"""
        WITH r AS (SELECT i, h64(i, '{seed}') AS h FROM range({n}) t(i))
        SELECT
          strftime(TIMESTAMP '{tgl}' + INTERVAL (h % 86400) SECOND + INTERVAL 30 SECOND, '%Y-%m-%dT%H:%M:%S.000Z') AS _ingested_at,
          'app' AS _source, '{tgl:%Y%m%d}-app-tra-0' AS _batch_id,
          md5(i::VARCHAR || '{seed}') AS _event_id, 'bank.transaksi.dibuat' AS _event_type,
          strftime(TIMESTAMP '{tgl}' + INTERVAL (h % 86400) SECOND, '%Y-%m-%dT%H:%M:%S.000+07:00') AS _occurred_at,
          CAST(epoch_ms(TIMESTAMP '{tgl}' + INTERVAL (h % 86400) SECOND) AS VARCHAR) AS _ts_ms,
          CAST(NULL AS VARCHAR) AS _op,
          'TRX{tgl:%Y%m%d}' || lpad(i::VARCHAR, 7, '0') AS id,
          1 + h % 8000 AS rekening_id,
          1 + h64(i, 'cab{seed}') % 8 AS cabang_id,
          ['mobile','mobile','mobile','atm','atm','teller','web','ecommerce'][1 + h64(i, 'k{seed}') % 8] AS kanal,
          ['setor','tarik','transfer_masuk','transfer_keluar','angsuran','bayar_belanja'][1 + h64(i, 'j{seed}') % 6] AS jenis,
          CAST([5,10,20,25,50,75,100,250,500,1000][1 + h64(i, 'n{seed}') % 10] * 10000 AS DOUBLE) AS nominal,
          TIMESTAMP '{tgl}' + INTERVAL (h % 86400) SECOND AS occurred_at
        FROM r ORDER BY occurred_at""").to_arrow_table()
    return t


def parquet_bytes(t: pa.Table, compression: str = "zstd", level: int | None = None) -> bytes:
    buf = io.BytesIO()
    pq.write_table(t, buf, compression=compression, compression_level=level)
    return buf.getvalue()


def hapus_prefix(prefix: str) -> int:
    # DeleteObjects (batch) di RustFS 1.0.4 lambat untuk ribuan objek kecil (±34 detik per 1000 saat diuji),
    # jadi objek dihapus satu per satu secara paralel.
    keys = [o["Key"] for o in daftar(prefix)]
    with cf.ThreadPoolExecutor(16) as ex:
        list(ex.map(lambda k: s3.delete_object(Bucket=BUCKET, Key=k), keys))
    return len(keys)


def daftar(prefix: str) -> list[dict]:
    out = []
    for hal in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        out += hal.get("Contents", [])
    return out


app = FastAPI(title="Lab DE Bab 3")
STATE = {"siap": False}


@app.on_event("startup")
def mulai() -> None:
    for i in range(60):
        try:
            try:
                s3.head_bucket(Bucket=BUCKET)
            except ClientError:
                s3.create_bucket(Bucket=BUCKET)
                catat("bucket dibuat", bucket=BUCKET)
            break
        except Exception as e:  # RustFS belum siap
            if i == 59:
                raise
            catat("menunggu RustFS", level=logging.WARNING, err=str(e)[:120])
            time.sleep(1)
    if not daftar("bronze/app/transaksi/"):
        siapkan_partisi(Hari(hari=30))
    STATE["siap"] = True


@app.middleware("http")
async def mw(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
    t = time.perf_counter()
    try:
        resp = await call_next(request)
    except Exception as e:
        catat("galat", logging.ERROR, request_id=rid, path=request.url.path, err=str(e))
        return JSONResponse({"error": str(e)}, status_code=500)
    if request.url.path != "/healthz" and request.url.path != "/api/objek/daftar":
        catat("http", request_id=rid, method=request.method, path=request.url.path, kode=resp.status_code,
              durasi_ms=round((time.perf_counter() - t) * 1000, 1))
    return resp


@app.get("/healthz")
def healthz():
    if not STATE["siap"]:
        raise HTTPException(503, "belum siap")
    return {"status": "ok"}


# ------------------------------------------------------------------ object storage dasar

@app.get("/api/objek/daftar")
def objek_daftar(prefix: str = "", delimiter: str = "/"):
    r = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, Delimiter=delimiter, MaxKeys=200)
    return {"bucket": BUCKET, "prefix": prefix, "folder": [p["Prefix"] for p in r.get("CommonPrefixes", [])],
            "objek": [{"key": o["Key"], "ukuran": o["Size"], "kelas": o.get("StorageClass"), "diubah": o["LastModified"].isoformat()}
                      for o in r.get("Contents", [])], "terpotong": r.get("IsTruncated", False)}


@app.post("/api/objek/dasar")
def objek_dasar():
    """PUT dengan metadata, HEAD, GET range, LIST dengan delimiter, DELETE: satu per satu dengan latensinya."""
    langkah = []
    def ukur(nama, http, fn):
        t = time.perf_counter()
        try:
            hasil = fn()
            langkah.append({"operasi": nama, "http": http, "ms": round((time.perf_counter() - t) * 1000, 2), "hasil": hasil, "ok": True})
        except ClientError as e:
            langkah.append({"operasi": nama, "http": http, "ms": round((time.perf_counter() - t) * 1000, 2),
                            "hasil": e.response.get("Error", {}), "ok": False})
    key = "demo/laporan/2026/transaksi-contoh.csv"
    isi = ("id,kanal,nominal\n" + "".join(f"TRX{i:05d},mobile,{(i % 9 + 1) * 10000}\n" for i in range(2000))).encode()
    ukur("PUT objek + metadata", f"PUT /{BUCKET}/{key}", lambda: {"ETag": s3.put_object(Bucket=BUCKET, Key=key, Body=isi, ContentType="text/csv",
         Metadata={"pemilik": "tim-data", "klasifikasi": "internal"})["ETag"], "byte": len(isi)})
    ukur("HEAD (metadata saja)", f"HEAD /{BUCKET}/{key}", lambda: {k: v for k, v in s3.head_object(Bucket=BUCKET, Key=key).items()
         if k in ("ContentLength", "ContentType", "ETag", "Metadata", "StorageClass")})
    def rentang():
        r = s3.get_object(Bucket=BUCKET, Key=key, Range="bytes=0-63")
        return {"ContentRange": r.get("ContentRange"), "isi": r["Body"].read().decode()}
    ukur("GET range bytes=0-63", f"GET /{BUCKET}/{key} Range: bytes=0-63", rentang)
    ukur("LIST prefix=demo/ delimiter=/", f"GET /{BUCKET}?list-type=2&prefix=demo/&delimiter=/",
         lambda: {"CommonPrefixes": [p["Prefix"] for p in s3.list_objects_v2(Bucket=BUCKET, Prefix="demo/", Delimiter="/").get("CommonPrefixes", [])]})
    ukur("GET objek yang tidak ada", f"GET /{BUCKET}/demo/tidak-ada.csv", lambda: s3.get_object(Bucket=BUCKET, Key="demo/tidak-ada.csv"))
    return {"langkah": langkah, "catatan": "Direktori 'demo/laporan/2026/' tidak pernah dibuat; ia hanya awalan key."}


class Ganti(BaseModel):
    jumlah: int = 200


@app.post("/api/objek/ganti-nama")
def ganti_nama(g: Ganti):
    """'Ganti nama direktori' di object storage = salin lalu hapus per objek."""
    if not 1 <= g.jumlah <= 2000:
        raise HTTPException(400, "jumlah 1 sampai 2000")
    with KUNCI:
        hapus_prefix("rename/")
        kecil = b"x" * 512
        with cf.ThreadPoolExecutor(16) as ex:
            list(ex.map(lambda i: s3.put_object(Bucket=BUCKET, Key=f"rename/lama/obj-{i:05d}.txt", Body=kecil), range(g.jumlah)))
        t = time.perf_counter()
        keys = [o["Key"] for o in daftar("rename/lama/")]
        def pindah(k):
            s3.copy_object(Bucket=BUCKET, Key=k.replace("rename/lama/", "rename/baru/"), CopySource={"Bucket": BUCKET, "Key": k})
            s3.delete_object(Bucket=BUCKET, Key=k)
        with cf.ThreadPoolExecutor(16) as ex:
            list(ex.map(pindah, keys))
        ms = (time.perf_counter() - t) * 1000
    return {"objek": len(keys), "request": 1 + 2 * len(keys), "ms": round(ms, 1),
            "catatan": "HDFS: satu operasi metadata di NameNode. Object storage: LIST + COPY + DELETE per objek."}


class Multipart(BaseModel):
    mb: int = 24
    part_mb: int = 5


@app.post("/api/objek/multipart")
def multipart(m: Multipart):
    if not 5 <= m.part_mb <= 64 or not m.part_mb <= m.mb <= 256:
        raise HTTPException(400, "part_mb 5-64 (minimum S3 5 MB), mb part_mb-256")
    key = f"demo/besar/berkas-{m.mb}mb.bin"
    data = os.urandom(m.mb * 1024 * 1024)
    ukuran = m.part_mb * 1024 * 1024
    t0 = time.perf_counter()
    up = s3.create_multipart_upload(Bucket=BUCKET, Key=key)
    bagian = [(i + 1, data[o:o + ukuran]) for i, o in enumerate(range(0, len(data), ukuran))]
    def kirim(p):
        n, isi = p
        t = time.perf_counter()
        r = s3.upload_part(Bucket=BUCKET, Key=key, UploadId=up["UploadId"], PartNumber=n, Body=isi)
        return {"PartNumber": n, "ETag": r["ETag"], "byte": len(isi), "mulai_ms": round((t - t0) * 1000, 1),
                "selesai_ms": round((time.perf_counter() - t0) * 1000, 1)}
    with cf.ThreadPoolExecutor(4) as ex:
        hasil = list(ex.map(kirim, bagian))
    s3.complete_multipart_upload(Bucket=BUCKET, Key=key, UploadId=up["UploadId"],
                                 MultipartUpload={"Parts": [{"PartNumber": h["PartNumber"], "ETag": h["ETag"]} for h in hasil]})
    head = s3.head_object(Bucket=BUCKET, Key=key)
    return {"key": key, "byte": len(data), "part": hasil, "total_ms": round((time.perf_counter() - t0) * 1000, 1),
            "etag": head["ETag"], "catatan": "ETag berakhiran -N menandai objek multipart dengan N bagian; 4 bagian dikirim paralel."}


# ------------------------------------------------------------------ partition

class Hari(BaseModel):
    hari: int = 30


@app.post("/api/partisi/siapkan")
def siapkan_partisi(h: Hari):
    if not 1 <= h.hari <= 90:
        raise HTTPException(400, "hari 1 sampai 90")
    with KUNCI:
        hapus_prefix("bronze/app/transaksi/")
        akhir = dt.date.today() - dt.timedelta(days=1)
        tanggal = [akhir - dt.timedelta(days=i) for i in range(h.hari)][::-1]
        def tulis(tgl):
            b = parquet_bytes(transaksi_hari(tgl))
            s3.put_object(Bucket=BUCKET, Key=f"bronze/app/transaksi/dt={tgl}/part-{tgl:%Y%m%d}-app-tra-0.parquet", Body=b)
            return len(b)
        t = time.perf_counter()
        with cf.ThreadPoolExecutor(4) as ex:
            ukuran = list(ex.map(tulis, tanggal))
    catat("partisi disiapkan", hari=h.hari, byte=sum(ukuran))
    return {"hari": h.hari, "berkas": len(ukuran), "byte": sum(ukuran), "baris_per_hari": BARIS_HARIAN,
            "ms": round((time.perf_counter() - t) * 1000)}


@app.get("/api/partisi/query")
def partisi_query(tanggal: str = "", semua: bool = False):
    """SUM(nominal) per kanal: satu partisi (pruning) vs semua partisi. Dibaca PyArrow langsung dari RustFS."""
    objek = daftar("bronze/app/transaksi/")
    partisi = sorted({o["Key"].split("dt=")[1].split("/")[0] for o in objek})
    if not partisi:
        raise HTTPException(409, "belum ada partisi; jalankan /api/partisi/siapkan")
    tanggal = tanggal or partisi[-1]
    d = ds.dataset(f"{BUCKET}/bronze/app/transaksi", filesystem=fs_s3(), format="parquet", partitioning="hive")
    filt = None if semua else (ds.field("dt") == tanggal)
    t = time.perf_counter()
    fragmen = list(d.get_fragments(filter=filt))
    tbl = d.to_table(columns=["kanal", "nominal"], filter=filt)
    agregat = tbl.group_by("kanal").aggregate([("nominal", "sum"), ("nominal", "count")]).to_pylist()
    ms = (time.perf_counter() - t) * 1000
    dibuka = {f.path.split("dt=")[1].split("/")[0] for f in fragmen}
    ukuran = {o["Key"].split("dt=")[1].split("/")[0]: o["Size"] for o in objek}
    return {"tanggal": None if semua else tanggal, "partisi": partisi, "dibuka": sorted(dibuka), "berkas_dibuka": len(fragmen),
            "byte_berkas_dibuka": sum(ukuran[p] for p in dibuka), "byte_semua": sum(ukuran.values()), "baris": tbl.num_rows,
            "ms": round(ms, 1), "agregat": sorted(agregat, key=lambda r: -r["nominal_sum"]),
            "sql_setara": f"SELECT kanal, SUM(nominal) FROM read_parquet('s3://{BUCKET}/bronze/app/transaksi/*/*.parquet', hive_partitioning=true)"
                          + ("" if semua else f" WHERE dt = '{tanggal}'") + " GROUP BY kanal"}


# ------------------------------------------------------------------ small files problem

HARGA_REQUEST = {"put_per_1000": 0.005, "get_per_1000": 0.0004, "sumber": "contoh harga S3 Standard (AWS us-east-1), cek harga terkini"}


class Kecil(BaseModel):
    jumlah: list[int] = [1, 10, 100, 1000]


@app.post("/api/kecil/jalankan")
def kecil(k: Kecil):
    if any(n < 1 or n > 2000 for n in k.jumlah) or len(k.jumlah) > 6:
        raise HTTPException(400, "jumlah berkas 1-2000, maksimal 6 skenario")
    tgl = dt.date.today() - dt.timedelta(days=1)
    t_hari = transaksi_hari(tgl)
    hasil = []
    with KUNCI:
        lama = {o["Key"].split("/")[1] for o in daftar("kecil/")}  # run=<epoch> sebelumnya
        run = f"run={int(time.time())}"
        STATE["kecil_run"] = run
        # run sebelumnya dihapus di latar: menghapus ribuan objek kecil butuh belasan detik
        threading.Thread(target=lambda: [hapus_prefix(f"kecil/{r}/") for r in lama], daemon=True).start()
        for n in k.jumlah:
            prefix = f"kecil/{run}/n={n:04d}/dt={tgl}/"
            potong = max(1, -(-t_hari.num_rows // n))
            bagian = [parquet_bytes(t_hari.slice(i * potong, potong)) for i in range(n) if i * potong < t_hari.num_rows]
            t = time.perf_counter()
            with cf.ThreadPoolExecutor(16) as ex:
                list(ex.map(lambda p: s3.put_object(Bucket=BUCKET, Key=f"{prefix}part-{p[0]:05d}.parquet", Body=p[1]), enumerate(bagian)))
            t_tulis = (time.perf_counter() - t) * 1000
            t = time.perf_counter()
            halaman, objek = 0, []
            for hal in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix, PaginationConfig={"PageSize": 1000}):
                halaman += 1
                objek += hal.get("Contents", [])
            t_list = (time.perf_counter() - t) * 1000
            t = time.perf_counter()
            tbl = ds.dataset(f"{BUCKET}/{prefix}", filesystem=fs_s3(), format="parquet").to_table(columns=["kanal", "nominal"])
            t_baca = (time.perf_counter() - t) * 1000
            req_put, req_get = len(bagian), len(bagian) * 2 + halaman  # baca footer + data per berkas
            ukuran = [o["Size"] for o in objek]
            hasil.append({"berkas": len(bagian), "byte_total": sum(ukuran), "byte_rata": int(sum(ukuran) / max(1, len(ukuran))),
                          "tulis_ms": round(t_tulis, 1), "list_ms": round(t_list, 1), "baca_ms": round(t_baca, 1), "baris": tbl.num_rows,
                          "request_put": req_put, "request_get_list": req_get,
                          "biaya_request_usd_per_bulan": round((req_put * HARGA_REQUEST["put_per_1000"] + req_get * HARGA_REQUEST["get_per_1000"]) / 1000 * 8640, 4)})
    catat("small files diuji", skenario=k.jumlah)
    return {"tanggal": str(tgl), "baris": t_hari.num_rows, "hasil": hasil, "harga": HARGA_REQUEST,
            "asumsi_biaya": "pipeline menulis lalu membaca pola yang sama tiap 5 menit selama 30 hari (8.640 kali)"}


@app.post("/api/kecil/compaction")
def compaction():
    """Gabungkan skenario berkas terbanyak menjadi satu berkas (seperti rewrite_data_files di Iceberg, Bab 4)."""
    with KUNCI:
        run = STATE.get("kecil_run")
        objek = daftar(f"kecil/{run}/") if run else []
        if not objek:
            raise HTTPException(409, "jalankan uji small files dulu")
        n_max = max(int(re.search(r"/n=(\d{4})/", o["Key"]).group(1)) for o in objek)
        prefix = next(o["Key"] for o in objek if f"/n={n_max:04d}/" in o["Key"]).rsplit("/", 1)[0] + "/"
        keys = [o["Key"] for o in objek if o["Key"].startswith(prefix)]
        if len(keys) <= 1:
            raise HTTPException(409, "skenario ini sudah satu berkas")
        t = time.perf_counter()
        tbl = ds.dataset(f"{BUCKET}/{prefix}", filesystem=fs_s3(), format="parquet").to_table()
        t_baca = (time.perf_counter() - t) * 1000
        t = time.perf_counter()
        b = parquet_bytes(tbl)
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}part-compacted.parquet", Body=b)
        t_tulis = (time.perf_counter() - t) * 1000
        t = time.perf_counter()
        with cf.ThreadPoolExecutor(16) as ex:
            list(ex.map(lambda k: s3.delete_object(Bucket=BUCKET, Key=k), keys))
        t_hapus = (time.perf_counter() - t) * 1000
        t = time.perf_counter()
        ds.dataset(f"{BUCKET}/{prefix}", filesystem=fs_s3(), format="parquet").to_table(columns=["kanal", "nominal"])
        baca = (time.perf_counter() - t) * 1000
    return {"prefix": prefix, "berkas_sebelum": len(keys), "berkas_sesudah": 1, "byte": len(b), "baca_semua_ms": round(t_baca, 1),
            "tulis_ms": round(t_tulis, 1), "hapus_ms": round(t_hapus, 1), "baca_sesudah_ms": round(baca, 1)}


# ------------------------------------------------------------------ kompresi

@app.post("/api/kompresi")
def kompresi():
    akhir = dt.date.today() - dt.timedelta(days=1)
    tbl = pa.concat_tables([transaksi_hari(akhir - dt.timedelta(days=i)) for i in range(5)])
    skenario = [("CSV", None, None), ("CSV + gzip", None, None), ("Parquet tanpa kompresi", "none", None),
                ("Parquet snappy", "snappy", None), ("Parquet lz4", "lz4", None), ("Parquet gzip", "gzip", None),
                ("Parquet zstd (level 3)", "zstd", 3), ("Parquet zstd (level 19)", "zstd", 19)]
    hasil = []
    with KUNCI:
        hapus_prefix("kompresi/")
        for nama, codec, level in skenario:
            t = time.perf_counter()
            if nama.startswith("CSV"):
                buf = io.BytesIO()
                pacsv.write_csv(tbl, buf)
                b = buf.getvalue()
                if "gzip" in nama:
                    b = gzip.compress(b, compresslevel=6)
            else:
                b = parquet_bytes(tbl, codec, level)
            tulis = (time.perf_counter() - t) * 1000
            ext = "csv.gz" if "gzip" in nama and nama.startswith("CSV") else "csv" if nama.startswith("CSV") else "parquet"
            key = f"kompresi/{nama.lower().replace(' ', '-').replace('(', '').replace(')', '')}.{ext}"
            s3.put_object(Bucket=BUCKET, Key=key, Body=b)
            t = time.perf_counter()
            isi = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            if nama.startswith("CSV"):
                raw = gzip.decompress(isi) if "gzip" in nama else isi
                pacsv.read_csv(io.BytesIO(raw))
            else:
                pq.read_table(io.BytesIO(isi), columns=["kanal", "nominal"])
            baca = (time.perf_counter() - t) * 1000
            hasil.append({"skenario": nama, "byte": len(b), "tulis_ms": round(tulis, 1), "baca_ms": round(baca, 1), "key": key})
    csv = hasil[0]["byte"]
    for h in hasil:
        h["rasio_vs_csv"] = round(h["byte"] / csv, 3)
    return {"baris": tbl.num_rows, "hasil": hasil, "catatan": "baca = GET dari RustFS lalu decode; Parquet hanya kolom kanal dan nominal"}


# ------------------------------------------------------------------ kelas penyimpanan, lifecycle, tier

@app.post("/api/kelas/coba")
def kelas_coba():
    hasil = []
    for kelas in ["STANDARD", "REDUCED_REDUNDANCY", "STANDARD_IA", "GLACIER_IR", "GLACIER", "DEEP_ARCHIVE"]:
        key = f"kelas/contoh-{kelas.lower()}.txt"
        try:
            s3.put_object(Bucket=BUCKET, Key=key, Body=b"contoh", StorageClass=kelas)
            h = s3.head_object(Bucket=BUCKET, Key=key)
            hasil.append({"kelas": kelas, "diterima": True, "head_storage_class": h.get("StorageClass", "STANDARD (tidak dikirim)")})
        except ClientError as e:
            hasil.append({"kelas": kelas, "diterima": False, "galat": f"{e.response['Error'].get('Code')}: {e.response['Error'].get('Message')}"})
    return {"hasil": hasil}


class Aturan(BaseModel):
    prefix: str = "bronze/"
    hari_warm: int = 30
    hari_cold: int = 395
    hari_archive: int = 1095
    hari_hapus: int = 3653


def aturan_s3(a: Aturan) -> dict:
    return {"Rules": [{"ID": "transaksi-tiering", "Status": "Enabled", "Filter": {"Prefix": a.prefix},
                       "Transitions": [{"Days": a.hari_warm, "StorageClass": "STANDARD_IA"},
                                       {"Days": a.hari_cold, "StorageClass": "GLACIER_IR"},
                                       {"Days": a.hari_archive, "StorageClass": "DEEP_ARCHIVE"}],
                       "Expiration": {"Days": a.hari_hapus}},
                      {"ID": "multipart-tak-selesai-7-hari", "Status": "Enabled", "Filter": {"Prefix": ""},
                       "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}]}


@app.post("/api/lifecycle/pasang")
def lifecycle_pasang(a: Aturan):
    """Coba pasang kebijakan lengkap (dengan Transition); RustFS menolak tanpa tier remote, lalu pasang Expiration saja."""
    if not 0 < a.hari_warm < a.hari_cold < a.hari_archive < a.hari_hapus:
        raise HTTPException(400, "hari harus naik: warm < cold < archive < hapus")
    lengkap = aturan_s3(a)
    per_rule = {"Rules": [{"ID": f"transisi-{t['StorageClass'].lower()}", "Status": "Enabled", "Filter": {"Prefix": a.prefix},
                           "Transitions": [t]} for t in lengkap["Rules"][0]["Transitions"]]}
    dasar = {"Rules": [{"ID": "transaksi-retensi", "Status": "Enabled", "Filter": {"Prefix": a.prefix},
                        "Expiration": {"Days": a.hari_hapus}}, lengkap["Rules"][1]]}
    langkah = []
    for nama, cfg in [("1. format S3: tiga Transition dalam satu rule + Expiration", lengkap),
                      ("2. satu Transition per rule", per_rule),
                      ("3. Expiration + AbortIncompleteMultipartUpload saja", dasar)]:
        try:
            s3.put_bucket_lifecycle_configuration(Bucket=BUCKET, LifecycleConfiguration=cfg)
            langkah.append({"percobaan": nama, "diterima": True, "konfigurasi": cfg})
            break
        except ClientError as e:
            langkah.append({"percobaan": nama, "diterima": False, "konfigurasi": cfg,
                            "galat": f"{e.response['Error'].get('Code')}: {e.response['Error'].get('Message')}"})
    terpasang = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET).get("Rules", [])
    catat("lifecycle dipasang", langkah=langkah)
    return {"diminta": lengkap, "langkah": langkah, "terpasang": terpasang}


@app.post("/api/tier/simulasi")
def tier_simulasi(a: Aturan, geser_hari: int = 0, tandai: bool = False):
    """Hitung tier tiap objek partisi bila 'hari ini' digeser; opsional menandai objek dengan tag tier-simulasi."""
    hari_ini = dt.date.today() + dt.timedelta(days=geser_hari)
    hasil = []
    for o in daftar("bronze/app/transaksi/"):
        tgl = dt.date.fromisoformat(o["Key"].split("dt=")[1].split("/")[0])
        umur = (hari_ini - tgl).days
        tier = ("hot" if umur < a.hari_warm else "warm" if umur < a.hari_cold else "cold" if umur < a.hari_archive
                else "archive" if umur < a.hari_hapus else "dihapus")
        if tandai:
            s3.put_object_tagging(Bucket=BUCKET, Key=o["Key"], Tagging={"TagSet": [{"Key": "tier-simulasi", "Value": tier}]})
        hasil.append({"key": o["Key"], "dt": str(tgl), "umur_hari": umur, "tier": tier, "byte": o["Size"]})
    contoh_tag = None
    if tandai and hasil:
        contoh_tag = s3.get_object_tagging(Bucket=BUCKET, Key=hasil[-1]["key"]).get("TagSet")
    return {"hari_ini_simulasi": str(hari_ini), "objek": hasil, "tag_contoh": contoh_tag}


# ------------------------------------------------------------------ platform (opsional, hanya baca)

@app.get("/api/platform/bronze")
def platform_bronze():
    ep = os.environ.get("PLATFORM_S3_ENDPOINT", "").strip()
    if not ep:
        return {"aktif": False, "pesan": "PLATFORM_S3_ENDPOINT kosong: lab memakai RustFS sendiri"}
    try:
        p = klien(ep, os.environ.get("PLATFORM_S3_ACCESS_KEY", ""), os.environ.get("PLATFORM_S3_SECRET_KEY", ""))
        bucket = os.environ.get("PLATFORM_S3_BUCKET", "lake")
        per = {}
        ukuran = []
        for hal in p.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="bronze/"):
            for o in hal.get("Contents", []):
                bagian = o["Key"].split("/")
                if len(bagian) < 5:
                    continue
                k = f"{bagian[1]}/{bagian[2]}"
                e = per.setdefault(k, {"entitas": k, "berkas": 0, "byte": 0, "partisi": set()})
                e["berkas"] += 1
                e["byte"] += o["Size"]
                e["partisi"].add(bagian[3])
                ukuran.append(o["Size"])
        kelas = [("< 64 KB", 0, 64 << 10), ("64 KB-1 MB", 64 << 10, 1 << 20), ("1-16 MB", 1 << 20, 16 << 20),
                 ("16-128 MB", 16 << 20, 128 << 20), ("≥ 128 MB", 128 << 20, 1 << 62)]
        return {"aktif": True, "endpoint": ep, "bucket": bucket, "total_berkas": len(ukuran), "total_byte": sum(ukuran),
                "entitas": [{**{k: v for k, v in e.items() if k != "partisi"}, "partisi": len(e["partisi"])} for e in sorted(per.values(), key=lambda x: -x["berkas"])],
                "histogram": [{"kelas": n, "berkas": sum(1 for u in ukuran if a <= u < b)} for n, a, b in kelas]}
    except Exception as e:
        return {"aktif": False, "pesan": f"platform tidak terbaca: {str(e)[:300]}"}
