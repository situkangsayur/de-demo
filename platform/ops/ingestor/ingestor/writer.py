"""Menulis baris bronze ke Parquet di RustFS: satu file per (sumber, entitas, dt) per batch."""

from __future__ import annotations

import secrets
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs

from .common import bucket, get_logger, s3fs
from .mapping import META_COLS, Row

log = get_logger()

META_TYPES = {
    "_ingested_at": pa.timestamp("us", tz="UTC"),
    "_source": pa.string(),
    "_batch_id": pa.string(),
    "_event_id": pa.string(),
    "_event_type": pa.string(),
    "_occurred_at": pa.string(),
    "_ts_ms": pa.int64(),
    "_op": pa.string(),
}


def new_batch_id(now: datetime) -> str:
    return now.strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(3)


def object_path(source: str, entity: str, dt: str, batch_id: str) -> str:
    return f"{bucket()}/bronze/{source}/{entity}/dt={dt}/part-{batch_id}.parquet"


def to_table(rows: list[Row], ingested_at: datetime, batch_id: str) -> pa.Table:
    """Kolom metadata bertipe tetap (urutan kontrak), kolom data semua string (urut pertama muncul)."""
    data_cols: list[str] = []
    seen = set(META_COLS)
    for r in rows:
        for k in r.record:
            if k not in seen:
                seen.add(k)
                data_cols.append(k)
    cols = {}
    n = len(rows)
    cols["_ingested_at"] = pa.array([ingested_at] * n, type=META_TYPES["_ingested_at"])
    cols["_source"] = pa.array([r.record["_source"] for r in rows], type=pa.string())
    cols["_batch_id"] = pa.array([batch_id] * n, type=pa.string())
    for c in ("_event_id", "_event_type", "_occurred_at", "_op"):
        cols[c] = pa.array([r.record.get(c) for r in rows], type=pa.string())
    cols["_ts_ms"] = pa.array([r.record.get("_ts_ms") for r in rows], type=pa.int64())
    ordered = {c: cols[c] for c in META_COLS}
    for c in data_cols:
        ordered[c] = pa.array([r.record.get(c) for r in rows], type=pa.string())
    return pa.table(ordered)


class BronzeWriter:
    def __init__(self) -> None:
        self.fs = s3fs()
        self.lock = threading.Lock()
        self.rows_by_path: dict[str, int] = {}  # cache jumlah baris per file (untuk statistik UI)
        self.recent: list[dict] = []
        self.files_written = 0
        self.rows_written = 0

    def write(self, rows: list[Row]) -> list[dict]:
        """Menulis semua baris; error dilempar agar pemanggil tidak meng-ack pesan."""
        if not rows:
            return []
        now = datetime.now(timezone.utc)
        batch_id = new_batch_id(now)
        groups: dict[tuple[str, str, str], list[Row]] = defaultdict(list)
        for r in rows:
            groups[(r.source, r.entity, r.dt)].append(r)
        written = []
        for (source, entity, dt), grp in sorted(groups.items()):
            path = object_path(source, entity, dt, batch_id)
            table = to_table(grp, now, batch_id)
            t0 = time.monotonic()
            with self.fs.open_output_stream(path) as out:
                pq.write_table(table, out, compression="zstd")
            info = {
                "path": "s3://" + path,
                "source": source,
                "entity": entity,
                "dt": dt,
                "rows": table.num_rows,
                "ms": int((time.monotonic() - t0) * 1000),
                "at": now.isoformat().replace("+00:00", "Z"),
            }
            written.append(info)
            with self.lock:
                self.rows_by_path[path] = table.num_rows
        with self.lock:
            self.files_written += len(written)
            self.rows_written += sum(w["rows"] for w in written)
            self.recent = (written + self.recent)[:40]
        log.info(
            "batch bronze ditulis",
            extra={"f": {"batch_id": batch_id, "files": len(written), "rows": sum(w["rows"] for w in written)}},
        )
        return written

    def partitions(self, max_footer_reads: int = 300) -> list[dict]:
        """Daftar partisi bronze dari listing RustFS: jumlah file, byte, dan baris per partisi."""
        sel = pafs.FileSelector(f"{bucket()}/bronze", recursive=True, allow_not_found=True)
        infos = [i for i in self.fs.get_file_info(sel) if i.type == pafs.FileType.File and i.path.endswith(".parquet")]
        agg: dict[tuple[str, str, str], dict] = {}
        reads = 0
        for i in infos:
            parts = i.path.split("/")  # lake/bronze/<source>/<entity>/dt=.../part-...
            if len(parts) < 6 or not parts[4].startswith("dt="):
                continue
            key = (parts[2], parts[3], parts[4][3:])
            a = agg.setdefault(key, {"source": key[0], "entity": key[1], "dt": key[2], "files": 0, "bytes": 0, "rows": 0, "rows_unknown": 0})
            a["files"] += 1
            a["bytes"] += i.size or 0
            with self.lock:
                n = self.rows_by_path.get(i.path)
            if n is None and reads < max_footer_reads:
                try:
                    with self.fs.open_input_file(i.path) as f:
                        n = pq.ParquetFile(f).metadata.num_rows
                    reads += 1
                    with self.lock:
                        self.rows_by_path[i.path] = n
                except Exception:
                    n = None
            if n is None:
                a["rows_unknown"] += 1
            else:
                a["rows"] += n
        return sorted(agg.values(), key=lambda a: (a["source"], a["entity"], a["dt"]))
