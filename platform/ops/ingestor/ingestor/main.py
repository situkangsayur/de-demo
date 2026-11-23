"""Ingestor: NATS JetStream (BANK, SHOP, CDC) -> bronze Parquet di RustFS.

- Durable pull consumer per stream (de_bronze_bank, de_bronze_shop, de_bronze_cdc).
- Pesan ditampung lalu ditulis per batch tiap INGEST_FLUSH_SECONDS detik atau INGEST_FLUSH_MAX pesan.
- Ack dikirim SESUDAH file Parquet berhasil ditulis (at-least-once). Bila tulis gagal, pesan di-nak
  dan dikirim ulang oleh JetStream; duplikat antarbatch dibuang di silver (event_id).
- HTTP :8080 /healthz dan /stats (partisi bronze, counter consumer) untuk UI "Aliran data".
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import nats
from nats.errors import TimeoutError as NatsTimeout
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import NotFoundError

from .common import env, get_logger
from .mapping import Row, dedup, map_message
from .writer import BronzeWriter, new_batch_id, object_path

log = get_logger()

STREAMS = [
    {"stream": "BANK", "subject": "bank.>", "durable": "de_bronze_bank"},
    {"stream": "SHOP", "subject": "shop.>", "durable": "de_bronze_shop"},
    {"stream": "CDC", "subject": "cdc.>", "durable": "de_bronze_cdc"},
]

FLUSH_SECONDS = float(env("INGEST_FLUSH_SECONDS", "10"))
FLUSH_MAX = int(env("INGEST_FLUSH_MAX", "2000"))


class State:
    def __init__(self) -> None:
        self.started = datetime.now(timezone.utc)
        self.consumers = {s["stream"]: {"durable": s["durable"], "status": "menunggu stream", "received": 0, "acked": 0, "last_stream_seq": 0} for s in STREAMS}
        self.batches = 0
        self.last_flush: dict | None = None
        self.last_error: str | None = None
        self.buffer: list[tuple[object, list[Row]]] = []
        self.buffer_since = time.monotonic()
        self.cache: tuple[float, list] = (0.0, [])


state = State()
writer = BronzeWriter()


async def consume(js, spec: dict) -> None:
    c = state.consumers[spec["stream"]]
    psub = None
    while psub is None:
        try:
            cfg = ConsumerConfig(
                durable_name=spec["durable"],
                ack_policy=AckPolicy.EXPLICIT,
                deliver_policy=DeliverPolicy.ALL,
                ack_wait=300,  # detik; harus jauh lebih lama dari interval flush
                max_ack_pending=100_000,
                max_deliver=-1,
                filter_subject=spec["subject"],
            )
            psub = await js.pull_subscribe(spec["subject"], durable=spec["durable"], stream=spec["stream"], config=cfg)
            c["status"] = "aktif"
            log.info("consumer aktif", extra={"f": {"stream": spec["stream"], "durable": spec["durable"]}})
        except NotFoundError:
            c["status"] = "menunggu stream"
            await asyncio.sleep(5)
        except Exception as e:
            c["status"] = f"error: {e}"
            log.warning("consumer belum siap", extra={"f": {"stream": spec["stream"], "err": str(e)}})
            await asyncio.sleep(5)
    while True:
        if len(state.buffer) >= 3 * FLUSH_MAX:  # backpressure: tunggu flush
            await asyncio.sleep(0.2)
            continue
        try:
            msgs = await psub.fetch(batch=min(500, FLUSH_MAX), timeout=1)
        except NatsTimeout:
            continue
        except Exception as e:
            c["status"] = f"error fetch: {e}"
            await asyncio.sleep(1)
            continue
        c["status"] = "aktif"
        for m in msgs:
            md = m.metadata
            ts = md.timestamp if md and md.timestamp else datetime.now(timezone.utc)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            msg_id = (m.headers or {}).get("Nats-Msg-Id")
            try:
                rows = map_message(spec["stream"], m.subject, m.data, msg_id, ts)
            except Exception as e:  # pesan rusak: tetap di-ack agar tidak macet, dicatat di log
                log.warning("pesan tidak dapat dipetakan", extra={"f": {"subject": m.subject, "err": str(e)}})
                rows = []
            if not state.buffer:
                state.buffer_since = time.monotonic()
            state.buffer.append((m, rows))
            c["received"] += 1
            if md:
                c["last_stream_seq"] = md.sequence.stream


async def flusher() -> None:
    while True:
        await asyncio.sleep(0.5)
        if not state.buffer:
            continue
        if len(state.buffer) < FLUSH_MAX and time.monotonic() - state.buffer_since < FLUSH_SECONDS:
            continue
        batch, state.buffer = state.buffer, []
        rows = dedup([r for _, rs in batch for r in rs])
        try:
            written = await asyncio.to_thread(writer.write, rows)
        except Exception as e:
            state.last_error = f"{datetime.now(timezone.utc).isoformat()}: {e}"
            log.error("tulis bronze gagal; pesan di-nak", extra={"f": {"err": str(e), "msgs": len(batch)}})
            for m, _ in batch:
                try:
                    await m.nak(delay=5)
                except Exception:
                    pass
            await asyncio.sleep(2)
            continue
        for m, _ in batch:
            try:
                await m.ack()
                stream = m.metadata.stream if m.metadata else ""
                if stream in state.consumers:
                    state.consumers[stream]["acked"] += 1
            except Exception as e:
                log.warning("ack gagal (akan dikirim ulang)", extra={"f": {"err": str(e)}})
        state.batches += 1
        state.cache = (0.0, [])
        state.last_flush = {
            "at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "messages": len(batch),
            "rows": len(rows),
            "files": len(written),
        }


async def product_snapshot() -> None:
    """Snapshot katalog produk harian (catalog bab05 tidak menerbitkan event dan tidak di-CDC)."""
    from .mapping import _meta  # noqa: PLC0415

    url = env("CATALOG_URL", "http://apisix:9080/api/catalog") + "/products"
    await asyncio.sleep(20)
    while True:
        now = datetime.now(timezone.utc)
        dt = now.strftime("%Y-%m-%d")
        prefix = object_path("app", "product", dt, "x").rsplit("/", 1)[0]
        try:
            from pyarrow import fs as pafs  # noqa: PLC0415

            exists = writer.fs.get_file_info(pafs.FileSelector(prefix, allow_not_found=True))
            if not exists:
                body = await asyncio.to_thread(lambda: urllib.request.urlopen(url, timeout=10).read())
                items = json.loads(body)
                if isinstance(items, dict):
                    items = items.get("products") or items.get("items") or []
                rows = []
                for p in items:
                    rec = _meta("app", f"product:{p.get('id')}:{dt}", "product.snapshot", now, int(now.timestamp() * 1000), None)
                    rec.update({k: (None if v is None else str(v)) for k, v in p.items()})
                    rows.append(Row("app", "product", dt, rec))
                await asyncio.to_thread(writer.write, rows)
                log.info("snapshot produk ditulis", extra={"f": {"rows": len(rows), "dt": dt}})
        except Exception as e:
            log.warning("snapshot produk gagal, dicoba lagi", extra={"f": {"err": str(e)}})
            await asyncio.sleep(60)
            continue
        await asyncio.sleep(3600)


def stats() -> dict:
    ts, parts = state.cache
    if time.monotonic() - ts > 3:
        parts = writer.partitions()
        state.cache = (time.monotonic(), parts)
    return {
        "started": state.started.isoformat().replace("+00:00", "Z"),
        "flush": {"seconds": FLUSH_SECONDS, "max": FLUSH_MAX, "buffered": len(state.buffer)},
        "consumers": state.consumers,
        "batches": state.batches,
        "files_written": writer.files_written,
        "rows_written": writer.rows_written,
        "last_flush": state.last_flush,
        "last_error": state.last_error,
        "recent_files": writer.recent[:20],
        "partitions": parts,
        "totals": {
            "files": sum(p["files"] for p in parts),
            "rows": sum(p["rows"] for p in parts),
            "bytes": sum(p["bytes"] for p in parts),
        },
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a) -> None:  # akses log tidak perlu
        pass

    def _json(self, code: int, body: dict) -> None:
        b = json.dumps(body, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._json(200, {"ok": True, "consumers": {k: v["status"] for k, v in state.consumers.items()}})
        elif self.path == "/stats":
            try:
                self._json(200, stats())
            except Exception as e:
                self._json(500, {"error": str(e)})
        else:
            self._json(404, {"error": "tidak ada"})


async def main() -> None:
    srv = ThreadingHTTPServer(("0.0.0.0", 8080), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = env("NATS_URL", "nats://nats:4222")
    nc = None
    while nc is None:
        try:
            nc = await nats.connect(url, name="de-ingestor", max_reconnect_attempts=-1, reconnect_time_wait=1)
        except Exception as e:
            log.warning("menunggu nats", extra={"f": {"err": str(e)}})
            await asyncio.sleep(2)
    js = nc.jetstream()
    log.info("ingestor mulai", extra={"f": {"flush_seconds": FLUSH_SECONDS, "flush_max": FLUSH_MAX, "batch_example": new_batch_id(datetime.now(timezone.utc))}})
    await asyncio.gather(flusher(), product_snapshot(), *(consume(js, s) for s in STREAMS))


if __name__ == "__main__":
    asyncio.run(main())
