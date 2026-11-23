"""Pemetaan pesan NATS -> baris bronze (fungsi murni, diuji di test_mapping.py).

Satu pesan menjadi satu atau lebih baris. Setiap baris membawa kolom metadata W1 (kontrak):
_ingested_at (diisi saat tulis), _source, _batch_id (diisi saat tulis), _event_id, _event_type,
_occurred_at, _ts_ms, _op, lalu kolom data entitas yang sudah diratakan (semua string; casting di silver).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

META_COLS = ["_ingested_at", "_source", "_batch_id", "_event_id", "_event_type", "_occurred_at", "_ts_ms", "_op"]

# Subject bank.<entitas>.<aksi>: nama entitas di subject -> nama entitas bronze.
BANK_ENTITY = {"akad": "akad_pembiayaan"}
# Subject shop.<entitas>.<aksi> (bab05 apa adanya) -> entitas bronze.
SHOP_ENTITY = {"order": "orders", "payment": "payment", "inventory": "inventory"}

# Kolom waktu bisnis per tabel CDC untuk menentukan partisi dt (kontrak: pakai waktu bisnis bila ada,
# selain itu ts_ms). Urutan = prioritas. Untuk op c/r kolom "waktu buat" juga dipakai.
CDC_BIZ_TIME = {
    "transaksi": ["occurred_at"],
    "nasabah": ["updated_at", "created_at"],
    "rekening": ["updated_at"],  # perubahan saldo: tidak ada kolom waktu -> ts_ms
    "akad_pembiayaan": ["updated_at"],
    "orders": ["updated_at", "created_at"],
    "order_history": ["at"],
}
CDC_CREATE_TIME = {
    "rekening": ["dibuka_at"],
    "akad_pembiayaan": ["created_at", "diajukan_at"],
}
DEFAULT_BIZ_TIME = ["occurred_at", "updated_at", "created_at"]


@dataclass
class Row:
    source: str  # app | cdc
    entity: str
    dt: str  # YYYY-MM-DD (UTC)
    record: dict[str, Any] = field(default_factory=dict)


def flatten(obj: Any, prefix: str = "", out: dict[str, Any] | None = None) -> dict[str, Any]:
    """Meratakan objek bersarang: {"a": {"b": 1}} -> {"a_b": "1"}. List disimpan sebagai JSON string.
    Semua nilai menjadi string (atau None) agar schema bronze stabil antar batch."""
    if out is None:
        out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}_{k}" if prefix else str(k)
            if isinstance(v, dict):
                flatten(v, key, out)
            else:
                out[key] = to_str(v)
    elif prefix:
        out[prefix] = to_str(obj)
    return out


def to_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (list, tuple)):
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def parse_time(v: Any) -> datetime | None:
    """Waktu dari string RFC3339/ISO (mis. ZonedTimestamp Debezium) atau epoch (ms/us)."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        if x > 1e17:  # nanodetik
            x /= 1e9
        elif x > 1e14:  # mikrodetik
            x /= 1e6
        elif x > 1e11:  # milidetik
            x /= 1e3
        else:
            return None  # mis. tanggal sebagai jumlah hari: bukan waktu kejadian
        return datetime.fromtimestamp(x, tz=timezone.utc)
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            return parse_time(int(s))
        try:
            t = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return t.astimezone(timezone.utc)
    return None


def iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _meta(source: str, event_id: str, event_type: str, occurred: datetime, ts_ms: int, op: str | None) -> dict:
    return {
        "_source": source,
        "_event_id": event_id,
        "_event_type": event_type,
        "_occurred_at": iso(occurred),
        "_ts_ms": int(ts_ms),
        "_op": op,
    }


def _ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def map_app(stream: str, subject: str, payload: Any, msg_id: str | None, msg_time: datetime) -> list[Row]:
    """Event aplikasi dari stream BANK (envelope kontrak) atau SHOP (payload bab05 apa adanya)."""
    parts = subject.split(".")
    if len(parts) < 3:
        return []
    ent_raw, aksi = parts[1], ".".join(parts[2:])
    is_env = isinstance(payload, dict) and "data" in payload and ("event_id" in payload or "occurred_at" in payload)
    if stream == "BANK":
        entity = BANK_ENTITY.get(ent_raw, ent_raw)
    else:
        entity = SHOP_ENTITY.get(ent_raw, ent_raw)
    if is_env:
        data = payload.get("data")
        event_id = str(payload.get("event_id") or msg_id or "")
        event_type = str(payload.get("event_type") or subject)
        occurred = parse_time(payload.get("occurred_at")) or msg_time
    else:
        # SHOP bab05: payload = OrderEvent tanpa envelope. event_id = Nats-Msg-Id (id outbox),
        # waktu kejadian = waktu publish (stream timestamp) karena payload tidak membawa waktu.
        data = payload
        event_id = str(msg_id or "")
        event_type = subject
        occurred = parse_time(payload.get("occurred_at")) if isinstance(payload, dict) else None
        occurred = occurred or msg_time
    if not event_id:
        event_id = "sha1:" + hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
    if not isinstance(data, dict):
        data = {"value": data}
    dt = occurred.astimezone(timezone.utc).strftime("%Y-%m-%d")
    rows: list[Row] = []
    rec = _meta("app", event_id, event_type, occurred, _ms(msg_time), None)
    rec["aksi"] = aksi
    rec.update(flatten(data))
    rows.append(Row("app", entity, dt, rec))
    # Pesanan dibuat: pecah items menjadi entitas order_items (satu baris per produk).
    items = data.get("items") if isinstance(data, dict) else None
    if entity == "orders" and aksi == "created" and isinstance(items, list):
        for i, it in enumerate(items):
            if not isinstance(it, dict):
                continue
            r = _meta("app", f"{event_id}:{i}", event_type, occurred, _ms(msg_time), None)
            r["order_id"] = to_str(data.get("order_id") or data.get("id"))
            r["line_no"] = str(i + 1)
            r.update(flatten(it))
            rows.append(Row("app", "order_items", dt, r))
    return rows


def _pk(row: dict) -> str:
    for k in ("id", "order_id", "cif", "no_rekening", "kode"):
        if row.get(k) not in (None, ""):
            return str(row[k])
    return "h" + hashlib.sha1(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()[:16]


def cdc_business_time(table: str, op: str, row: dict) -> datetime | None:
    cols = list(CDC_BIZ_TIME.get(table, DEFAULT_BIZ_TIME))
    if op in ("c", "r"):
        cols += CDC_CREATE_TIME.get(table, [])
    if table == "akad_pembiayaan" and row.get("status") in ("cair", "lunas"):
        cols.insert(0, "cair_at")
    for c in cols:
        t = parse_time(row.get(c))
        if t is not None:
            return t
    return None


def map_cdc(subject: str, payload: Any, msg_time: datetime) -> list[Row]:
    """Envelope Debezium (JSON tanpa schema) dari subject cdc.<database>.<tabel>."""
    if not isinstance(payload, dict):
        return []
    if "payload" in payload and "schema" in payload:  # bila schemas.enable=true
        payload = payload["payload"] or {}
    parts = subject.split(".")
    if len(parts) < 3:
        return []
    table = parts[-1]
    op = payload.get("op") or ""
    if op not in ("c", "u", "d", "r", "t", "m"):
        return []
    if op in ("t", "m"):  # truncate / message: tidak ada baris data
        return []
    row = payload.get("after") if op != "d" else payload.get("before")
    if not isinstance(row, dict):
        row = payload.get("before") if isinstance(payload.get("before"), dict) else {}
    src = payload.get("source") or {}
    ts_ms = payload.get("ts_ms") or src.get("ts_ms") or _ms(msg_time)
    ts = parse_time(int(ts_ms)) or msg_time
    biz = cdc_business_time(table, op, row) or ts
    lsn = src.get("lsn")
    if lsn is None:
        lsn = f"ts{ts_ms}"
    event_id = f"{table}:{lsn}:{_pk(row)}"
    rec = _meta("cdc", event_id, f"{table}.{op}", biz, int(ts_ms), op)
    rec.update(flatten(row))
    return [Row("cdc", table, biz.astimezone(timezone.utc).strftime("%Y-%m-%d"), rec)]


def map_message(stream: str, subject: str, data: bytes, msg_id: str | None, msg_time: datetime) -> list[Row]:
    try:
        payload = json.loads(data) if data else None
    except (ValueError, UnicodeDecodeError):
        payload = {"raw": data.decode("utf-8", "replace")}
    if stream == "CDC":
        return map_cdc(subject, payload, msg_time)
    return map_app(stream, subject, payload, msg_id, msg_time)


def dedup(rows: list[Row]) -> list[Row]:
    """Idempotent dalam satu batch: event_id yang sama (per sumber+entitas) hanya ditulis sekali."""
    seen: set[tuple[str, str, str]] = set()
    out = []
    for r in rows:
        k = (r.source, r.entity, r.record["_event_id"])
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out
