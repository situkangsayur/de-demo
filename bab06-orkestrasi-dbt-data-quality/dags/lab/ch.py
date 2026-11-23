"""Klien ClickHouse lewat HTTP interface (urllib). Kredensial dari environment container."""
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

URL = os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123")
USER = os.environ.get("CLICKHOUSE_USER", "lab")
PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")


def kueri(sql: str, data: bytes | None = None, timeout: int = 120) -> str:
    """Kirim satu pernyataan SQL; `data` dipakai sebagai body INSERT ... FORMAT."""
    if data is None:
        url, body = URL + "/", sql.encode()
    else:
        url, body = URL + "/?" + urllib.parse.urlencode({"query": sql}), data
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("X-ClickHouse-User", USER)
    req.add_header("X-ClickHouse-Key", PASSWORD)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode()
    except urllib.error.HTTPError as e:  # pesan ClickHouse lebih berguna daripada kode HTTP saja
        raise RuntimeError(f"ClickHouse {e.code}: {e.read().decode()[:500]}") from None


def baris(sql: str) -> list[dict]:
    teks = kueri(sql + " FORMAT JSONEachRow")
    return [json.loads(x) for x in teks.splitlines() if x.strip()]


def sisip_json(tabel: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows).encode()
    kueri(f"INSERT INTO {tabel} SETTINGS input_format_skip_unknown_fields=1 FORMAT JSONEachRow", body)
    return len(rows)


def log(msg: str, **kw) -> None:
    """Log JSON satu baris (time, level, msg, service, ...) agar terbaca Loki/Grafana."""
    import datetime

    rec = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(), "level": kw.pop("level", "info"),
           "msg": msg, "service": "airflow-trx_harian", **kw}
    print(json.dumps(rec, ensure_ascii=False), flush=True)
