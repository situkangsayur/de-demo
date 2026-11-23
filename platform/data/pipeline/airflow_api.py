"""Klien kecil Airflow REST API v2 (JWT dari /auth/token), dipakai muat-awal dan uji-data.sh."""
from __future__ import annotations

import json
import os
import urllib.request

URL = os.environ.get("AIRFLOW_URL", "http://airflow-apiserver:8080")


def _token() -> str:
    body = json.dumps({"username": os.environ["AIRFLOW_ADMIN_USER"],
                       "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]}).encode()
    req = urllib.request.Request(f"{URL}/auth/token", data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)["access_token"]


def panggil(metode: str, path: str, isi: dict | None = None) -> dict:
    data = json.dumps(isi).encode() if isi is not None else None
    req = urllib.request.Request(f"{URL}/api/v2{path}", data=data, method=metode,
                                 headers={"Content-Type": "application/json", "Authorization": f"Bearer {_token()}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        teks = r.read()
        return json.loads(teks) if teks else {}
