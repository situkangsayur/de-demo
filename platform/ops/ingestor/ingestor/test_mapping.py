"""Uji pemetaan pesan -> baris bronze. Jalankan: python -m unittest ingestor.test_mapping"""

import json
import unittest
from datetime import datetime, timezone

from .mapping import dedup, flatten, map_message

T = datetime(2026, 11, 23, 3, 0, 0, tzinfo=timezone.utc)


class TestMapping(unittest.TestCase):
    def test_bank_envelope_late_goes_to_original_partition(self):
        env = {
            "event_id": "e-1",
            "event_type": "bank.transaksi.dibuat",
            "occurred_at": "2026-11-20T23:30:00+07:00",  # = 2026-11-20 16:30 UTC
            "data": {"id": 7, "kanal": None, "nominal": "150000", "meta": {"x": 1}},
        }
        rows = map_message("BANK", "bank.transaksi.dibuat", json.dumps(env).encode(), "e-1", T)
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual((r.source, r.entity, r.dt), ("app", "transaksi", "2026-11-20"))
        self.assertEqual(r.record["_event_id"], "e-1")
        self.assertEqual(r.record["_occurred_at"], "2026-11-20T16:30:00Z")
        self.assertIsNone(r.record["kanal"])
        self.assertEqual(r.record["id"], "7")
        self.assertEqual(r.record["meta_x"], "1")
        self.assertIsNone(r.record["_op"])

    def test_bank_akad_entity_alias(self):
        env = {"event_id": "e-2", "event_type": "bank.akad.dicairkan", "occurred_at": "2026-11-23T01:00:00Z", "data": {"id": 1}}
        rows = map_message("BANK", "bank.akad.dicairkan", json.dumps(env).encode(), None, T)
        self.assertEqual(rows[0].entity, "akad_pembiayaan")

    def test_shop_raw_payload_and_items(self):
        p = {"order_id": "ord-1", "customer": "C1", "items": [{"product_id": "P001", "qty": 2, "price": 85000}], "total": 170000}
        rows = map_message("SHOP", "shop.order.created", json.dumps(p).encode(), "msg-9", T)
        self.assertEqual([r.entity for r in rows], ["orders", "order_items"])
        self.assertEqual(rows[0].record["_event_id"], "msg-9")
        self.assertEqual(rows[0].dt, "2026-11-23")
        self.assertEqual(json.loads(rows[0].record["items"])[0]["product_id"], "P001")
        self.assertEqual(rows[1].record["_event_id"], "msg-9:0")
        self.assertEqual(rows[1].record["qty"], "2")

    def test_cdc_update_uses_business_time_and_event_id(self):
        env = {
            "before": {"id": 5, "kota": "Bandung"},
            "after": {"id": 5, "kota": "Bogor", "updated_at": "2026-10-01T02:00:00Z"},
            "source": {"lsn": 123456, "db": "bank", "table": "nasabah"},
            "op": "u",
            "ts_ms": 1795000000000,
        }
        rows = map_message("CDC", "cdc.bank.nasabah", json.dumps(env).encode(), None, T)
        r = rows[0]
        self.assertEqual((r.source, r.entity, r.dt), ("cdc", "nasabah", "2026-10-01"))
        self.assertEqual(r.record["_event_id"], "nasabah:123456:5")
        self.assertEqual(r.record["_op"], "u")
        self.assertEqual(r.record["kota"], "Bogor")
        self.assertEqual(r.record["_ts_ms"], 1795000000000)

    def test_cdc_delete_uses_before_and_ts_ms(self):
        env = {"before": {"id": 9}, "after": None, "source": {"lsn": 1}, "op": "d", "ts_ms": 1795000000000}
        rows = map_message("CDC", "cdc.bank.cabang", json.dumps(env).encode(), None, T)
        self.assertEqual(rows[0].record["id"], "9")
        self.assertEqual(rows[0].dt, datetime.fromtimestamp(1795000000, tz=timezone.utc).strftime("%Y-%m-%d"))

    def test_dedup_in_batch(self):
        env = {"event_id": "same", "occurred_at": "2026-11-23T01:00:00Z", "data": {"id": 1}}
        b = json.dumps(env).encode()
        rows = map_message("BANK", "bank.nasabah.dibuat", b, None, T) + map_message("BANK", "bank.nasabah.dibuat", b, None, T)
        self.assertEqual(len(dedup(rows)), 1)

    def test_flatten_types(self):
        self.assertEqual(flatten({"a": True, "b": 1.0, "c": [1, 2], "d": {"e": None}}), {"a": "true", "b": "1", "c": "[1,2]", "d_e": None})


if __name__ == "__main__":
    unittest.main()
