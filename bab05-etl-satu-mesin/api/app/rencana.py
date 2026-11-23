"""Query plan, lazy vs eager, dan apa yang benar-benar dibaca dari Parquet.

- Polars: explain(optimized=False) vs explain() memperlihatkan predicate dan projection pushdown.
- DuckDB: EXPLAIN memperlihatkan filter dan kolom yang didorong ke READ_PARQUET.
- DataFusion: EXPLAIN untuk query yang sama.
- Metadata Parquet: byte kolom yang dipakai vs seluruh kolom, dan row group yang dilewati karena
  statistik min/maks kolom `tanggal` (tanpa membaca datanya).
"""
from __future__ import annotations

import datetime as dt
import glob
import re

import pyarrow.parquet as pq

from .kerja import MULAI, SQL, _lazy

KOLOM_DIPAKAI = ["tanggal", "nasabah_id", "nominal"]
SOROT = re.compile(r"(SELECTION|PROJECT|FILTER|Filters|Projections|pushdown|predicate|"
                   r"projection=|full_filters|pruning_predicate|READ_PARQUET|TABLE_SCAN|Parquet SCAN|"
                   r"DataSourceExec|ParquetExec|\d+/\d+ COLUMNS)", re.I)


def _baris(teks: str) -> list[dict]:
    return [{"teks": b, "sorot": bool(SOROT.search(b))} for b in teks.splitlines()]


def rencana(folder: str, nas: str) -> dict:
    import duckdb
    from datafusion import SessionContext

    q = _lazy(folder, nas)
    con = duckdb.connect()
    sql = SQL.format(trx=f"read_parquet('{folder}/*.parquet')", nas=f"read_parquet('{nas}')")
    dd = "\n".join(r[1] for r in con.execute("EXPLAIN " + sql).fetchall())
    ctx = SessionContext()
    ctx.register_parquet("trx", folder + "/")
    ctx.register_parquet("nas", nas)
    df_plan = ctx.sql("EXPLAIN " + SQL.format(trx="trx", nas="nas")).to_arrow_table()
    df_teks = "\n\n".join(f"-- {t}\n{p}" for t, p in zip(df_plan.column(0).to_pylist(), df_plan.column(1).to_pylist()))
    return {
        "polars_tanpa_optimasi": _baris(q.explain(optimized=False)),
        "polars_optimasi": _baris(q.explain()),
        "duckdb": _baris(dd),
        "datafusion": _baris(df_teks),
        "sql": sql.strip(),
        "parquet": metadata(folder),
    }


def metadata(folder: str) -> dict:
    """Ukuran per kolom dan statistik row group dari footer Parquet (tidak membaca data)."""
    berkas = sorted(glob.glob(f"{folder}/*.parquet"))
    per_kolom: dict[str, int] = {}
    grup = []
    for b in berkas:
        md = pq.ParquetFile(b).metadata
        nama = md.schema.to_arrow_schema().names
        it = nama.index("tanggal")
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            for c in range(rg.num_columns):
                cc = rg.column(c)
                per_kolom[nama[c]] = per_kolom.get(nama[c], 0) + cc.total_compressed_size
            st = rg.column(it).statistics
            maks = st.max if st is not None and st.has_min_max else None
            if isinstance(maks, dt.datetime):
                maks = maks.date()
            grup.append({"berkas": b.rsplit("/", 1)[1], "rg": g, "baris": rg.num_rows,
                         "maks_tanggal": str(maks), "dibaca": maks is None or maks >= MULAI})
    total = sum(per_kolom.values())
    dipakai = sum(v for k, v in per_kolom.items() if k in KOLOM_DIPAKAI)
    return {"kolom": [{"nama": k, "byte": v, "dipakai": k in KOLOM_DIPAKAI} for k, v in per_kolom.items()],
            "byte_total": total, "byte_dipakai": dipakai, "row_group": grup,
            "rg_dibaca": sum(1 for g in grup if g["dibaca"]), "rg_total": len(grup),
            "filter": f"tanggal >= {MULAI}"}
