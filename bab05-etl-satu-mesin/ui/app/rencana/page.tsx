"use client";
import { useState } from "react";
import { Batang, byte, Galat, juta, useApi } from "@/components/lib";

function Rencana({ judul, baris, ket }: { judul: string; baris?: { teks: string; sorot: boolean }[]; ket?: string }) {
  return (
    <div className="kartu">
      <h3>{judul}</h3>
      {ket && <p className="kecil">{ket}</p>}
      <div className="rencana">{(baris ?? []).map((b, i) => <div key={i} className={b.sorot ? "s" : ""}>{b.teks || " "}</div>)}</div>
    </div>
  );
}

export default function HalRencana() {
  const info = useApi<any>("info");
  const [baris, setBaris] = useState(1000000);
  const r = useApi<any>(`rencana?baris=${baris}`);
  const d = r.data && !r.data.error ? r.data : null;
  const p = d?.parquet;
  return (
    <>
      <h1>Lazy vs eager, query plan, dan pushdown</h1>
      <p className="redup">Baris yang disorot = tempat filter dan pemilihan kolom didorong ke pembacaan Parquet
        (predicate dan projection pushdown). Bandingkan rencana Polars sebelum dan sesudah optimizer.</p>
      <div className="kartu baris">
        <label>Dataset <select value={baris} onChange={e => setBaris(Number(e.target.value))}>
          {(info.data?.dataset ?? []).filter((x: any) => x.ada).map((x: any) => <option key={x.baris} value={x.baris}>{juta(x.baris)}</option>)}
        </select></label>
        {r.muat && <span className="lencana warn">memuat…</span>}
      </div>
      <Galat teks={r.data?.error ?? r.galat} />
      {d && (<>
        <div className="kisi">
          <Rencana judul="Polars: explain(optimized=False)" baris={d.polars_tanpa_optimasi}
            ket="Rencana apa adanya: FILTER di atas SCAN yang membaca */13 kolom (seperti eager)." />
          <Rencana judul="Polars: explain() sesudah optimizer" baris={d.polars_optimasi}
            ket="SELECTION pindah ke dalam Parquet SCAN dan hanya 3/13 kolom yang dibaca." />
        </div>
        <div className="kisi">
          <Rencana judul="DuckDB: EXPLAIN" baris={d.duckdb} ket="Filters dan Projections berada di operator READ_PARQUET." />
          <Rencana judul="DataFusion: EXPLAIN" baris={d.datafusion} ket="pruning_predicate memakai statistik tanggal_max per row group." />
        </div>
        {p && (
          <div className="kisi">
            <div className="kartu">
              <h2>Projection: byte per kolom di Parquet</h2>
              <p>Query memakai {p.kolom.filter((k: any) => k.dipakai).length} dari {p.kolom.length} kolom:
                {" "}<b>{byte(p.byte_dipakai)}</b> dari {byte(p.byte_total)} ({Math.round((100 * p.byte_dipakai) / p.byte_total)}%).</p>
              <Batang satuan=" B" data={[...p.kolom].sort((a: any, b: any) => b.byte - a.byte).map((k: any) => ({
                label: k.nama, nilai: k.byte, warna: k.dipakai ? "var(--utama)" : "var(--garis)", ket: k.dipakai ? "dibaca" : undefined }))} />
            </div>
            <div className="kartu">
              <h2>Predicate: row group yang dilewati</h2>
              <p>Filter <code>{p.filter}</code>: <b>{p.rg_dibaca}</b> dari {p.rg_total} row group perlu dibaca.
                Sisanya dilewati hanya dengan membaca statistik maks <code>tanggal</code> di footer.</p>
              <div className="grid-rg">{p.row_group.map((g: any) => (
                <span key={g.berkas + g.rg} className={g.dibaca ? "" : "lewat"} title={`${g.berkas} rg${g.rg}: maks ${g.maks_tanggal}, ${g.baris} baris`} />))}</div>
              <p className="kecil">Kotak berwarna = dibaca, abu = dilewati. Satu kotak = satu row group (maks 128.000 baris).</p>
            </div>
          </div>)}
        <details className="kartu"><summary>SQL yang dijalankan DuckDB</summary><pre>{d.sql}</pre></details>
      </>)}
    </>
  );
}
