"use client";
import { useState } from "react";
import { api, Galat, Tabel } from "@/components/lib";

const CONTOH = [
  "SELECT tgl, SUM(jml_trx) AS jml, SUM(total_nominal) AS total\nFROM gold.fakta_trx_harian\nGROUP BY tgl ORDER BY tgl",
  "SELECT cabang_kode, kanal, SUM(total_nominal) AS total\nFROM gold.fakta_trx_harian\nGROUP BY ALL ORDER BY total DESC LIMIT 10",
  "SELECT _source, COUNT(*) AS n, SUM(CASE WHEN _terlambat THEN 1 ELSE 0 END) AS terlambat\nFROM silver.transaksi GROUP BY 1",
  "SELECT kode_akad, jml_akad, total_pokok FROM gold.pembiayaan_per_akad",
  "SELECT akad_id, kode_akad_asli, alasan FROM silver.akad_karantina",
];

export default function Sql() {
  const [sql, setSql] = useState(CONTOH[0]);
  const [hasil, setHasil] = useState<any>(null);
  const [galat, setGalat] = useState("");
  const [sibuk, setSibuk] = useState(false);
  const jalan = async () => {
    setSibuk(true); setGalat("");
    try { setHasil(await api("iceberg/sql", { sql })); } catch (e) { setGalat(String(e)); setHasil(null); }
    setSibuk(false);
  };
  return (
    <>
      <h1>DuckDB sebagai query engine di atas lakehouse</h1>
      <p className="redup">Nama tabel <code>lapisan.entitas</code> diganti dengan <code>iceberg_scan(&apos;metadata.json terkini&apos;)</code>.
        DuckDB membaca metadata dan Parquet langsung dari RustFS; tidak ada server warehouse. Hanya SELECT yang diizinkan.</p>
      <div className="kartu">
        <div className="baris">{CONTOH.map((c, i) => <button key={i} onClick={() => setSql(c)}>contoh {i + 1}</button>)}</div>
        <textarea rows={6} value={sql} onChange={e => setSql(e.target.value)} style={{ marginTop: 8 }} />
        <div className="baris"><button className="utama" onClick={jalan} disabled={sibuk}>Jalankan</button>
          {hasil && <span className="redup">{hasil.baris.length} baris · {hasil.ms} ms</span>}</div>
        <Galat teks={galat} />
      </div>
      {hasil && (<div className="kartu">
        <Tabel kolom={hasil.kolom} baris={hasil.baris} />
        <details><summary>SQL yang dijalankan DuckDB</summary><pre>{hasil.sql_dijalankan}</pre></details>
      </div>)}
    </>
  );
}
