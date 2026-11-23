"use client";
import { useState } from "react";
import { Batang, byte, Galat, juta, rupiah, useApi, useJob, WARNA } from "@/components/lib";

const LABEL: Record<string, string> = { pandas: "pandas (semua kolom)", polars_eager: "Polars eager",
  polars_streaming: "Polars streaming", duckdb: "DuckDB + memory_limit" };

export default function Memori() {
  const info = useApi<any>("info");
  const [baris, setBaris] = useState(10000000);
  const [anggaran, setAnggaran] = useState(512);
  const j = useJob();
  const h = j.job?.jenis === "memori" && j.job.status === "selesai" ? j.job.hasil : null;
  const ada = (info.data?.dataset ?? []).filter((x: any) => x.ada);
  return (
    <>
      <h1>Batas memori, streaming, dan out-of-core</h1>
      <p className="redup">Query berat: GROUP BY rekening_id (±300 ribu grup) dengan COUNT, SUM, MAX, COUNT DISTINCT, lalu ORDER BY.
        DuckDB diberi <code>memory_limit</code> sebesar anggaran dan boleh menumpahkan (spill) ke disk; Polars streaming
        memproses per batch; pandas dan Polars eager memuat semua kolom ke memori.</p>
      <div className="kartu baris">
        <label>Dataset <select value={baris} onChange={e => setBaris(Number(e.target.value))}>
          {ada.map((x: any) => <option key={x.baris} value={x.baris}>{juta(x.baris)}</option>)}
        </select></label>
        <label>Anggaran memori <select value={anggaran} onChange={e => setAnggaran(Number(e.target.value))}>
          {[256, 512, 1024].map(n => <option key={n} value={n}>{n} MB</option>)}</select></label>
        <button className="utama" disabled={j.berjalan || !ada.some((x: any) => x.baris === baris)}
          onClick={() => j.mulai(`memori?baris=${baris}&anggaran_mb=${anggaran}`)}>Jalankan</button>
        {j.berjalan && <span className="lencana warn">{j.job.progres?.slice(-1)[0]}…</span>}
      </div>
      {!ada.some((x: any) => x.baris === baris) && <p className="redup">Dataset {juta(baris)} belum ada; bangkitkan di halaman Benchmark.</p>}
      <Galat teks={j.galat || (j.job?.status === "gagal" ? j.job.error : "")} />
      {h && (
        <div className="kisi">
          <div className="kartu">
            <h2>Memori puncak (MB) · {juta(h.baris)} baris</h2>
            <Batang satuan=" MB" garis={[{ nilai: h.anggaran_mb, label: `anggaran ${h.anggaran_mb}` },
              ...(info.data?.batas_memori_mb ? [{ nilai: info.data.batas_memori_mb, label: "mem_limit" }] : [])]}
              data={h.hasil.map((r: any) => ({ label: LABEL[r.engine], nilai: r.ok ? r.puncak_mb : null, warna: WARNA[r.engine],
                ket: r.ok ? undefined : r.error }))} />
          </div>
          <div className="kartu">
            <h2>Waktu (ms) dan spill</h2>
            <Batang satuan=" ms" data={h.hasil.map((r: any) => ({ label: LABEL[r.engine], nilai: r.ok ? r.ms : null, warna: WARNA[r.engine],
              ket: r.ok ? (r.spill_puncak_byte ? `spill ${byte(r.spill_puncak_byte)}` : undefined) : r.error }))} />
            <p className="kecil">{h.hasil.filter((r: any) => r.spill_puncak_byte).map((r: any) =>
              `DuckDB menumpahkan ${byte(r.spill_puncak_byte)} ke disk dan tetap selesai dengan RSS ${rupiah(r.puncak_mb)} MB.`).join(" ")}
              {" "}Out-of-core menukar memori dengan waktu dan disk.</p>
          </div>
        </div>)}
    </>
  );
}
