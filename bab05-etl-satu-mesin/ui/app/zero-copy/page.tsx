"use client";
import { useState } from "react";
import { Batang, Galat, juta, rupiah, Status, useApi, useJob } from "@/components/lib";

export default function ZeroCopy() {
  const info = useApi<any>("info");
  const [baris, setBaris] = useState(1000000);
  const j = useJob();
  const h = j.job?.jenis === "zerocopy" && j.job.status === "selesai" ? j.job.hasil : null;
  return (
    <>
      <h1>Zero-copy dengan Apache Arrow</h1>
      <p className="redup">Satu tabel Arrow dibaca dari Parquet, lalu diteruskan ke Polars, DuckDB, dan pandas. Bila alamat buffer
        kolom <code>nominal</code> sama dengan buffer Arrow asal, data tidak disalin.</p>
      <div className="kartu baris">
        <label>Dataset <select value={baris} onChange={e => setBaris(Number(e.target.value))}>
          {(info.data?.dataset ?? []).filter((x: any) => x.ada).map((x: any) => <option key={x.baris} value={x.baris}>{juta(x.baris)}</option>)}
        </select></label>
        <button className="utama" disabled={j.berjalan} onClick={() => j.mulai(`zerocopy?baris=${baris}`)}>Jalankan eksperimen</button>
        {j.berjalan && <span className="lencana warn">berjalan…</span>}
      </div>
      <Galat teks={j.galat || h?.error || (j.job?.status === "gagal" ? j.job.error : "")} />
      {h?.langkah && (<>
        <div className="kartu">
          <h2>{rupiah(h.baris)} baris, 4 kolom</h2>
          <div className="tabel-wadah"><table>
            <thead><tr><th>langkah</th><th className="angka">ms</th><th className="angka">tambah RSS (MB)</th><th>buffer nominal</th><th>keterangan</th></tr></thead>
            <tbody>{h.langkah.map((l: any) => (
              <tr key={l.langkah}><td style={{ whiteSpace: "normal" }}>{l.langkah}</td><td className="angka">{rupiah(l.ms)}</td>
                <td className="angka">{rupiah(l.tambah_mb)}</td>
                <td>{l.alamat_sama === null ? "-" : l.alamat_sama ? <Status s="lulus" /> : <span className="lencana warn">disalin</span>}
                  {l.alamat_sama === true ? " sama" : ""}</td>
                <td style={{ whiteSpace: "normal" }} className="kecil">{l.ket}</td></tr>))}
            </tbody></table></div>
        </div>
        <div className="kartu">
          <h2>Tambahan memori per langkah</h2>
          <Batang satuan=" MB" data={h.langkah.map((l: any) => ({ label: l.langkah.split(":")[0].slice(0, 22), nilai: Math.max(0, l.tambah_mb),
            warna: l.alamat_sama === false ? "var(--kuning)" : "var(--utama)" }))} />
          <p className="kecil">Umumnya tanpa salin: kolom angka dipakai bersama, tetapi beberapa tipe (string Arrow ke string view Polars,
            Arrow ke NumPy) tetap dikonversi. Ukur bila datanya besar.</p>
        </div>
      </>)}
    </>
  );
}
