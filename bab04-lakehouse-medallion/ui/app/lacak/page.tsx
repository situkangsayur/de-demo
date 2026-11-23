"use client";
import { useState } from "react";
import { Galat, useApi } from "@/components/lib";

function Rekaman({ r, sorot }: { r: Record<string, string | null>; sorot: string[] }) {
  return (
    <table><tbody>
      {Object.entries(r).map(([k, v]) => (
        <tr key={k} style={sorot.includes(k) ? { background: "var(--utama-muda)" } : undefined}>
          <td className="redup">{k}</td><td><code>{v ?? "∅"}</code></td></tr>))}
    </tbody></table>
  );
}

export default function Lacak() {
  const [id, setId] = useState("");
  const [cari, setCari] = useState("");
  const { data, galat, muat } = useApi<any>(`medallion/lacak?trx_id=${encodeURIComponent(cari)}`);
  return (
    <>
      <h1>Satu transaksi melewati tiga lapisan</h1>
      <p className="redup">Bronze menyimpan apa adanya (dua baris: event aplikasi dan CDC, semua teks). Silver: satu baris,
        bertipe, kanal baku, sumber dicatat. Gold: hanya angka agregat tempat transaksi ini dijumlahkan.</p>
      <div className="kartu baris">
        <input placeholder="trx_id (kosong = pilih otomatis)" value={id} onChange={e => setId(e.target.value)} style={{ minWidth: 280 }} />
        <button className="utama" onClick={() => setCari(id)}>Lacak</button>
        {muat && <span className="lencana warn">memuat…</span>}
        {data?.trx_id && <span>trx_id: <code>{data.trx_id}</code></span>}
      </div>
      <Galat teks={galat} />
      {data && !data.error && (
        <div className="tiga">
          <div className="kartu"><h2>Bronze <span className="redup">({data.bronze.length} baris)</span></h2>
            {data.bronze.map((r: any, i: number) => (
              <div key={i}><h3>sumber: {r._source}</h3>
                <Rekaman r={r} sorot={["_source", "_event_id", "kanal", "nominal", "occurred_at"]} /></div>))}
          </div>
          <div className="kartu"><h2>Silver <span className="redup">silver.transaksi</span></h2>
            {data.silver.map((r: any, i: number) => <Rekaman key={i} r={r} sorot={["_source", "kanal", "kanal_asli", "nominal", "waktu", "nasabah_id"]} />)}
            {data.silver.length === 0 && <p className="redup">belum ada di silver</p>}
          </div>
          <div className="kartu"><h2>Gold <span className="redup">gold.fakta_trx_harian</span></h2>
            {data.gold.map((r: any, i: number) => <Rekaman key={i} r={r} sorot={["jml_trx", "total_nominal"]} />)}
            <p className="redup">Baris gold ini menjumlahkan semua transaksi pada tanggal WIB, cabang, dan kanal yang sama.
              Rekening, NIK, dan nama tidak ada di gold.</p>
          </div>
        </div>)}
    </>
  );
}
