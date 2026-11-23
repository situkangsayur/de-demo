"use client";
import { useState } from "react";
import { Tabel, api, rupiah, useApi } from "@/components/lib";

const TABEL = ["gold.fakta_transaksi", "gold.dim_nasabah", "gold.fakta_pembiayaan", "silver.transaksi", "silver.nasabah"];

export default function TimeTravel() {
  const [tabel, setTabel] = useState(TABEL[0]);
  const { data, galat } = useApi<any>(`time-travel?tabel=${tabel}`, 15000);
  const [waktu, setWaktu] = useState("");
  const [asof, setAsof] = useState<any>(null);
  const snap: any[] = data?.snapshot || [];
  return (
    <>
      <h1>Time travel Iceberg</h1>
      <p className="redup">Setiap penulisan membuat snapshot baru; snapshot lama tetap bisa dibaca. Isi tiap snapshot dihitung dengan DuckDB
        <code>iceberg_scan(metadata, snapshot_from_id =&gt; …)</code>. Pilih waktu untuk query <b>AS OF</b>.</p>
      {galat && <p className="galat">{galat}</p>}
      <div className="kartu baris">
        <select value={tabel} onChange={e => setTabel(e.target.value)}>{TABEL.map(t => <option key={t}>{t}</option>)}</select>
        <span className="redup">snapshot sekarang: <code>{data?.snapshot_sekarang ?? "-"}</code></span>
      </div>
      <div className="kartu">
        <h2>Riwayat snapshot (15 terbaru)</h2>
        <Tabel kolom={["snapshot_id", "waktu (UTC)", "operasi", "+baris", "-baris", "total baris", ...(data?.kolom_isi || []).map((k: string) => `isi: ${k}`)]}
          baris={snap.map(s => [String(s.snapshot_id), s.waktu.slice(0, 19), s.operasi, Number(s.ringkasan["added-records"] ?? 0),
            Number(s.ringkasan["deleted-records"] ?? 0), Number(s.ringkasan["total-records"] ?? 0), ...s.isi])} />
        <p className="redup">Overwrite partisi = hapus + tambah dalam satu transaksi. Jumlah baris sama setelah run ulang membuktikan idempotency;
          gold tidak mendapat snapshot baru saat data quality gagal.</p>
      </div>
      <div className="kartu">
        <h2>Query AS OF</h2>
        <div className="baris">
          <input type="datetime-local" step="1" value={waktu} onChange={e => setWaktu(e.target.value)} />
          <button className="utama" disabled={!waktu} onClick={async () => {
            setAsof(await api(`time-travel/asof?tabel=gold.fakta_transaksi&waktu=${encodeURIComponent(new Date(waktu).toISOString())}`).catch(e => ({ pesan: String(e) })));
          }}>Jalankan</button>
          {snap.slice(0, 4).map(s => (
            <button key={s.snapshot_id} onClick={async () => setAsof(await api(`time-travel/asof?tabel=gold.fakta_transaksi&waktu=${encodeURIComponent(s.waktu)}`))}>
              AS OF {s.waktu.slice(11, 19)}</button>))}
        </div>
        {asof && <>
          {asof.pesan && <p className="redup">{asof.pesan}</p>}
          {asof.sql && <pre>{asof.sql}</pre>}
          {asof.snapshot_id && <p className="redup">snapshot {String(asof.snapshot_id)} ({asof.snapshot_waktu}) · 10 tgl terbaru</p>}
          {asof.kolom && <Tabel kolom={asof.kolom} baris={asof.baris} />}
        </>}
      </div>
    </>
  );
}
