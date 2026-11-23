"use client";
import { useState } from "react";
import { Garis, Tabel, byte, rupiah, useApi } from "@/components/lib";

const WARNA: Record<string, string> = { semua_hot: "#b3402f", kebijakan: "#0f6e63", paling_hemat: "#b8862b" };

export default function Tiering() {
  const [geser, setGeser] = useState(0);
  const { data, galat } = useApi<any>(`tiering?geser_hari=${geser}`);
  const [laju, setLaju] = useState("1x");
  const cost: any[][] = data?.cost?.baris || [];
  const sk = ["semua_hot", "kebijakan", "paling_hemat"];
  const bulan = [...new Set(cost.map(r => r[2]))].sort((a, b) => a - b);
  const seri = sk.map(s => ({ nama: s, warna: WARNA[s], titik: bulan.map(b => Number(cost.find(r => r[0] === s && r[1] === laju && r[2] === b)?.[10] ?? 0)) }));
  const sim = data?.lake?.simulasi;
  return (
    <>
      <h1>Tiering dan cost 3 tahun</h1>
      {galat && <p className="galat">{galat}</p>}
      <div className="kartu">
        <h2>Kebijakan</h2>
        <Tabel kolom={["data", "warm (S3 Standard)", "cold (Glacier Instant Retrieval)", "archive (Glacier Flexible)", "dihapus"]} baris={[
          ["bronze", "0–13 bulan", "13–24 bulan", "24 bulan–10 tahun", "sesudah 10 tahun"],
          ["silver (Iceberg)", "0–13 bulan", "sesudah 13 bulan (tetap bisa di-query)", "—", "lewat Iceberg (hapus partisi + expire_snapshots)"],
          ["gold (Iceberg & ClickHouse)", "selama dipakai dashboard", "—", "—", "dibangun ulang dari silver"]]} />
        <p className="redup">Lifecycle terpasang di bucket RustFS (mode: <b>{data?.status?.baris?.[0]?.[1] ?? "-"}</b>).
          {data?.status?.baris?.[0]?.[2] && <> Aturan transisi ditolak RustFS: <code>{data.status.baris[0][2]}</code>. RustFS hanya memindahkan objek ke tier remote yang sudah didaftarkan admin;
            transisi produksi ditulis di <code>tiering/lifecycle-produksi-s3.json</code> dan disimulasikan di bawah.</>}</p>
        <pre>{JSON.stringify(data?.lake?.aturan ?? [], null, 1)}</pre>
      </div>
      <div className="kartu">
        <h2>Simulasi tier objek di bucket</h2>
        <div className="baris"><label>Geser &quot;hari ini&quot; ke depan:
          <select value={geser} onChange={e => setGeser(Number(e.target.value))}>
            {[0, 400, 760, 3700].map(g => <option key={g} value={g}>{g} hari</option>)}</select></label>
          <span className="redup">hari acuan {sim?.hari_acuan}</span></div>
        <Tabel kolom={["lapisan", "tier", "objek", "ukuran"]} baris={(sim?.ringkasan || []).map((r: any) => [r.lapisan, r.tier, r.objek, byte(r.byte)])} />
      </div>
      <div className="kartu">
        <h2>Cost kumulatif 36 bulan (USD)</h2>
        <div className="sakelar">{["1x", "2x"].map(l => <button key={l} className={laju === l ? "on" : ""} onClick={() => setLaju(l)}>laju {l}</button>)}</div>
        <Garis seri={seri} label={bulan.map(b => `bln ${b}`)} />
        <Tabel kolom={["skenario", "laju", "total 3 tahun (USD)", "bulan 36 (USD/bulan)"]}
          baris={sk.flatMap(s => ["1x", "2x"].map(l => {
            const r = cost.filter(x => x[0] === s && x[1] === l); const akhir = r[r.length - 1];
            return [s, l, akhir ? Number(akhir[10]) : 0, akhir ? Number(akhir[9]) : 0];
          }))} />
        <p className="redup">Model: B_m = Σ V(t,m)·h(t) + R_m·h_ambil + E_m·h_egress. Asumsi: bronze 1 GB/hari, tumbuh 5%/bulan, silver 50%, gold 20%,
          analisis tahunan membaca 24 bulan silver, egress 150 GB/bulan. Kebijakan mulai lebih murah daripada semua_hot pada bulan ke-14.</p>
        <h3>Harga (AWS S3 Jakarta ap-southeast-3)</h3>
        <Tabel kolom={data?.harga?.kolom || []} baris={data?.harga?.baris || []} />
      </div>
    </>
  );
}
