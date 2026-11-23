"use client";
import { useState } from "react";
import { api, useApi, angka, BatangLog, warnaStore, Galat } from "@/components/lib";

type Info = { versi: string; ttl_detik: number; jumlah_nasabah: number; sinkron_terakhir: Sinkron | null };
type Sinkron = { tgl_fitur: string; baris: number; hitung_clickhouse_ms: number; salin_redis_ms: number; salin_postgres_ms: number; versi_lama: string };
type L = { store: string; cara: string; ms: number; data?: Record<string, unknown>; galat?: string };

const URUT = ["tgl_fitur", "trx_7h", "nominal_7h", "trx_30h", "nominal_30h", "hari_sejak_trx_terakhir", "rasio_digital_30h", "belanja_ecom_30h", "nominal_belanja_ecom_30h", "segmen_saat_itu", "kota_saat_itu"];

export default function Fitur() {
  const info = useApi<Info>("fitur/info", 10000);
  const [id, setId] = useState(7);
  const [hasil, setHasil] = useState<L[]>([]);
  const [sink, setSink] = useState<Sinkron | null>(null);
  const [galat, setGalat] = useState("");
  const [jalan, setJalan] = useState(false);
  async function cari(x = id) {
    setGalat("");
    try { const r = await api<{ hasil: L[] }>(`fitur/${x}`); setHasil(r.hasil); } catch (e) { setGalat(String(e)); }
  }
  async function sinkron() {
    setJalan(true); setGalat("");
    try { setSink(await api<Sinkron>("fitur/sinkron", {})); info.segarkan(); } catch (e) { setGalat(String(e)); }
    setJalan(false);
  }
  const s = sink || info.data?.sinkron_terakhir;
  const redis = hasil.find(h => h.store === "Redis")?.data || {};
  return (
    <>
      <h1>Serving store: <code>fitur_nasabah</code> untuk tim AI</h1>
      <p className="redup">Gold di warehouse (ClickHouse) tetap sumber kebenaran. Model skor fraud butuh fitur satu nasabah dalam hitungan milidetik untuk setiap transaksi, sehingga fitur yang sudah dihitung disalin ke Redis (hash per nasabah, TTL 2 hari) dan bisa dibangun ulang kapan saja dari gold.</p>
      <Galat teks={info.galat || galat} />
      <div className="kisi">
        <div className="kartu">
          <h2>Pipeline salin gold → serving</h2>
          <svg width="100%" viewBox="0 0 520 90" role="img" aria-label="ClickHouse ke Redis dan PostgreSQL">
            {[["fakta_transaksi", 10, "#d9a400"], ["gold.fitur_nasabah", 185, "#d9a400"], ["Redis fitur:<id>", 360, "#c6302b"]].map(([t, x, c]) => (
              <g key={String(t)}><rect x={Number(x)} y={20} width={150} height={40} rx={8} fill={String(c)} opacity={0.85} /><text x={Number(x) + 75} y={45} textAnchor="middle" style={{ fill: "#fff", fontSize: 12 }}>{t}</text></g>))}
            <path d="M160,40 L185,40 M335,40 L360,40" stroke="var(--redup)" strokeWidth={2} />
            <text x={172} y={80} textAnchor="middle">GROUP BY</text><text x={347} y={80} textAnchor="middle">HSET</text>
          </svg>
          <div className="baris"><button className="utama" disabled={jalan} onClick={sinkron}>{jalan ? "menghitung…" : "Hitung ulang dan sinkronkan"}</button>
            <span className="redup">versi di Redis: <b>{info.data?.versi || "–"}</b>, TTL {angka(info.data?.ttl_detik)} dtk</span></div>
          {s && <table style={{ marginTop: 8 }}><tbody>
            <tr><td>tgl_fitur (point-in-time)</td><td className="angka">{s.tgl_fitur}</td></tr>
            <tr><td>nasabah</td><td className="angka">{angka(s.baris)}</td></tr>
            <tr><td>hitung di ClickHouse</td><td className="angka">{s.hitung_clickhouse_ms.toFixed(0)} ms</td></tr>
            <tr><td>salin ke Redis (pipeline)</td><td className="angka">{s.salin_redis_ms.toFixed(0)} ms</td></tr>
            <tr><td>salin ke PostgreSQL (COPY, satu transaksi)</td><td className="angka">{s.salin_postgres_ms.toFixed(0)} ms</td></tr>
          </tbody></table>}
        </div>
        <div className="kartu">
          <h2>Lookup satu nasabah (hot path)</h2>
          <div className="baris">
            <label>nasabah_id <input type="number" value={id} min={1} max={info.data?.jumlah_nasabah || 5000} onChange={e => setId(Number(e.target.value))} style={{ width: 90 }} /></label>
            <button className="utama" onClick={() => cari()}>Ambil fitur</button>
            <button onClick={() => { const x = 1 + Math.floor(Math.random() * (info.data?.jumlah_nasabah || 5000)); setId(x); cari(x); }}>Nasabah acak</button>
          </div>
          {hasil.length > 0 && <div style={{ marginTop: 8 }}><BatangLog data={hasil.map(h => ({ nama: h.store, sub: h.cara, nilai: h.ms, warna: warnaStore(h.store) }))} /></div>}
        </div>
      </div>
      {hasil.length > 0 && (
        <div className="kartu">
          <h2>Vektor fitur nasabah {id}</h2>
          <div className="tabel-wadah">
            <table>
              <thead><tr><th>fitur</th>{hasil.map((h, i) => <th key={i}>{h.store}<div className="redup" style={{ fontWeight: 400 }}>{h.cara.slice(0, 30)}</div></th>)}</tr></thead>
              <tbody>{URUT.map(k => <tr key={k}><td><code>{k}</code></td>{hasil.map((h, i) => <td key={i}>{h.data && h.data[k] !== undefined ? String(h.data[k]).slice(0, 24) : "–"}</td>)}</tr>)}</tbody>
            </table>
          </div>
          <p className="redup">Redis mengembalikan semua nilai sebagai string. Kolom &quot;hitung langsung&quot; memakai jendela relatif terhadap jam sekarang sehingga nilainya bisa berbeda dari fitur point-in-time per tgl_fitur; inilah alasan fitur training dan serving harus dihitung dengan logika yang sama ({Object.keys(redis).length} kolom di Redis).</p>
        </div>
      )}
    </>
  );
}
