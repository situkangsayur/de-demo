"use client";
import { useState } from "react";
import { api, useApi, angka, BatangLog, warnaStore, Galat } from "@/components/lib";

type Hasil = { pola: string; store: string; engine: string; n: number; p50_ms: number; p95_ms: number; p99_ms: number; rata_ms: number; ops_per_detik: number; baca?: string; query: string; catatan?: string; galat?: string };
type Status = { siap: boolean; muat: { tahap: string; persen: number; selesai: boolean; galat?: string; jumlah: Record<string, number>; throughput_bulk: Record<string, number> } };

const POLA: [string, string, string][] = [
  ["point", "Point lookup", "Berapa saldo rekening 1234?"],
  ["range", "Range scan per key", "50 transaksi terakhir nasabah X"],
  ["agregasi", "Aggregation", "Total transaksi per cabang 30 hari"],
  ["teks", "Pencarian teks", "Keterangan memuat \"zakat\" dan \"wakaf\""],
  ["fitur", "Lookup fitur (serving)", "fitur_nasabah untuk skor fraud/AI"],
];

const PILIHAN: [string, string, string][] = [
  ["Point lookup per key, latency sangat rendah", "Redis (salinan) atau PostgreSQL (sumber kebenaran)", "ClickHouse membaca satu granule 8.192 baris untuk satu kunci"],
  ["Range scan per key", "PostgreSQL dengan indeks komposit, atau ClickHouse bila ORDER BY cocok", "ClickHouse dengan ORDER BY yang tidak cocok membaca seluruh tabel"],
  ["Aggregation atas jutaan baris", "ClickHouse (kolom, vectorized)", "PostgreSQL membaca semua kolom setiap baris; Redis tidak bisa"],
  ["Pencarian teks bebas", "OpenSearch (inverted index) atau PostgreSQL GIN", "ILIKE dan scan kolom memindai semua baris"],
  ["Fitur untuk hot path model", "Redis, dibangun ulang dari gold", "Menghitung fitur dari fakta pada setiap request"],
];

export default function Benchmark() {
  const st = useApi<Status>("status", 3000);
  const siap = !!st.data?.siap;
  const hasil = useApi<{ hasil: Hasil[]; throughput_bulk: Record<string, number> }>(siap ? "bench" : null);
  const [n, setN] = useState(200);
  const [jalan, setJalan] = useState("");
  const [galat, setGalat] = useState("");
  const data = hasil.data?.hasil || [];
  async function ukur(pola: string) {
    setJalan(pola); setGalat("");
    try { await api("bench", { pola, n }); await hasil.segarkan(); } catch (e) { setGalat(String(e)); }
    setJalan("");
  }
  const m = st.data?.muat;
  const p50 = (pola: string, store: string) => data.find(h => h.pola === pola && h.store === store && h.n > 0)?.p50_ms;
  return (
    <>
      <h1>Benchmark empat pola akses</h1>
      <p className="redup">Pertanyaan yang benar bukan &quot;database apa yang paling cepat&quot;, tetapi &quot;paling cepat untuk pola akses apa&quot;. Data yang sama ({angka(m?.jumlah?.transaksi)} transaksi, {angka(m?.jumlah?.rekening)} rekening, {angka(m?.jumlah?.nasabah)} nasabah) dimuat ke empat store; setiap pola diukur dari API dengan kunci acak, sesudah pemanasan.</p>
      {!siap && (
        <div className="kartu">
          <b>Memuat data: {m?.tahap}</b>
          <div className="progres" style={{ margin: "8px 0" }}><div style={{ width: `${m?.persen || 0}%` }} /></div>
          <Galat teks={m?.galat || ""} />
        </div>
      )}
      <div className="kartu">
        <div className="baris">
          <label>Iterasi per store <input type="number" value={n} min={20} max={2000} onChange={e => setN(Number(e.target.value))} style={{ width: 90 }} /></label>
          {POLA.map(([p, nama]) => <button key={p} disabled={!siap || !!jalan} onClick={() => ukur(p)}>{jalan === p ? "mengukur…" : nama}</button>)}
          <button className="utama" disabled={!siap || !!jalan} onClick={() => ukur("semua")}>{jalan === "semua" ? "mengukur semua…" : "Ukur semua"}</button>
        </div>
        <p className="redup" style={{ margin: "6px 0 0" }}>Aggregation dan teks memakai n/10 iterasi karena tiap query memindai banyak data. Batang tua = p50, batang muda = p95.</p>
        <Galat teks={galat || hasil.galat} />
      </div>
      {POLA.map(([p, nama, tanya]) => {
        const hs = data.filter(h => h.pola === p);
        if (!hs.length) return null;
        return (
          <div key={p} className="kartu">
            <h2>{nama} <span className="redup">· {tanya}</span></h2>
            <BatangLog data={hs.filter(h => h.n > 0).map(h => ({ nama: h.store, sub: h.engine, nilai: h.p50_ms, nilai2: h.p95_ms, warna: warnaStore(h.store) }))} />
            <div className="tabel-wadah" style={{ marginTop: 8 }}>
              <table>
                <thead><tr><th>store</th><th>engine / struktur</th><th className="angka">p50 ms</th><th className="angka">p95 ms</th><th className="angka">p99 ms</th><th className="angka">ops/detik</th><th>dibaca engine</th><th>catatan</th></tr></thead>
                <tbody>{hs.map((h, i) => (
                  <tr key={i} title={h.query} style={{ opacity: h.n ? 1 : .6 }}>
                    <td><b style={{ color: warnaStore(h.store) }}>{h.store}</b></td><td>{h.engine}</td>
                    <td className="angka">{h.n ? h.p50_ms.toFixed(2) : "–"}</td><td className="angka">{h.n ? h.p95_ms.toFixed(2) : "–"}</td><td className="angka">{h.n ? h.p99_ms.toFixed(2) : "–"}</td>
                    <td className="angka">{h.n ? angka(h.ops_per_detik) : "–"}</td><td>{h.baca || ""}</td>
                    <td style={{ whiteSpace: "normal", minWidth: 220 }}>{h.galat ? <span className="galat">{h.galat}</span> : h.catatan}</td>
                  </tr>))}</tbody>
              </table>
            </div>
          </div>
        );
      })}
      <div className="kisi">
        <div className="kartu">
          <h2>Throughput tulis bulk saat muat awal</h2>
          <BatangLog data={Object.entries(hasil.data?.throughput_bulk || m?.throughput_bulk || {}).map(([s, v]) => ({
            nama: { postgres: "PostgreSQL", clickhouse: "ClickHouse", redis: "Redis", opensearch: "OpenSearch" }[s] || s,
            sub: { postgres: "COPY", clickhouse: "INSERT batch 100 ribu", redis: "pipeline RPUSH", opensearch: "_bulk 5.000, 1 replica" }[s], nilai: v,
            warna: warnaStore({ postgres: "PostgreSQL", clickhouse: "ClickHouse", redis: "Redis", opensearch: "OpenSearch" }[s] || s) }))}
            label={v => `${angka(v)} baris/detik`} />
        </div>
        <div className="kartu">
          <h2>Kapan memilih (dari angka di atas)</h2>
          <table><thead><tr><th>Kebutuhan</th><th>Pilihan</th><th>Hindari karena</th></tr></thead>
            <tbody>{PILIHAN.map(([k, p, h], i) => <tr key={i}><td style={{ whiteSpace: "normal" }}>{k}</td><td style={{ whiteSpace: "normal" }}>{p}</td><td style={{ whiteSpace: "normal" }} className="redup">{h}</td></tr>)}</tbody></table>
          {p50("point", "Redis") !== undefined && <p className="redup">Contoh terukur: point lookup Redis {p50("point", "Redis")?.toFixed(2)} ms vs ClickHouse {p50("point", "ClickHouse")?.toFixed(2)} ms; aggregation ClickHouse {p50("agregasi", "ClickHouse")?.toFixed(2) ?? "–"} ms vs PostgreSQL {p50("agregasi", "PostgreSQL")?.toFixed(2) ?? "–"} ms.</p>}
        </div>
      </div>
    </>
  );
}
