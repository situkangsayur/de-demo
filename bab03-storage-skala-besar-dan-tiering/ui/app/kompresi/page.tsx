"use client";
import { useEffect, useState } from "react";
import { ambil } from "@/components/data";
import { GrafikBatang, ukuran, WARNA } from "@/components/grafik";

type H = { skenario: string; byte: number; tulis_ms: number; baca_ms: number; key: string; rasio_vs_csv: number };
type R = { baris: number; hasil: H[]; catatan: string };
const warna = (s: string) => s.startsWith("CSV") ? WARNA[2] : s.includes("zstd") ? WARNA[0] : s.includes("tanpa") ? WARNA[4] : WARNA[1];

export default function Kompresi() {
  const [r, setR] = useState<R | null>(null);
  const [sibuk, setSibuk] = useState(false);
  // ?jalan=1 langsung menjalankan uji (untuk dibagikan atau tangkapan layar)
  useEffect(() => { if (new URLSearchParams(window.location.search).get("jalan")) jalan(); }, []); // eslint-disable-line react-hooks/exhaustive-deps
  async function jalan() { setSibuk(true); try { setR(await ambil<R>("/api/kompresi", { method: "POST" })); } finally { setSibuk(false); } }
  return (
    <>
      <h1>Kompresi: ruang simpan vs CPU</h1>
      <p className="redup">Lima hari transaksi bronze (100.000 baris) ditulis dengan delapan pilihan, diunggah ke <code>s3://lab3/kompresi/</code>, lalu
        diunduh dan dibaca lagi. Ruang yang dihemat mengurangi storage cost dan byte yang lewat jaringan; harganya CPU saat menulis.</p>
      <button className="utama" disabled={sibuk} onClick={jalan}>{sibuk ? "Menulis dan membaca (zstd level 19 butuh beberapa detik)…" : "Jalankan perbandingan"}</button>
      {r && <div className="lebar">
        <section className="kartu"><h2>Ukuran objek</h2>
          <GrafikBatang format={ukuran} lebarLabel={180} data={r.hasil.map((h) => ({ label: h.skenario, nilai: h.byte, warna: warna(h.skenario), catatan: `(${h.rasio_vs_csv}×)` }))} /></section>
        <section className="kartu"><h2>Waktu tulis (encode + kompresi)</h2>
          <GrafikBatang format={(v) => `${v} ms`} lebarLabel={180} data={r.hasil.map((h) => ({ label: h.skenario, nilai: h.tulis_ms, warna: warna(h.skenario) }))} /></section>
        <section className="kartu"><h2>Waktu baca (GET + decode)</h2>
          <GrafikBatang format={(v) => `${v} ms`} lebarLabel={180} data={r.hasil.map((h) => ({ label: h.skenario, nilai: h.baca_ms, warna: warna(h.skenario) }))} />
          <p className="kecil redup">{r.catatan}. CSV harus diurai seluruhnya.</p></section>
        <section className="kartu"><h2>Kapan memakai apa</h2><table><tbody>
          <tr><td>snappy / lz4</td><td>cepat menulis dan membaca, rasio sedang; bawaan banyak mesin (Spark, DuckDB)</td></tr>
          <tr><td>zstd level 3</td><td>rasio mendekati gzip dengan kecepatan mendekati snappy; pilihan umum untuk lake</td></tr>
          <tr><td>zstd level tinggi / gzip</td><td>data dingin yang ditulis sekali dan jarang dibaca (archive)</td></tr>
          <tr><td>CSV + gzip</td><td>kecil, tetapi tidak bisa dibaca per kolom dan tidak bisa dipecah paralel</td></tr>
        </tbody></table></section>
      </div>}
    </>
  );
}
