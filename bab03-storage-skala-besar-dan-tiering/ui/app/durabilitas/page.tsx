"use client";
import { useState } from "react";
import { GrafikBatang, WARNA } from "@/components/grafik";

function komb(n: number, k: number): number { let r = 1; for (let i = 1; i <= k; i++) r = (r * (n - k + i)) / i; return r; }
// peluang kehilangan data: replication r hilang bila semua salinan hilang; RS(k,m) hilang bila >= m+1 dari k+m potongan hilang
function hilang(s: Skema, p: number): number {
  if (s.jenis === "rep") return Math.pow(p, s.r);
  const n = s.k + s.m; let t = 0;
  for (let i = s.m + 1; i <= n; i++) t += komb(n, i) * Math.pow(p, i) * Math.pow(1 - p, n - i);
  return t;
}
type Skema = { nama: string; jenis: "rep" | "ec"; r: number; k: number; m: number };
const SKEMA: Skema[] = [
  { nama: "Replication 2×", jenis: "rep", r: 2, k: 1, m: 1 },
  { nama: "Replication 3×", jenis: "rep", r: 3, k: 1, m: 2 },
  { nama: "RS(4,2)", jenis: "ec", r: 0, k: 4, m: 2 },
  { nama: "RS(6,3)", jenis: "ec", r: 0, k: 6, m: 3 },
  { nama: "RS(10,4)", jenis: "ec", r: 0, k: 10, m: 4 },
];
const ruang = (s: Skema) => s.jenis === "rep" ? s.r : (s.k + s.m) / s.k;
const mesin = (s: Skema) => s.jenis === "rep" ? s.r : s.k + s.m;

export default function Durabilitas() {
  const [eks, setEks] = useState(3);
  const [tb, setTb] = useState(200);
  const p = Math.pow(10, -eks);
  return (
    <>
      <h1>Replication vs erasure coding</h1>
      <p className="redup">Model sederhana slide P03: p = peluang satu mesin kehilangan datanya sebelum sempat dipulihkan, kegagalan antar-mesin saling bebas.
        Geser p dan volume data, lalu bandingkan ruang mentah dan peluang kehilangan.</p>
      <section className="kartu"><div className="formulir">
        <label className="isian">p = 10^-{eks} = {p.toExponential(0)}<input type="range" min={2} max={5} step={1} value={eks} onChange={(e) => setEks(+e.target.value)} /></label>
        <label className="isian">data analitik (TB)<input type="number" min={1} value={tb} onChange={(e) => setTb(+e.target.value)} /></label>
      </div></section>
      <section className="kartu"><h2>Susunan potongan</h2>
        {SKEMA.map((s) => <div key={s.nama} className="baris kecil" style={{ margin: "4px 0" }}><span style={{ width: 110 }}>{s.nama}</span>
          {Array.from({ length: mesin(s) }).map((_, i) => <span key={i} className="sel-tier" style={{ width: 34, background: s.jenis === "rep" ? WARNA[0] : i < s.k ? WARNA[0] : WARNA[1], color: "#fff" }}>
            {s.jenis === "rep" ? "D" : i < s.k ? `d${i + 1}` : `p${i - s.k + 1}`}</span>)}
          <span className="redup">tahan kehilangan {s.jenis === "rep" ? s.r - 1 : s.m}, pemulihan membaca {s.jenis === "rep" ? 1 : s.k} potongan</span></div>)}
      </section>
      <div className="lebar">
        <section className="kartu"><h2>Kapasitas mentah untuk {tb} TB</h2>
          <GrafikBatang format={(v) => `${v.toLocaleString("id-ID")} TB`} data={SKEMA.map((s, i) => ({ label: s.nama, nilai: tb * ruang(s), warna: WARNA[i % 5], catatan: `(${ruang(s).toFixed(2)}×)` }))} /></section>
        <section className="kartu"><h2>Peluang kehilangan (makin panjang makin aman)</h2>
          <GrafikBatang format={(v) => `10^-${v.toFixed(1)}`} data={SKEMA.map((s, i) => ({ label: s.nama, nilai: -Math.log10(hilang(s, p)), warna: WARNA[i % 5] }))} />
          <p className="kecil redup">Dengan p = 10^-3: replication 3× ≈ 10^-9, RS(6,3) ≈ 1,26 × 10^-10 dengan separuh ruang. Model ini terlalu optimis karena kegagalan sering
            berkorelasi (satu rak padam); penempatan per rak sama pentingnya dengan rumus.</p></section>
      </div>
      <section className="kartu"><table>
        <thead><tr><th>Skema</th><th className="angka">Ruang</th><th className="angka">Mentah</th><th className="angka">Hemat vs 3×</th><th className="angka">P hilang</th><th className="angka">Mesin minimal</th></tr></thead>
        <tbody>{SKEMA.map((s) => <tr key={s.nama}><td>{s.nama}</td><td className="angka">{ruang(s).toFixed(2)}×</td><td className="angka">{(tb * ruang(s)).toLocaleString("id-ID")} TB</td>
          <td className="angka">{(tb * 3 - tb * ruang(s)).toLocaleString("id-ID")} TB</td><td className="angka">{hilang(s, p).toExponential(2)}</td><td className="angka">{mesin(s)}</td></tr>)}</tbody></table>
        <p className="kecil redup">RustFS dan MinIO memakai erasure coding per set disk. Lab ini memakai satu volume sehingga tidak ada redundansi; di produksi minimal 4 disk.</p></section>
    </>
  );
}
