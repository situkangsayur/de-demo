"use client";
import { useEffect, useState } from "react";
import { ambil, usePoll } from "@/components/data";
import { GrafikBatang, WARNA } from "@/components/grafik";
import { Skema } from "@/components/Skema";

type Model = { sql: string; join: number; kedalaman_join: number; median_ms: number; semua_ms: number[]; baris_hasil: number;
  kolom: string[]; contoh: (string | number)[][]; beda_dengan_star: number };
type Banding = { q: string; judul: string; model: Record<"oltp" | "star" | "obt", Model>; baris: Record<string, number> };
type Ringkasan = { n_transaksi: number; dibangkitkan_ms: number; tabel: Record<string, { baris: number; kolom: string[] }> };

const OLTP = {
  tabel: [
    { nama: "transaksi", x: 10, y: 60, kolom: ["id (PK)", "rekening_id (FK)", "cabang_id (FK)", "kanal", "jenis", "nominal", "occurred_at"] },
    { nama: "rekening", x: 215, y: 10, kolom: ["id (PK)", "nasabah_id (FK)", "cabang_id (FK)", "jenis"] },
    { nama: "nasabah", x: 420, y: 10, kolom: ["id (PK)", "cif, nama", "kota (sekarang)", "segmen", "cabang_id (FK)"] },
    { nama: "cabang", x: 215, y: 150, kolom: ["id (PK)", "kode, nama", "kota", "wilayah"] },
  ],
  relasi: [["transaksi", "rekening"], ["rekening", "nasabah"], ["transaksi", "cabang"], ["nasabah", "cabang"]] as [string, string][],
};
const STAR = {
  tabel: [
    { nama: "fakta_transaksi", x: 215, y: 75, fakta: true, kolom: ["sk_tanggal", "sk_nasabah", "sk_produk", "sk_cabang", "sk_kanal", "nominal", "id_transaksi"] },
    { nama: "dim_tanggal", x: 10, y: 10, kolom: ["sk_tanggal", "tahun_bulan, hari", "akhir_pekan"] },
    { nama: "dim_nasabah", x: 420, y: 10, kolom: ["sk_nasabah", "id_nasabah", "kota, segmen", "berlaku_dari/sampai"] },
    { nama: "dim_cabang", x: 10, y: 150, kolom: ["sk_cabang", "nama, kota", "wilayah"] },
    { nama: "dim_produk", x: 420, y: 150, kolom: ["sk_produk", "jenis_rekening", "akad"] },
    { nama: "dim_kanal", x: 215, y: 205, kolom: ["sk_kanal", "kanal, kelompok"] },
  ],
  relasi: [["fakta_transaksi", "dim_tanggal"], ["fakta_transaksi", "dim_nasabah"], ["fakta_transaksi", "dim_cabang"],
    ["fakta_transaksi", "dim_produk"], ["fakta_transaksi", "dim_kanal"]] as [string, string][],
};
const SOROT: Record<string, Record<string, string[]>> = {
  q1: { oltp: ["transaksi", "cabang"], star: ["fakta_transaksi", "dim_tanggal", "dim_cabang", "dim_kanal"] },
  q2: { oltp: ["transaksi", "rekening", "nasabah"], star: ["fakta_transaksi", "dim_nasabah", "dim_produk"] },
};
const NAMA = { oltp: "OLTP 3NF", star: "Star schema", obt: "One big table" };

export default function Halaman() {
  const { data: ring } = usePoll<Ringkasan>("/api/ringkasan", 10000);
  const [q, setQ] = useState("q1");
  const [b, setB] = useState<Banding | null>(null);
  const [sibuk, setSibuk] = useState(false);
  const [galat, setGalat] = useState("");
  const jalankan = (qq: string) => {
    setSibuk(true);
    ambil<Banding>(`/api/model/banding?q=${qq}`).then((d) => { setB(d); setGalat(""); }).catch((e) => setGalat(String(e))).finally(() => setSibuk(false));
  };
  useEffect(() => jalankan(q), [q]);
  async function bangkit(n: number) {
    setSibuk(true);
    await ambil("/api/bangkitkan", { method: "POST", body: JSON.stringify({ n }) }).catch((e) => setGalat(String(e)));
    jalankan(q);
  }
  return (
    <>
      <h1>Pertanyaan yang sama di tiga model data</h1>
      <p className="redup">Data bank sintetis dengan skema platform: {ring ? ring.n_transaksi.toLocaleString("id-ID") : "…"} transaksi setahun,
        8.000 nasabah, 8 cabang. OLTP ternormalisasi seperti database aplikasi; star schema seperti gold di warehouse; one big table
        menggabungkan semuanya menjadi satu tabel lebar.</p>
      <div className="baris">
        <div className="sakelar">
          {["q1", "q2"].map((x) => <button key={x} className={q === x ? "on" : ""} onClick={() => setQ(x)}>{x === "q1" ? "Pertanyaan 1" : "Pertanyaan 2"}</button>)}
        </div>
        <span className="kecil redup">ukuran data:</span>
        {[100000, 300000, 1000000, 2000000].map((n) => <button key={n} disabled={sibuk} onClick={() => bangkit(n)}>{(n / 1000).toLocaleString("id-ID")} rb</button>)}
        {sibuk && <span className="lencana netral">menghitung…</span>}
      </div>
      {galat && <p className="galat">{galat}</p>}
      {b && <>
        <section className="kartu">
          <h2>{b.judul}</h2>
          <div className="dua">
            <div><h3>OLTP ternormalisasi (3NF)</h3><Skema {...OLTP} sorot={SOROT[q].oltp} /></div>
            <div><h3>Star schema</h3><Skema {...STAR} sorot={SOROT[q].star} tinggi={270} /></div>
          </div>
          <p className="kecil redup">Tabel dan garis yang disorot adalah yang disentuh query. Pada star schema semua dimensi langsung
            menempel ke fakta (kedalaman join 1); pada OLTP join bisa berantai (transaksi → rekening → nasabah).</p>
        </section>
        <div className="dua">
          <section className="kartu">
            <h2>Waktu query (median 5 kali, DuckDB)</h2>
            <GrafikBatang format={(v) => `${v.toFixed(1)} ms`} data={(["oltp", "star", "obt"] as const).map((m, i) => ({
              label: NAMA[m], nilai: b.model[m].median_ms, warna: WARNA[i], catatan: `· ${b.model[m].join} join` }))} />
            <table><thead><tr><th>Model</th><th className="angka">join</th><th className="angka">baris hasil</th><th className="angka">beda dengan star</th><th>5 kali jalan (ms)</th></tr></thead>
              <tbody>{(["oltp", "star", "obt"] as const).map((m) => (
                <tr key={m} className={b.model[m].beda_dengan_star > 0 ? "sorot" : ""}><td>{NAMA[m]}</td><td className="angka">{b.model[m].join}</td>
                  <td className="angka">{b.model[m].baris_hasil}</td><td className="angka">{b.model[m].beda_dengan_star}</td>
                  <td className="kecil">{b.model[m].semua_ms.join(" · ")}</td></tr>))}</tbody></table>
            {b.model.oltp.beda_dengan_star > 0 ? <p className="kecil"><b>Hasil OLTP berbeda dari star schema.</b> Tabel nasabah di OLTP hanya
              menyimpan kota sekarang, sehingga transaksi lama nasabah yang pindah ikut dihitung di kota barunya. Star schema memakai
              dim_nasabah SCD tipe 2 (kota saat transaksi). Lanjutkan ke halaman SCD.</p>
              : <p className="kecil">Ketiga model memberi hasil yang sama untuk pertanyaan ini. Perbedaannya ada pada bentuk query dan biaya join.</p>}
            <p className="kecil redup">Pada skala lab perbedaan waktu kecil karena DuckDB sangat cepat dan data muat di memori. Naikkan ukuran data
              ke 1–2 juta untuk melihat jaraknya melebar. Ukuran tabel: transaksi {b.baris.transaksi.toLocaleString("id-ID")}, fakta {b.baris.fakta_transaksi.toLocaleString("id-ID")}, OBT {b.baris.obt_transaksi.toLocaleString("id-ID")} baris.</p>
          </section>
          <section className="kartu">
            <h2>Contoh hasil (star schema)</h2>
            <div className="tabel-wadah"><table>
              <thead><tr>{b.model.star.kolom.map((k) => <th key={k}>{k}</th>)}</tr></thead>
              <tbody>{b.model.star.contoh.map((r, i) => <tr key={i}>{r.map((v, j) => <td key={j} className={typeof v === "number" ? "angka" : ""}>
                {typeof v === "number" ? v.toLocaleString("id-ID") : String(v)}</td>)}</tr>)}</tbody>
            </table></div>
          </section>
        </div>
        <section className="kartu">
          <h2>SQL</h2>
          <div className="tiga">
            {(["oltp", "star", "obt"] as const).map((m) => <div key={m}><h3>{NAMA[m]}</h3><pre className="sql">{b.model[m].sql}</pre></div>)}
          </div>
        </section>
      </>}
      {ring && <section className="kartu">
        <h2>Tabel di lab</h2>
        <div className="tabel-wadah"><table><thead><tr><th>Tabel</th><th className="angka">Baris</th><th>Kolom</th></tr></thead>
          <tbody>{Object.entries(ring.tabel).map(([t, v]) => <tr key={t}><td>{t}</td><td className="angka">{v.baris.toLocaleString("id-ID")}</td>
            <td className="kecil" style={{ whiteSpace: "normal" }}>{v.kolom.join(", ")}</td></tr>)}</tbody></table></div>
      </section>}
    </>
  );
}
