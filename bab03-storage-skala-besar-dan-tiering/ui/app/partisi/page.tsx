"use client";
import { useEffect, useState } from "react";
import { ambil } from "@/components/data";
import { GrafikBatang, rupiah, ukuran, WARNA } from "@/components/grafik";

type Q = { tanggal: string | null; partisi: string[]; dibuka: string[]; berkas_dibuka: number; byte_berkas_dibuka: number; byte_semua: number;
  baris: number; ms: number; agregat: { kanal: string; nominal_sum: number; nominal_count: number }[]; sql_setara: string };

// kalkulator skema partition: jumlah berkas dan ukuran rata-rata per berkas
const SKEMA = [
  { nama: "dt (harian)", faktor: 1 },
  { nama: "dt + cabang (8)", faktor: 8 },
  { nama: "dt + jam (24)", faktor: 24 },
  { nama: "dt + jam + cabang + kanal (24×8×5)", faktor: 960 },
  { nama: "dt + rekening (8.000)", faktor: 8000 },
];

export default function Partisi() {
  const [pilih, setPilih] = useState<string>("");
  const [satu, setSatu] = useState<Q | null>(null);
  const [semua, setSemua] = useState<Q | null>(null);
  const [gbHari, setGbHari] = useState(1);
  const [tahun, setTahun] = useState(3);
  useEffect(() => { ambil<Q>("/api/partisi/query?semua=true").then(setSemua); }, []);
  useEffect(() => { ambil<Q>(`/api/partisi/query?tanggal=${pilih}`).then((q) => { setSatu(q); if (!pilih && q.tanggal) setPilih(q.tanggal); }); }, [pilih]);
  const partisi = semua?.partisi || [];
  return (
    <>
      <h1>Partition gaya Hive: query satu hari hanya membuka satu awalan</h1>
      <p className="redup">Bronze transaksi 30 hari ditulis ke <code>s3://lab3/bronze/app/transaksi/dt=YYYY-MM-DD/</code>, satu berkas Parquet per hari
        ({semua ? ukuran(semua.byte_semua / Math.max(1, partisi.length)) : "…"} per berkas). PyArrow membaca langsung dari RustFS dengan
        <code>partitioning=&quot;hive&quot;</code>; filter pada kolom <code>dt</code> menyingkirkan berkas sebelum dibuka.</p>
      <section className="kartu">
        <h2>Pilih tanggal (WHERE dt = …)</h2>
        <div className="hari">
          {partisi.map((p) => <button key={p} className={satu?.dibuka.includes(p) ? "dibuka" : ""} onClick={() => setPilih(p)}>{p.slice(5)}</button>)}
        </div>
        <p className="kecil redup">Kotak hijau: partisi yang benar-benar dibuka query.</p>
      </section>
      {satu && semua && <div className="dua">
        <section className="kartu"><h2>Satu hari vs 30 hari</h2>
          <GrafikBatang format={(v) => `${v} ms`} lebarLabel={170} data={[
            { label: `dt = ${satu.tanggal}`, nilai: satu.ms, warna: WARNA[0], catatan: `· ${satu.berkas_dibuka} berkas, ${ukuran(satu.byte_berkas_dibuka)}` },
            { label: "semua partisi", nilai: semua.ms, warna: WARNA[2], catatan: `· ${semua.berkas_dibuka} berkas, ${ukuran(semua.byte_berkas_dibuka)}` }]} />
          <p className="kecil">Baris dibaca: {satu.baris.toLocaleString("id-ID")} vs {semua.baris.toLocaleString("id-ID")}. Byte yang diambil dari object storage turun
            {" "}{(semua.byte_berkas_dibuka / Math.max(1, satu.byte_berkas_dibuka)).toFixed(0)} kali.</p>
          <pre>{satu.sql_setara}</pre></section>
        <section className="kartu"><h2>Hasil: nominal per kanal pada {satu.tanggal}</h2>
          <GrafikBatang format={rupiah} data={satu.agregat.map((a) => ({ label: a.kanal, nilai: a.nominal_sum, catatan: `· ${a.nominal_count} trx` }))} /></section>
      </div>}
      <section className="kartu">
        <h2>Kalkulator: berapa berkas yang dihasilkan skema partition?</h2>
        <div className="formulir">
          <label className="isian">volume bronze per hari (GB)
            <input type="number" min={0.01} step={0.1} value={gbHari} onChange={(e) => setGbHari(+e.target.value)} /></label>
          <label className="isian">lama penyimpanan (tahun)
            <input type="number" min={1} max={10} value={tahun} onChange={(e) => setTahun(+e.target.value)} /></label>
        </div>
        <div className="tabel-wadah" style={{ marginTop: 8 }}><table>
          <thead><tr><th>Skema</th><th className="angka">Berkas per hari</th><th className="angka">Total berkas</th><th className="angka">Ukuran rata-rata</th><th>Penilaian</th></tr></thead>
          <tbody>{SKEMA.map((s) => {
            const rata = (gbHari * 1024 ** 3) / s.faktor;
            const nilai = rata < 16 * 1024 ** 2 ? ["gagal", "small files: LIST lambat, request cost tinggi"]
              : rata < 128 * 1024 ** 2 ? ["warn", "di bawah target, perlu compaction"]
              : rata > 2 * 1024 ** 3 ? ["warn", "terlalu besar, pecah per ±1 GB"] : ["ok", "sesuai target ratusan MB–1 GB"];
            return <tr key={s.nama}><td>{s.nama}</td><td className="angka">{s.faktor.toLocaleString("id-ID")}</td>
              <td className="angka">{(s.faktor * 365 * tahun).toLocaleString("id-ID")}</td><td className="angka">{ukuran(rata)}</td>
              <td><span className={`lencana ${nilai[0]}`}>{nilai[1]}</span></td></tr>;
          })}</tbody></table></div>
        <p className="kecil redup">Pilih kolom partition yang sering difilter dan kardinalitasnya rendah (tanggal, bukan nomor rekening). Mulai kasar; perhalus hanya bila query membuktikan perlu.</p>
      </section>
    </>
  );
}
