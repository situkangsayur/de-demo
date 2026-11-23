"use client";
import { useEffect, useState } from "react";
import { ambil, usePoll } from "@/components/data";
import { GrafikBatang, ukuran, WARNA } from "@/components/grafik";

type Hasil = { format: string; ukuran_byte: number; tulis_ms: number; baca_agregat_ms: number; baca_semua_ms: number; mesin: string;
  layout: string; skema: string; rasio_ukuran_vs_csv: number };
type Jalan = { n: number; tabel: string; hasil: Hasil[]; contoh: Record<string, string>; query_agregat: string; detail?: string };
type Platform = { aktif: boolean; pesan?: string; baris_bronze_app_transaksi?: number };

const warnaFormat = (f: string) => f.startsWith("Parquet") ? WARNA[0] : f.startsWith("Avro") ? WARNA[3] : f.startsWith("JSON") ? WARNA[2] : WARNA[1];

export default function HalamanFormat() {
  const [n, setN] = useState(200000);
  const [sumber, setSumber] = useState("lab");
  const [r, setR] = useState<Jalan | null>(null);
  const [sibuk, setSibuk] = useState(false);
  const [galat, setGalat] = useState("");
  const { data: pf } = usePoll<Platform>("/api/platform", 30000);
  async function jalankan() {
    setSibuk(true); setGalat("");
    try {
      const d = await ambil<Jalan>("/api/format/jalankan", { method: "POST", body: JSON.stringify({ n, sumber }) });
      if (d.detail) setGalat(String(d.detail)); else setR(d);
    } catch (e) { setGalat(String(e)); }
    setSibuk(false);
  }
  // ?jalan=1 langsung menjalankan perbandingan (untuk dibagikan atau tangkapan layar)
  useEffect(() => { if (new URLSearchParams(window.location.search).get("jalan")) jalankan(); }, []); // eslint-disable-line react-hooks/exhaustive-deps
  const bar = (k: keyof Hasil) => r!.hasil.map((h) => ({ label: h.format, nilai: h[k] as number, warna: warnaFormat(h.format) }));
  return (
    <>
      <h1>CSV, JSON, Avro, dan Parquet untuk data yang sama</h1>
      <p className="redup">Transaksi yang sama ditulis ke lima berkas, lalu dibaca dengan query agregat
        <code>SELECT kanal, SUM(nominal) ... GROUP BY kanal</code> (hanya butuh dua kolom) dan dengan <code>SELECT *</code>. Setiap baca
        diulang lima kali dan diambil median, seperti langkah praktikum 2 di slide.</p>
      <div className="baris">
        <select value={n} onChange={(e) => setN(+e.target.value)}>
          {[50000, 200000, 1000000].map((v) => <option key={v} value={v}>{v.toLocaleString("id-ID")} baris</option>)}</select>
        <select value={sumber} onChange={(e) => setSumber(e.target.value)}>
          <option value="lab">sumber: generator lab</option>
          <option value="platform" disabled={!pf?.aktif}>sumber: bronze platform {pf?.aktif ? `(${pf.baris_bronze_app_transaksi} baris)` : "(tidak aktif)"}</option>
        </select>
        <button className="utama" disabled={sibuk} onClick={jalankan}>{sibuk ? "Menulis dan membaca…" : "Jalankan perbandingan"}</button>
      </div>
      {galat && <p className="galat">{galat}</p>}
      <section className="kartu">
        <h2>Row-based vs columnar</h2>
        <svg viewBox="0 0 600 120" width="100%" style={{ maxWidth: 820 }} role="img">
          <text x={0} y={12} style={{ fontWeight: 700 }}>Baris (CSV, JSON, Avro): satu transaksi utuh berdampingan</text>
          {Array.from({ length: 4 }).map((_, r) => ["id", "rek", "kanal", "nominal", "waktu"].map((c, i) => (
            <rect key={`${r}${c}`} x={(r * 5 + i) * 14} y={20} width={13} height={22} fill={c === "kanal" || c === "nominal" ? WARNA[1] : "var(--garis)"} />)))}
          <text x={0} y={62} style={{ fontWeight: 700 }}>Kolom (Parquet): semua nilai satu kolom berdampingan</text>
          {["id", "rek", "kanal", "nominal", "waktu"].map((c, i) => Array.from({ length: 4 }).map((_, r) => (
            <rect key={`${c}${r}`} x={(i * 4 + r) * 14} y={70} width={13} height={22} fill={c === "kanal" || c === "nominal" ? WARNA[0] : "var(--garis)"} />)))}
          <text x={0} y={112}>Kotak berwarna: byte yang dibaca untuk SUM(nominal) per kanal.</text>
        </svg>
      </section>
      {r && <>
        <p className="kecil">Data: {r.n.toLocaleString("id-ID")} baris dari tabel <code>{r.tabel}</code>.</p>
        <div className="lebar">
          <section className="kartu"><h2>Ukuran berkas</h2><GrafikBatang format={ukuran} data={bar("ukuran_byte").map((b, i) => ({ ...b, catatan: `(${r.hasil[i].rasio_ukuran_vs_csv}× CSV)` }))} /></section>
          <section className="kartu"><h2>Baca untuk agregat 2 kolom (median)</h2><GrafikBatang format={(v) => `${v} ms`} data={bar("baca_agregat_ms")} />
            <p className="kecil redup">Parquet hanya membuka column chunk kanal dan nominal (column pruning). CSV dan JSON harus mengurai seluruh teks.</p></section>
          <section className="kartu"><h2>Baca semua kolom (median)</h2><GrafikBatang format={(v) => `${v} ms`} data={bar("baca_semua_ms")} />
            <p className="kecil redup">Saat semua kolom dibutuhkan, keunggulan Parquet mengecil, tetapi tetap tidak perlu mengurai teks.</p></section>
          <section className="kartu"><h2>Waktu tulis</h2><GrafikBatang format={(v) => `${v} ms`} data={bar("tulis_ms")} />
            <p className="kecil redup">Avro ditulis dengan fastavro (Python, per baris) sehingga lebih lambat di sini; di Kafka atau NATS Avro ditulis per pesan,
              bukan jutaan baris sekaligus.</p></section>
        </div>
        <section className="kartu"><h2>Ringkasan</h2>
          <div className="tabel-wadah"><table>
            <thead><tr><th>Format</th><th>Layout</th><th>Skema</th><th className="angka">Ukuran</th><th className="angka">Tulis</th><th className="angka">Agregat</th><th className="angka">SELECT *</th><th>Mesin</th></tr></thead>
            <tbody>{r.hasil.map((h) => <tr key={h.format}><td><b>{h.format}</b></td><td>{h.layout}</td><td>{h.skema}</td><td className="angka">{ukuran(h.ukuran_byte)}</td>
              <td className="angka">{h.tulis_ms} ms</td><td className="angka">{h.baca_agregat_ms} ms</td><td className="angka">{h.baca_semua_ms} ms</td><td className="kecil">{h.mesin}</td></tr>)}</tbody>
          </table></div></section>
        <section className="kartu"><h2>Isi berkas apa adanya</h2>
          <div className="tiga">{Object.entries(r.contoh).map(([k, v]) => <div key={k}><h3>{k}</h3><pre>{v}</pre></div>)}</div>
          <p className="kecil redup">CSV tidak membawa tipe: nominal dan waktu hanya teks. JSON mengulang nama kolom di setiap baris sehingga paling besar.</p></section>
      </>}
      <section className="kartu"><h2>Memilih format (slide P02)</h2>
        <table><thead><tr><th>Kebutuhan</th><th>Pilihan</th><th>Alasan</th></tr></thead><tbody>
          <tr><td>Menerima berkas dari mitra</td><td>CSV atau JSON, lalu segera ubah</td><td>Simpan aslinya di bronze</td></tr>
          <tr><td>Event message di Kafka/NATS</td><td>Avro atau Protobuf</td><td>Biner kecil, skema eksplisit, schema registry</td></tr>
          <tr><td>Tabel analitik di lake</td><td>Parquet (+ Iceberg)</td><td>Columnar, statistik, didukung hampir semua mesin</td></tr>
          <tr><td>Berkas kecil sekali pakai</td><td>CSV</td><td>Tidak perlu rekayasa berlebihan</td></tr>
        </tbody></table></section>
    </>
  );
}
