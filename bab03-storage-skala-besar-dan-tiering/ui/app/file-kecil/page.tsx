"use client";
import { useEffect, useState } from "react";
import { ambil, usePoll } from "@/components/data";
import { GrafikBatang, ukuran, WARNA } from "@/components/grafik";

type H = { berkas: number; byte_total: number; byte_rata: number; tulis_ms: number; list_ms: number; baca_ms: number; baris: number;
  request_put: number; request_get_list: number; biaya_request_usd_per_bulan: number };
type R = { tanggal: string; baris: number; hasil: H[]; harga: { put_per_1000: number; get_per_1000: number; sumber: string }; asumsi_biaya: string };
type C = { prefix: string; berkas_sebelum: number; byte: number; baca_semua_ms: number; tulis_ms: number; hapus_ms: number; baca_sesudah_ms: number };
type P = { aktif: boolean; pesan?: string; bucket?: string; total_berkas?: number; total_byte?: number;
  entitas?: { entitas: string; berkas: number; byte: number; partisi: number }[]; histogram?: { kelas: string; berkas: number }[] };

export default function FileKecil() {
  const [r, setR] = useState<R | null>(null);
  const [c, setC] = useState<C | null>(null);
  const [sibuk, setSibuk] = useState("");
  const { data: pf } = usePoll<P>("/api/platform/bronze", 60000);
  // ?jalan=1 langsung menjalankan uji (untuk dibagikan atau tangkapan layar)
  useEffect(() => { if (new URLSearchParams(window.location.search).get("jalan")) jalan(); }, []); // eslint-disable-line react-hooks/exhaustive-deps
  async function jalan() { setSibuk("uji"); setC(null); try { setR(await ambil<R>("/api/kecil/jalankan", { method: "POST", body: JSON.stringify({ jumlah: [1, 10, 100, 1000] }) })); } finally { setSibuk(""); } }
  async function kompak() { setSibuk("kompak"); try { setC(await ambil<C>("/api/kecil/compaction", { method: "POST" })); } finally { setSibuk(""); } }
  const bar = (k: keyof H, w: number) => r!.hasil.map((h) => ({ label: `${h.berkas} berkas`, nilai: h[k] as number, warna: WARNA[w] }));
  return (
    <>
      <h1>Small files problem: data yang sama, jumlah berkas berbeda</h1>
      <p className="redup">Satu hari transaksi (20.000 baris) ditulis ke RustFS sebagai 1, 10, 100, dan 1.000 berkas Parquet, lalu di-LIST dan dibaca
        kembali. Isi datanya sama persis; yang berbeda hanya jumlah objek dan permintaan HTTP.</p>
      <div className="baris">
        <button className="utama" disabled={!!sibuk} onClick={jalan}>{sibuk === "uji" ? "Menulis 1.111 objek…" : "Jalankan uji 1 / 10 / 100 / 1.000 berkas"}</button>
        <button disabled={!!sibuk || !r} onClick={kompak}>{sibuk === "kompak" ? "Compaction…" : "Compaction 1.000 berkas → 1"}</button>
      </div>
      {r && <>
        <div className="lebar">
          <section className="kartu"><h2>Waktu tulis (PUT, 16 paralel)</h2><GrafikBatang format={(v) => `${v} ms`} data={bar("tulis_ms", 1)} /></section>
          <section className="kartu"><h2>Waktu baca (PyArrow dari RustFS)</h2><GrafikBatang format={(v) => `${v} ms`} data={bar("baca_ms", 2)} /></section>
          <section className="kartu"><h2>Waktu LIST</h2><GrafikBatang format={(v) => `${v} ms`} data={bar("list_ms", 3)} /></section>
          <section className="kartu"><h2>Biaya request per bulan (ilustrasi)</h2>
            <GrafikBatang format={(v) => `USD ${v.toFixed(2)}`} data={bar("biaya_request_usd_per_bulan", 0)} />
            <p className="kecil redup">Asumsi: {r.asumsi_biaya}. Harga: PUT USD {r.harga.put_per_1000}/1.000, GET USD {r.harga.get_per_1000}/1.000 ({r.harga.sumber}).</p></section>
        </div>
        <section className="kartu"><h2>Rincian</h2><table>
          <thead><tr><th className="angka">Berkas</th><th className="angka">Total byte</th><th className="angka">Rata-rata per berkas</th><th className="angka">PUT</th><th className="angka">GET + LIST</th><th className="angka">Tulis</th><th className="angka">LIST</th><th className="angka">Baca</th></tr></thead>
          <tbody>{r.hasil.map((h) => <tr key={h.berkas}><td className="angka">{h.berkas}</td><td className="angka">{ukuran(h.byte_total)}</td><td className="angka">{ukuran(h.byte_rata)}</td>
            <td className="angka">{h.request_put}</td><td className="angka">{h.request_get_list}</td><td className="angka">{h.tulis_ms} ms</td><td className="angka">{h.list_ms} ms</td><td className="angka">{h.baca_ms} ms</td></tr>)}</tbody></table>
          <p className="kecil redup">Total byte ikut membengkak: setiap berkas Parquet membawa footer, skema, dan statistiknya sendiri, dan berkas kecil sulit dikompresi.</p></section>
      </>}
      {c && <section className="kartu"><h2>Compaction</h2>
        <GrafikBatang format={(v) => `${v} ms`} lebarLabel={200} data={[
          { label: `baca ${c.berkas_sebelum} berkas`, nilai: c.baca_semua_ms, warna: WARNA[2] },
          { label: "tulis 1 berkas", nilai: c.tulis_ms, warna: WARNA[0] },
          { label: `hapus ${c.berkas_sebelum} berkas lama`, nilai: c.hapus_ms, warna: WARNA[4] },
          { label: "baca ulang sesudah compaction", nilai: c.baca_sesudah_ms, warna: WARNA[0] }]} />
        <p className="kecil">Hasil: satu berkas {ukuran(c.byte)}. Compaction sendiri mahal (menghapus ribuan objek kecil pun butuh waktu), jadi dijalankan berkala
          di luar jam sibuk. Di Iceberg (Bab 4) ini dikerjakan <code>rewrite_data_files</code> tanpa mengganggu pembaca.</p></section>}
      <section className="kartu"><h2>Bronze platform</h2>
        {pf?.aktif ? <>
          <p className="kecil">Bucket <code>{pf.bucket}</code> di platform: {pf.total_berkas?.toLocaleString("id-ID")} berkas, {ukuran(pf.total_byte || 0)}.</p>
          <div className="dua">
            <div><h3>Sebaran ukuran berkas</h3><GrafikBatang data={(pf.histogram || []).map((h) => ({ label: h.kelas, nilai: h.berkas, warna: WARNA[1] }))} /></div>
            <div><h3>Per sumber/entitas</h3><table><thead><tr><th>entitas</th><th className="angka">partisi</th><th className="angka">berkas</th><th className="angka">rata-rata</th></tr></thead>
              <tbody>{(pf.entitas || []).map((e) => <tr key={e.entitas}><td>{e.entitas}</td><td className="angka">{e.partisi}</td><td className="angka">{e.berkas}</td><td className="angka">{ukuran(e.byte / Math.max(1, e.berkas))}</td></tr>)}</tbody></table></div>
          </div>
          <p className="kecil redup">Ingestor platform menulis micro-batch tiap beberapa detik; berkas bronze karena itu kecil. Compaction ke silver menyelesaikannya.</p>
        </> : <p className="kecil redup">{pf?.pesan || "memeriksa…"}. Isi PLATFORM_S3_* di .env untuk melihat sebaran ukuran berkas bronze platform yang sebenarnya.</p>}
      </section>
    </>
  );
}
