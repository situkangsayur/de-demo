"use client";
import { useEffect, useState } from "react";
import { ambil } from "@/components/data";
import { GrafikBatang, GrafikTumpuk, rupiah, WARNA } from "@/components/grafik";

type Scd = { nasabah_id: number; oltp: (string | number)[]; oltp_kolom: string[]; scd1: (string | number)[][];
  scd2: (string | number | boolean)[][]; per_bulan: { bulan: string; kota_scd2: string; kota_scd1: string; nominal: number; jumlah: number }[];
  fakta_menunjuk_versi_baru?: number };
type Dampak = { kota: { kota: string; scd1: number; scd2: number; selisih: number }[]; baris_dim_scd2: number; nasabah: number };
const KOTA = ["jakarta pusat", "jakarta selatan", "bogor", "bandung", "surabaya", "medan", "makassar", "yogyakarta"];

export default function HalamanScd() {
  const [id, setId] = useState(7712);
  const [d, setD] = useState<Scd | null>(null);
  const [dm, setDm] = useState<Dampak | null>(null);
  const [kota, setKota] = useState("surabaya");
  const [tgl, setTgl] = useState(() => new Date(Date.now() - 45 * 86400000).toISOString().slice(0, 10));
  const [pesan, setPesan] = useState("");
  const muat = (n = id) => {
    ambil<Scd & { detail?: string }>(`/api/scd?nasabah_id=${n}`).then((r) => { if (r.detail) setPesan(r.detail); else setD(r); });
    ambil<Dampak>("/api/scd/dampak").then(setDm);
  };
  useEffect(() => muat(), []); // eslint-disable-line react-hooks/exhaustive-deps
  async function pindah() {
    const r = await ambil<Scd & { detail?: string; sk_baru?: number }>("/api/scd/pindah", { method: "POST", body: JSON.stringify({ nasabah_id: id, kota, tanggal: tgl }) });
    if (r.detail) { setPesan(String(r.detail)); return; }
    setPesan(`Versi baru sk_nasabah=${r.sk_baru}; ${r.fakta_menunjuk_versi_baru} baris fakta sesudah ${tgl} kini menunjuk versi baru.`);
    setD(r);
    ambil<Dampak>("/api/scd/dampak").then(setDm);
  }
  const kotaSeri = d ? Array.from(new Set(d.per_bulan.flatMap((b) => [b.kota_scd2, b.kota_scd1]))) : [];
  const perKota = (k: "kota_scd1" | "kota_scd2") => kotaSeri.map((kt) => ({ nama: kt, nilai: d!.per_bulan.filter((b) => b[k] === kt).reduce((a, b) => a + b.nominal, 0) }));
  return (
    <>
      <h1>Nasabah pindah kota: apa yang terjadi pada laporan lama?</h1>
      <p className="redup">Contoh slide: nasabah 7712 tinggal di Bogor sampai 30 Juni 2026, lalu pindah ke Bandung. OLTP dan SCD tipe 1
        menimpa kota lama. SCD tipe 2 menambah baris baru dengan rentang berlaku, sehingga transaksi Maret tetap tercatat di Bogor.</p>
      <div className="baris">
        <label className="kecil">nasabah id <input type="number" value={id} onChange={(e) => setId(+e.target.value)} style={{ width: 90 }} /></label>
        <button onClick={() => muat()}>Tampilkan</button>
        <span className="redup kecil">| pindahkan ke</span>
        <select value={kota} onChange={(e) => setKota(e.target.value)}>{KOTA.map((k) => <option key={k}>{k}</option>)}</select>
        <span className="kecil">mulai</span><input type="date" value={tgl} onChange={(e) => setTgl(e.target.value)} />
        <button className="utama" onClick={pindah}>Pindah kota</button>
      </div>
      {pesan && <p className="kecil">{pesan}</p>}
      {d && <>
        <div className="dua">
          <section className="kartu"><h2>OLTP: tabel nasabah</h2>
            <table><tbody>{d.oltp_kolom.map((k, i) => <tr key={k}><th>{k}</th><td>{String(d.oltp[i])}</td></tr>)}</tbody></table>
            <p className="kecil redup">Database aplikasi hanya perlu keadaan sekarang. UPDATE menimpa kota lama.</p></section>
          <section className="kartu"><h2>SCD tipe 1: dim_nasabah_scd1</h2>
            <table><thead><tr><th>sk</th><th>id</th><th>kota</th><th>segmen</th></tr></thead>
              <tbody>{d.scd1.map((r, i) => <tr key={i}>{r.map((v, j) => <td key={j}>{String(v)}</td>)}</tr>)}</tbody></table>
            <p className="kecil redup">Satu baris, nilai ditimpa. Sejarah hilang.</p></section>
        </div>
        <div>
          <section className="kartu"><h2>SCD tipe 2: dim_nasabah</h2>
            <div className="tabel-wadah"><table><thead><tr><th>sk_nasabah</th><th>id</th><th>kota</th><th>segmen</th><th>berlaku_dari</th><th>berlaku_sampai</th><th>aktif</th></tr></thead>
              <tbody>{d.scd2.map((r, i) => <tr key={i} className={r[6] ? "sorot" : ""}>{r.map((v, j) => <td key={j}>{String(v)}</td>)}</tr>)}</tbody></table></div>
            <p className="kecil redup">Satu baris per versi. 9999-12-31 berarti belum berakhir; keadaan sekarang cukup difilter aktif = true.</p></section>
        </div>
        <section className="kartu">
          <h2>Transaksi nasabah {d.nasabah_id} per kota</h2>
          <GrafikTumpuk format={rupiah} seri={kotaSeri} data={[
            { label: "SCD 1 (kota sekarang)", bagian: perKota("kota_scd1") },
            { label: "SCD 2 (saat transaksi)", bagian: perKota("kota_scd2") }]} lebarLabel={170} />
          <div className="tabel-wadah" style={{ marginTop: 10 }}><table>
            <thead><tr><th>bulan</th><th className="angka">jumlah</th><th className="angka">nominal</th><th>kota menurut SCD 1</th><th>kota menurut SCD 2</th></tr></thead>
            <tbody>{d.per_bulan.map((b, i) => <tr key={i} className={b.kota_scd1 !== b.kota_scd2 ? "sorot" : ""}><td>{b.bulan}</td>
              <td className="angka">{b.jumlah}</td><td className="angka">{rupiah(b.nominal)}</td><td>{b.kota_scd1}</td><td>{b.kota_scd2}</td></tr>)}</tbody>
          </table></div>
          <p className="kecil redup">Baris yang disorot adalah bulan yang laporannya berubah bila dimensi memakai SCD tipe 1.</p>
        </section>
      </>}
      {dm && <section className="kartu">
        <h2>Dampak ke seluruh laporan: total nominal setahun per kota nasabah</h2>
        <p className="kecil redup">{dm.nasabah.toLocaleString("id-ID")} nasabah, {dm.baris_dim_scd2.toLocaleString("id-ID")} baris di dim_nasabah SCD 2
          (sekitar 12% nasabah pernah pindah). Batang menunjukkan selisih laporan SCD 1 dikurangi SCD 2: hijau berarti SCD 1
          melebih-lebihkan kota itu (menarik transaksi lama nasabah yang baru pindah ke sana), merah berarti kurang.</p>
        <GrafikBatang format={rupiah} lebarLabel={140} data={dm.kota.slice().sort((a, b) => b.selisih - a.selisih).map((k) => ({
          label: k.kota, nilai: Math.abs(k.selisih), warna: k.selisih >= 0 ? WARNA[0] : WARNA[2], catatan: k.selisih >= 0 ? "lebih" : "kurang" }))} />
        <div className="tabel-wadah" style={{ marginTop: 8 }}><table>
          <thead><tr><th>kota</th><th className="angka">SCD 1</th><th className="angka">SCD 2</th><th className="angka">selisih</th></tr></thead>
          <tbody>{dm.kota.map((k) => <tr key={k.kota}><td>{k.kota}</td><td className="angka">{rupiah(k.scd1)}</td><td className="angka">{rupiah(k.scd2)}</td>
            <td className="angka">{rupiah(k.selisih)}</td></tr>)}</tbody></table></div>
      </section>}
    </>
  );
}
