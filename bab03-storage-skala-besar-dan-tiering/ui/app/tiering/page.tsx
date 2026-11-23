"use client";
import { useEffect, useMemo, useState } from "react";
import { ambil } from "@/components/data";
import { GrafikGaris, GrafikTumpuk, rupiah } from "@/components/grafik";

type Kelas = { kelas: string; diterima: boolean; head_storage_class?: string; galat?: string };
type Langkah = { percobaan: string; diterima: boolean; galat?: string; konfigurasi: unknown };
type Pasang = { diminta: unknown; langkah: Langkah[]; terpasang: unknown };
type Sim = { hari_ini_simulasi: string; objek: { key: string; dt: string; umur_hari: number; tier: string; byte: number }[]; tag_contoh: unknown };
type Aturan = { hari_warm: number; hari_cold: number; hari_archive: number; hari_hapus: number };
const TIER = ["hot", "warm", "cold", "archive"] as const;

function tierUmur(umur: number, a: Aturan): string {
  return umur < a.hari_warm ? "hot" : umur < a.hari_cold ? "warm" : umur < a.hari_archive ? "cold" : umur < a.hari_hapus ? "archive" : "dihapus";
}

export default function Tiering() {
  const [a, setA] = useState<Aturan>({ hari_warm: 30, hari_cold: 395, hari_archive: 1095, hari_hapus: 3653 });
  const [kelas, setKelas] = useState<Kelas[] | null>(null);
  const [pasang, setPasang] = useState<Pasang | null>(null);
  const [geser, setGeser] = useState(0);
  const [sim, setSim] = useState<Sim | null>(null);
  // kalkulator cost (angka ilustrasi slide P03, bukan harga sungguhan)
  const [harga, setHarga] = useState({ hot: 350, warm: 200, cold: 70, archive: 15 });
  const [ambilGb, setAmbilGb] = useState({ warm: 100, cold: 300, archive: 400 });
  const [tb, setTb] = useState(10);
  const [tahun, setTahun] = useState(7);
  const [bacaArsip, setBacaArsip] = useState(0);

  useEffect(() => {
    const t = setTimeout(() => ambil<Sim>(`/api/tier/simulasi?geser_hari=${geser}`, { method: "POST", body: JSON.stringify({ prefix: "bronze/", ...a }) }).then(setSim), 200);
    return () => clearTimeout(t);
  }, [geser, a]);

  const kebijakan = { Rules: [{ ID: "transaksi-tiering", Filter: { Prefix: "bronze/transaksi/" }, Status: "Enabled",
    Transitions: [{ Days: a.hari_warm, StorageClass: "STANDARD_IA" }, { Days: a.hari_cold, StorageClass: "GLACIER_IR" }, { Days: a.hari_archive, StorageClass: "DEEP_ARCHIVE" }],
    Expiration: { Days: a.hari_hapus } }] };

  // kohort bulanan 7 tahun: tier tiap bulan data bila "hari ini" digeser
  const kohort = Array.from({ length: 84 }, (_, i) => { const umur = i * 30 + geser; return { bulan: i, tier: tierUmur(umur, a) }; });

  // cost: volume tetap tb TB disimpan selama tahun*12 bulan; skenario A semua hot, B mengikuti kebijakan
  const biaya = useMemo(() => {
    const gb = tb * 1000, bulan = tahun * 12;
    const A: number[] = [], B: number[] = [], BR: number[] = [];
    const porsi = { hot: 0, warm: 0, cold: 0, archive: 0 } as Record<string, number>;
    let ka = 0, kb = 0, kbr = 0;
    for (let m = 0; m < bulan; m++) {
      const t = tierUmur(m * 30.4 + 15, a); // umur di pertengahan bulan ke-m
      const h = t === "dihapus" ? 0 : harga[t as keyof typeof harga];
      if (t !== "dihapus") porsi[t] += gb * h;
      ka += gb * harga.hot; kb += gb * h;
      // pembacaan ulang arsip: kali per tahun x seluruh volume, dikenai retrieval cost tier saat itu
      const retr = t === "hot" || t === "dihapus" ? 0 : ambilGb[t as keyof typeof ambilGb];
      kbr += gb * h + (bacaArsip / 12) * gb * retr;
      A.push(ka); B.push(kb); BR.push(kbr);
    }
    return { A, B, BR, porsi };
  }, [tb, tahun, harga, ambilGb, bacaArsip, a]);

  const ubahA = (k: keyof Aturan, v: number) => setA({ ...a, [k]: v });
  return (
    <>
      <h1>Kelas penyimpanan, lifecycle policy, dan cost</h1>
      <div className="lebar">
        <section className="kartu"><h2>1. Kelas penyimpanan yang diterima RustFS 1.0.4</h2>
          <button onClick={() => ambil<{ hasil: Kelas[] }>("/api/kelas/coba", { method: "POST" }).then((r) => setKelas(r.hasil))}>Coba PUT dengan x-amz-storage-class</button>
          {kelas && <table><thead><tr><th>Kelas</th><th>Hasil</th><th>Keterangan</th></tr></thead><tbody>
            {kelas.map((k) => <tr key={k.kelas}><td>{k.kelas}</td><td><span className={`lencana ${k.diterima ? "ok" : "gagal"}`}>{k.diterima ? "diterima" : "ditolak"}</span></td>
              <td className="kecil" style={{ whiteSpace: "normal" }}>{k.diterima ? `HEAD: ${k.head_storage_class}` : k.galat}</td></tr>)}</tbody></table>}
          <p className="kecil redup">RustFS hanya punya kelas lokal (STANDARD, REDUCED_REDUNDANCY). Kelas dingin seperti GLACIER hanya ada bila admin mendaftarkan tier remote
            (S3 lain, cloud). Karena itu tier di lab ini disimulasikan, seperti di platform.</p></section>
        <section className="kartu"><h2>2. Lifecycle policy</h2>
          <div className="formulir">
            <label className="isian">hot → warm (hari)<input type="number" value={a.hari_warm} onChange={(e) => ubahA("hari_warm", +e.target.value)} /></label>
            <label className="isian">warm → cold<input type="number" value={a.hari_cold} onChange={(e) => ubahA("hari_cold", +e.target.value)} /></label>
            <label className="isian">cold → archive<input type="number" value={a.hari_archive} onChange={(e) => ubahA("hari_archive", +e.target.value)} /></label>
            <label className="isian">hapus (retensi)<input type="number" value={a.hari_hapus} onChange={(e) => ubahA("hari_hapus", +e.target.value)} /></label>
          </div>
          <pre>{JSON.stringify(kebijakan, null, 1)}</pre>
          <button className="utama" onClick={() => ambil<Pasang>("/api/lifecycle/pasang", { method: "POST", body: JSON.stringify({ prefix: "bronze/", ...a }) }).then(setPasang)}>Pasang di RustFS</button>
          {pasang && <><table><tbody>{pasang.langkah.map((l) => <tr key={l.percobaan}><td style={{ whiteSpace: "normal" }}>{l.percobaan}</td>
            <td><span className={`lencana ${l.diterima ? "ok" : "gagal"}`}>{l.diterima ? "diterima" : "ditolak"}</span></td><td className="kecil" style={{ whiteSpace: "normal" }}>{l.galat}</td></tr>)}</tbody></table>
            <details><summary>aturan yang sekarang terpasang (GetBucketLifecycleConfiguration)</summary><pre>{JSON.stringify(pasang.terpasang, null, 1)}</pre></details></>}
        </section>
      </div>

      <section className="kartu"><h2>3. Simulasi: geser "hari ini"</h2>
        <label className="isian">hari ini + {geser} hari ({(geser / 365).toFixed(1)} tahun) → {sim?.hari_ini_simulasi}
          <input type="range" min={0} max={4000} step={5} value={geser} onChange={(e) => setGeser(+e.target.value)} /></label>
        <div className="baris" style={{ marginTop: 6 }}>{[0, 30, 400, 1830, 3700].map((g) => <button key={g} onClick={() => setGeser(g)}>{g} hari</button>)}</div>
        <h3>30 partisi nyata di s3://lab3/bronze/app/transaksi/</h3>
        <div className="hari">{sim?.objek.map((o) => <div key={o.key} className={`sel-tier tier-${o.tier}`} title={`${o.key}: umur ${o.umur_hari} hari`}>{o.dt.slice(5)}<br />{o.tier}</div>)}</div>
        <h3>Riwayat 7 tahun (84 kohort bulanan, kiri = data terbaru)</h3>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(84, 1fr)", gap: 1 }}>
          {kohort.map((k) => <div key={k.bulan} className={`tier-${k.tier}`} style={{ height: 26 }} title={`data berumur ${k.bulan} bulan: ${k.tier}`} />)}</div>
        <div className="legenda" style={{ marginTop: 6 }}>{["hot", "warm", "cold", "archive", "dihapus"].map((t) => <span key={t}><i className={`tier-${t}`} />{t}</span>)}</div>
        <p className="kecil redup">Tier dihitung API dari umur partisi (tanggal dt) terhadap kebijakan di atas. Tambahkan <code>tandai=true</code> pada
          <code>/api/tier/simulasi</code> untuk menulis tag <code>tier-simulasi</code> ke setiap objek.</p>
      </section>

      <section className="kartu"><h2>4. Kalkulator cost: {tb} TB disimpan {tahun} tahun</h2>
        <div className="formulir">
          <label className="isian">volume (TB)<input type="number" value={tb} min={1} onChange={(e) => setTb(+e.target.value)} /></label>
          <label className="isian">lama simpan (tahun)<input type="number" value={tahun} min={1} max={10} onChange={(e) => setTahun(+e.target.value)} /></label>
          {TIER.map((t) => <label key={t} className="isian">simpan {t} (Rp/GB-bulan)<input type="number" value={harga[t]} onChange={(e) => setHarga({ ...harga, [t]: +e.target.value })} /></label>)}
          {(["warm", "cold", "archive"] as const).map((t) => <label key={t} className="isian">ambil {t} (Rp/GB)<input type="number" value={ambilGb[t]} onChange={(e) => setAmbilGb({ ...ambilGb, [t]: +e.target.value })} /></label>)}
          <label className="isian">seluruh data dibaca ulang (kali/tahun)<input type="number" value={bacaArsip} min={0} max={24} onChange={(e) => setBacaArsip(+e.target.value)} /></label>
        </div>
        <div className="lebar" style={{ marginTop: 10 }}>
          <div><h3>Total biaya</h3>
            <GrafikTumpuk format={rupiah} seri={["hot", "warm", "cold", "archive", "retrieval"]} lebarLabel={190}
              warnaSeri={["var(--s3)", "var(--s2)", "var(--s1)", "var(--s5)", "var(--s4)"]} data={[
              { label: "A: semua hot", bagian: [{ nama: "hot", nilai: biaya.A[biaya.A.length - 1] }] },
              { label: "B: bertingkat", bagian: TIER.map((t) => ({ nama: t, nilai: biaya.porsi[t] })) },
              { label: `B + baca ulang ${bacaArsip}×/thn`, bagian: [...TIER.map((t) => ({ nama: t, nilai: biaya.porsi[t] })), { nama: "retrieval", nilai: biaya.BR[biaya.BR.length - 1] - biaya.B[biaya.B.length - 1] }] },
            ]} />
            <p className="kecil">A = {rupiah(biaya.A[biaya.A.length - 1])}, B = {rupiah(biaya.B[biaya.B.length - 1])} (selisih {(biaya.A[biaya.A.length - 1] / Math.max(1, biaya.B[biaya.B.length - 1])).toFixed(1)} kali).
              Angka bawaan sama dengan contoh slide: hot 1 bulan, warm 12, cold 23, archive 48 (A ≈ Rp 294 jt, B ≈ Rp 50,8 jt). Contoh lifecycle JSON di slide memakai 1825 hari untuk archive; ubah isian untuk membandingkan.</p></div>
          <div><h3>Biaya kumulatif per bulan</h3>
            <GrafikGaris format={rupiah} labelX="bulan" seri={[{ nama: "A semua hot", titik: biaya.A }, { nama: "B bertingkat", titik: biaya.B }, { nama: "B + baca ulang", titik: biaya.BR }]} /></div>
        </div>
        <p className="kecil redup">Harga di sini rasio ilustrasi slide P03, bukan harga penyedia. Naikkan "dibaca ulang" ke 12 untuk kasus tim analitik memindai arsip setiap bulan:
          retrieval cost dapat menghapus penghematan. Tier harus mengikuti pola akses nyata, bukan hanya umur data. Platform menghitung versi dengan harga AWS Jakarta di
          halaman Tiering & cost dashboard (port 52012).</p>
      </section>
    </>
  );
}
