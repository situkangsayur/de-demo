"use client";
import { useState } from "react";
import { byte, Galat, rupiah, Status, Tabel, useAksi, useApi } from "@/components/lib";

type Cek = { aturan: string; dimensi: string; pelanggar: number; lulus: boolean; contoh: Record<string, unknown>[] };
type Langkah = { langkah: string; ms: number; lulus: boolean; cek?: Cek[]; statistik?: any; baris?: any; pesan?: string; dilewati?: boolean };
type Run = { run_id: string; waktu: string; hari: string[]; lulus: boolean; langkah: Langkah[] };

export default function Medallion() {
  const br = useApi<any>("bronze");
  const lp = useApi<{ tabel: any[]; riwayat: Run[] }>("medallion/lapisan");
  const gold = useApi<any>("medallion/gold");
  const kar = useApi<any>("medallion/karantina");
  const [entitas, setEntitas] = useState("transaksi");
  const contoh = useApi<any>(`bronze/contoh?entitas=${entitas}`);
  const segarkan = () => { br.segarkan(); lp.segarkan(); gold.segarkan(); kar.segarkan(); contoh.segarkan(); };
  const a = useAksi(segarkan);
  const run = lp.data?.riwayat?.[0];
  const tab = (n: string) => lp.data?.tabel.find(t => t.tabel === n);
  const g1 = run?.langkah[1], g2 = run?.langkah[2];

  return (
    <>
      <h1>Medallion: bronze → silver → gold</h1>
      <p className="redup">Bronze = Parquet mentah dari ingestion (skema platform: 8 kolom metadata, kolom data string).
        Silver dan gold = tabel Apache Iceberg di RustFS. Di antara lapisan ada gerbang data quality.</p>

      <div className="kartu">
        <div className="baris">
          <button onClick={() => a.jalankan("bronze/tambah-hari?hari=1", "Tambah 1 hari bronze")} disabled={!!a.sibuk}>+ 1 hari bronze</button>
          <button className="utama" onClick={() => a.jalankan("medallion/jalankan", "Jalankan pipeline")} disabled={!!a.sibuk}>Jalankan pipeline</button>
          <button className="bahaya" onClick={() => a.jalankan("bronze/kotor", "Sisipkan data kotor")} disabled={!!a.sibuk || br.data?.kotor}>Sisipkan data kotor</button>
          <button onClick={() => a.jalankan("bronze/pulihkan", "Pulihkan bronze")} disabled={!!a.sibuk || !br.data?.kotor}>Pulihkan (hapus berkas kotor)</button>
          {br.data?.platform_tersedia && <button onClick={() => a.jalankan("bronze/dari-platform?hari=2", "Ambil bronze platform")} disabled={!!a.sibuk}>Ambil bronze dari platform</button>}
          {a.sibuk && <span className="lencana warn">{a.sibuk}…</span>}
          {br.data?.kotor && <span className="lencana gagal">bronze berisi berkas kotor</span>}
        </div>
        <Galat teks={a.galat || br.galat || lp.galat} />
      </div>

      <div className="kartu">
        <h2>Alur dan gerbang {run && <span className="redup">· run terakhir {run.run_id} <Status s={run.lulus ? "lulus" : "gagal"} /></span>}</h2>
        <div className="alur">
          <div className="lapis bronze">
            <b>Bronze</b> <span className="redup">Parquet, append-only</span>
            {br.data?.entitas?.map((e: any) => (
              <div key={e.entitas} className="kecil">{e.entitas}: {e.berkas} berkas ({e.app} app, {e.cdc} cdc), {byte(e.byte)}</div>))}
            <div className="redup">hari dibangkitkan: {br.data?.hari ?? "…"}</div>
          </div>
          <div className="panah">→</div>
          <div className={`gerbang ${g1 ? (g1.lulus ? "lulus" : "gagal") : ""}`}>gerbang 1<br />{g1 ? (g1.lulus ? "lulus" : "GAGAL") : "-"}</div>
          <div className="panah">→</div>
          <div className="lapis silver">
            <b>Silver</b> <span className="redup">Iceberg, overwrite per hari</span>
            {["silver.transaksi", "silver.nasabah", "silver.rekening", "silver.akad_pembiayaan", "silver.akad_karantina", "silver.karantina_transaksi"].map(n => {
              const t = tab(n); return <div key={n}>{n.replace("silver.", "")}: {t?.ada ? `${rupiah(t.baris)} baris, ${t.berkas} berkas, ${t.snapshot} snapshot` : "belum ada"}</div>;
            })}
          </div>
          <div className="panah">→</div>
          <div className={`gerbang ${g2 && !g2.dilewati ? (g2.lulus ? "lulus" : "gagal") : ""}`}>gerbang 2<br />{g2 ? (g2.dilewati ? "dilewati" : g2.lulus ? "lulus" : "GAGAL") : "-"}</div>
          <div className="panah">→</div>
          <div className="lapis gold">
            <b>Gold</b> <span className="redup">Iceberg, dibangun ulang utuh</span>
            {["gold.fakta_trx_harian", "gold.pembiayaan_per_akad"].map(n => {
              const t = tab(n); return <div key={n}>{n.replace("gold.", "")}: {t?.ada ? `${rupiah(t.baris)} baris, ${t.snapshot} snapshot` : "belum ada"}</div>;
            })}
          </div>
        </div>
        {g1?.statistik && (
          <p>Silver transaksi: {rupiah(g1.statistik.baris_bronze)} baris bronze → {rupiah(g1.statistik.baris_unik)} transaksi unik
            ({rupiah(g1.statistik.duplikat_dibuang)} duplikat app vs cdc dibuang; sumber {Object.entries(g1.statistik.sumber).map(([k, v]) => `${k}=${v}`).join(", ")});
            {" "}{g1.statistik.kanal_dibakukan} kode kanal dibakukan; {g1.statistik.terlambat} transaksi terlambat (&gt; 1 menit).</p>)}
      </div>

      {run && (
        <div>
          {run.langkah.map(l => (
            <div className="kartu" key={l.langkah}>
              <h3>{l.langkah} <Status s={l.dilewati ? "dilewati" : l.lulus ? "lulus" : "gagal"} /> <span className="redup">{l.ms} ms</span></h3>
              {l.pesan && <p className="redup">{l.pesan}</p>}
              {l.baris && <p className="redup">{Object.entries(l.baris).map(([k, v]) => `${k}: ${v}`).join(" · ")}</p>}
              {l.cek && l.cek.length > 0 && (
                <div className="tabel-wadah"><table><thead><tr><th>aturan</th><th>dimensi</th><th className="angka">pelanggar</th><th>contoh</th></tr></thead>
                  <tbody>{l.cek.map(c => (
                    <tr key={c.aturan}><td style={{ whiteSpace: "normal" }}><Status s={c.lulus ? "lulus" : "gagal"} /> {c.aturan}</td><td>{c.dimensi}</td>
                      <td className="angka">{c.pelanggar}</td><td><code>{c.contoh.length ? JSON.stringify(c.contoh[0]) : ""}</code></td></tr>))}
                  </tbody></table></div>)}
            </div>))}
        </div>)}

      <div className="kartu">
        <h2>Isi bronze (partisi dt terakhir)</h2>
        <div className="baris">
          {["transaksi", "nasabah", "rekening", "akad_pembiayaan", "cabang"].map(e => (
            <button key={e} className={e === entitas ? "utama" : ""} onClick={() => setEntitas(e)}>{e}</button>))}
        </div>
        <p className="redup">Perhatikan: semua kolom data bertipe String; satu transaksi muncul dua kali (app dan cdc); kanal belum seragam; NIK dan no HP masih mentah.</p>
        {contoh.data?.kolom && <Tabel kolom={contoh.data.kolom} baris={contoh.data.baris} />}
        {contoh.data?.skema && <details><summary>skema Parquet</summary><pre>{Object.entries(contoh.data.skema).map(([k, v]) => `${k}: ${v}`).join("\n")}</pre></details>}
      </div>

      <div className="kisi">
        <div className="kartu">
          <h2>Gold: fakta_trx_harian</h2>
          <p className="redup">Tanpa PII; per tanggal WIB, cabang, kanal.</p>
          {gold.data && <Tabel kolom={gold.data.kolom} baris={gold.data.baris} maks={60} />}
        </div>
        <div className="kartu">
          <h2>Karantina</h2>
          {kar.data && Object.entries(kar.data).map(([k, v]: any) => (
            <div key={k}><h3>{k} <span className="redup">({v.total} baris)</span></h3>
              <Tabel kolom={v.kolom} baris={v.baris} maks={10} /></div>))}
        </div>
      </div>

      {lp.data?.riwayat && lp.data.riwayat.length > 1 && (
        <div className="kartu">
          <h2>Riwayat run</h2>
          <Tabel kolom={["run_id", "waktu", "hari", "hasil"]} baris={lp.data.riwayat.map(r => [r.run_id, r.waktu, r.hari.join(", "), r.lulus ? "lulus" : "gagal"])} />
        </div>)}
      {a.pesan && <p className="redup">Terakhir: {a.pesan}</p>}
    </>
  );
}
