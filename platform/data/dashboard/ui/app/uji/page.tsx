"use client";
import { useState } from "react";
import { Status, Tabel, api, useApi } from "@/components/lib";

const hariLalu = (n: number) => { const d = new Date(Date.now() + 7 * 3600e3); d.setUTCDate(d.getUTCDate() - n); return d.toISOString().slice(0, 10); };

export default function Uji() {
  const [dari, setDari] = useState(hariLalu(8));
  const [sampai, setSampai] = useState(hariLalu(1));
  const [tglQ2, setTglQ2] = useState(hariLalu(1));
  const [pesan, setPesan] = useState("");
  const q1 = useApi<any>(`uji/q1?dari=${dari}&sampai=${sampai}`, 10000);
  const st = useApi<any>("uji/status", 10000);
  const aksi = async (p: Promise<any>, label: string) => { try { const r = await p; setPesan(`${label}: ${JSON.stringify(r).slice(0, 200)}`); } catch (e) { setPesan(`${label}: ${e}`); } };
  const gab = (() => {
    const w = q1.data?.warehouse, l = q1.data?.lake; if (!w || !l) return [];
    return w.baris.map((r: any[]) => { const x = l.baris.find((y: any[]) => y[0] === r[0]) || [];
      return [r[0], r[1], x[1] ?? null, Number(r[1]) - Number(x[1] ?? 0), r[3], x[3] ?? null, Number(r[1]) === Number(r[3]) && Number(x[1]) === Number(x[3]) ? "ya" : "TIDAK"]; });
  })();
  return (
    <>
      <h1>Tiga uji saat demo (slide P08)</h1>
      <p className="redup">Tulis dugaan hasil sebelum menjalankan. Query pemeriksaan ada di <code>platform/data/checks/</code>.</p>
      {pesan && <p className="kartu">{pesan}</p>}
      <section className="kartu">
        <h2>Q1 · Jalankan DAG untuk satu hari dua kali berturut-turut</h2>
        <p className="redup">Harapan: baris dan total nominal di silver dan gold identik setelah eksekusi pertama dan kedua.</p>
        <div className="baris">
          <input type="date" value={sampai} onChange={e => setSampai(e.target.value)} />
          <button className="utama" onClick={() => aksi(api("airflow/trigger", { tgl: sampai }), "run 1")}>Jalankan run 1</button>
          <button onClick={() => aksi(api("airflow/trigger", { tgl: sampai }), "run 2")}>Jalankan run 2</button>
        </div>
      </section>
      <section className="kartu">
        <h2>Q3 · Backfill 7 hari, lalu ulangi backfill yang sama</h2>
        <p className="redup">Harapan: hasil identik, tidak ada partisi hilang atau ganda (baris = trx_unik), kedua sumber sama.</p>
        <div className="baris">
          <label>dari <input type="date" value={dari} onChange={e => setDari(e.target.value)} /></label>
          <label>sampai <input type="date" value={sampai} onChange={e => setSampai(e.target.value)} /></label>
          <button className="utama" onClick={() => aksi(api("airflow/backfill", { dari, sampai }), "backfill")}>Backfill</button>
        </div>
        <h3>Pemeriksaan per tgl (checks/q1_idempoten.sql), warehouse vs lake</h3>
        <Tabel kolom={["tgl", "baris warehouse", "baris lake", "selisih", "trx_unik warehouse", "trx_unik lake", "tanpa duplikat"]} baris={gab} />
      </section>
      <section className="kartu">
        <h2>Q2 · Data rusak di sumber: DQ gagal, DAG berhenti, gold tidak berubah</h2>
        <p className="redup">Menyisipkan berkas bronze berisi duplikat trx_id bernominal beda, kanal di luar domain, rekening_id kosong, dan nominal negatif.</p>
        <div className="baris">
          <input type="date" value={tglQ2} onChange={e => setTglQ2(e.target.value)} />
          <button className="bahaya" onClick={() => aksi(api("uji/sabotase", { aksi: "sisipkan", tgl: tglQ2 }), "sisipkan")}>1. Sisipkan data rusak</button>
          <button onClick={() => aksi(api("airflow/trigger", { tgl: tglQ2 }), "run")}>2. Jalankan DAG</button>
          <button onClick={() => aksi(api("uji/sabotase", { aksi: "pulihkan", tgl: tglQ2 }), "pulihkan")}>3. Pulihkan</button>
          <span>berkas sabotase aktif: {(st.data?.lake?.sabotase_aktif || []).length ? <Status s="gagal" /> : <Status s="lulus" />}</span>
        </div>
        <div className="kisi">
          <div><h3>Gold lake: snapshot terakhir</h3>
            <Tabel kolom={["tabel", "snapshot", "diperbarui (UTC)", "baris"]}
              baris={(st.data?.lake?.iceberg || []).filter((t: any) => t.tabel.startsWith("gold.")).map((t: any) => [t.tabel, String(t.snapshot_id), t.diperbarui?.slice(0, 19), t.baris])} /></div>
          <div><h3>Gold warehouse: part terakhir diubah</h3>
            <Tabel kolom={st.data?.warehouse_gold?.kolom || []} baris={st.data?.warehouse_gold?.baris || []} /></div>
        </div>
        <h3>Test gagal terakhir (pesan menunjuk baris dan aturan)</h3>
        <Tabel kolom={st.data?.dq_gagal_terakhir?.kolom || []} baris={st.data?.dq_gagal_terakhir?.baris || []} />
      </section>
    </>
  );
}
