"use client";
import { useState } from "react";
import { useApi, Lencana, Galat, angka } from "@/components/lib";

type Hasil = { nama: string; model: string; kolom: string; dimensi: string; keparahan: string; status: string; failures: number; pesan: string; dicatat: string };
type RunDQ = { run_id: string; tgl: string; terakhir: string; lulus: number; gagal: number; peringatan: number };

const DIMENSI: [string, string][] = [
  ["completeness", "Ada yang hilang?"], ["validity", "Sesuai format dan domain?"], ["uniqueness", "Ada yang ganda?"],
  ["timeliness", "Sampai tepat waktu?"], ["consistency", "Cocok antar tabel/sistem?"], ["accuracy", "Sesuai laporan resmi?"],
];

export default function Mutu() {
  const [run, setRun] = useState("");
  const dq = useApi<{ runs: RunDQ[]; run_id: string; hasil: Hasil[] }>(`dq${run ? `?run_id=${encodeURIComponent(run)}` : ""}`, 5000);
  const gold = useApi<{ per_tgl: any[] }>("gold", 8000);
  const hasil = dq.data?.hasil || [];
  const runDipilih = dq.data?.runs.find(r => r.run_id === dq.data?.run_id);
  const g = gold.data?.per_tgl.find(x => x.tgl === runDipilih?.tgl);
  const adaGagal = hasil.some(h => h.status === "fail" || h.status === "error");
  return (
    <>
      <h1>Data quality: gerbang write-audit-publish</h1>
      <p className="redup">dbt membangun model di database <code>audit</code> (write), <code>dbt test</code> memeriksanya (audit), dan Airflow menyalin partisi ke <code>gold</code> hanya bila semua test berkeparahan error lulus (publish). Test berkeparahan <code>warn</code> tidak menghentikan publikasi.</p>
      <Galat teks={dq.galat} />
      <div className="kartu">
        <div className="baris">
          <label>Run <select value={dq.data?.run_id || ""} onChange={e => setRun(e.target.value)}>
            {(dq.data?.runs || []).map(r => <option key={r.run_id} value={r.run_id}>{r.tgl} · {r.run_id} · lulus {r.lulus}, gagal {r.gagal}, warn {r.peringatan}</option>)}
          </select></label>
          {runDipilih && (adaGagal ? <span className="lencana gagal">gerbang menutup: gold {runDipilih.tgl} tidak dipublikasikan</span> : <span className="lencana ok">gerbang terbuka: gold {runDipilih.tgl} dipublikasikan</span>)}
        </div>
        {g && <p className="redup" style={{ marginTop: 8 }}>Gold {g.tgl} saat ini: {angka(g.gold_trx)} trx, sidik <code>{String(g.gold_sidik || "–").slice(0, 10)}</code>, part terakhir diubah {g.gold_part_diubah || "–"}, publikasi {angka(g.publikasi)} kali. Kontrol core banking: {angka(g.kontrol_trx)} trx. gold_naif: {angka(g.naif_trx)} trx.</p>}
      </div>
      <div className="dimensi">
        {DIMENSI.map(([d, tanya]) => {
          const hs = hasil.filter(h => h.dimensi === d);
          const gagal = hs.filter(h => h.status === "fail" || h.status === "error").length, warn = hs.filter(h => h.status === "warn").length;
          const kelas = gagal ? "gagal" : warn ? "warn" : hs.length ? "ok" : "netral";
          return (
            <div key={d} className="kartu" style={{ borderTop: `4px solid var(--${gagal ? "merah" : warn ? "kuning" : "hijau"})` }}>
              <b>{d}</b><div className="redup">{tanya}</div>
              <div className="angka-besar">{hs.length - gagal - warn}/{hs.length}</div>
              <span className={`lencana ${kelas}`}>{gagal ? `${gagal} gagal` : warn ? `${warn} peringatan` : hs.length ? "lulus" : "belum diuji"}</span>
            </div>
          );
        })}
      </div>
      <div className="kartu">
        <h2>Hasil per test</h2>
        <div className="tabel-wadah">
          <table>
            <thead><tr><th>dimensi</th><th>test</th><th>model</th><th>kolom</th><th>keparahan</th><th>status</th><th className="angka">baris melanggar</th></tr></thead>
            <tbody>{hasil.map((h, i) => (
              <tr key={i} title={h.pesan}><td>{h.dimensi}</td><td>{h.nama}</td><td>{h.model}</td><td>{h.kolom}</td><td>{h.keparahan}</td><td><Lencana s={h.status} /></td><td className="angka">{angka(h.failures)}</td></tr>
            ))}</tbody>
          </table>
        </div>
        <p className="redup">Test generik bawaan dbt: <code>unique</code>, <code>not_null</code>, <code>accepted_values</code>, <code>relationships</code>. Test buatan sendiri: <code>rentang_nilai</code> (macro), serta test singular di <code>dbt/trx_lab/tests/</code> untuk kelengkapan cabang, rekonsiliasi dengan core banking (accuracy), konsistensi agregat, dan latensi ingest (timeliness, warn).</p>
      </div>
    </>
  );
}
