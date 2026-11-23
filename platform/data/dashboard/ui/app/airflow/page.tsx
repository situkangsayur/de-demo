"use client";
import { useState } from "react";
import { Status, Tabel, api, useApi } from "@/components/lib";

const URUT = ["tentukan_tgl", "cek_bronze", "etl.silver_cabang", "etl.silver_nasabah", "etl.silver_rekening",
  "etl.silver_akad_pembiayaan", "etl.silver_transaksi", "etl.dq_silver", "etl.gold_siapkan_staging", "etl.dq_gold",
  "etl.gold_publikasi", "elt.muat_raw_clickhouse", "elt.dbt_build_gold", "rekonsiliasi_lake_vs_warehouse", "selesai"];

export default function Airflow() {
  const { data, galat, segarkan } = useApi<any>("airflow/runs", 5000);
  const [pilih, setPilih] = useState<string>("");
  const runs: any[] = data?.runs?.dag_runs || [];
  const aktif = pilih || runs[0]?.dag_run_id || "";
  const { data: ti } = useApi<any>(aktif ? `airflow/runs/${encodeURIComponent(aktif)}/tasks` : null, 5000);
  const [tgl, setTgl] = useState("");
  const [pesan, setPesan] = useState("");
  const tasks: any[] = (ti?.task_instances || []).sort((a: any, b: any) => URUT.indexOf(a.task_id) - URUT.indexOf(b.task_id));
  return (
    <>
      <h1>Status DAG <code>bank_harian</code></h1>
      <p className="redup">Dibaca dari Airflow REST API v2 (JWT). UI Airflow lengkap ada di port 52010 (Graph view memperlihatkan grup <b>etl</b> dan <b>elt</b>).
        Jadwal: <code>{data?.dag?.timetable_summary ?? "-"}</code> · paused: {String(data?.dag?.is_paused ?? "-")}</p>
      {galat && <p className="galat">{galat}</p>}
      <div className="kartu baris">
        <label>Jalankan untuk tgl <input type="date" value={tgl} onChange={e => setTgl(e.target.value)} /></label>
        <button className="utama" disabled={!tgl} onClick={async () => {
          try { const r = await api("airflow/trigger", { tgl }); setPesan(`run ${r.dag_run_id} dibuat`); segarkan(); } catch (e) { setPesan(String(e)); }
        }}>Picu run</button>
        <span className="redup">{pesan}</span>
      </div>
      <div className="kisi">
        <div className="kartu">
          <h2>Run terakhir</h2>
          <div className="tabel-wadah"><table><thead><tr><th>run_id</th><th>status</th><th>tgl (conf)</th><th>mulai</th><th>selesai</th></tr></thead>
            <tbody>{runs.map(r => (
              <tr key={r.dag_run_id} onClick={() => setPilih(r.dag_run_id)} style={{ cursor: "pointer", background: r.dag_run_id === aktif ? "var(--utama-muda)" : undefined }}>
                <td>{r.dag_run_id}</td><td><Status s={r.state} /></td><td>{r.conf?.tgl ?? (r.logical_date || "").slice(0, 10)}</td>
                <td>{(r.start_date || "").slice(0, 19)}</td><td>{(r.end_date || "").slice(0, 19)}</td></tr>))}</tbody></table></div>
        </div>
        <div className="kartu">
          <h2>Task di run {aktif}</h2>
          <Tabel kolom={["task", "status", "percobaan", "durasi (s)"]}
            baris={tasks.map(t => [t.task_id, t.state ?? "-", t.try_number, t.duration ? Number(t.duration.toFixed(1)) : null])} />
          <p className="redup">Task DQ (dq_silver, dq_gold, dbt_build_gold, rekonsiliasi) tidak di-retry; task lain retries=2.</p>
        </div>
      </div>
      <div className="kartu">
        <h2>Backfill</h2>
        <Tabel kolom={["id", "dari", "sampai", "reprocess", "dibuat", "selesai"]}
          baris={(data?.backfills?.backfills || []).map((b: any) => [b.id, b.from_date?.slice(0, 10), b.to_date?.slice(0, 10), b.reprocess_behavior, b.created_at?.slice(0, 19), b.completed_at?.slice(0, 19) ?? "-"])} />
      </div>
    </>
  );
}
