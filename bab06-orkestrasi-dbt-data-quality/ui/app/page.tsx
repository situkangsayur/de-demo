"use client";
import { useEffect, useMemo, useState } from "react";
import { api, useApi, warna, Lencana, Galat, jam, WARNA } from "@/components/lib";

type Task = { task_id: string; downstream_task_ids: string[]; operator_name: string; trigger_rule: string; retries?: number };
type TI = { task_id: string; state: string | null; try_number: number; max_tries: number; start_date: string | null; end_date: string | null };
type Run = { dag_run_id: string; tgl: string; state: string; run_type: string; queued_at: string | null; start_date: string | null; end_date: string | null; tasks: Record<string, TI> };
type Status = { hari: { tgl: string; siap: boolean; sabotase: boolean; berkas: number }[]; flaky: boolean; gagal_pertama: number; airflow_ok: boolean; sla_menit: string };

// Tingkat (level) setiap task = jalur terpanjang dari akar; dipakai untuk tata letak graph dan urutan kolom grid.
function tingkat(tasks: Task[]) {
  const hulu: Record<string, string[]> = {};
  tasks.forEach(t => { hulu[t.task_id] ||= []; t.downstream_task_ids.forEach(d => { (hulu[d] ||= []).push(t.task_id); }); });
  const lv: Record<string, number> = {};
  const hitung = (id: string): number => {
    if (lv[id] !== undefined) return lv[id];
    lv[id] = (hulu[id] || []).length ? Math.max(...hulu[id].map(hitung)) + 1 : 0;
    return lv[id];
  };
  tasks.forEach(t => hitung(t.task_id));
  return lv;
}

function GraphDAG({ tasks, run }: { tasks: Task[]; run?: Run }) {
  const lv = tingkat(tasks);
  const kol: Record<number, string[]> = {};
  tasks.forEach(t => (kol[lv[t.task_id]] ||= []).push(t.task_id));
  const W = 168, H = 40, GX = 46, GY = 16;
  const pos: Record<string, { x: number; y: number }> = {};
  const maksBaris = Math.max(...Object.values(kol).map(k => k.length));
  Object.entries(kol).forEach(([l, ids]) => ids.sort().forEach((id, i) => {
    const off = (maksBaris - ids.length) * (H + GY) / 2;
    pos[id] = { x: Number(l) * (W + GX) + 4, y: off + i * (H + GY) + 4 };
  }));
  const lebar = (Math.max(...Object.values(lv)) + 1) * (W + GX), tinggi = maksBaris * (H + GY) + 8;
  return (
    <div style={{ overflowX: "auto" }}>
      <svg width={lebar} height={tinggi} role="img" aria-label="Graph DAG trx_harian">
        {tasks.flatMap(t => t.downstream_task_ids.map(d => {
          const a = pos[t.task_id], b = pos[d];
          if (!a || !b) return null;
          const x1 = a.x + W, y1 = a.y + H / 2, x2 = b.x, y2 = b.y + H / 2;
          return <path key={t.task_id + d} d={`M${x1},${y1} C${x1 + 24},${y1} ${x2 - 24},${y2} ${x2},${y2}`} fill="none" stroke="var(--redup)" strokeWidth={1.2} markerEnd="url(#panah)" />;
        }))}
        <defs><marker id="panah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="var(--redup)" /></marker></defs>
        {tasks.map(t => {
          const p = pos[t.task_id]; const ti = run?.tasks[t.task_id];
          return (
            <g key={t.task_id} className="simpul" transform={`translate(${p.x},${p.y})`}>
              <title>{`${t.task_id} (${t.operator_name}) · status ${ti?.state || "-"} · percobaan ${ti?.try_number ?? 0}/${ti?.max_tries ?? "-"}`}</title>
              <rect width={W} height={H} rx={7} fill="var(--panel)" />
              <rect width={7} height={H} rx={3} fill={warna(ti?.state)} />
              <text x={14} y={16} style={{ fontWeight: 600 }}>{t.task_id}</text>
              <text x={14} y={31} style={{ fill: "var(--redup)", fontSize: 10 }}>
                {t.operator_name}{ti?.state ? ` · ${ti.state}` : ""}{ti && ti.try_number > 1 ? ` · coba ${ti.try_number}` : ""}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function Gantt({ runId }: { runId: string }) {
  const { data } = useApi<{ tries: Record<string, TI[]> }>(runId ? `run/tries?run_id=${encodeURIComponent(runId)}` : null, 4000);
  if (!data) return <p className="redup">Memuat percobaan…</p>;
  const semua = Object.values(data.tries).flat().filter(t => t.start_date);
  if (!semua.length) return <p className="redup">Belum ada task yang berjalan.</p>;
  const t0 = Math.min(...semua.map(t => +new Date(t.start_date!)));
  const t1 = Math.max(...semua.map(t => +new Date(t.end_date || Date.now())));
  const span = Math.max(t1 - t0, 1000), L = 190, W = 640, BH = 18;
  const tasks = Object.keys(data.tries).sort((a, b) => {
    const sa = Math.min(...data.tries[a].map(x => +new Date(x.start_date || t1))), sb = Math.min(...data.tries[b].map(x => +new Date(x.start_date || t1)));
    return sa - sb;
  });
  const x = (s: string) => L + ((+new Date(s) - t0) / span) * W;
  return (
    <div style={{ overflowX: "auto" }}>
      <svg width={L + W + 20} height={tasks.length * (BH + 6) + 30} role="img" aria-label="Gantt percobaan task">
        {[0, 0.25, 0.5, 0.75, 1].map(f => (
          <g key={f}><line x1={L + f * W} x2={L + f * W} y1={0} y2={tasks.length * (BH + 6)} stroke="var(--garis)" />
            <text x={L + f * W} y={tasks.length * (BH + 6) + 14} textAnchor="middle">+{Math.round((f * span) / 1000)} dtk</text></g>
        ))}
        {tasks.map((task, i) => (
          <g key={task} transform={`translate(0,${i * (BH + 6)})`}>
            <text x={0} y={BH - 5} style={{ fill: "var(--teks)" }}>{task}</text>
            {data.tries[task].filter(t => t.start_date).map(t => {
              const a = x(t.start_date!), b = Math.max(x(t.end_date || new Date().toISOString()), a + 3);
              return (
                <g key={t.try_number}>
                  <title>{`${task} percobaan ${t.try_number}: ${t.state} (${jam(t.start_date)}–${jam(t.end_date)})`}</title>
                  <rect x={a} y={0} width={b - a} height={BH} rx={3} fill={warna(t.state)} />
                  {b - a > 14 && <text x={a + 3} y={BH - 5} style={{ fill: "#fff", fontSize: 10 }}>{t.try_number}</text>}
                </g>
              );
            })}
          </g>
        ))}
      </svg>
      <p className="redup">Setiap batang = satu percobaan (angka = try_number). Celah antara batang task yang sama adalah jeda retry; pada task flaky jedanya membesar (exponential backoff). Sensor mode reschedule tetap satu percobaan: di antara poke statusnya up_for_reschedule dan slot worker dilepas.</p>
    </div>
  );
}

export default function Orkestrasi() {
  const dag = useApi<{ tasks: Task[] }>("dag", 15000);
  const runs = useApi<{ runs: Run[] }>("runs?limit=15", 3000);
  const st = useApi<Status>("status", 4000);
  const sla = useApi<{ terlewat: any[]; sla_menit: string }>("sla", 8000);
  const [tgl, setTgl] = useState("");
  const [dari, setDari] = useState("");
  const [sampai, setSampai] = useState("");
  const [pilih, setPilih] = useState("");
  const [pesan, setPesan] = useState("");

  const hari = st.data?.hari || [];
  useEffect(() => {
    if (hari.length && !tgl) {
      setTgl(hari[hari.length - 1].tgl);
      setDari(hari[Math.max(0, hari.length - 4)].tgl);
      setSampai(hari[hari.length - 2]?.tgl || hari[0].tgl);
    }
  }, [hari, tgl]);

  const tasks = dag.data?.tasks || [];
  const lv = useMemo(() => tingkat(tasks), [tasks]);
  const urut = [...tasks].sort((a, b) => lv[a.task_id] - lv[b.task_id] || a.task_id.localeCompare(b.task_id)).map(t => t.task_id);
  const daftarRun = runs.data?.runs || [];
  const runPilih = daftarRun.find(r => r.dag_run_id === pilih) || daftarRun[0];
  const hariPilih = hari.find(h => h.tgl === tgl);

  async function aksi(fn: () => Promise<any>, label: string) {
    setPesan(`${label}…`);
    try { const r = await fn(); setPesan(`${label}: ${JSON.stringify(r)}`); } catch (e) { setPesan(`${label} gagal: ${e}`); }
    st.segarkan(); runs.segarkan();
  }

  return (
    <>
      <h1>Orkestrasi: DAG <code>trx_harian</code></h1>
      <p className="redup">Airflow memicu dan memantau; ClickHouse dan dbt yang mengolah. Setiap run membawa tanggal logisnya sendiri (kolom tgl = logical_date dalam WIB), sehingga run 16 Nov selalu mengolah data 16 Nov kapan pun dijalankan.</p>
      <Galat teks={dag.galat || runs.galat} />

      <div className="kartu">
        <h2>Kendali demo</h2>
        <div className="baris">
          <label>Tanggal <select value={tgl} onChange={e => setTgl(e.target.value)}>
            {hari.map(h => <option key={h.tgl} value={h.tgl}>{h.tgl}{h.siap ? "" : " (tertahan)"}{h.sabotase ? " (rusak)" : ""}</option>)}
          </select></label>
          <button className="utama" onClick={() => aksi(() => api("jalankan", { tgl }), `Jalankan ${tgl}`)}>Jalankan / ulang</button>
          {hariPilih?.siap
            ? <button onClick={() => aksi(() => api("kendali", { aksi: "tahan", tgl }), `Tahan data ${tgl}`)}>Tahan data (sensor menunggu)</button>
            : <button onClick={() => aksi(() => api("kendali", { aksi: "kirim", tgl }), `Kirim data ${tgl}`)}>Kirim data (_SUCCESS)</button>}
          {hariPilih?.sabotase
            ? <button onClick={() => aksi(() => api("kendali", { aksi: "pulihkan", tgl }), `Pulihkan ${tgl}`)}>Pulihkan data</button>
            : <button className="bahaya" onClick={() => aksi(() => api("kendali", { aksi: "sabotase", tgl }), `Sisipkan data rusak ${tgl}`)}>Sisipkan data rusak</button>}
          <button className={st.data?.flaky ? "bahaya" : ""} onClick={() => aksi(() => api("kendali", { aksi: st.data?.flaky ? "flaky_off" : "flaky_on" }), "Sumber flaky")}>
            Core banking flaky: {st.data?.flaky ? `ON (${st.data.gagal_pertama} panggilan pertama 503)` : "OFF"}
          </button>
        </div>
        <div className="baris" style={{ marginTop: 8 }}>
          <span>Backfill</span>
          <select value={dari} onChange={e => setDari(e.target.value)}>{hari.map(h => <option key={h.tgl}>{h.tgl}</option>)}</select>
          <span>s.d.</span>
          <select value={sampai} onChange={e => setSampai(e.target.value)}>{hari.map(h => <option key={h.tgl}>{h.tgl}</option>)}</select>
          <button onClick={() => aksi(() => api("backfill", { dari, sampai }), `Backfill ${dari}..${sampai}`)}>Buat backfill</button>
          <span className="redup">POST /api/v2/backfills, reprocess_behavior=completed, max_active_runs=1</span>
        </div>
        {pesan && <pre style={{ marginTop: 8, whiteSpace: "pre-wrap" }}>{pesan}</pre>}
      </div>

      <div className="kartu">
        <h2>Graph DAG {runPilih && <span className="redup">· run {runPilih.dag_run_id} ({runPilih.tgl}) <Lencana s={runPilih.state} /></span>}</h2>
        {tasks.length ? <GraphDAG tasks={tasks} run={runPilih} /> : <p className="redup">Menunggu DAG diparse Airflow…</p>}
        <div className="legenda">{["success", "running", "failed", "upstream_failed", "up_for_retry", "up_for_reschedule", "queued", "skipped"].map(s => <span key={s}><i style={{ background: WARNA[s] }} />{s}</span>)}</div>
      </div>

      <div className="kartu">
        <h2>Riwayat run (grid)</h2>
        <p className="redup">Baris = DAG run (terbaru di atas), kolom = task. Angka di sel = jumlah percobaan bila lebih dari satu. Klik baris untuk melihat graph dan Gantt-nya.</p>
        <div className="tabel-wadah" style={{ maxHeight: 520 }}>
          <table className="grid-run">
            <thead><tr><th>tgl</th><th>tipe</th><th>status</th><th>mulai</th>{urut.map(t => <th key={t} className="rot">{t}</th>)}</tr></thead>
            <tbody>
              {daftarRun.map(r => (
                <tr key={r.dag_run_id} className={runPilih?.dag_run_id === r.dag_run_id ? "pilih" : ""} onClick={() => setPilih(r.dag_run_id)}>
                  <td><b>{r.tgl}</b></td><td className="redup">{r.run_type}</td><td><Lencana s={r.state} /></td><td>{jam(r.start_date)}</td>
                  {urut.map(t => { const ti = r.tasks[t]; return (
                    <td key={t} className="sel" style={{ background: warna(ti?.state) }} title={`${t}: ${ti?.state || "-"} (percobaan ${ti?.try_number ?? 0})`}>
                      {ti && ti.try_number > 1 ? ti.try_number : ""}
                    </td>); })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="kisi">
        <div className="kartu">
          <h2>Percobaan task (retry, backoff, sensor)</h2>
          {runPilih ? <Gantt runId={runPilih.dag_run_id} /> : <p className="redup">Belum ada run.</p>}
        </div>
        <div className="kartu">
          <h2>Bronze per tanggal</h2>
          <table><thead><tr><th>dt</th><th>_SUCCESS</th><th className="angka">berkas</th><th>data rusak</th></tr></thead>
            <tbody>{hari.map(h => <tr key={h.tgl}><td>{h.tgl}</td><td>{h.siap ? <span className="lencana ok">ada</span> : <span className="lencana warn">ditahan</span>}</td><td className="angka">{h.berkas}</td><td>{h.sabotase ? <span className="lencana gagal">disisipkan</span> : ""}</td></tr>)}</tbody></table>
          <h3>SLA (Deadline Alert, {sla.data?.sla_menit || "?"} menit sejak run masuk antrean)</h3>
          {sla.data?.terlewat?.length
            ? <table><thead><tr><th>run</th><th>tgl</th><th>batas</th><th>dicatat</th></tr></thead>
                <tbody>{sla.data.terlewat.map((s, i) => <tr key={i}><td>{s.run_id}</td><td>{s.tgl}</td><td>{String(s.batas).slice(0, 19)}</td><td>{s.dicatat}</td></tr>)}</tbody></table>
            : <p className="redup">Belum ada run yang melewati batas.</p>}
        </div>
      </div>
    </>
  );
}
