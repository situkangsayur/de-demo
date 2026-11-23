"use client";

// Halaman "Aliran data": transaksi di aplikasi -> NATS JetStream (BANK/SHOP) dan
// WAL Postgres -> Debezium -> NATS (CDC) -> ingestor -> bronze Parquet di RustFS, secara live.
import { useEffect, useMemo, useRef, useState } from "react";
import { api, bytes, fmt, jam } from "./lib";
import Envelope from "./components/Envelope";
import Sparkline from "./components/Sparkline";

type Consumer = { name: string; num_pending: number; num_ack_pending: number; delivered_seq: number; num_redelivered: number };
type Stream = { name: string; kind: string; exists: boolean; messages: number; bytes: number; last_time?: string; max_age: string; subjects: Record<string, number>; consumers: Consumer[] };
type Partition = { source: string; entity: string; dt: string; files: number; rows: number; bytes: number; rows_unknown: number };
type Bronze = {
  flush: { seconds: number; max: number; buffered: number };
  consumers: Record<string, { durable: string; status: string; received: number; acked: number }>;
  batches: number;
  last_flush?: { at: string; messages: number; rows: number; files: number };
  last_error?: string;
  recent_files: { path: string; rows: number; at: string; entity: string; source: string }[];
  partitions: Partition[];
  totals: { files: number; rows: number; bytes: number };
};
type Stats = { streams: Stream[]; rates: Record<string, number>; series: { t: number; app: number; cdc: number }[]; bronze?: Bronze; bronze_error?: string };
type Live = { at: string; subject: string; stream: string; kind: "app" | "cdc"; bytes: number; msg_id?: string; op?: string; preview: string };

const opLabel: Record<string, string> = { c: "insert", u: "update", d: "delete", r: "snapshot" };

export default function Page() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [err, setErr] = useState("");
  const [feed, setFeed] = useState<Live[]>([]);
  const [dots, setDots] = useState<{ id: number; kind: string }[]>([]);
  const [live, setLive] = useState(true);
  const [subject, setSubject] = useState("");
  const [filter, setFilter] = useState("");
  const dotId = useRef(0);
  const liveRef = useRef(live);
  liveRef.current = live;

  useEffect(() => {
    let stop = false;
    const tick = async () => {
      try {
        const s = await api<Stats>("flow/stats");
        if (!stop) {
          setStats(s);
          setErr("");
        }
      } catch (e) {
        if (!stop) setErr(String(e));
      }
    };
    tick();
    const t = setInterval(tick, 2000);
    return () => {
      stop = true;
      clearInterval(t);
    };
  }, []);

  useEffect(() => {
    // ?nolive=1 mematikan feed SSE (mis. untuk tangkapan layar headless atau jaringan lambat)
    if (new URLSearchParams(window.location.search).has("nolive")) return;
    const es = new EventSource("/api/ops/flow/live");
    es.onmessage = (ev) => {
      if (!liveRef.current) return;
      const m = JSON.parse(ev.data) as Live;
      setFeed((f) => [m, ...f].slice(0, 120));
      const id = ++dotId.current;
      setDots((d) => [...d.slice(-30), { id, kind: m.kind }]);
      setTimeout(() => setDots((d) => d.filter((x) => x.id !== id)), 900);
    };
    return () => es.close();
  }, []);

  const byName = useMemo(() => Object.fromEntries((stats?.streams ?? []).map((s) => [s.name, s])), [stats]);
  const rate = (prefix: string) => Object.entries(stats?.rates ?? {}).filter(([k]) => k.startsWith(prefix)).reduce((a, [, v]) => a + v, 0);
  const lag = (names: string[]) => names.reduce((a, n) => a + (byName[n]?.consumers.find((c) => c.name.startsWith("de_bronze"))?.num_pending ?? 0), 0);
  const ackp = (names: string[]) => names.reduce((a, n) => a + (byName[n]?.consumers.find((c) => c.name.startsWith("de_bronze"))?.num_ack_pending ?? 0), 0);
  const b = stats?.bronze;
  const part = (src: string) => (b?.partitions ?? []).filter((p) => p.source === src);
  const sum = (ps: Partition[], k: "files" | "rows" | "bytes") => ps.reduce((a, p) => a + p[k], 0);
  const appFeed = feed.filter((m) => m.kind === "app").slice(0, 40);
  const cdcFeed = feed.filter((m) => m.kind === "cdc").slice(0, 40);
  const subjects = Array.from(new Set((stats?.streams ?? []).flatMap((s) => Object.keys(s.subjects)))).sort();
  const parts = (b?.partitions ?? []).filter((p) => !filter || `${p.source}/${p.entity}`.includes(filter)).sort((x, y) => (x.dt < y.dt ? 1 : -1));

  const Arrow = ({ kind }: { kind: string }) => (
    <div className={`arrow ${kind}`}>
      {dots.filter((d) => d.kind === kind).map((d) => (
        <span key={d.id} className="dot" />
      ))}
    </div>
  );

  return (
    <>
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 12 }}>
        <div>
          <h1>Aliran data: aplikasi → NATS JetStream → bronze</h1>
          <div className="muted">
            Jalur <span className="pill app">app</span> event bisnis dari outbox aplikasi · jalur <span className="pill cdc">cdc</span> perubahan WAL ditangkap Debezium · ingestor ack sesudah Parquet tertulis
          </div>
        </div>
        <div className="row">
          <label className="row small">
            <input type="checkbox" checked={live} onChange={(e) => setLive(e.target.checked)} /> feed live
          </label>
          {err && <span className="pill bad">{err}</span>}
        </div>
      </div>

      <div className="card" style={{ marginBottom: 12 }}>
        <div className="flow">
          <div className="node app">
            <b>Aplikasi Sister</b>
            <span className="small muted">bank + toko · transactional outbox</span>
            <div className="big">{rate("bank.").toFixed(1)} + {rate("shop.").toFixed(1)} msg/s</div>
          </div>
          <Arrow kind="app" />
          <div className="node app">
            <b>NATS: BANK · SHOP</b>
            <span className="small muted">bank.&lt;entitas&gt;.&lt;aksi&gt; · shop.*</span>
            <div className="big">{fmt(byName.BANK?.messages)} · {fmt(byName.SHOP?.messages)}</div>
            {!byName.BANK?.exists && <span className="pill warn">stream BANK belum ada</span>}
          </div>
          <Arrow kind="app" />
          <div className="node app">
            <b>Ingestor (app)</b>
            <span className="small muted">lag {fmt(lag(["BANK", "SHOP"]))} · menunggu ack {fmt(ackp(["BANK", "SHOP"]))}</span>
            <div className="small">
              {b?.consumers?.BANK?.status ?? "-"} / {b?.consumers?.SHOP?.status ?? "-"}
            </div>
          </div>
          <Arrow kind="app" />
          <div className="node store">
            <b>bronze/app</b>
            <span className="small muted">s3://lake/bronze/app/…</span>
            <div className="big">{fmt(sum(part("app"), "rows"))} baris</div>
            <span className="small muted">{fmt(sum(part("app"), "files"))} file · {bytes(sum(part("app"), "bytes"))}</span>
          </div>

          <div className="node cdc" style={{ marginTop: 10 }}>
            <b>Postgres WAL → Debezium</b>
            <span className="small muted">database bank + orders · pgoutput</span>
            <div className="big">{rate("cdc.").toFixed(1)} msg/s</div>
          </div>
          <Arrow kind="cdc" />
          <div className="node cdc" style={{ marginTop: 10 }}>
            <b>NATS: CDC</b>
            <span className="small muted">cdc.&lt;database&gt;.&lt;tabel&gt;</span>
            <div className="big">{fmt(byName.CDC?.messages)}</div>
          </div>
          <Arrow kind="cdc" />
          <div className="node cdc" style={{ marginTop: 10 }}>
            <b>Ingestor (cdc)</b>
            <span className="small muted">lag {fmt(lag(["CDC"]))} · menunggu ack {fmt(ackp(["CDC"]))}</span>
            <div className="small">
              batch tiap {b?.flush.seconds ?? "?"} s / {fmt(b?.flush.max)} pesan · buffer {fmt(b?.flush.buffered)}
            </div>
          </div>
          <Arrow kind="cdc" />
          <div className="node store" style={{ marginTop: 10 }}>
            <b>bronze/cdc</b>
            <span className="small muted">s3://lake/bronze/cdc/…</span>
            <div className="big">{fmt(sum(part("cdc"), "rows"))} baris</div>
            <span className="small muted">{fmt(sum(part("cdc"), "files"))} file · {bytes(sum(part("cdc"), "bytes"))}</span>
          </div>
        </div>
        <div className="row small muted" style={{ marginTop: 8, justifyContent: "space-between" }}>
          <span>
            Batch terakhir: {b?.last_flush ? `${jam(b.last_flush.at)} · ${fmt(b.last_flush.messages)} pesan → ${fmt(b.last_flush.rows)} baris, ${b.last_flush.files} file` : "belum ada"}
            {b?.last_error && <span className="pill bad"> {b.last_error}</span>}
            {stats?.bronze_error && <span className="pill bad"> ingestor: {stats.bronze_error}</span>}
          </span>
          <Sparkline series={stats?.series ?? []} />
        </div>
      </div>

      <div className="grid g2" style={{ marginBottom: 12 }}>
        <div className="card">
          <h2>
            Event aplikasi live <span className="pill app">bank.* · shop.*</span>
          </h2>
          <div className="feed">
            {appFeed.map((m, i) => (
              <div key={m.at + i} onClick={() => setSubject(m.subject)} title={m.preview}>
                <span className="muted">{jam(m.at)}</span> <b>{m.subject}</b> <span className="muted">{m.preview}</span>
              </div>
            ))}
            {appFeed.length === 0 && <span className="muted small">Belum ada event. Nyalakan generator atau lakukan checkout di toko (port 50500).</span>}
          </div>
        </div>
        <div className="card">
          <h2>
            CDC live <span className="pill cdc">cdc.*</span>
          </h2>
          <div className="feed">
            {cdcFeed.map((m, i) => (
              <div key={m.at + i} onClick={() => setSubject(m.subject)} title={m.preview}>
                <span className="muted">{jam(m.at)}</span> <b>{m.subject}</b> <span className="pill cdc">{m.op ? `${m.op} ${opLabel[m.op] ?? ""}` : "?"}</span>{" "}
                <span className="muted">{m.preview}</span>
              </div>
            ))}
            {cdcFeed.length === 0 && <span className="muted small">Belum ada perubahan WAL yang tertangkap.</span>}
          </div>
        </div>
      </div>

      <div className="grid g2" style={{ marginBottom: 12 }}>
        <Envelope subjects={subjects} subject={subject} setSubject={setSubject} />
        <div className="card">
          <h2>Stream dan consumer JetStream</h2>
          <table>
            <thead>
              <tr>
                <th>stream</th>
                <th className="num">pesan</th>
                <th className="num">ukuran</th>
                <th>retensi</th>
                <th>consumer</th>
                <th className="num">lag</th>
                <th className="num">ack pending</th>
                <th className="num">redelivered</th>
              </tr>
            </thead>
            <tbody>
              {(stats?.streams ?? []).flatMap((s) =>
                (s.consumers.length ? s.consumers : [null]).map((c, i) => (
                  <tr key={s.name + (c?.name ?? "")}>
                    <td>{i === 0 && <span className={`pill ${s.kind}`}>{s.name}</span>}</td>
                    <td className="num">{i === 0 ? fmt(s.messages) : ""}</td>
                    <td className="num">{i === 0 ? bytes(s.bytes) : ""}</td>
                    <td>{i === 0 ? (s.exists ? s.max_age : "belum ada") : ""}</td>
                    <td>{c?.name ?? "-"}</td>
                    <td className="num">{c ? fmt(c.num_pending) : ""}</td>
                    <td className="num">{c ? fmt(c.num_ack_pending) : ""}</td>
                    <td className="num">{c ? fmt(c.num_redelivered) : ""}</td>
                  </tr>
                )),
              )}
            </tbody>
          </table>
          <p className="small muted">
            Consumer <code>de_bronze_*</code> milik ingestor DE; consumer lain milik service Sister (saga). Stream yang sama dibaca banyak consumer tanpa saling mengganggu: tiap durable consumer
            punya posisi baca sendiri.
          </p>
          <h2 style={{ marginTop: 12 }}>Pesan per subject (di stream)</h2>
          <div className="scroll" style={{ maxHeight: 220 }}>
            <table>
              <tbody>
                {(stats?.streams ?? []).flatMap((s) =>
                  Object.entries(s.subjects)
                    .sort()
                    .map(([k, v]) => (
                      <tr key={k} onClick={() => setSubject(k)} style={{ cursor: "pointer" }}>
                        <td>
                          <span className={`pill ${s.kind}`}>{s.name}</span> {k}
                        </td>
                        <td className="num">{fmt(v)}</td>
                        <td className="num muted">{(stats?.rates[k] ?? 0).toFixed(1)}/s</td>
                      </tr>
                    )),
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="grid g2">
        <div className="card">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <h2>Partisi bronze (RustFS)</h2>
            <input placeholder="saring: cdc/transaksi" value={filter} onChange={(e) => setFilter(e.target.value)} />
          </div>
          <div className="small muted">
            Total {fmt(b?.totals.files)} file · {fmt(b?.totals.rows)} baris · {bytes(b?.totals.bytes)}. Partisi <code>dt</code> = tanggal UTC waktu bisnis, sehingga data terlambat dan backfill masuk
            ke tanggal aslinya.
          </div>
          <div className="scroll">
            <table>
              <thead>
                <tr>
                  <th>sumber/entitas</th>
                  <th>dt</th>
                  <th className="num">file</th>
                  <th className="num">baris</th>
                  <th className="num">ukuran</th>
                </tr>
              </thead>
              <tbody>
                {parts.map((p) => (
                  <tr key={`${p.source}/${p.entity}/${p.dt}`}>
                    <td>
                      <span className={`pill ${p.source}`}>{p.source}</span> {p.entity}
                    </td>
                    <td>{p.dt}</td>
                    <td className="num">{fmt(p.files)}</td>
                    <td className="num">
                      {fmt(p.rows)}
                      {p.rows_unknown ? "+" : ""}
                    </td>
                    <td className="num">{bytes(p.bytes)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="card">
          <h2>File Parquet terbaru</h2>
          <div className="scroll">
            <table>
              <tbody>
                {(b?.recent_files ?? []).map((f) => (
                  <tr key={f.path}>
                    <td className="muted">{jam(f.at)}</td>
                    <td>
                      <code>{f.path.replace("s3://lake/", "")}</code>
                    </td>
                    <td className="num">{fmt(f.rows)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="small muted">
            Kolom metadata W1 setiap file: <code>_ingested_at, _source, _batch_id, _event_id, _event_type, _occurred_at, _ts_ms, _op</code>, lalu kolom data entitas (string). Buka console RustFS di port
            52004 untuk melihat objeknya.
          </p>
        </div>
      </div>
    </>
  );
}
