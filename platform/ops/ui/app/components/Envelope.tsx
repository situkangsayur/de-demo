"use client";

// Contoh pesan terakhir per subject. Untuk subject cdc.* ditampilkan struktur envelope Debezium:
// op, before vs after (kolom yang berubah ditandai), source (db, table, lsn, txId), ts_ms.
import { useEffect, useState } from "react";
import { api } from "../lib";

type Sample = { stream: string; subject: string; seq: number; time: string; headers: Record<string, string>; data: unknown };
type Env = { op?: string; before?: Record<string, unknown> | null; after?: Record<string, unknown> | null; source?: Record<string, unknown>; ts_ms?: number };

const opLabel: Record<string, string> = { c: "c = create (INSERT)", u: "u = update (UPDATE)", d: "d = delete (DELETE)", r: "r = read (snapshot awal)" };

export default function Envelope({ subjects, subject, setSubject }: { subjects: string[]; subject: string; setSubject: (s: string) => void }) {
  const [s, setS] = useState<Sample | null>(null);
  const [err, setErr] = useState("");
  const [raw, setRaw] = useState(false);

  const load = async (subj: string) => {
    if (!subj) return;
    try {
      setS(await api<Sample>(`flow/sample?subject=${encodeURIComponent(subj)}`));
      setErr("");
    } catch (e) {
      setS(null);
      setErr(String(e));
    }
  };
  useEffect(() => {
    load(subject);
  }, [subject]);
  useEffect(() => {
    if (!subject) {
      const first = subjects.find((x) => x === "cdc.bank.nasabah") ?? subjects.find((x) => x.startsWith("cdc."));
      if (first) setSubject(first);
    }
  }, [subjects, subject, setSubject]);

  const env = s && s.stream === "CDC" ? (s.data as Env) : null;
  const keys = env ? Array.from(new Set([...Object.keys(env.before ?? {}), ...Object.keys(env.after ?? {})])) : [];

  return (
    <div className="card">
      <div className="row" style={{ justifyContent: "space-between" }}>
        <h2>Contoh pesan {env ? "(envelope Debezium)" : ""}</h2>
        <div className="row">
          <select value={subject} onChange={(e) => setSubject(e.target.value)}>
            <option value="">pilih subject…</option>
            {subjects.map((x) => (
              <option key={x}>{x}</option>
            ))}
          </select>
          <button onClick={() => load(subject)}>muat ulang</button>
          <label className="row small">
            <input type="checkbox" checked={raw} onChange={(e) => setRaw(e.target.checked)} /> JSON mentah
          </label>
        </div>
      </div>
      {err && <div className="pill bad">{err}</div>}
      {s && (
        <div className="small muted" style={{ margin: "4px 0 8px" }}>
          stream {s.stream} · seq {s.seq} · {s.time} {s.headers["Nats-Msg-Id"] && <>· Nats-Msg-Id {s.headers["Nats-Msg-Id"]}</>}
        </div>
      )}
      {env && !raw ? (
        <>
          <div className="row" style={{ marginBottom: 6 }}>
            <span className="pill cdc">op: {opLabel[env.op ?? ""] ?? env.op}</span>
            <span className="pill">ts_ms: {env.ts_ms}</span>
            {env.source && (
              <span className="pill">
                source: {String(env.source.db)}.{String(env.source.schema)}.{String(env.source.table)} · lsn {String(env.source.lsn)} · txId {String(env.source.txId)}
              </span>
            )}
          </div>
          <div className="scroll">
            <table>
              <thead>
                <tr>
                  <th>kolom</th>
                  <th>before</th>
                  <th>after</th>
                </tr>
              </thead>
              <tbody>
                {keys.map((k) => {
                  const b = env.before ? env.before[k] : undefined;
                  const a = env.after ? env.after[k] : undefined;
                  const changed = env.before && env.after && JSON.stringify(a) !== JSON.stringify(b);
                  return (
                    <tr key={k}>
                      <td>
                        <code>{k}</code>
                      </td>
                      <td className={changed ? "diff-del" : ""}>{env.before ? <code>{JSON.stringify(b)}</code> : <span className="muted">null</span>}</td>
                      <td className={changed ? "diff-add" : ""}>{env.after ? <code>{JSON.stringify(a)}</code> : <span className="muted">null</span>}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <p className="small muted">
            {env.op === "u" && !env.before ? (
              <>
                <code>before</code> bernilai null: tabel memakai <code>REPLICA IDENTITY DEFAULT</code> (bawaan Postgres), sehingga WAL hanya membawa nilai baru. Dengan{" "}
                <code>ALTER TABLE … REPLICA IDENTITY FULL</code> nilai lama ikut terkirim. Riwayat SCD-2 tetap bisa dibangun dari urutan <code>after</code> per <code>id</code>.
              </>
            ) : (
              <>Baris berwarna = kolom yang berubah antara before dan after, bahan SCD tipe 2 di silver/gold.</>
            )}
          </p>
        </>
      ) : (
        s && <pre>{JSON.stringify(s.data, null, 2)}</pre>
      )}
    </div>
  );
}
