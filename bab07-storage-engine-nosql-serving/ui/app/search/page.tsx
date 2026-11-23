"use client";
import { useEffect, useState } from "react";
import { api, useApi, angka, Galat } from "@/components/lib";

type Shard = { index: string; shard: string; prirep: string; state: string; docs: string; store: string; node: string };
type Cluster = { health: { status: string; number_of_nodes: number; active_shards: number; unassigned_shards: number }; nodes: { name: string; "heap.percent": string; "ram.percent": string; cluster_manager: string }[]; shards: Shard[]; disk_threshold: any };
type Hit = { _id: string; _score: number; _source: Record<string, any>; highlight?: { keterangan?: string[] } };
type Cari = { q: string; token: { token: string; posting: number }[]; opensearch: { took_ms: number; ms_klien: number; total: number; hits: Hit[] }; pembanding: { cara: string; jumlah: number; ms: number }[] };

const WARNA_STATUS: Record<string, string> = { green: "var(--hijau)", yellow: "var(--kuning)", red: "var(--merah)" };

function Sorot({ html }: { html: string }) {
  // highlight OpenSearch memakai <em>; tampilkan sebagai <mark> tanpa menyuntikkan HTML lain
  const bagian = html.split(/(<em>.*?<\/em>)/g);
  return <>{bagian.map((b, i) => b.startsWith("<em>") ? <mark key={i}>{b.slice(4, -5)}</mark> : <span key={i}>{b}</span>)}</>;
}

export default function Search() {
  const cl = useApi<Cluster>("search/cluster", 8000);
  const [q, setQ] = useState("zakat wakaf");
  const [hasil, setHasil] = useState<Cari | null>(null);
  const [galat, setGalat] = useState("");
  async function cari(x = q) {
    setGalat("");
    try { setHasil(await api<Cari>(`search/cari?q=${encodeURIComponent(x)}`)); } catch (e) { setGalat(String(e)); }
  }
  useEffect(() => { cari("zakat wakaf"); }, []); // eslint-disable-line react-hooks/exhaustive-deps
  const c = cl.data;
  const nodes = c?.nodes.map(n => n.name).sort() || [];
  const maksPosting = Math.max(1, ...(hasil?.token || []).map(t => t.posting));
  return (
    <>
      <h1>Search: inverted index di OpenSearch (2 node)</h1>
      <p className="redup">OpenSearch menyimpan setiap kata (token) beserta daftar dokumen yang memuatnya (posting list). Query &quot;zakat wakaf&quot; cukup mengiriskan dua daftar, tanpa memindai seluruh transaksi. Perlakukan OpenSearch sebagai indeks turunan, bukan sumber kebenaran.</p>
      <Galat teks={cl.galat || galat} />
      <div className="kartu">
        <h2>Cluster <span className="lencana" style={{ background: `color-mix(in srgb, ${WARNA_STATUS[c?.health.status || ""] || "var(--redup)"} 22%, transparent)`, color: WARNA_STATUS[c?.health.status || ""] }}>{c?.health.status || "?"}</span>
          <span className="redup"> · {c?.health.number_of_nodes} node · {c?.health.active_shards} shard aktif · {c?.health.unassigned_shards} belum dialokasikan</span></h2>
        <div className="baris" style={{ alignItems: "stretch" }}>
          {nodes.map(nm => {
            const n = c!.nodes.find(x => x.name === nm)!;
            return (
              <div key={nm} className="node-os">
                <b>{nm}</b> {n.cluster_manager === "*" && <span className="lencana netral">cluster manager</span>}
                <div className="redup" style={{ fontSize: 12 }}>heap {n["heap.percent"]}% · RAM {n["ram.percent"]}%</div>
                <div>{c!.shards.filter(s => s.node === nm).sort((a, b) => (a.index + a.shard).localeCompare(b.index + b.shard)).map((s, i) => (
                  <span key={i} className="shard" style={{ background: s.prirep === "p" ? "#005eb8" : "#6aa6d9" }} title={`${s.index} shard ${s.shard} ${s.prirep === "p" ? "primary" : "replica"} · ${s.docs} dokumen · ${s.store}`}>
                    {s.index.slice(0, 4)}/{s.shard} {s.prirep.toUpperCase()}<div style={{ fontWeight: 400, fontSize: 10 }}>{angka(s.docs)}</div>
                  </span>))}</div>
              </div>
            );
          })}
        </div>
        <p className="redup">P = primary, R = replica. Setiap index punya 2 shard, setiap shard punya 1 replica di node lain: bila satu node mati, cluster menjadi kuning tetapi data tetap terbaca. Watermark disk dimatikan untuk lab karena disk server hampir penuh (cluster.routing.allocation.disk.threshold_enabled=false).</p>
      </div>
      <div className="kartu">
        <div className="baris">
          <input value={q} onChange={e => setQ(e.target.value)} onKeyDown={e => e.key === "Enter" && cari()} style={{ minWidth: 260 }} />
          <button className="utama" onClick={() => cari()}>Cari</button>
          {["zakat wakaf", "infak masjid", "tabungan haji", "zakat", "belanja daring"].map(x => <button key={x} onClick={() => { setQ(x); cari(x); }}>{x}</button>)}
        </div>
        {hasil && (
          <div className="kisi" style={{ marginTop: 10 }}>
            <div>
              <h3>Token dan posting list</h3>
              {hasil.token.map(t => (
                <div key={t.token} style={{ display: "grid", gridTemplateColumns: "90px 1fr 80px", gap: 6, alignItems: "center", margin: "3px 0" }}>
                  <code>{t.token}</code>
                  <div style={{ height: 14, background: "var(--kode)", borderRadius: 3 }}><div style={{ height: "100%", width: `${(t.posting / maksPosting) * 100}%`, background: "#005eb8", borderRadius: 3 }} /></div>
                  <span className="angka">{angka(t.posting)}</span>
                </div>
              ))}
              <div style={{ display: "grid", gridTemplateColumns: "90px 1fr 80px", gap: 6, alignItems: "center", margin: "3px 0" }}>
                <b>irisan</b>
                <div style={{ height: 14, background: "var(--kode)", borderRadius: 3 }}><div style={{ height: "100%", width: `${(hasil.opensearch.total / maksPosting) * 100}%`, background: "var(--emas)", borderRadius: 3 }} /></div>
                <span className="angka">{angka(hasil.opensearch.total)}</span>
              </div>
              <h3>Waktu</h3>
              <table><tbody>
                <tr><td>OpenSearch match (took)</td><td className="angka">{hasil.opensearch.took_ms} ms</td><td className="angka">{angka(hasil.opensearch.total)} hit</td></tr>
                {hasil.pembanding.map(p => <tr key={p.cara}><td>{p.cara}</td><td className="angka">{p.ms.toFixed(1)} ms</td><td className="angka">{angka(p.jumlah)}</td></tr>)}
              </tbody></table>
              <p className="redup" style={{ fontSize: 12 }}>ILIKE mencocokkan potongan string (&quot;zakat&quot; juga cocok di dalam kata lain), sedangkan full-text mencocokkan token utuh, sehingga jumlahnya bisa berbeda.</p>
            </div>
            <div>
              <h3>Hasil teratas (skor BM25)</h3>
              <table><thead><tr><th>trx</th><th>keterangan</th><th className="angka">nominal</th><th className="angka">skor</th></tr></thead>
                <tbody>{hasil.opensearch.hits.map(h => <tr key={h._id}><td>{h._id}</td><td><Sorot html={h.highlight?.keterangan?.[0] || String(h._source.keterangan)} /></td><td className="angka">{angka(h._source.nominal)}</td><td className="angka">{h._score.toFixed(2)}</td></tr>)}</tbody></table>
            </div>
          </div>
        )}
      </div>
    </>
  );
}
