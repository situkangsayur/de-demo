"use client";
import { useEffect, useRef, useState } from "react";
import { api, useApi, angka, byte, Galat } from "@/components/lib";

type BT = { indeks: string; kedalaman: number; page: number; tuple: number; kunci_per_page: number };
type HT = { store: string; n: number; ms: number; ops_per_detik: number; byte_logis: number; metrik: Record<string, number>; amplifikasi_tulis: number; penjelasan: string };
type Sampel = { t: number; ch_part?: number; ch_baris?: number; ch_merge_berjalan?: number; os_segment?: number };

// ---------- simulasi LSM-tree (di browser) ----------
type SST = { id: number; kunci: number[]; bloom: Set<number> };
type LSM = { mem: number[]; l0: SST[]; l1: SST[]; l2: SST[]; userTulis: number; diskTulis: number; log: string[]; sorot: number | null; nextId: number };
const MEM = 4, L0MAX = 3, L1MAX = 12;
const lsmBaru = (): LSM => ({ mem: [], l0: [], l1: [], l2: [], userTulis: 0, diskTulis: 0, log: [], sorot: null, nextId: 1 });
function sst(s: LSM, kunci: number[]): SST {
  const k = Array.from(new Set(kunci)).sort((a, b) => a - b);
  return { id: s.nextId++, kunci: k, bloom: new Set(k) };
}
function lsmTulis(s0: LSM, k: number): LSM {
  const s = { ...s0, mem: [...s0.mem, k], userTulis: s0.userTulis + 1, log: [...s0.log] };
  if (s.mem.length >= MEM) {
    const t = sst(s, s.mem); s.l0 = [...s.l0, t]; s.diskTulis += t.kunci.length; s.mem = [];
    s.log.push(`flush memtable -> SSTable L0 #${t.id} (${t.kunci.length} kunci ditulis)`);
  }
  if (s.l0.length > L0MAX) {
    const semua = [...s.l0.flatMap(x => x.kunci), ...s.l1.flatMap(x => x.kunci)];
    const t = sst(s, semua); s.diskTulis += t.kunci.length;
    s.log.push(`compaction L0+L1 -> L1 #${t.id}: ${t.kunci.length} kunci ditulis ULANG`); s.l0 = []; s.l1 = [t];
  }
  if (s.l1.length && s.l1[0].kunci.length > L1MAX) {
    const t = sst(s, [...s.l1[0].kunci, ...s.l2.flatMap(x => x.kunci)]); s.diskTulis += t.kunci.length;
    s.log.push(`compaction L1+L2 -> L2 #${t.id}: ${t.kunci.length} kunci ditulis ULANG`); s.l1 = []; s.l2 = [t];
  }
  s.log = s.log.slice(-8);
  return s;
}
function lsmBaca(s: LSM, k: number): { s: LSM; langkah: string[] } {
  const langkah: string[] = [];
  if (s.mem.includes(k)) return { s: { ...s, sorot: k }, langkah: ["memtable: ketemu (tanpa baca disk)"] };
  langkah.push("memtable: tidak ada");
  for (const [lvl, daftar] of [["L0", [...s.l0].reverse()], ["L1", s.l1], ["L2", s.l2]] as [string, SST[]][]) {
    for (const t of daftar) {
      if (!t.bloom.has(k)) { langkah.push(`${lvl} #${t.id}: bloom filter bilang PASTI TIDAK ADA -> dilewati`); continue; }
      langkah.push(`${lvl} #${t.id}: bloom bilang mungkin ada -> baca disk -> KETEMU`);
      return { s: { ...s, sorot: k }, langkah };
    }
  }
  langkah.push("tidak ada di semua level");
  return { s: { ...s, sorot: k }, langkah };
}

function SimulasiLSM() {
  const [s, setS] = useState<LSM>(lsmBaru);
  const [kunciBaca, setKunciBaca] = useState(7);
  const [langkah, setLangkah] = useState<string[]>([]);
  const tulisAcak = (n: number) => setS(prev => { let x = prev; for (let i = 0; i < n; i++) x = lsmTulis(x, 1 + Math.floor(Math.random() * 40)); return x; });
  const Kotak = ({ t, label }: { t: SST; label: string }) => (
    <div className="lsm-kotak" title={`${label} #${t.id}`}><span className="redup" style={{ fontSize: 10 }}>#{t.id}</span>
      {t.kunci.map(k => <span key={k} className={`lsm-kunci ${s.sorot === k ? "sorot" : ""}`}>{k}</span>)}</div>
  );
  return (
    <div className="kartu">
      <h2>Simulasi LSM-tree: memtable, flush, compaction, bloom filter</h2>
      <div className="baris">
        <button onClick={() => tulisAcak(1)}>Tulis 1 kunci acak</button>
        <button className="utama" onClick={() => tulisAcak(10)}>Tulis 10 kunci acak</button>
        <label>Baca kunci <input type="number" value={kunciBaca} min={1} max={40} onChange={e => setKunciBaca(Number(e.target.value))} style={{ width: 70 }} /></label>
        <button onClick={() => { const r = lsmBaca(s, kunciBaca); setS(r.s); setLangkah(r.langkah); }}>Baca</button>
        <button onClick={() => { setS(lsmBaru()); setLangkah([]); }}>Reset</button>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "120px 1fr", gap: 6, marginTop: 10, alignItems: "center" }}>
        <b>memtable (RAM)</b><div className="lsm-kotak">{s.mem.map((k, i) => <span key={i} className="lsm-kunci">{k}</span>)}<span className="redup" style={{ fontSize: 11 }}>{s.mem.length}/{MEM}</span></div>
        <b>L0 (disk)</b><div>{s.l0.map(t => <Kotak key={t.id} t={t} label="L0" />)}{!s.l0.length && <span className="redup">kosong</span>}</div>
        <b>L1</b><div>{s.l1.map(t => <Kotak key={t.id} t={t} label="L1" />)}{!s.l1.length && <span className="redup">kosong</span>}</div>
        <b>L2</b><div>{s.l2.map(t => <Kotak key={t.id} t={t} label="L2" />)}{!s.l2.length && <span className="redup">kosong</span>}</div>
      </div>
      <div className="kisi" style={{ marginTop: 8 }}>
        <div>
          <div>Kunci ditulis aplikasi: <b>{s.userTulis}</b> · kunci ditulis ke disk (flush + compaction): <b>{s.diskTulis}</b></div>
          <div className="angka-besar">amplifikasi tulis ≈ {s.userTulis ? (s.diskTulis / s.userTulis).toFixed(2) : "–"}×</div>
          <pre style={{ whiteSpace: "pre-wrap", minHeight: 60 }}>{s.log.join("\n") || "log flush/compaction"}</pre>
        </div>
        <div>
          <b>Jalur baca kunci {kunciBaca}</b>
          <ol style={{ margin: "4px 0", paddingLeft: 18, fontSize: 13 }}>{langkah.map((l, i) => <li key={i}>{l}</li>)}</ol>
          <p className="redup" style={{ fontSize: 12 }}>Memtable menampung {MEM} kunci; flush menjadi SSTable L0 (immutable); lebih dari {L0MAX} SSTable L0 memicu compaction ke L1; L1 lebih dari {L1MAX} kunci digabung ke L2. Kunci yang sama ditulis berkali-kali ke disk: itulah amplifikasi tulis. Bloom filter tiap SSTable melewati berkas yang pasti tidak memuat kunci.</p>
        </div>
      </div>
    </div>
  );
}

function GrafikPart({ sampel }: { sampel: Sampel[] }) {
  if (sampel.length < 2) return <p className="redup">Jalankan eksperimen ClickHouse untuk melihat jumlah part naik lalu turun karena merge.</p>;
  const t0 = sampel[0].t, t1 = sampel[sampel.length - 1].t, maks = Math.max(2, ...sampel.map(s => s.ch_part || 0));
  const W = 560, H = 160, L = 36;
  const xy = (s: Sampel) => `${L + ((s.t - t0) / Math.max(t1 - t0, 1)) * (W - L - 10)},${H - 20 - ((s.ch_part || 0) / maks) * (H - 40)}`;
  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Jumlah part aktif ClickHouse terhadap waktu">
      <line x1={L} x2={W - 10} y1={H - 20} y2={H - 20} stroke="var(--garis)" />
      <text x={L - 4} y={24} textAnchor="end">{maks}</text><text x={L - 4} y={H - 20} textAnchor="end">0</text>
      <polyline fill="none" stroke="#d9a400" strokeWidth={2.5} points={sampel.map(xy).join(" ")} />
      <text x={W - 10} y={H - 4} textAnchor="end">{((t1 - t0) / 1000).toFixed(0)} dtk</text>
      <text x={L + 4} y={14}>part aktif tabel tulis_uji</text>
    </svg>
  );
}

export default function Engine() {
  const bt = useApi<BT[]>("engine/btree");
  const [store, setStore] = useState("postgres");
  const [n, setN] = useState(300);
  const [hasil, setHasil] = useState<HT[]>([]);
  const [jalan, setJalan] = useState(false);
  const [galat, setGalat] = useState("");
  const [sampel, setSampel] = useState<Sampel[]>([]);
  const pantau = useRef<ReturnType<typeof setInterval> | null>(null);

  function mulaiPantau(detik: number) {
    if (pantau.current) clearInterval(pantau.current);
    const akhir = Date.now() + detik * 1000;
    pantau.current = setInterval(async () => {
      try { const s = await api<Sampel>("engine/sampel"); setSampel(prev => [...prev, s].slice(-300)); } catch { /* abaikan */ }
      if (Date.now() > akhir && pantau.current) clearInterval(pantau.current);
    }, 500);
  }
  useEffect(() => () => { if (pantau.current) clearInterval(pantau.current); }, []);

  async function jalankan() {
    setJalan(true); setGalat("");
    if (store === "clickhouse") { setSampel([]); mulaiPantau(40); }
    try { const h = await api<HT>("engine/tulis", { store, n }); setHasil(prev => [h, ...prev].slice(0, 8)); } catch (e) { setGalat(String(e)); }
    setJalan(false);
  }
  return (
    <>
      <h1>B-tree vs LSM-tree: amplifikasi tulis</h1>
      <p className="redup">Di bawah setiap database ada storage engine. B-tree (PostgreSQL) memperbarui page di tempat dan menulis WAL; keluarga LSM (ClickHouse MergeTree, segment Lucene di OpenSearch, RocksDB) menulis berurutan ke berkas immutable lalu menggabungkannya di latar.</p>
      <div className="kartu">
        <h2>B-tree PostgreSQL: pohon lebar yang dangkal</h2>
        <Galat teks={bt.galat} />
        <div className="kisi">
          {(bt.data || []).map(b => (
            <div key={b.indeks} className="kartu" style={{ margin: 0 }}>
              <b>{b.indeks}</b>
              <div className="angka-besar">{b.kedalaman} level</div>
              <div className="redup">{angka(b.tuple)} kunci dalam {angka(b.page)} page 8 KB (≈{angka(b.kunci_per_page)} kunci/page daun)</div>
              <svg width="100%" viewBox="0 0 260 70" aria-hidden="true">
                {Array.from({ length: b.kedalaman }).map((_, l) => {
                  const k = Math.min(Math.pow(6, l), 24), w = 250 / k;
                  return Array.from({ length: k }).map((__, i) => <rect key={`${l}-${i}`} x={5 + i * w + 1} y={4 + l * (60 / Math.max(b.kedalaman, 1))} width={Math.max(w - 2, 2)} height={10} rx={2} fill={l === b.kedalaman - 1 ? "var(--emas)" : "var(--utama)"} opacity={0.8} />);
                })}
              </svg>
              <div className="redup" style={{ fontSize: 12 }}>Mencari satu kunci = membaca {b.kedalaman} page (akar → daun).</div>
            </div>
          ))}
        </div>
      </div>
      <div className="kartu">
        <h2>Eksperimen: tulis satu baris per operasi</h2>
        <div className="baris">
          <select value={store} onChange={e => setStore(e.target.value)}>
            <option value="postgres">PostgreSQL (B-tree + WAL)</option>
            <option value="clickhouse">ClickHouse (MergeTree, 1 INSERT = 1 part)</option>
            <option value="redis">Redis (memori + AOF)</option>
            <option value="opensearch">OpenSearch (segment Lucene)</option>
          </select>
          <label>baris <input type="number" value={n} min={10} max={2000} onChange={e => setN(Number(e.target.value))} style={{ width: 80 }} /></label>
          <button className="utama" disabled={jalan} onClick={jalankan}>{jalan ? "menulis…" : "Tulis"}</button>
          <button onClick={() => api("engine/optimize").then(() => mulaiPantau(10))}>OPTIMIZE FINAL (paksa merge)</button>
          <button onClick={() => api("engine/reset").then(() => { setHasil([]); setSampel([]); })}>Reset tabel uji</button>
        </div>
        <Galat teks={galat} />
        <div className="tabel-wadah" style={{ marginTop: 8 }}>
          <table>
            <thead><tr><th>store</th><th className="angka">baris</th><th className="angka">ops/detik</th><th className="angka">byte logis</th><th>metrik</th><th className="angka">amplifikasi</th></tr></thead>
            <tbody>{hasil.map((h, i) => (
              <tr key={i} title={h.penjelasan}>
                <td><b>{h.store}</b></td><td className="angka">{h.n}</td><td className="angka">{angka(h.ops_per_detik)}</td><td className="angka">{byte(h.byte_logis)}</td>
                <td style={{ whiteSpace: "normal" }}>{Object.entries(h.metrik).map(([k, v]) => `${k}=${k.includes("byte") ? byte(v) : angka(v, 1)}`).join(" · ")}</td>
                <td className="angka"><b>{h.amplifikasi_tulis ? `${h.amplifikasi_tulis.toFixed(1)}×` : "–"}</b></td>
              </tr>))}</tbody>
          </table>
        </div>
        {hasil[0] && <p className="redup">{hasil[0].penjelasan}</p>}
        <GrafikPart sampel={sampel} />
      </div>
      <SimulasiLSM />
    </>
  );
}
