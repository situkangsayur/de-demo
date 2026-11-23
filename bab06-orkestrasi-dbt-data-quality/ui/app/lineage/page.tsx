"use client";
import { useState } from "react";
import { useApi, warna, Lencana, Galat } from "@/components/lib";

type Simpul = { id: string; nama: string; jenis: string; materialisasi?: string; dimensi?: string; status?: string; failures?: number; deskripsi?: string; hulu: string[]; sql?: string };

export default function Lineage() {
  const { data, galat } = useApi<{ simpul: Simpul[] }>("lineage", 6000);
  const [pilih, setPilih] = useState<string>("");
  const semua = data?.simpul || [];
  const byId = Object.fromEntries(semua.map(s => [s.id, s]));
  const graf = semua.filter(s => s.jenis !== "test");
  const tes = semua.filter(s => s.jenis === "test");
  // test ditempel pada model terakhir yang diujinya
  const tesPer: Record<string, Simpul[]> = {};
  tes.forEach(t => { const m = t.hulu.filter(h => byId[h]?.jenis === "model").slice(-1)[0] || t.hulu[0]; (tesPer[m] ||= []).push(t); });
  const lv: Record<string, number> = {};
  const hitung = (id: string): number => {
    if (lv[id] !== undefined) return lv[id];
    const h = (byId[id]?.hulu || []).filter(x => byId[x] && byId[x].jenis !== "test");
    lv[id] = h.length ? Math.max(...h.map(hitung)) + 1 : 0;
    return lv[id];
  };
  graf.forEach(s => hitung(s.id));
  const kol: Record<number, Simpul[]> = {};
  graf.forEach(s => (kol[lv[s.id]] ||= []).push(s));
  const W = 190, GX = 60, BH = 52;
  const pos: Record<string, { x: number; y: number; h: number }> = {};
  let tinggi = 0;
  Object.entries(kol).forEach(([l, ss]) => {
    let y = 6;
    ss.sort((a, b) => a.nama.localeCompare(b.nama)).forEach(s => {
      const h = BH + Math.ceil((tesPer[s.id]?.length || 0) / 9) * 14;
      pos[s.id] = { x: Number(l) * (W + GX) + 6, y, h };
      y += h + 14;
    });
    tinggi = Math.max(tinggi, y);
  });
  const lebar = (Math.max(0, ...Object.keys(kol).map(Number)) + 1) * (W + GX);
  const sel = byId[pilih];
  const tesSel = sel ? tesPer[sel.id] || [] : [];
  return (
    <>
      <h1>dbt lineage</h1>
      <p className="redup">Graph dibangun dbt dari <code>source()</code> dan <code>ref()</code> (manifest.json). Kotak kecil di bawah model = test yang menguji model itu, berwarna sesuai hasil test terakhir. Dokumentasi lengkap: <a href="/dbt-docs/index.html" target="_blank" rel="noreferrer">dbt docs</a>.</p>
      <Galat teks={galat} />
      <div className="kartu" style={{ overflowX: "auto" }}>
        <svg width={Math.max(lebar, 320)} height={tinggi + 10} role="img" aria-label="Lineage dbt">
          <defs><marker id="p" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="var(--redup)" /></marker></defs>
          {graf.flatMap(s => s.hulu.filter(h => pos[h]).map(h => {
            const a = pos[h], b = pos[s.id];
            const x1 = a.x + W, y1 = a.y + 20, x2 = b.x, y2 = b.y + 20;
            return <path key={h + s.id} d={`M${x1},${y1} C${x1 + 30},${y1} ${x2 - 30},${y2} ${x2},${y2}`} fill="none" stroke="var(--redup)" markerEnd="url(#p)" />;
          }))}
          {graf.map(s => {
            const p = pos[s.id];
            const warnaLatar = s.jenis === "source" ? "color-mix(in srgb, var(--emas) 25%, var(--panel))" : "var(--panel)";
            return (
              <g key={s.id} className="simpul" transform={`translate(${p.x},${p.y})`} onClick={() => setPilih(s.id)} style={{ cursor: "pointer" }}>
                <rect width={W} height={p.h} rx={8} fill={warnaLatar} strokeWidth={pilih === s.id ? 2.5 : 1} style={{ stroke: pilih === s.id ? "var(--utama)" : undefined }} />
                {s.jenis === "model" && <rect width={6} height={p.h} rx={3} fill={warna(s.status)} />}
                <text x={12} y={17} style={{ fontWeight: 600 }}>{s.nama}</text>
                <text x={12} y={32} style={{ fill: "var(--redup)", fontSize: 10 }}>{s.jenis === "source" ? "source" : `model · ${s.materialisasi}${s.status ? " · " + s.status : ""}`}</text>
                {(tesPer[s.id] || []).map((t, i) => (
                  <rect key={t.id} x={12 + (i % 9) * 19} y={40 + Math.floor(i / 9) * 14} width={15} height={10} rx={2} fill={warna(t.status)}>
                    <title>{`${t.nama} [${t.dimensi}] ${t.status || "belum"}${t.failures ? ` (${t.failures} baris melanggar)` : ""}`}</title>
                  </rect>
                ))}
              </g>
            );
          })}
        </svg>
      </div>
      {sel && (
        <div className="kartu">
          <h2>{sel.nama} <span className="redup">({sel.jenis}{sel.materialisasi ? `, ${sel.materialisasi}` : ""})</span></h2>
          {sel.deskripsi && <p>{sel.deskripsi}</p>}
          {sel.sql && <pre>{sel.sql}</pre>}
          {tesSel.length > 0 && (
            <table><thead><tr><th>test</th><th>dimensi</th><th>status</th><th className="angka">baris melanggar</th></tr></thead>
              <tbody>{tesSel.map(t => <tr key={t.id}><td>{t.nama}</td><td>{t.dimensi}</td><td><Lencana s={t.status} /></td><td className="angka">{t.failures || 0}</td></tr>)}</tbody></table>
          )}
        </div>
      )}
    </>
  );
}
