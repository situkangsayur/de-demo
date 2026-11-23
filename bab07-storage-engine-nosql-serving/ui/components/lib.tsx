"use client";
import { useCallback, useEffect, useState } from "react";

export async function api<T = any>(path: string, body?: unknown): Promise<T> {
  const r = await fetch(`/api/${path}`, body === undefined ? { cache: "no-store" } :
    { method: "POST", body: JSON.stringify(body), headers: { "Content-Type": "application/json" } });
  const j = await r.json();
  if (!r.ok) throw new Error(j?.error || `HTTP ${r.status}`);
  return j as T;
}

export function useApi<T = any>(path: string | null, ulangMs = 0) {
  const [data, setData] = useState<T | null>(null);
  const [galat, setGalat] = useState<string>("");
  const segarkan = useCallback(async () => {
    if (!path) return;
    try { setData(await api<T>(path)); setGalat(""); } catch (e) { setGalat(String(e)); }
  }, [path]);
  useEffect(() => {
    segarkan();
    if (ulangMs > 0) { const t = setInterval(segarkan, ulangMs); return () => clearInterval(t); }
  }, [segarkan, ulangMs]);
  return { data, galat, segarkan, setData };
}

export const angka = (v: unknown, d = 0) =>
  v === null || v === undefined || v === "" ? "–" : new Intl.NumberFormat("id-ID", { maximumFractionDigits: d }).format(Number(v));

export const byte = (n: number) => {
  const u = ["B", "KB", "MB", "GB"]; let i = 0; let x = Number(n) || 0;
  while (x >= 1024 && i < u.length - 1) { x /= 1024; i++; }
  return `${x.toFixed(i ? 1 : 0)} ${u[i]}`;
};

// Warna per store (konsisten di semua halaman).
export const WARNA_STORE: Record<string, string> = {
  PostgreSQL: "#336791", Redis: "#c6302b", ClickHouse: "#d9a400", OpenSearch: "#005eb8",
};
export const warnaStore = (s: string) => WARNA_STORE[s] || "var(--redup)";

export function Galat({ teks }: { teks: string }) {
  return teks ? <p className="galat">{teks}</p> : null;
}

// Batang horizontal berskala log untuk latency (ms).
export function BatangLog({ data, maks, label = (v: number) => `${v.toFixed(2)} ms` }: {
  data: { nama: string; nilai: number; warna: string; sub?: string; nilai2?: number }[]; maks?: number; label?: (v: number) => string;
}) {
  const vals = data.flatMap(d => [d.nilai, d.nilai2 || 0]).filter(v => v > 0);
  const atas = Math.log10(Math.max(maks || 0, ...vals, 1) * 1.5), bawah = Math.log10(Math.max(Math.min(...vals, 1) / 2, 0.01));
  const x = (v: number) => v <= 0 ? 0 : Math.max(2, ((Math.log10(v) - bawah) / (atas - bawah)) * 100);
  return (
    <div>
      {data.map(d => (
        <div key={d.nama} style={{ display: "grid", gridTemplateColumns: "minmax(140px, 32%) 1fr", gap: 8, alignItems: "center", margin: "4px 0" }}>
          <div style={{ fontSize: 12 }}><b>{d.nama}</b>{d.sub && <div className="redup" style={{ fontSize: 11 }}>{d.sub}</div>}</div>
          <div style={{ position: "relative", height: 20, background: "var(--kode)", borderRadius: 4 }}>
            {d.nilai2 !== undefined && <div title="p95" style={{ position: "absolute", height: "100%", width: `${x(d.nilai2)}%`, background: d.warna, opacity: .3, borderRadius: 4 }} />}
            <div style={{ position: "absolute", height: "100%", width: `${x(d.nilai)}%`, background: d.warna, borderRadius: 4 }} />
            <span style={{ position: "absolute", left: 6, top: 2, fontSize: 11, color: "#fff", textShadow: "0 0 3px #000" }}>
              {label(d.nilai)}{d.nilai2 !== undefined ? ` · p95 ${label(d.nilai2)}` : ""}
            </span>
          </div>
        </div>
      ))}
      <div className="redup" style={{ fontSize: 11 }}>Skala logaritmik: setiap kelipatan sepuluh mendapat panjang yang sama.</div>
    </div>
  );
}
