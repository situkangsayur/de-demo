"use client";
import { useCallback, useEffect, useState } from "react";

export type Hasil = {
  sumber: string; mesin: string; sql: string; kolom: string[]; baris: unknown[][];
  latency_ms: number; engine_ms: number; error?: string;
};

export async function api<T = any>(path: string, body?: unknown): Promise<T> {
  const r = await fetch(`/api/${path}`, body === undefined ? { cache: "no-store" } :
    { method: "POST", body: JSON.stringify(body), headers: { "Content-Type": "application/json" } });
  const j = await r.json();
  if (!r.ok && j?.error) throw new Error(j.error);
  return j as T;
}

export function useApi<T = any>(path: string | null, ulangMs = 0) {
  const [data, setData] = useState<T | null>(null);
  const [galat, setGalat] = useState<string>("");
  const [muat, setMuat] = useState(false);
  const segarkan = useCallback(async () => {
    if (!path) return;
    setMuat(true);
    try { setData(await api<T>(path)); setGalat(""); } catch (e) { setGalat(String(e)); }
    setMuat(false);
  }, [path]);
  useEffect(() => {
    segarkan();
    if (ulangMs > 0) { const t = setInterval(segarkan, ulangMs); return () => clearInterval(t); }
  }, [segarkan, ulangMs]);
  return { data, galat, muat, segarkan };
}

export const rupiah = (v: unknown) =>
  typeof v === "number" || (typeof v === "string" && v !== "" && !isNaN(Number(v)))
    ? new Intl.NumberFormat("id-ID", { maximumFractionDigits: 2 }).format(Number(v)) : String(v ?? "∅");

export const byte = (n: number) => {
  const u = ["B", "KB", "MB", "GB", "TB"]; let i = 0; let x = n;
  while (x >= 1024 && i < u.length - 1) { x /= 1024; i++; }
  return `${x.toFixed(i ? 1 : 0)} ${u[i]}`;
};

export function Tabel({ kolom, baris, maks = 200 }: { kolom: string[]; baris: unknown[][]; maks?: number }) {
  const angka = kolom.map((_, i) => baris.length > 0 && baris.every(r => r[i] === null || typeof r[i] === "number"));
  return (
    <div className="tabel-wadah">
      <table>
        <thead><tr>{kolom.map((k, i) => <th key={k + i} className={angka[i] ? "angka" : ""}>{k}</th>)}</tr></thead>
        <tbody>
          {baris.slice(0, maks).map((r, j) => (
            <tr key={j}>{r.map((v, i) => (
              <td key={i} className={angka[i] ? "angka" : ""}>
                {typeof v === "boolean" ? (v ? "ya" : "tidak") : typeof v === "number" ? rupiah(v) : String(v ?? "∅")}
              </td>))}</tr>
          ))}
        </tbody>
      </table>
      {baris.length > maks && <p className="redup">… {baris.length - maks} baris lagi</p>}
      {baris.length === 0 && <p className="redup">tidak ada baris</p>}
    </div>
  );
}

export function Batang({ data, satuan = "", tinggi = 22 }: { data: { label: string; nilai: number; tanda?: boolean }[]; satuan?: string; tinggi?: number }) {
  const maks = Math.max(1, ...data.map(d => d.nilai));
  const lebarLabel = 150;
  return (
    <svg width="100%" viewBox={`0 0 640 ${data.length * tinggi + 4}`} role="img">
      {data.map((d, i) => {
        const w = (d.nilai / maks) * (640 - lebarLabel - 110);
        return (
          <g key={d.label + i} transform={`translate(0, ${i * tinggi})`}>
            <text x={lebarLabel - 6} y={tinggi * 0.65} textAnchor="end">{d.label}</text>
            <rect x={lebarLabel} y={3} width={Math.max(1, w)} height={tinggi - 7} rx={3}
              fill={d.tanda ? "var(--merah)" : "var(--utama)"} />
            <text x={lebarLabel + w + 6} y={tinggi * 0.65}>{rupiah(d.nilai)}{satuan}</text>
          </g>
        );
      })}
    </svg>
  );
}

export function Garis({ seri, label }: { seri: { nama: string; warna: string; titik: number[] }[]; label: string[] }) {
  const W = 640, H = 200, P = 36;
  const maks = Math.max(1, ...seri.flatMap(s => s.titik));
  const x = (i: number) => P + (i * (W - 2 * P)) / Math.max(1, label.length - 1);
  const y = (v: number) => H - P + 8 - (v / maks) * (H - 2 * P);
  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} role="img">
      <line x1={P} x2={W - P} y1={H - P + 8} y2={H - P + 8} stroke="var(--garis)" />
      {seri.map(s => (
        <polyline key={s.nama} fill="none" stroke={s.warna} strokeWidth={2}
          points={s.titik.map((v, i) => `${x(i)},${y(v)}`).join(" ")} />
      ))}
      {label.map((l, i) => (i % Math.ceil(label.length / 8) === 0 || i === label.length - 1) &&
        <text key={l} x={x(i)} y={H - 6} textAnchor="middle">{l.length >= 10 ? l.slice(5) : l}</text>)}
      <text x={P} y={14}>maks {rupiah(maks)}</text>
      {seri.map((s, i) => <text key={s.nama} x={W - P} y={14 + i * 13} textAnchor="end" style={{ fill: s.warna }}>{s.nama}</text>)}
    </svg>
  );
}

export function Status({ s }: { s: string }) {
  const k = ["success", "lulus", "identik"].includes(s) ? "ok"
    : ["failed", "gagal", "upstream_failed"].includes(s) ? "gagal"
    : ["peringatan", "up_for_retry", "running", "queued"].includes(s) ? "warn" : "netral";
  return <span className={`lencana ${k}`}>{s}</span>;
}
