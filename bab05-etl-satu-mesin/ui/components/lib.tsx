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

export function Status({ s }: { s: string }) {
  const k = ["lulus", "ok", "ya"].includes(s) ? "ok" : ["gagal", "error"].includes(s) ? "gagal"
    : ["peringatan", "dilewati"].includes(s) ? "warn" : "netral";
  return <span className={`lencana ${k}`}>{s}</span>;
}

export function Galat({ teks }: { teks: string }) {
  return teks ? <p className="galat">{teks}</p> : null;
}

// Tombol aksi: memanggil API POST, menampilkan status sibuk, lalu memanggil selesai().
export function useAksi(selesai?: () => void) {
  const [sibuk, setSibuk] = useState("");
  const [pesan, setPesan] = useState("");
  const [galat, setGalat] = useState("");
  const jalankan = async (path: string, label: string) => {
    setSibuk(label); setGalat("");
    try {
      const r = await api<any>(path, {});
      setPesan(r?.pesan ?? `${label}: selesai`);
      selesai?.();
      return r;
    } catch (e) { setGalat(String(e)); } finally { setSibuk(""); }
  };
  return { sibuk, pesan, galat, jalankan };
}

// Pekerjaan panjang di API (benchmark, memori, zero-copy, dataset): polling /api/job tiap 1,5 detik.
export function useJob(selesai?: () => void) {
  const [job, setJob] = useState<any>(null);
  const [galat, setGalat] = useState("");
  const tarik = useCallback(async () => {
    try { const j = await api<any>("job"); setJob(j); return j; } catch (e) { setGalat(String(e)); }
  }, []);
  useEffect(() => { tarik(); }, [tarik]);
  useEffect(() => {
    if (job?.status !== "berjalan") return;
    const t = setInterval(async () => {
      const j = await tarik();
      if (j && j.status !== "berjalan") selesai?.();
    }, 1500);
    return () => clearInterval(t);
  }, [job?.status, tarik, selesai]);
  const mulai = async (path: string) => {
    setGalat("");
    try { setJob(await api<any>(path, {})); } catch (e) { setGalat(String(e)); }
  };
  return { job, galat, mulai, berjalan: job?.status === "berjalan" };
}

// Batang horizontal SVG; garis = nilai pembanding (mis. anggaran memori) digambar sebagai garis putus.
export function Batang({ data, satuan = "", garis }: {
  data: { label: string; nilai: number | null; warna?: string; ket?: string }[]; satuan?: string;
  garis?: { nilai: number; label: string }[];
}) {
  const tinggi = 26, kiri = 150, lebar = 640;
  const maks = Math.max(1, ...data.map(d => d.nilai ?? 0), ...(garis ?? []).map(g => g.nilai));
  const sx = (v: number) => kiri + (v / maks) * (lebar - kiri - 120);
  const h = data.length * tinggi + 18;
  return (
    <svg width="100%" viewBox={`0 0 ${lebar} ${h}`} role="img" style={{ maxWidth: 720 }}>
      {data.map((d, i) => (
        <g key={d.label + i} transform={`translate(0, ${i * tinggi})`}>
          <text x={kiri - 6} y={tinggi * 0.62} textAnchor="end">{d.label}</text>
          {d.nilai !== null ? <>
            <rect x={kiri} y={4} width={Math.max(1, sx(d.nilai) - kiri)} height={tinggi - 9} rx={3} fill={d.warna ?? "var(--utama)"} />
            <text x={sx(d.nilai) + 6} y={tinggi * 0.62}>{rupiah(d.nilai)}{satuan}{d.ket ? ` · ${d.ket}` : ""}</text>
          </> : <text x={kiri + 4} y={tinggi * 0.62} style={{ fill: "var(--merah)" }}>{d.ket ?? "gagal"}</text>}
        </g>))}
      {(garis ?? []).map(g => (
        <g key={g.label}>
          <line x1={sx(g.nilai)} x2={sx(g.nilai)} y1={0} y2={h - 14} stroke="var(--merah)" strokeDasharray="4 3" />
          <text x={sx(g.nilai)} y={h - 3} textAnchor="middle" style={{ fill: "var(--merah)" }}>{g.label}</text>
        </g>))}
    </svg>
  );
}

export const WARNA: Record<string, string> = {
  pandas: "#8a6d3b", polars_eager: "#6b8fb3", polars_lazy: "#2f6fa8", polars_streaming: "#1d4f7a",
  duckdb: "#b8862b", datafusion: "#7a4f9a",
};
export const juta = (n: number) => n >= 1_000_000 ? `${n / 1_000_000} juta` : `${n / 1000} ribu`;
