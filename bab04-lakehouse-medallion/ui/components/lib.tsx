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
