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
  return { data, galat, segarkan };
}

export const angka = (v: unknown) =>
  v === null || v === undefined || v === "" ? "–" : new Intl.NumberFormat("id-ID").format(Number(v));

export const jam = (s?: string | null) => {
  if (!s) return "–";
  const d = new Date(s);
  return isNaN(d.getTime()) ? s : d.toLocaleTimeString("id-ID", { hour12: false });
};

// Warna status task Airflow / test dbt (variabel CSS dari globals.css).
export const WARNA: Record<string, string> = {
  success: "var(--hijau)", pass: "var(--hijau)", failed: "var(--merah)", fail: "var(--merah)", error: "var(--merah)",
  upstream_failed: "#d98b2b", running: "var(--utama)", queued: "#8a94a6", scheduled: "#8a94a6",
  up_for_retry: "#e3b505", up_for_reschedule: "#4a90c2", warn: "var(--kuning)", skipped: "#c4a7e7",
  deferred: "#9b7fd1", removed: "#aaa", restarting: "#b085f5",
};
export const warna = (s?: string | null) => (s ? WARNA[s] || "#9aa59f" : "var(--garis)");

export function Lencana({ s }: { s?: string | null }) {
  const st = s || "belum";
  return <span className="lencana" style={{ background: `color-mix(in srgb, ${warna(s)} 22%, transparent)`, color: warna(s) }}>{st}</span>;
}

export function Galat({ teks }: { teks: string }) {
  return teks ? <p className="galat">{teks}</p> : null;
}
