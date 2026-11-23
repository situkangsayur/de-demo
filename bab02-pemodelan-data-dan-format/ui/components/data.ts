"use client";
import { useEffect, useState } from "react";

export async function ambil<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path, { cache: "no-store", ...init, headers: { "Content-Type": "application/json" } });
  const t = await r.text();
  let j: unknown;
  try { j = JSON.parse(t); } catch { throw new Error(`HTTP ${r.status}: ${t.slice(0, 200)}`); }
  if (!r.ok && r.status >= 500) throw new Error((j as { error?: string }).error || `HTTP ${r.status}`);
  return j as T;
}

export function usePoll<T>(path: string | null, ms: number): { data: T | null; galat: string } {
  const [data, setData] = useState<T | null>(null);
  const [galat, setGalat] = useState("");
  useEffect(() => {
    if (!path) return;
    let hidup = true;
    const jalan = () => ambil<T>(path).then((d) => { if (hidup) { setData(d); setGalat(""); } })
      .catch((e) => hidup && setGalat(String(e)));
    jalan();
    const t = setInterval(jalan, ms);
    return () => { hidup = false; clearInterval(t); };
  }, [path, ms]);
  return { data, galat };
}
