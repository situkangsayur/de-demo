// Grafik SVG sederhana tanpa pustaka tambahan: batang horizontal, batang bertumpuk, dan garis.
import React from "react";

export const WARNA = ["var(--s1)", "var(--s2)", "var(--s3)", "var(--s4)", "var(--s5)"];

export type Batang = { label: string; nilai: number; warna?: string; catatan?: string };

export function GrafikBatang({ data, format = (v: number) => v.toLocaleString("id-ID"), lebarLabel = 150 }:
  { data: Batang[]; format?: (v: number) => string; lebarLabel?: number }) {
  const maks = Math.max(1e-9, ...data.map((d) => d.nilai));
  const tinggi = 26;
  return (
    <svg viewBox={`0 0 600 ${data.length * tinggi + 4}`} width="100%" style={{ maxWidth: 820 }} role="img">
      {data.map((d, i) => {
        const w = Math.max(1, (d.nilai / maks) * (600 - lebarLabel - 150));
        return (
          <g key={d.label + i} transform={`translate(0 ${i * tinggi})`}>
            <text x={lebarLabel - 6} y={17} textAnchor="end">{d.label}</text>
            <rect x={lebarLabel} y={5} width={w} height={16} rx={3} fill={d.warna || WARNA[0]} />
            <text x={lebarLabel + w + 6} y={17}>{format(d.nilai)}{d.catatan ? ` ${d.catatan}` : ""}</text>
          </g>
        );
      })}
    </svg>
  );
}

export type Tumpuk = { label: string; bagian: { nama: string; nilai: number }[] };

export function GrafikTumpuk({ data, seri, format = (v: number) => v.toLocaleString("id-ID"), lebarLabel = 150, warnaSeri = WARNA }:
  { data: Tumpuk[]; seri: string[]; format?: (v: number) => string; lebarLabel?: number; warnaSeri?: string[] }) {
  const maks = Math.max(1e-9, ...data.map((d) => d.bagian.reduce((a, b) => a + b.nilai, 0)));
  const tinggi = 28;
  const ruang = 600 - lebarLabel - 110;
  return (
    <div>
      <svg viewBox={`0 0 600 ${data.length * tinggi + 4}`} width="100%" style={{ maxWidth: 820 }} role="img">
        {data.map((d, i) => {
          let x = lebarLabel;
          const total = d.bagian.reduce((a, b) => a + b.nilai, 0);
          return (
            <g key={d.label} transform={`translate(0 ${i * tinggi})`}>
              <text x={lebarLabel - 6} y={18} textAnchor="end">{d.label}</text>
              {d.bagian.map((b) => {
                const w = (b.nilai / maks) * ruang;
                const el = <rect key={b.nama} x={x} y={5} width={Math.max(0, w)} height={18}
                  fill={warnaSeri[seri.indexOf(b.nama) % warnaSeri.length]}><title>{`${b.nama}: ${format(b.nilai)}`}</title></rect>;
                x += w;
                return el;
              })}
              <text x={x + 6} y={18}>{format(total)}</text>
            </g>
          );
        })}
      </svg>
      <Legenda seri={seri} warna={warnaSeri} />
    </div>
  );
}

export function Legenda({ seri, warna = WARNA }: { seri: string[]; warna?: string[] }) {
  return (
    <div className="legenda">
      {seri.map((s, i) => <span key={s}><i style={{ background: warna[i % warna.length] }} />{s}</span>)}
    </div>
  );
}

export function GrafikGaris({ seri, labelX = "", format = (v: number) => v.toLocaleString("id-ID") }:
  { seri: { nama: string; titik: number[] }[]; labelX?: string; format?: (v: number) => string }) {
  const n = Math.max(1, ...seri.map((s) => s.titik.length));
  const maks = Math.max(1e-9, ...seri.flatMap((s) => s.titik));
  const W = 600, H = 220, kiri = 70, bawah = 24;
  const x = (i: number) => kiri + (i / Math.max(1, n - 1)) * (W - kiri - 10);
  const y = (v: number) => H - bawah - (v / maks) * (H - bawah - 10);
  return (
    <div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" role="img">
        {[0, 0.5, 1].map((f) => (
          <g key={f}>
            <line x1={kiri} x2={W - 10} y1={y(maks * f)} y2={y(maks * f)} stroke="var(--garis)" />
            <text x={kiri - 6} y={y(maks * f) + 4} textAnchor="end">{format(maks * f)}</text>
          </g>
        ))}
        <text x={W - 10} y={H - 6} textAnchor="end">{labelX}</text>
        {seri.map((s, k) => (
          <polyline key={s.nama} fill="none" stroke={WARNA[k % WARNA.length]} strokeWidth={2}
            points={s.titik.map((v, i) => `${x(i)},${y(v)}`).join(" ")} />
        ))}
      </svg>
      <Legenda seri={seri.map((s) => s.nama)} />
    </div>
  );
}

export function rupiah(v: number): string {
  if (Math.abs(v) >= 1e12) return `Rp ${(v / 1e12).toLocaleString("id-ID", { maximumFractionDigits: 2 })} T`;
  if (Math.abs(v) >= 1e9) return `Rp ${(v / 1e9).toLocaleString("id-ID", { maximumFractionDigits: 2 })} M`;
  if (Math.abs(v) >= 1e6) return `Rp ${(v / 1e6).toLocaleString("id-ID", { maximumFractionDigits: 1 })} jt`;
  return `Rp ${Math.round(v).toLocaleString("id-ID")}`;
}

export function ukuran(b: number): string {
  if (b >= 1 << 30) return `${(b / (1 << 30)).toFixed(2)} GB`;
  if (b >= 1 << 20) return `${(b / (1 << 20)).toFixed(2)} MB`;
  if (b >= 1 << 10) return `${(b / (1 << 10)).toFixed(1)} KB`;
  return `${b} B`;
}
