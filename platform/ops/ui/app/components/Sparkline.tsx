// Grafik kecil msg/detik 60 detik terakhir: biru = event aplikasi, oranye = CDC.
export default function Sparkline({ series }: { series: { t: number; app: number; cdc: number }[] }) {
  const w = 240;
  const h = 36;
  const max = Math.max(1, ...series.map((s) => Math.max(s.app, s.cdc)));
  const pts = (k: "app" | "cdc") =>
    series.map((s, i) => `${((i / Math.max(1, series.length - 1)) * w).toFixed(1)},${(h - (s[k] / max) * (h - 2) - 1).toFixed(1)}`).join(" ");
  return (
    <span className="row small muted" title="pesan per detik, 60 detik terakhir">
      60 dtk (maks {max}/s)
      <svg width={w} height={h} role="img" aria-label="laju pesan per detik">
        <polyline points={pts("app")} fill="none" stroke="var(--app)" strokeWidth="1.5" />
        <polyline points={pts("cdc")} fill="none" stroke="var(--cdc)" strokeWidth="1.5" />
      </svg>
    </span>
  );
}
