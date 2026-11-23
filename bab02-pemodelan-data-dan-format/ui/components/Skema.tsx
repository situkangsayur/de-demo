// Diagram skema sederhana: kotak tabel dan garis relasi. Tabel yang dipakai query disorot.
type Tabel = { nama: string; x: number; y: number; kolom: string[]; fakta?: boolean };

export function Skema({ tabel, relasi, sorot, tinggi = 270 }: { tabel: Tabel[]; relasi: [string, string][]; sorot: string[]; tinggi?: number }) {
  const pos = Object.fromEntries(tabel.map((t) => [t.nama, t]));
  const lebar = 130;
  const tinggiT = (t: Tabel) => 26 + t.kolom.length * 13;
  return (
    <svg viewBox={`0 0 560 ${tinggi}`} width="100%" role="img">
      {relasi.map(([a, b]) => {
        const p = pos[a], q = pos[b];
        const aktif = sorot.includes(a) && sorot.includes(b);
        return <line key={a + b} x1={p.x + lebar / 2} y1={p.y + tinggiT(p) / 2} x2={q.x + lebar / 2} y2={q.y + tinggiT(q) / 2}
          stroke={aktif ? "var(--s2)" : "var(--garis)"} strokeWidth={aktif ? 3 : 1.5} />;
      })}
      {tabel.map((t) => {
        const aktif = sorot.includes(t.nama);
        return (
          <g key={t.nama} transform={`translate(${t.x} ${t.y})`}>
            <rect width={lebar} height={tinggiT(t)} rx={6} fill="var(--panel)" stroke={aktif ? "var(--s1)" : "var(--garis)"} strokeWidth={aktif ? 2.5 : 1} />
            <rect width={lebar} height={18} rx={6} fill={t.fakta ? "var(--s2)" : aktif ? "var(--s1)" : "var(--s5)"} />
            <text x={6} y={13} style={{ fill: "#fff", fontWeight: 700 }}>{t.nama}</text>
            {t.kolom.map((k, i) => <text key={k} x={6} y={32 + i * 13} style={{ fontSize: 10 }}>{k}</text>)}
          </g>
        );
      })}
    </svg>
  );
}
