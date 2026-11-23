// Halaman statis: peran di sepanjang pipeline dan tier hot/warm/cold/archive (slide P01).
const PERAN = [
  ["Data engineer", "ingestion, bronze, silver", "Pipeline andal, lakehouse, stream event", "duplikat app+cdc, redelivery, kanal tidak seragam, nominal kosong"],
  ["Analytics engineer", "gold", "Model dbt, definisi metric", "grain tidak jelas, rekonsiliasi silver vs gold"],
  ["Data analyst", "dashboard", "Analisis, dashboard, rekomendasi", "membaca angka hot path sebagai angka final"],
  ["Data scientist", "fitur dari gold/silver", "Model, eksperimen", "kebocoran data dari masa depan saat training"],
  ["ML engineer", "serving model", "Layanan model, feature store", "fitur di produksi berbeda dengan saat training"],
  ["Data steward / DPO", "semua tahap (undercurrent)", "Katalog, kebijakan, bukti audit", "PII (NIK, no HP) bocor ke dashboard"],
];
const TIER = [
  ["Hot", "memori, SSD/NVMe, Redis, ClickHouse", "mikro-milidetik", "paling mahal", "saldo, skor fraud, 7 hari terakhir", "var(--s3)"],
  ["Warm", "SSD/HDD, lakehouse di object storage", "detik", "sedang", "13 bulan terakhir untuk dashboard", "var(--s2)"],
  ["Cold", "object storage kelas infrequent access", "detik, ada retrieval cost", "murah", "data historis untuk melatih model", "var(--s1)"],
  ["Archive", "Glacier, pita magnetik", "menit-jam", "sangat murah", "bukti transaksi 10 tahun", "var(--s5)"],
];

export default function Peran() {
  return (
    <>
      <h1>Siapa mengerjakan apa, dan di tier mana data tinggal</h1>
      <section className="kartu">
        <h2>Peran di sepanjang pipeline</h2>
        <p className="kecil redup">Data engineer berada di upstream: kesalahan yang lolos dari bronze dan silver mengalir ke semua peran downstream.</p>
        <div className="tabel-wadah"><table>
          <thead><tr><th>Peran</th><th>Tahap di lab</th><th>Output khas</th><th>Kesalahan yang harus ia tangkap</th></tr></thead>
          <tbody>{PERAN.map((r) => <tr key={r[0]}>{r.map((c, i) => <td key={i} style={{ whiteSpace: "normal" }}>{i === 0 ? <b>{c}</b> : c}</td>)}</tr>)}</tbody>
        </table></div>
      </section>
      <section className="kartu">
        <h2>Empat tier</h2>
        <svg viewBox="0 0 600 70" width="100%" role="img">
          {TIER.map((t, i) => (
            <g key={t[0]} transform={`translate(${i * 150} 0)`}>
              <rect x={2} y={10} width={144} height={30} rx={6} fill={t[5]} />
              <text x={74} y={30} textAnchor="middle" style={{ fill: "#fff", fontWeight: 700 }}>{t[0]}</text>
              <text x={74} y={58} textAnchor="middle">{t[2]}</text>
            </g>
          ))}
        </svg>
        <p className="kecil redup">Dari kiri ke kanan: storage cost per GB turun, latency baca dan retrieval cost naik. Bab 3 menghitungnya.</p>
        <div className="tabel-wadah"><table>
          <thead><tr><th>Tier</th><th>Media khas</th><th>Latency baca</th><th>Storage cost</th><th>Contoh di bank</th></tr></thead>
          <tbody>{TIER.map((r) => <tr key={r[0]}>{r.slice(0, 5).map((c, i) => <td key={i} style={{ whiteSpace: "normal" }}>{i === 0 ? <b>{c}</b> : c}</td>)}</tr>)}</tbody>
        </table></div>
      </section>
    </>
  );
}
