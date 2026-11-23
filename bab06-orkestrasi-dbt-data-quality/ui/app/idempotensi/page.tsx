"use client";
import { useApi, angka, Galat } from "@/components/lib";

type Baris = {
  tgl: string; kontrol_trx?: number; kontrol_nominal?: number; gold_trx?: number; gold_nominal?: number; gold_sidik?: string;
  gold_part_diubah?: string; naif_trx?: number; naif_nominal?: number; naif_insert?: number; publikasi?: number; publikasi_terakhir?: string;
};

function Grafik({ data }: { data: Baris[] }) {
  const maks = Math.max(1, ...data.flatMap(d => [d.kontrol_trx || 0, d.gold_trx || 0, d.naif_trx || 0]));
  const H = 200, BW = 14, G = 18, L = 40;
  const seri: [keyof Baris, string, string][] = [["kontrol_trx", "var(--redup)", "kontrol core banking"], ["gold_trx", "var(--utama)", "gold (timpa partisi + gerbang)"], ["naif_trx", "var(--merah)", "gold_naif (INSERT biasa)"]];
  const W = L + data.length * (3 * BW + G) + 10;
  return (
    <div style={{ overflowX: "auto" }}>
      <svg width={Math.max(W, 320)} height={H + 50} role="img" aria-label="Jumlah transaksi per tanggal: kontrol, gold, naif">
        {[0, 0.5, 1].map(f => <g key={f}><line x1={L} x2={W} y1={H - f * H + 5} y2={H - f * H + 5} stroke="var(--garis)" /><text x={L - 4} y={H - f * H + 9} textAnchor="end">{angka(Math.round(f * maks))}</text></g>)}
        {data.map((d, i) => (
          <g key={d.tgl} transform={`translate(${L + 6 + i * (3 * BW + G)},0)`}>
            {seri.map(([k, c, label], j) => {
              const v = Number(d[k] || 0), h = (v / maks) * H;
              return <rect key={String(k)} x={j * BW} y={H - h + 5} width={BW - 2} height={h} fill={c} rx={2}><title>{`${d.tgl} ${label}: ${angka(v)}`}</title></rect>;
            })}
            <text x={1.5 * BW} y={H + 20} textAnchor="middle">{d.tgl.slice(5)}</text>
          </g>
        ))}
      </svg>
      <div className="legenda">{seri.map(([k, c, l]) => <span key={String(k)}><i style={{ background: c }} />{l}</span>)}</div>
    </div>
  );
}

export default function Idempotensi() {
  const { data, galat } = useApi<{ per_tgl: Baris[] }>("gold", 4000);
  const rows = data?.per_tgl || [];
  return (
    <>
      <h1>Idempotency dan backfill</h1>
      <p className="redup">Dua pipeline membaca raw yang sama. <b>gold</b> menimpa partisi tanggalnya (<code>REPLACE PARTITION</code>) dan hanya dipublikasikan bila dbt test lulus. <b>gold_naif</b> memakai <code>INSERT</code> biasa tanpa gerbang: setiap retry, run ulang, atau backfill menambah baris.</p>
      <Galat teks={galat} />
      <div className="kartu"><h2>Jumlah transaksi per tanggal</h2><Grafik data={rows} /></div>
      <div className="kartu">
        <h2>Bukti per tanggal</h2>
        <div className="tabel-wadah">
          <table>
            <thead><tr><th>tgl</th><th className="angka">kontrol trx</th><th className="angka">gold trx</th><th className="angka">gold nominal</th><th>sidik gold</th><th>part gold diubah</th><th className="angka">publikasi</th><th className="angka">naif trx</th><th className="angka">naif INSERT</th><th>status</th></tr></thead>
            <tbody>{rows.map(r => {
              const cocok = r.gold_trx !== undefined && r.gold_trx === r.kontrol_trx;
              const ganda = (r.naif_insert || 0) > 1;
              return (
                <tr key={r.tgl}>
                  <td>{r.tgl}</td><td className="angka">{angka(r.kontrol_trx)}</td><td className="angka">{angka(r.gold_trx)}</td><td className="angka">{angka(r.gold_nominal)}</td>
                  <td><code>{r.gold_sidik ? r.gold_sidik.slice(0, 10) : "–"}</code></td><td>{r.gold_part_diubah || "–"}</td><td className="angka">{angka(r.publikasi)}</td>
                  <td className="angka" style={{ color: ganda ? "var(--merah)" : undefined }}>{angka(r.naif_trx)}</td><td className="angka">{angka(r.naif_insert)}</td>
                  <td>{r.gold_trx === undefined ? <span className="lencana netral">belum publish</span> : cocok ? <span className="lencana ok">gold = kontrol</span> : <span className="lencana gagal">selisih</span>}
                    {ganda && <span className="lencana gagal" style={{ marginLeft: 4 }}>naif ×{r.naif_insert}</span>}</td>
                </tr>);
            })}</tbody>
          </table>
        </div>
        <p className="redup">Sidik gold = <code>groupBitXor(cityHash64(cabang_id, jumlah_trx, total_nominal))</code> per partisi. Jalankan ulang satu tanggal: kolom publikasi bertambah, waktu part berubah, tetapi sidik dan angka tetap. Pada run dengan data rusak, gold tidak dipublikasikan sehingga sidik dan waktu part tidak berubah, sedangkan gold_naif ikut tercemar.</p>
      </div>
    </>
  );
}
