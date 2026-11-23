"use client";
import { usePoll } from "@/components/data";
import { GrafikBatang, rupiah, WARNA } from "@/components/grafik";

type Dash = {
  versi_gold: number; kesegaran_detik: number; per_kanal: Record<string, number>; per_cabang: Record<string, number>;
  per_tanggal: Record<string, number>; hari_ini: Record<string, number>; jumlah_hari_ini: Record<string, number>;
  rekonsiliasi: { silver: number; gold: number; cocok: boolean };
};
type Platform = { aktif: boolean; pesan?: string; url?: string; stats?: { streams?: unknown; bronze?: unknown; seen_since_start?: unknown } };

export default function Dashboard() {
  const { data: d, galat } = usePoll<Dash>("/api/dashboard", 2000);
  const { data: pf } = usePoll<Platform>("/api/platform", 5000);
  const urutTanggal = d ? Object.keys(d.per_tanggal).sort() : [];
  return (
    <>
      <h1>Dashboard: hot path dan cold path</h1>
      <p className="redup">Angka hari ini dihitung dengan tiga cara. Hot path menjumlah event begitu tiba (detik, belum dibersihkan).
        Bronze mentah menjumlah semua baris bronze apa adanya. Gold menjumlah hasil silver yang sudah dibersihkan, tetapi menunggu batch.</p>
      {galat && <p className="galat">{galat}</p>}
      {d && <>
        <section className="kartu">
          <h2>Total nominal hari ini menurut tiga jalur</h2>
          <GrafikBatang format={rupiah} lebarLabel={190} data={[
            { label: "hot path (event BANK)", nilai: d.hari_ini.hot || 0, warna: WARNA[2], catatan: `· ${d.jumlah_hari_ini.hot || 0} event` },
            { label: "bronze mentah (app+cdc)", nilai: d.hari_ini.bronze_mentah || 0, warna: WARNA[1], catatan: `· ${d.jumlah_hari_ini.bronze_mentah || 0} baris` },
            { label: "gold (bersih)", nilai: d.hari_ini.gold || 0, warna: WARNA[0], catatan: `· ${d.jumlah_hari_ini.gold || 0} trx` },
          ]} />
          <ul className="kecil">
            <li>Bronze mentah kira-kira dua kali gold karena satu transaksi tercatat dari dua sumber (event aplikasi dan CDC), ditambah redelivery.</li>
            <li>Hot path cepat, tetapi ikut menghitung transaksi yang kelak dikarantina, dan menghitung per waktu tiba, bukan per tanggal kejadian.</li>
            <li>Gold baru berubah setelah batch berjalan. Kesegaran gold sekarang: <b>{d.kesegaran_detik.toFixed(0)} detik</b> (versi {d.versi_gold}).</li>
          </ul>
          <p className="kecil">Rekonsiliasi silver vs gold: <span className={`lencana ${d.rekonsiliasi.cocok ? "ok" : "gagal"}`}>
            {d.rekonsiliasi.cocok ? "cocok" : "tidak cocok"}</span> ({rupiah(d.rekonsiliasi.silver)} vs {rupiah(d.rekonsiliasi.gold)})</p>
        </section>
        <div className="kisi">
          <section className="kartu"><h2>Per kanal (gold)</h2>
            <GrafikBatang format={rupiah} data={Object.entries(d.per_kanal).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ label: k, nilai: v }))} /></section>
          <section className="kartu"><h2>Per cabang (gold)</h2>
            <GrafikBatang format={rupiah} lebarLabel={170} data={Object.entries(d.per_cabang).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ label: k, nilai: v, warna: WARNA[3] }))} /></section>
        </div>
        <section className="kartu"><h2>Per tanggal kejadian (gold)</h2>
          <GrafikBatang format={rupiah} data={urutTanggal.map((t) => ({ label: t, nilai: d.per_tanggal[t], warna: WARNA[4] }))} />
          <p className="kecil redup">Transaksi dengan occurred_at mundur masuk ke tanggal kejadiannya. Kirim transaksi "26 jam lalu" lalu jalankan batch:
            batang kemarin bertambah, batang hari ini tidak.</p></section>
      </>}
      <section className="kartu">
        <h2>Platform penuh</h2>
        {pf?.aktif ? <>
          <p className="kecil">Terhubung ke {pf.url}. Statistik aliran data nyata dari platform (stream NATS dan bronze di RustFS):</p>
          <pre>{JSON.stringify({ streams: pf.stats?.streams, bronze: pf.stats?.bronze }, null, 2).slice(0, 4000)}</pre>
        </> : <p className="kecil redup">{pf?.pesan || "memeriksa..."}. Bila platform menyala, isi PLATFORM_FLOW_URL di .env (mis. http://10.100.21.22:52001 di server lab)
          untuk melihat angka aliran data yang sebenarnya.</p>}
      </section>
    </>
  );
}
