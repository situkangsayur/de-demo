"use client";
import { useState } from "react";
import { api, byte, Galat, rupiah, Status, Tabel, useApi } from "@/components/lib";

export default function Pipeline() {
  const st = useApi<any>("pipeline/status");
  const [hasilBaru, setHasil] = useState<any>(null);
  const [sibuk, setSibuk] = useState("");
  const [galat, setGalat] = useState("");
  const [ukuran, setUkuran] = useState(300000);
  const aksi = async (path: string, label: string, simpan = false) => {
    setSibuk(label); setGalat("");
    try { const r = await api<any>(path, {}); if (simpan) setHasil(r); st.segarkan(); }
    catch (e) { setGalat(String(e)); } finally { setSibuk(""); }
  };
  const riw: any[] = st.data?.riwayat ?? [];
  const hasil = hasilBaru ?? st.data?.terakhir;
  const sumber = hasil?.dedup?.sumber ?? {};
  const totalSumber = Object.values(sumber).reduce((a: number, b: any) => a + Number(b), 0) as number;
  return (
    <>
      <h1>Pipeline bronze → silver → gold di satu mesin</h1>
      <p className="redup">Bronze bergaya platform (app + cdc, kolom string) → Polars lazy (cast, dedup app vs cdc, kanal baku)
        → tokenization rekening → pemeriksaan ringan → silver per tgl → DuckDB SQL → gold. Partisi ditulis ke folder sementara
        lalu ditukar, jadi menjalankan ulang tanggal yang sama tidak menggandakan data.</p>
      <div className="kartu baris">
        <label>Transaksi bronze <select value={ukuran} onChange={e => setUkuran(Number(e.target.value))}>
          {[100000, 300000, 1000000].map(n => <option key={n} value={n}>{rupiah(n)}</option>)}</select></label>
        <button disabled={!!sibuk} onClick={() => aksi(`pipeline/bronze?baris=${ukuran}`, "Bangkitkan bronze")}>Bangkitkan bronze</button>
        <button className="utama" disabled={!!sibuk || !st.data?.bronze?.ada} onClick={() => aksi("pipeline/jalankan", "Jalankan pipeline", true)}>Jalankan pipeline</button>
        {sibuk && <span className="lencana warn">{sibuk}…</span>}
        {st.data?.bronze?.ada && <span className="kecil">bronze: {st.data.bronze.berkas} berkas, {byte(st.data.bronze.byte)}</span>}
      </div>
      <Galat teks={galat || st.galat} />

      {hasil && (<>
        <div className="kisi">
          <div className="kartu">
            <h2>Dedup app vs cdc</h2>
            <p>{rupiah(hasil.dedup.baris_bronze)} baris bronze → <b>{rupiah(hasil.dedup.transaksi_unik)}</b> transaksi unik.</p>
            <div style={{ display: "flex", height: 22, borderRadius: 4, overflow: "hidden" }}>
              {Object.entries(sumber).map(([k, v]: any, i) => (
                <div key={k} title={`${k}: ${v}`} style={{ width: `${(100 * v) / totalSumber}%`, background: ["#b8862b", "#0f6e63", "#6b8fb3"][i % 3] }} />))}
            </div>
            <p className="kecil">{Object.entries(sumber).map(([k, v]: any) => `${k} = ${rupiah(v)}`).join(" · ")}. Kolom
              _source di silver mencatat asal setiap transaksi, sama dengan platform.</p>
          </div>
          <div className="kartu">
            <h2>Tokenization rekening</h2>
            <table><tbody>
              <tr><td>UDF Python per baris (map_elements)</td><td className="angka">{hasil.token.ms_udf_python} ms</td><td className="angka">≈{hasil.token.per_juta_udf_detik} dtk/juta</td></tr>
              <tr><td>DuckDB sha256() vektor</td><td className="angka">{hasil.token.ms_duckdb} ms</td><td className="angka">≈{hasil.token.per_juta_duckdb_detik} dtk/juta</td></tr>
            </tbody></table>
            <p className="kecil">Sampel {rupiah(hasil.token.sampel)} rekening; token kedua cara {hasil.token.identik ? <Status s="lulus" /> : <Status s="gagal" />} identik.
              Contoh: {hasil.token.contoh.map((c: any) => `${c.rekening_id} → ${c.tok}`).join(", ")}</p>
          </div>
        </div>
        <div className="kisi">
          <div className="kartu">
            <h2>Langkah</h2>
            <Tabel kolom={["langkah", "ms", "baris/partisi"]} baris={hasil.langkah.map((l: any) => [l.langkah, l.ms, l.baris ?? (l.partisi ?? []).join(", ")])} />
            <h3>Pemeriksaan sebelum menulis</h3>
            {hasil.cek.map((c: any) => <div key={c.aturan}><Status s={c.lulus ? "lulus" : "gagal"} /> {c.aturan} <span className="kecil">({c.pelanggar} pelanggar)</span></div>)}
          </div>
          <div className="kartu">
            <h2>Rencana Polars lazy untuk silver</h2>
            <pre style={{ maxHeight: 300 }}>{hasil.rencana_silver}</pre>
          </div>
        </div>
      </>)}

      <div className="kartu">
        <h2>Idempotent: jalankan ulang, hasil sama</h2>
        <p className="redup">Tekan Jalankan pipeline dua kali. Jumlah baris silver dan gold per tanggal tidak boleh bertambah.</p>
        <Tabel kolom={["run", "waktu", "lulus", "baris silver", "baris gold", "silver per tgl"]}
          baris={riw.map((r, i) => [riw.length - i, r.waktu, r.lulus ? "ya" : "tidak", r.lapisan?.silver ?? 0, r.lapisan?.gold ?? 0,
            Object.entries(r.lapisan?.silver_per_tgl ?? {}).map(([k, v]) => `${k}: ${v}`).join(" · ")])} />
      </div>
      {st.data?.gold?.kolom?.length > 0 && (
        <div className="kartu"><h2>Gold: fakta_trx_harian</h2><Tabel kolom={st.data.gold.kolom} baris={st.data.gold.baris} maks={40} /></div>)}
    </>
  );
}
