"use client";
import { useState } from "react";
import { Batang, byte, Galat, juta, rupiah, Status, useApi, useJob, WARNA } from "@/components/lib";

const LABEL: Record<string, string> = {
  pandas: "pandas", polars_eager: "Polars eager", polars_lazy: "Polars lazy",
  polars_streaming: "Polars streaming", duckdb: "DuckDB", datafusion: "DataFusion",
};

export default function Benchmark() {
  const info = useApi<any>("info");
  const riw = useApi<any>("benchmark/riwayat");
  const j = useJob(() => { info.segarkan(); riw.segarkan(); });
  const [baris, setBaris] = useState(1000000);
  const [ulang, setUlang] = useState(3);
  const [pilih, setPilih] = useState<string[]>([]);
  const engine: string[] = info.data?.engine ?? [];
  const aktif = pilih.length ? pilih : engine;
  const terakhir = riw.data?.riwayat?.[0];
  const ds = (info.data?.dataset ?? []).find((d: any) => d.baris === baris);

  return (
    <>
      <h1>Query yang sama, enam cara, satu mesin</h1>
      <p className="redup">Transaksi tanggal ≥ 2026-10-11, JOIN nasabah, GROUP BY tanggal dan cabang (jumlah, total nominal).
        Setiap pengukuran berjalan di proses Python baru; memori puncak = VmHWM proses itu. Container dibatasi
        {" "}{info.data?.batas_memori_mb ?? "…"} MB dan {info.data?.cpu ?? "…"} CPU.</p>

      <div className="kartu">
        <h2>Dataset transaksi (30 hari, 13 kolom, Parquet zstd)</h2>
        <table><thead><tr><th>ukuran</th><th>status</th><th className="angka">berkas</th><th className="angka">di disk</th><th></th></tr></thead>
          <tbody>{(info.data?.dataset ?? []).map((d: any) => (
            <tr key={d.baris}><td>{juta(d.baris)} baris</td><td><Status s={d.ada ? "ada" : "belum"} /></td>
              <td className="angka">{d.berkas ?? ""}</td><td className="angka">{d.ada ? byte(d.byte) : ""}</td>
              <td>{d.ada
                ? <button disabled={j.berjalan} onClick={async () => { await fetch(`/api/dataset/hapus?baris=${d.baris}`, { method: "POST" }); info.segarkan(); }}>hapus</button>
                : <button disabled={j.berjalan} onClick={() => j.mulai(`dataset?baris=${d.baris}`)}>bangkitkan</button>}</td></tr>))}
          </tbody></table>
        <p className="kecil">10 juta baris ±220 MB di volume Docker; terhapus bersama <code>docker compose down -v</code>.</p>
      </div>

      <div className="kartu">
        <h2>Jalankan benchmark</h2>
        <div className="baris">
          <label>Dataset <select value={baris} onChange={e => setBaris(Number(e.target.value))}>
            {(info.data?.dataset ?? []).map((d: any) => <option key={d.baris} value={d.baris} disabled={!d.ada}>{juta(d.baris)}{d.ada ? "" : " (belum ada)"}</option>)}
          </select></label>
          <label>Ulang <select value={ulang} onChange={e => setUlang(Number(e.target.value))}>{[1, 3, 5].map(n => <option key={n}>{n}</option>)}</select></label>
          <button className="utama" disabled={j.berjalan || !ds?.ada}
            onClick={() => j.mulai(`benchmark?baris=${baris}&ulang=${ulang}&engine=${aktif.join(",")}`)}>Mulai</button>
          {j.berjalan && <span className="lencana warn">{j.job.jenis}: {j.job.progres?.slice(-1)[0] ?? "mulai"}…</span>}
        </div>
        <div className="cek" style={{ marginTop: 8 }}>
          {engine.map(e => (
            <label key={e}><input type="checkbox" checked={aktif.includes(e)}
              onChange={() => setPilih(aktif.includes(e) ? aktif.filter(x => x !== e) : [...aktif, e])} /> {LABEL[e]}</label>))}
        </div>
        <Galat teks={j.galat || (j.job?.status === "gagal" ? j.job.error : "")} />
      </div>

      {terakhir && (
        <div className="kisi">
          <div className="kartu">
            <h2>Waktu (median {terakhir.ulang}×, ms) · {juta(terakhir.baris)} baris</h2>
            <Batang satuan=" ms" data={terakhir.hasil.map((h: any) => ({ label: LABEL[h.engine], nilai: h.median_ms, warna: WARNA[h.engine], ket: h.error ?? undefined }))} />
            <p className="kecil">Hasil keenam engine {terakhir.hasil_sama ? <Status s="lulus" /> : <Status s="gagal" />} identik (jumlah grup dan total nominal sama).</p>
          </div>
          <div className="kartu">
            <h2>Memori puncak (MB)</h2>
            <Batang satuan=" MB" garis={info.data?.batas_memori_mb ? [{ nilai: info.data.batas_memori_mb, label: "mem_limit" }] : []}
              data={terakhir.hasil.map((h: any) => ({ label: LABEL[h.engine], nilai: h.puncak_mb, warna: WARNA[h.engine] }))} />
            <p className="kecil">Polars eager membaca 13 kolom lalu memfilter; lazy dan DuckDB hanya membaca 3 kolom dan memangkas row group.</p>
          </div>
        </div>)}

      {riw.data?.tabel?.ukuran?.length > 0 && (
        <div className="kartu">
          <h2>Tabel praktikum (slide P05): median ms / memori puncak MB</h2>
          <div className="tabel-wadah"><table>
            <thead><tr><th>engine</th>{riw.data.tabel.ukuran.map((u: number) => <th key={u} className="angka">{juta(u)}</th>)}</tr></thead>
            <tbody>{riw.data.tabel.baris.map((b: any) => (
              <tr key={b.engine}><td>{LABEL[b.engine]}</td>
                {riw.data.tabel.ukuran.map((u: number) => {
                  const s = b.sel[String(u)];
                  return <td key={u} className="angka">{s ? `${rupiah(s.ms)} ms / ${rupiah(s.mb)} MB` : "…"}</td>;
                })}</tr>))}
            </tbody></table></div>
          <p className="kecil">Isi dengan median minimal tiga kali jalan, lalu jelaskan mengapa urutannya begitu.</p>
        </div>)}
    </>
  );
}
