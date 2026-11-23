"use client";
import { useEffect, useState } from "react";
import { api, Batang, Garis, Hasil, Status, Tabel, rupiah, useApi } from "@/components/lib";

type Mode = "warehouse" | "lake" | "bandingkan";
const WARNA_AKAD: Record<string, string> = { Murabahah: "#0f6e63", Musyarakah: "#b8862b", Mudharabah: "#5b6fb3", Ijarah: "#b3402f" };

function Grafik({ no, h }: { no: number; h: Hasil }) {
  const i = (k: string) => h.kolom.indexOf(k);
  if (!h.baris.length) return null;
  if (no === 1) {
    const per: Record<string, number> = {};
    h.baris.forEach(r => { per[String(r[i("cabang")])] = (per[String(r[i("cabang")])] || 0) + Number(r[i("total_nominal")]); });
    return <Batang data={Object.entries(per).map(([label, nilai]) => ({ label, nilai }))} />;
  }
  if (no === 2) {
    const tgl = [...new Set(h.baris.map(r => String(r[i("tgl")])))].sort();
    const akad = [...new Set(h.baris.map(r => String(r[i("akad")])))].sort();
    const seri = akad.map(a => ({ nama: a, warna: WARNA_AKAD[a] || "var(--utama)",
      titik: tgl.map(t => h.baris.filter(r => r[i("tgl")] === t && r[i("akad")] === a).reduce((s, r) => s + Number(r[i("total_plafon")]), 0)) }));
    const total = akad.map(a => ({ label: a, nilai: seri.find(s => s.nama === a)!.titik.reduce((x, y) => x + y, 0) }));
    return <><h3>Total plafon 30 hari per akad</h3><Batang data={total} /><h3>Tren harian</h3><Garis seri={seri} label={tgl} /></>;
  }
  if (no === 3) return <p className="angka-besar">{h.baris.length} nasabah perlu ditindaklanjuti</p>;
  return <Batang data={h.baris.map(r => ({ label: String(r[i("cabang")]), nilai: Number(r[i("rasio")] ?? 0), tanda: Boolean(r[i("lonjakan")]) }))} satuan="×" />;
}

function KartuPertanyaan({ no, judul, penjelas, tgl }: { no: number; judul: string; penjelas: string; tgl: string }) {
  const [mode, setMode] = useState<Mode>("warehouse");
  const [data, setData] = useState<any>(null);
  const [galat, setGalat] = useState("");
  useEffect(() => {
    if (!tgl) return;
    setData(null); setGalat("");
    const path = mode === "bandingkan" ? `bandingkan/${no}?tgl=${tgl}` : `pertanyaan/${no}?tgl=${tgl}&sumber=${mode}`;
    api(path).then(setData).catch(e => setGalat(String(e)));
  }, [no, mode, tgl]);
  const h: Hasil | null = data && mode !== "bandingkan" ? data.hasil : null;
  return (
    <section className="kartu">
      <div className="baris" style={{ justifyContent: "space-between" }}>
        <h2>{no}. {judul}</h2>
        <div className="sakelar">
          {(["warehouse", "lake", "bandingkan"] as Mode[]).map(m => (
            <button key={m} className={mode === m ? "on" : ""} onClick={() => setMode(m)}>
              {m === "warehouse" ? "Warehouse (ClickHouse)" : m === "lake" ? "Lake (DuckDB+Iceberg)" : "Bandingkan"}
            </button>))}
        </div>
      </div>
      <p className="redup">{penjelas}</p>
      {galat && <p className="galat">{galat}</p>}
      {!data && !galat && <p className="redup">memuat…</p>}
      {h && (h.error ? <p className="galat">{h.error}</p> : <>
        <p className="redup">{h.mesin} · {h.baris.length} baris · latency {h.latency_ms.toFixed(1)} ms (mesin {h.engine_ms.toFixed(1)} ms)</p>
        <Grafik no={no} h={h} />
        <Tabel kolom={h.kolom} baris={h.baris} />
        <details><summary>SQL</summary><pre>{h.sql}</pre></details>
      </>)}
      {data && mode === "bandingkan" && <>
        <div className="baris">
          <Status s={data.identik ? "identik" : "gagal"} />
          <span>hasil {data.identik ? "identik" : "berbeda"} di kedua sumber</span>
        </div>
        <div className="kisi">
          {[data.warehouse, data.lake].map((x: Hasil) => (
            <div key={x.sumber}>
              <h3 className={x.sumber === "lake" ? "lk" : "wh"}>{x.sumber === "lake" ? "Lake" : "Warehouse"}</h3>
              {x.error ? <p className="galat">{x.error}</p> : <>
                <p><span className="angka-besar">{x.latency_ms.toFixed(1)} ms</span> <span className="redup">end-to-end · mesin {x.engine_ms.toFixed(1)} ms · {x.baris.length} baris</span></p>
                <p className="redup">{x.mesin}</p>
                <details><summary>SQL</summary><pre>{x.sql}</pre></details>
              </>}
            </div>))}
        </div>
        <Batang data={[{ label: "warehouse (ms)", nilai: data.warehouse.latency_ms }, { label: "lake (ms)", nilai: data.lake.latency_ms }]} />
        {data.selisih?.length > 0 && <><h3>Selisih</h3><pre>{data.selisih.join("\n")}</pre></>}
      </>}
    </section>
  );
}

export default function Beranda() {
  const { data: daftar } = useApi<any[]>("pertanyaan");
  const { data: ringkas } = useApi<any>("ringkas");
  const [tgl, setTgl] = useState("");
  useEffect(() => { if (ringkas?.kemarin && !tgl) setTgl(ringkas.kemarin); }, [ringkas, tgl]);
  return (
    <>
      <h1>Dashboard pagi direksi</h1>
      <p className="redup">Setiap pertanyaan dapat dijawab dari <b className="wh">warehouse</b> (ClickHouse, gold hasil jalur ELT/dbt)
        atau dari <b className="lk">lake</b> (DuckDB membaca tabel Iceberg gold hasil jalur ETL/Polars). Kedua gold dibangun terpisah dari bronze yang sama.</p>
      <div className="kartu baris">
        <label>Tanggal acuan (&quot;kemarin&quot;): <input type="date" value={tgl} onChange={e => setTgl(e.target.value)} /></label>
        {ringkas?.warehouse && <span className="redup">warehouse: {String(ringkas.warehouse[0])} s.d. {String(ringkas.warehouse[1])} · {rupiah(ringkas.warehouse[2])} transaksi</span>}
        {ringkas?.lake && <span className="redup">lake: {String(ringkas.lake[0])} s.d. {String(ringkas.lake[1])} · {rupiah(ringkas.lake[2])} transaksi</span>}
      </div>
      {daftar?.map(p => <KartuPertanyaan key={p.no} no={p.no} judul={p.judul} penjelas={p.penjelas} tgl={tgl} />)}
    </>
  );
}
