"use client";
import { useEffect, useState } from "react";
import { Garis, Hasil, Status, Tabel, useApi } from "@/components/lib";

export default function Fitur() {
  const { data: ringkas } = useApi<any>("ringkas");
  const [tgl, setTgl] = useState("");
  const [nid, setNid] = useState("1");
  const [sumber, setSumber] = useState<"warehouse" | "lake">("warehouse");
  useEffect(() => { if (ringkas?.kemarin && !tgl) setTgl(ringkas.kemarin); }, [ringkas, tgl]);
  const { data, galat, muat } = useApi<any>(tgl ? `fitur?tgl=${tgl}&nasabah_id=${encodeURIComponent(nid)}` : null);
  const h = (n: string): Hasil | undefined => data?.[`${sumber}_${n}`];
  const riw = h("riwayat");
  const ri = (k: string) => riw?.kolom.indexOf(k) ?? -1;
  const urut = riw ? [...riw.baris].reverse() : [];
  return (
    <>
      <h1>Fitur untuk tim AI: <code>gold.fitur_nasabah</code></h1>
      <p className="redup">Satu baris per nasabah per tanggal (<code>tgl_fitur</code>). Fitur dihitung <b>point-in-time</b>: nilai pada tanggal D hanya
        memakai transaksi dan akad dengan tanggal ≤ D, dan atribut profil diambil dari versi SCD-2 yang berlaku pada akhir hari D.
        Dengan begitu dataset training tidak membocorkan informasi dari masa depan (label leakage). Tersedia di lake (Iceberg) dan warehouse (ClickHouse).</p>
      <div className="kartu baris">
        <label>tgl_fitur <input type="date" value={tgl} onChange={e => setTgl(e.target.value)} /></label>
        <label>nasabah_id <input value={nid} onChange={e => setNid(e.target.value)} style={{ width: 90 }} /></label>
        <div className="sakelar">{(["warehouse", "lake"] as const).map(s =>
          <button key={s} className={sumber === s ? "on" : ""} onClick={() => setSumber(s)}>{s === "lake" ? "Lake (DuckDB+Iceberg)" : "Warehouse (ClickHouse)"}</button>)}</div>
        {data && <><Status s={data.identik ? "identik" : "gagal"} /><span className="redup">kedua sumber {data.identik ? "identik" : "berbeda"}</span></>}
        {muat && <span className="redup">memuat…</span>}
      </div>
      {galat && <p className="galat">{galat}</p>}
      {data?.selisih?.length > 0 && <pre>{data.selisih.join("\n")}</pre>}
      <div className="kartu"><h2>Ringkasan 14 hari</h2>
        {h("ringkas")?.error ? <p className="galat">{h("ringkas")!.error}</p> : <Tabel kolom={h("ringkas")?.kolom || []} baris={h("ringkas")?.baris || []} />}
        <p className="redup">latency {h("ringkas")?.latency_ms?.toFixed(1)} ms</p></div>
      <div className="kartu"><h2>Riwayat fitur nasabah {nid} (bukti point-in-time)</h2>
        {urut.length > 0 && <Garis label={urut.map(r => String(r[ri("tgl_fitur")]))} seri={[
          { nama: "trx_30h", warna: "var(--utama)", titik: urut.map(r => Number(r[ri("trx_30h")])) },
          { nama: "trx_7h", warna: "var(--emas)", titik: urut.map(r => Number(r[ri("trx_7h")])) }]} />}
        <Tabel kolom={riw?.kolom || []} baris={riw?.baris || []} />
        <p className="redup">Perhatikan <code>nasabah_sk_saat_itu</code>: bila profil berubah, tanggal sebelum perubahan tetap memakai versi lama.</p></div>
      <div className="kartu"><h2>25 nasabah dengan nominal 30 hari terbesar pada {tgl}</h2>
        <Tabel kolom={h("sampel")?.kolom || []} baris={h("sampel")?.baris || []} /></div>
    </>
  );
}
