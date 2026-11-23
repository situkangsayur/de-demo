"use client";
import { Tabel, byte, rupiah, useApi } from "@/components/lib";

function Kotak({ judul, isi, warna }: { judul: string; isi: string[]; warna: string }) {
  return (
    <div style={{ border: `2px solid ${warna}`, borderRadius: 10, padding: "8px 10px", minWidth: 170, background: "var(--panel)" }}>
      <b style={{ color: warna }}>{judul}</b>
      {isi.map(s => <div key={s} className="redup" style={{ fontSize: 12 }}>{s}</div>)}
    </div>
  );
}
const Panah = ({ label }: { label: string }) => (
  <div style={{ textAlign: "center", color: "var(--redup)", fontSize: 12, minWidth: 90 }}>{label}<div style={{ fontSize: 20 }}>→</div></div>
);

export default function Lineage() {
  const { data, galat } = useApi<any>("lineage", 15000);
  const ice: any[] = data?.lake?.iceberg || [];
  const wh: any[] = data?.warehouse?.baris || [];
  const br: any[] = data?.lake?.bronze || [];
  const n = (t: string) => rupiah(ice.find(x => x.tabel === t)?.baris ?? 0);
  const w = (db: string, t: string) => rupiah(wh.find(r => r[0] === db && r[1] === t)?.[3] ?? 0);
  const brT = (s: string) => br.find(x => x.sumber === s && x.entitas === "transaksi");
  return (
    <>
      <h1>Lineage bronze → silver → gold</h1>
      <p className="redup">Dua jalur dari bronze yang sama. <b>ETL</b>: data ditransformasi dulu di Python (Polars + DuckDB), baru dimuat ke lake Iceberg.
        <b> ELT</b>: data dimuat mentah ke ClickHouse, lalu ditransformasi di dalam warehouse dengan dbt. Angka = jumlah baris saat ini (diperbarui tiap 15 detik).</p>
      {galat && <p className="galat">{galat}</p>}
      <div className="kartu">
        <h2>ETL → data lake (Iceberg di RustFS)</h2>
        <div className="baris" style={{ alignItems: "stretch" }}>
          <Kotak judul="bronze (Parquet)" warna="var(--redup)" isi={[`app/transaksi: ${brT("app")?.berkas ?? 0} berkas`, `cdc/transaksi: ${brT("cdc")?.berkas ?? 0} berkas`, "dt=YYYY-MM-DD (UTC)"]} />
          <Panah label="Polars: tipe, dedup app+cdc, kode, token PII" />
          <Kotak judul="lake.silver.*" warna="var(--emas)" isi={[`transaksi ${n("silver.transaksi")}`, `nasabah (versi) ${n("silver.nasabah")}`, `akad ${n("silver.akad_pembiayaan")}`, `karantina ${n("silver.akad_karantina")}`]} />
          <Panah label="dq_silver (gerbang)" />
          <Kotak judul="lake.staging.*" warna="var(--redup)" isi={[`fakta_transaksi ${n("staging.fakta_transaksi")}`, "write-audit-publish"]} />
          <Panah label="dq_gold lalu publikasi" />
          <Kotak judul="lake.gold.*" warna="var(--utama)" isi={[`fakta_transaksi ${n("gold.fakta_transaksi")}`, `fakta_pembiayaan ${n("gold.fakta_pembiayaan")}`, `dim_nasabah (SCD-2) ${n("gold.dim_nasabah")}`, `dim_cabang ${n("gold.dim_cabang")}`, `fitur_nasabah (AI) ${n("gold.fitur_nasabah")}`]} />
          <Panah label="DuckDB iceberg_scan" />
          <Kotak judul="dashboard: sumber lake" warna="var(--lk)" isi={["lake-engine"]} />
        </div>
      </div>
      <div className="kartu">
        <h2>ELT → data warehouse (ClickHouse)</h2>
        <div className="baris" style={{ alignItems: "stretch" }}>
          <Kotak judul="bronze (Parquet)" warna="var(--redup)" isi={["dibaca langsung oleh ClickHouse", "s3(lake_s3, ...)"]} />
          <Panah label="muat mentah (semua kolom String)" />
          <Kotak judul="raw.*" warna="var(--emas)" isi={[`transaksi ${w("raw", "transaksi")}`, `nasabah ${w("raw", "nasabah")}`, `akad ${w("raw", "akad_pembiayaan")}`, "PII masih mentah: akses dibatasi"]} />
          <Panah label="dbt: view stg + test (gerbang)" />
          <Kotak judul="stg.* (view)" warna="var(--redup)" isi={["stg_transaksi", "stg_nasabah_versi", "stg_akad"]} />
          <Panah label="dbt: tabel gold" />
          <Kotak judul="gold.*" warna="var(--utama)" isi={[`fakta_transaksi ${w("gold", "fakta_transaksi")}`, `fakta_pembiayaan ${w("gold", "fakta_pembiayaan")}`, `dim_nasabah (SCD-2) ${w("gold", "dim_nasabah")}`, `dim_cabang ${w("gold", "dim_cabang")}`, `fitur_nasabah (AI) ${w("gold", "fitur_nasabah")}`]} />
          <Panah label="SQL langsung" />
          <Kotak judul="dashboard: sumber warehouse" warna="var(--wh)" isi={["ClickHouse HTTP"]} />
        </div>
        <p className="redup">Kedua gold direkonsiliasi di akhir DAG (task <code>rekonsiliasi_lake_vs_warehouse</code>): baris, total nominal, dan nasabah_sk per tanggal harus sama.</p>
      </div>
      <div className="kisi">
        <div className="kartu"><h2>Bronze per sumber dan entitas</h2>
          <Tabel kolom={["sumber", "entitas", "berkas", "partisi dt", "dt min", "dt max"]}
            baris={br.map(b => [b.sumber, b.entitas, b.berkas, b.partisi_dt, b.dt_min, b.dt_max])} /></div>
        <div className="kartu"><h2>Tabel Iceberg</h2>
          <Tabel kolom={["tabel", "baris", "berkas", "ukuran", "snapshot", "partisi"]}
            baris={ice.map(t => [t.tabel, t.baris, t.berkas, byte(t.byte), t.jumlah_snapshot, t.partisi.replace(/\s+/g, " ")])} /></div>
        <div className="kartu"><h2>Tabel ClickHouse</h2>
          <Tabel kolom={["database", "tabel", "engine", "baris", "byte"]} baris={wh} /></div>
      </div>
    </>
  );
}
