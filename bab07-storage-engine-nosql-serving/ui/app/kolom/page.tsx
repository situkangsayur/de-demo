"use client";
import { useState } from "react";
import { api, useApi, byte, angka, BatangLog, warnaStore, Galat } from "@/components/lib";

type Ukuran = {
  postgres: { heap_byte: number; indeks_byte: number; total_byte: number; baris: number; kolom: { kolom: string; lebar_rata_byte: number }[] };
  clickhouse: { kolom: string; tipe: string; terkompresi: number; mentah: number }[];
};
type Banding = { jenis: string; judul: string; hasil: { store: string; sql: string; ms: number; byte_dibaca: number; rincian: string }[] };

const JENIS: [string, string][] = [["agregasi", "Aggregation 3 kolom"], ["satu_kolom", "sum(nominal) seluruh tabel"], ["baris_penuh", "SELECT * satu nasabah"], ["point_trx", "Satu transaksi per id"]];

export default function Kolom() {
  const uk = useApi<Ukuran>("kolom/ukuran");
  const [hasil, setHasil] = useState<Banding[]>([]);
  const [galat, setGalat] = useState("");
  const u = uk.data;
  const totalCH = (u?.clickhouse || []).reduce((s, c) => s + Number(c.terkompresi), 0);
  const maksKol = Math.max(1, ...(u?.clickhouse || []).map(c => Number(c.mentah)));
  async function banding(j: string) {
    setGalat("");
    try { const b = await api<Banding>("kolom/banding", { jenis: j }); setHasil(prev => [b, ...prev.filter(x => x.jenis !== j)]); } catch (e) { setGalat(String(e)); }
  }
  return (
    <>
      <h1>Row vs columnar</h1>
      <p className="redup">PostgreSQL menyimpan satu baris utuh berdampingan dalam page 8 KB (cocok untuk membaca/mengubah satu baris). ClickHouse menyimpan setiap kolom di berkas sendiri yang terurut dan terkompresi (cocok untuk membaca sedikit kolom dari banyak baris).</p>
      <Galat teks={uk.galat || galat} />
      <div className="kisi">
        <div className="kartu">
          <h2>Ukuran tabel transaksi yang sama</h2>
          {u && <BatangLog label={v => byte(v)} data={[
            { nama: "PostgreSQL heap", sub: `${angka(u.postgres.baris)} baris`, nilai: u.postgres.heap_byte, warna: warnaStore("PostgreSQL") },
            { nama: "PostgreSQL indeks", sub: "PK + (nasabah_id, occurred_at) + GIN", nilai: u.postgres.indeks_byte, warna: warnaStore("PostgreSQL") },
            { nama: "ClickHouse terkompresi", sub: "fakta_transaksi, semua kolom", nilai: totalCH, warna: warnaStore("ClickHouse") },
          ]} />}
        </div>
        <div className="kartu">
          <h2>ClickHouse: ukuran per kolom</h2>
          {(u?.clickhouse || []).map(c => (
            <div key={c.kolom} style={{ display: "grid", gridTemplateColumns: "110px 1fr 120px", gap: 6, alignItems: "center", fontSize: 12, margin: "2px 0" }}>
              <code>{c.kolom}</code>
              <div style={{ position: "relative", height: 14, background: "var(--kode)", borderRadius: 3 }}>
                <div style={{ position: "absolute", height: "100%", width: `${(Number(c.mentah) / maksKol) * 100}%`, background: "#d9a400", opacity: .3, borderRadius: 3 }} />
                <div style={{ position: "absolute", height: "100%", width: `${(Number(c.terkompresi) / maksKol) * 100}%`, background: "#d9a400", borderRadius: 3 }} />
              </div>
              <span className="redup">{byte(Number(c.terkompresi))} / {byte(Number(c.mentah))} ({(Number(c.mentah) / Math.max(1, Number(c.terkompresi))).toFixed(1)}×)</span>
            </div>
          ))}
          <p className="redup">Batang tua = terkompresi, muda = mentah. Kolom LowCardinality dan kolom terurut (nasabah_id) terkompresi sangat baik; kolom unik acak (trx_id) paling sulit.</p>
        </div>
      </div>
      <div className="kartu">
        <h2>Query yang sama, byte yang dibaca berbeda</h2>
        <div className="baris">{JENIS.map(([j, l]) => <button key={j} onClick={() => banding(j)}>{l}</button>)}</div>
        {hasil.map(b => (
          <div key={b.jenis} style={{ marginTop: 12 }}>
            <h3>{b.judul}</h3>
            <BatangLog label={v => byte(v)} data={b.hasil.map(h => ({ nama: h.store, sub: `${h.ms.toFixed(1)} ms · ${h.rincian}`, nilai: h.byte_dibaca, warna: warnaStore(h.store) }))} />
            <details><summary>SQL</summary>{b.hasil.map(h => <pre key={h.store}>{h.store}: {h.sql}</pre>)}</details>
          </div>
        ))}
        <p className="redup">PostgreSQL: page 8 KB yang disentuh (EXPLAIN ANALYZE BUFFERS). ClickHouse: byte sesudah dekompresi yang dibaca engine (X-ClickHouse-Summary). Perhatikan &quot;satu transaksi per id&quot;: ClickHouse mengurutkan data per (nasabah_id, waktu), sehingga mencari trx_id harus memindai kolom trx_id; PostgreSQL cukup menuruni B-tree.</p>
      </div>
    </>
  );
}
