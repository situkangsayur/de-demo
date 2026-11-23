"use client";
import { useEffect, useState } from "react";
import { ambil } from "@/components/data";
import { ukuran } from "@/components/grafik";

type Kol = { kolom: string; byte: number; byte_mentah: number; encoding: string[]; min: string | null; max: string | null; dibaca: boolean };
type RG = { indeks: number; baris: number; dilewati: boolean; kolom: Kol[]; occurred_min: string | null; occurred_max: string | null };
type Anatomi = { path: string; urut: boolean; ukuran_byte: number; row_group_size: number; jumlah_row_group: number; baris: number;
  kolom_query: string[]; dari: string; sql: string; byte_total: number; byte_dibaca: number; row_group: RG[]; query_ms: number;
  scan_penuh_ms: number; baris_hasil: number; row_group_dilewati: number };
const SEMUA = ["id", "rekening_id", "cabang_id", "kanal", "jenis", "nominal", "occurred_at", "created_at"];

export default function HalamanParquet() {
  const [urut, setUrut] = useState(true);
  const [kolom, setKolom] = useState<string[]>(["kanal", "nominal"]);
  const [hari, setHari] = useState(30);
  const [a, setA] = useState<Anatomi | null>(null);
  const [sibuk, setSibuk] = useState(false);
  useEffect(() => {
    setSibuk(true);
    const dari = new Date(Date.now() - hari * 86400000).toISOString().slice(0, 10);
    ambil<Anatomi>(`/api/parquet/anatomi?urut=${urut}&kolom=${kolom.join(",")}&dari=${dari}`).then(setA).finally(() => setSibuk(false));
  }, [urut, kolom, hari]);
  const pct = a ? (100 * a.byte_dibaca) / a.byte_total : 0;
  return (
    <>
      <h1>Di dalam berkas Parquet: column pruning dan predicate pushdown</h1>
      <p className="redup">Seluruh transaksi ditulis ke satu berkas Parquet (zstd) yang dibagi menjadi beberapa row group. Setiap kotak di bawah
        adalah satu column chunk; angkanya ukuran terkompresi. Footer menyimpan min/max tiap kolom per row group, sehingga mesin query bisa
        melewati row group tanpa membukanya.</p>
      <div className="baris">
        <div className="sakelar">
          <button className={urut ? "on" : ""} onClick={() => setUrut(true)}>urut occurred_at</button>
          <button className={!urut ? "on" : ""} onClick={() => setUrut(false)}>urutan acak</button>
        </div>
        <span className="kecil">WHERE occurred_at ≥</span>
        <select value={hari} onChange={(e) => setHari(+e.target.value)}>
          {[7, 30, 90, 365].map((h) => <option key={h} value={h}>{h} hari terakhir</option>)}</select>
        <span className="kecil">SELECT</span>
        {SEMUA.filter((k) => k !== "occurred_at").map((k) => (
          <label key={k} className="kecil"><input type="checkbox" checked={kolom.includes(k)}
            onChange={(e) => setKolom(e.target.checked ? [...kolom, k] : kolom.filter((x) => x !== k))} />{k}</label>))}
        {sibuk && <span className="lencana netral">menulis ulang berkas…</span>}
      </div>
      {a && <>
        <section className="kartu">
          <div className="baris" style={{ justifyContent: "space-between" }}>
            <h2>{a.path}: {a.jumlah_row_group} row group × {SEMUA.length} kolom, {ukuran(a.ukuran_byte)}</h2>
            <span><span className="kotak-legenda sel-baca" />dibaca<span className="kotak-legenda sel-push" />dilewati (pushdown)<span className="kotak-legenda sel-prune" />tidak diminta (pruning)</span>
          </div>
          <div className="sel-rg" style={{ gridTemplateColumns: `150px repeat(${SEMUA.length}, 1fr)` }}>
            <div />{SEMUA.map((k) => <div key={k}><b>{k}</b></div>)}
            {a.row_group.map((rg) => [
              <div key={`l${rg.indeks}`} style={{ textAlign: "left" }} title={`${rg.occurred_min} s.d. ${rg.occurred_max}`}>
                RG {rg.indeks} · {(rg.occurred_max || "").slice(0, 10)}</div>,
              ...rg.kolom.map((c) => <div key={`${rg.indeks}${c.kolom}`} className={c.dibaca ? "sel-baca" : rg.dilewati && a.kolom_query.includes(c.kolom) ? "sel-push" : "sel-prune"}
                title={`${c.kolom}: ${ukuran(c.byte)} (mentah ${ukuran(c.byte_mentah)}), encoding ${c.encoding.join(", ")}, min ${c.min}, max ${c.max}`}>
                {ukuran(c.byte)}</div>)])}
          </div>
          <p className="kecil redup">Label kiri: tanggal maksimum occurred_at di row group itu (dari statistik footer). Arahkan kursor ke kotak untuk melihat encoding dan min/max.</p>
        </section>
        <div className="dua">
          <section className="kartu"><h2>Byte yang harus dibaca</h2>
            <svg viewBox="0 0 600 50" width="100%" role="img">
              <rect x={0} y={10} width={600} height={24} rx={4} fill="var(--kode)" />
              <rect x={0} y={10} width={Math.max(2, pct * 6)} height={24} rx={4} fill="var(--s1)" />
              <text x={pct > 15 ? 8 : Math.max(2, pct * 6) + 6} y={27} style={{ fill: pct > 15 ? "#fff" : "var(--teks)" }}>{pct.toFixed(1)}% dari berkas</text>
            </svg>
            <p>{ukuran(a.byte_dibaca)} dari {ukuran(a.byte_total)}; {a.row_group_dilewati} dari {a.jumlah_row_group} row group dilewati.
              Query mengembalikan {a.baris_hasil.toLocaleString("id-ID")} baris.</p>
            {!a.urut && <p className="kecil"><b>Urutan acak:</b> setiap row group berisi tanggal dari seluruh tahun, sehingga min/max tidak bisa
              menyingkirkan satu pun. Pruning kolom tetap bekerja, pushdown tidak.</p>}
            {a.urut && <p className="kecil">Data diurutkan menurut occurred_at, sehingga rentang tanggal tiap row group sempit dan sebagian besar bisa dilewati.</p>}
          </section>
          <section className="kartu"><h2>Waktu DuckDB (median 5 kali)</h2>
            <p>Query terfilter: <b>{a.query_ms} ms</b> · baca seluruh berkas (SELECT *): <b>{a.scan_penuh_ms} ms</b></p>
            <pre>{a.sql}</pre>
          </section>
        </div>
      </>}
    </>
  );
}
