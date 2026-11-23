"use client";
import { useEffect, useState } from "react";
import { ambil } from "@/components/data";

type Kasus = { kasus: string; slide: string; arah: string; berhasil: boolean; contoh: Record<string, unknown> | null; galat: string | null; penulis: string[]; pembaca: string[] };
type Evolusi = { avro: Kasus[]; parquet: { kolom: string[]; baris: unknown[][]; tanpa_union_by_name: string } };

export default function HalamanEvolusi() {
  const [e, setE] = useState<Evolusi | null>(null);
  useEffect(() => { ambil<Evolusi>("/api/evolusi").then(setE); }, []);
  return (
    <>
      <h1>Schema pasti berubah: mana yang aman?</h1>
      <p className="redup">Setiap baris di bawah dijalankan sungguhan: data ditulis dengan skema penulis (writer schema) lalu dibaca dengan skema
        pembaca (reader schema) memakai aturan schema resolution Avro (fastavro). Backward: pembaca baru membaca data lama. Forward: pembaca lama
        membaca data baru.</p>
      {e && <>
        <section className="kartu"><h2>Avro: aturan kompatibilitas diperiksa eksplisit</h2>
          <div className="tabel-wadah" style={{ maxHeight: "none" }}><table>
            <thead><tr><th>Perubahan</th><th>Arah</th><th>Menurut slide</th><th>Hasil</th><th>Skema penulis → pembaca</th><th>Baris pertama / galat</th></tr></thead>
            <tbody>{e.avro.map((k) => <tr key={k.kasus}>
              <td style={{ whiteSpace: "normal" }}><b>{k.kasus}</b></td><td>{k.arah}</td><td>{k.slide}</td>
              <td><span className={`lencana ${k.berhasil ? "ok" : "gagal"}`}>{k.berhasil ? "terbaca" : "gagal"}</span></td>
              <td className="kecil" style={{ whiteSpace: "normal" }}><code>{k.penulis.join(", ")}</code><br />→ <code>{k.pembaca.join(", ")}</code></td>
              <td className="kecil" style={{ whiteSpace: "normal" }}>{k.berhasil ? <code>{JSON.stringify(k.contoh)}</code> : <span className="galat">{k.galat}</span>}</td>
            </tr>)}</tbody></table></div>
          <p className="kecil redup">Perhatikan "ganti nama": Avro mengira kolom lama hilang dan kolom baru tidak punya default. Dengan alias, data lama tetap terbaca.
            Iceberg (Bab 4) melacak kolom dengan ID sehingga ganti nama aman tanpa alias.</p>
        </section>
        <section className="kartu"><h2>Parquet: dua versi berkas di satu folder</h2>
          <p className="kecil">v1.parquet punya kolom id, nominal (int32), kanal. v2.parquet menambah kolom catatan dan nominal int64. Dibaca dengan
            <code>read_parquet(&apos;v*.parquet&apos;, union_by_name = true)</code>:</p>
          <table><thead><tr>{e.parquet.kolom.map((k) => <th key={k}>{k}</th>)}</tr></thead>
            <tbody>{e.parquet.baris.map((r, i) => <tr key={i}>{r.map((v, j) => <td key={j}>{v === null ? <span className="redup">NULL</span> : String(v)}</td>)}</tr>)}</tbody></table>
          <p className="kecil">Tanpa <code>union_by_name</code>: {e.parquet.tanpa_union_by_name}. Kolom catatan hilang diam-diam; ini alasan
            table format seperti Iceberg menyimpan skema di metadata tabel, bukan di tiap berkas.</p>
        </section>
      </>}
    </>
  );
}
