"use client";
import { useState } from "react";
import { Status, Tabel, useApi } from "@/components/lib";

export default function DQ() {
  const [run, setRun] = useState("");
  const { data, galat } = useApi<any>(`dq${run ? `?run_id=${encodeURIComponent(run)}` : ""}`, 10000);
  const k: string[] = data?.hasil?.kolom || [];
  const b: any[][] = data?.hasil?.baris || [];
  const i = (n: string) => k.indexOf(n);
  return (
    <>
      <h1>Hasil data quality</h1>
      <p className="redup">Test berjalan sebagai gerbang: di jalur ETL (dq_silver, dq_gold) dan ELT (test dbt di staging). Test berkeparahan error yang gagal
        menghentikan pembaruan gold. Lima dimensi: completeness, uniqueness, validity, consistency, timeliness.</p>
      {galat && <p className="galat">{galat}</p>}
      <div className="kartu">
        <h2>Riwayat per run</h2>
        <div className="tabel-wadah"><table><thead><tr><th>run_id</th><th>terakhir</th><th className="angka">lulus</th><th className="angka">gagal</th><th className="angka">peringatan</th><th className="angka">dilewati</th></tr></thead>
          <tbody>{(data?.riwayat?.baris || []).map((r: any[]) => (
            <tr key={r[0]} onClick={() => setRun(r[0])} style={{ cursor: "pointer" }}>
              <td>{r[0]}</td><td>{r[1]}</td><td className="angka">{r[2]}</td><td className="angka">{r[3] > 0 ? <Status s={`${r[3]}`} /> : 0}</td><td className="angka">{r[4]}</td><td className="angka">{r[5]}</td></tr>))}</tbody></table></div>
      </div>
      <div className="kartu">
        <h2>Detail run {b[0]?.[i("run_id")] ?? ""}</h2>
        <div className="tabel-wadah"><table><thead><tr><th>jalur</th><th>lapisan</th><th>dimensi</th><th>test</th><th>aturan</th><th>status</th><th className="angka">gagal/total</th><th>nilai</th><th>contoh / pesan</th></tr></thead>
          <tbody>{b.map((r, j) => (
            <tr key={j}><td>{r[i("jalur")]}</td><td>{r[i("lapisan")]}</td><td>{r[i("dimensi")]}</td><td>{r[i("tes")]}</td>
              <td style={{ whiteSpace: "normal", minWidth: 240 }}>{r[i("aturan")]}</td><td><Status s={r[i("status")]} /></td>
              <td className="angka">{r[i("gagal")]}{r[i("total")] ? `/${r[i("total")]}` : ""}</td><td>{r[i("nilai")] ?? ""}</td>
              <td style={{ whiteSpace: "normal", maxWidth: 420, fontSize: 12 }}>{r[i("contoh")] === "[]" ? "" : String(r[i("contoh")]).slice(0, 300)}</td></tr>))}</tbody></table></div>
      </div>
      <div className="kartu"><h2>Cakupan dimensi</h2><Tabel kolom={data?.dimensi?.kolom || []} baris={data?.dimensi?.baris || []} /></div>
    </>
  );
}
