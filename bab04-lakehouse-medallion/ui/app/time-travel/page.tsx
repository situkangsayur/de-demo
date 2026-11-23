"use client";
import { useState } from "react";
import { byte, Galat, rupiah, Tabel, useAksi, useApi } from "@/components/lib";

export default function TimeTravel() {
  const [tabel, setTabel] = useState("lab.transaksi");
  const [snap, setSnap] = useState("");
  const [dari, setDari] = useState("");
  const daftar = useApi<{ tabel: string[] }>("iceberg/tabel");
  const p = useApi<any>(`iceberg/pohon?tabel=${tabel}`);
  const lama = useApi<any>(snap ? `iceberg/time-travel?tabel=${tabel}&snapshot_id=${snap}` : null);
  const kini = useApi<any>(`iceberg/time-travel?tabel=${tabel}`);
  const pr = useApi<any>(dari ? `iceberg/pruning?tabel=${tabel}&dari=${encodeURIComponent(dari + "T00:00:00+00:00")}` : null);
  const a = useAksi(() => { p.segarkan(); kini.segarkan(); });
  const snaps: any[] = p.data?.snapshots ?? [];

  return (
    <>
      <h1>Time travel, rollback, dan hidden partitioning</h1>
      <div className="kartu baris">
        <label>Tabel <select value={tabel} onChange={e => { setTabel(e.target.value); setSnap(""); }}>
          {(daftar.data?.tabel ?? ["lab.transaksi"]).map(t => <option key={t}>{t}</option>)}</select></label>
        <label>Snapshot lama <select value={snap} onChange={e => setSnap(e.target.value)}>
          <option value="">pilih…</option>
          {snaps.map(s => <option key={s.snapshot_id} value={s.snapshot_id}>{s.waktu.slice(11, 19)} · {s.operasi} · {s.keterangan || "-"} ({s.baris} baris)</option>)}
        </select></label>
        {snap && tabel === "lab.transaksi" && (
          <button className="bahaya" disabled={!!a.sibuk} onClick={() => a.jalankan(`iceberg/operasi/rollback?snapshot_id=${snap}`, "Rollback")}>Rollback ke snapshot ini</button>)}
        {a.pesan && <span>{a.pesan}</span>}
      </div>
      <Galat teks={a.galat || p.galat || lama.galat} />
      <div className="kisi">
        <div className="kartu">
          <h2>Snapshot lama {lama.data && <span className="redup">({rupiah(lama.data.baris)} baris)</span>}</h2>
          {lama.data ? (<>
            <pre>{lama.data.sql_spark}</pre>
            <Tabel kolom={lama.data.kolom} baris={lama.data.hasil} />
            <p className="kecil">Kolom pada skema snapshot itu: {lama.data.skema.join(", ")}</p></>)
            : <p className="redup">Pilih snapshot untuk membaca tabel seperti pada waktu itu.</p>}
        </div>
        <div className="kartu">
          <h2>Kini {kini.data && <span className="redup">({rupiah(kini.data.baris)} baris)</span>}</h2>
          {kini.data && (<>
            <pre>SELECT ... FROM {tabel}   -- snapshot {kini.data.snapshot_id}</pre>
            <Tabel kolom={kini.data.kolom} baris={kini.data.hasil} />
            <p className="kecil">Kolom kini: {kini.data.skema.join(", ")}</p></>)}
        </div>
      </div>

      <div className="kartu">
        <h2>Hidden partitioning: filter pada kolom asli</h2>
        <p className="redup">Partisi tabel adalah <code>day(waktu)</code>. Pengguna cukup memfilter <code>waktu</code>; Iceberg memangkas berkas
          lewat nilai partisi dan statistik min/maks di manifest tanpa membuka berkasnya.</p>
        <div className="baris">
          <label>waktu ≥ <input type="date" value={dari} onChange={e => setDari(e.target.value)} /></label>
          {dari === "" && <span className="kecil">pilih tanggal (mis. hari terakhir bronze)</span>}
        </div>
        {pr.data && !pr.data.error && (
          <div style={{ marginTop: 8 }}>
            <pre>{`SELECT * FROM ${tabel} ${pr.data.filter}`}</pre>
            <p>Berkas dibaca: <b>{pr.data.berkas_dibaca}</b> dari {pr.data.berkas_total} ({byte(pr.data.byte_dibaca)} dari {byte(pr.data.byte_total)})</p>
            <div style={{ background: "var(--garis)", borderRadius: 3 }}>
              <div className="batang" style={{ width: `${(100 * pr.data.berkas_dibaca) / Math.max(1, pr.data.berkas_total)}%` }} /></div>
          </div>)}
        <Galat teks={pr.galat} />
      </div>
    </>
  );
}
