"use client";
import { useState } from "react";
import { byte, Galat, rupiah, useAksi, useApi } from "@/components/lib";

const OPERASI: [string, string, string][] = [
  ["reset", "1. Salin silver → lab.transaksi", "tabel baru, snapshot pertama (append)"],
  ["append", "2. Append kecil ×5 (streaming)", "lima commit, lima berkas kecil"],
  ["compaction", "3. Compaction", "satukan berkas kecil per partisi (replace)"],
  ["merge", "4. MERGE / upsert", "koreksi 15 nominal, 5 baris baru, 3 BATAL dihapus"],
  ["delete", "5. DELETE kanal = atm", "copy-on-write: berkas terdampak ditulis ulang"],
  ["skema-tambah", "6. ADD COLUMN", "kategori_merchant; berkas lama tidak disentuh"],
  ["skema-ganti_nama", "7. RENAME COLUMN", "jenis → jenis_transaksi (field id tetap)"],
  ["skema-lebarkan", "8. Lebarkan tipe", "cabang_id int → long"],
  ["partisi", "9. Partition evolution", "tambah bucket(4, rekening_id)"],
  ["expire", "10. Expire snapshots", "sisakan 1 snapshot, hapus berkas yatim"],
];

export default function Iceberg() {
  const [tabel, setTabel] = useState("lab.transaksi");
  const [snap, setSnap] = useState<string>("");
  const daftar = useApi<{ tabel: string[] }>("iceberg/tabel");
  const p = useApi<any>(`iceberg/pohon?tabel=${tabel}${snap ? `&snapshot_id=${snap}` : ""}`);
  const a = useAksi(() => { setSnap(""); p.segarkan(); daftar.segarkan(); });
  const d = p.data && !p.data.error ? p.data : null;

  return (
    <>
      <h1>Pohon metadata Iceberg</h1>
      <p className="redup">Dibaca langsung dari RustFS setiap kali halaman disegarkan: katalog → metadata.json → manifest list
        (satu per snapshot) → manifest → data file Parquet. Kotak berbingkai emas = dibuat oleh commit snapshot yang dipilih.</p>

      <div className="kartu">
        <div className="baris">
          <label>Tabel <select value={tabel} onChange={e => { setTabel(e.target.value); setSnap(""); }}>
            {(daftar.data?.tabel ?? ["lab.transaksi"]).map(t => <option key={t}>{t}</option>)}
          </select></label>
          <button onClick={() => p.segarkan()}>Segarkan</button>
          {a.sibuk && <span className="lencana warn">{a.sibuk}…</span>}
        </div>
        {tabel === "lab.transaksi" && (
          <div className="tombol-op" style={{ marginTop: 10 }}>
            {OPERASI.map(([op, label, ket]) => (
              <button key={op} disabled={!!a.sibuk} onClick={() => a.jalankan(`iceberg/operasi/${op}`, label)}>
                {label}<small>{ket}</small></button>))}
          </div>)}
        {a.pesan && <p><b>Hasil:</b> {a.pesan}</p>}
        <Galat teks={a.galat || (p.data?.error ?? p.galat)} />
        {p.galat && tabel === "lab.transaksi" && <p className="redup">Belum ada lab.transaksi: jalankan pipeline di halaman Medallion, lalu tombol 1.</p>}
      </div>

      {d && (
        <>
          <div className="kartu">
            <h2>Linimasa snapshot <span className="redup">({d.snapshots.length}, terbaru di kiri; klik untuk melihat pohon snapshot itu)</span></h2>
            <div className="linimasa">
              {[...d.snapshots].reverse().map((s: any) => (
                <div key={s.snapshot_id} className={`snap ${s.kini ? "kini" : ""} ${String(s.snapshot_id) === (snap || String(d.snapshot?.snapshot_id)) ? "dipilih" : ""}`}
                  onClick={() => setSnap(String(s.snapshot_id))}>
                  <div className={`op op-${s.operasi}`}>{s.operasi}{s.kini ? " · kini" : ""}</div>
                  <div className="kecil">{s.waktu.slice(11, 19)} · skema {s.schema_id}</div>
                  <div>{s.keterangan}</div>
                  <div className="kecil">{rupiah(s.baris)} baris · {s.berkas} berkas (+{s.tambah_berkas} −{s.hapus_berkas})</div>
                </div>))}
            </div>
          </div>

          <div className="kartu">
            <div className="pohon">
              <div className="kolom">
                <h3>Katalog (SQLite)</h3>
                <div className="simpul">
                  <div><b>{d.katalog.table_namespace}.{d.katalog.table_name}</b></div>
                  <div className="kecil">metadata_location →</div>
                  <code>{(d.katalog.metadata_location || "").split("/").pop()}</code>
                  <div className="kecil" style={{ marginTop: 4 }}>previous:</div>
                  <code className="kecil">{(d.katalog.previous_metadata_location || "-").split("/").pop()}</code>
                </div>
                <p className="kecil">Satu-satunya yang berubah saat commit: pointer ini ditukar secara atomik.</p>
              </div>
              <div className="kolom">
                <h3>metadata.json</h3>
                <div className="simpul baru">
                  <code>{d.metadata.path}</code>
                  <div className="kecil">format v{d.metadata.format_version} · {d.metadata.jumlah_snapshot} snapshot · {d.metadata.jumlah_skema} skema · {d.metadata.jumlah_spec} partition spec</div>
                  <div className="kecil">current-snapshot-id {d.metadata.current_snapshot_id}</div>
                </div>
                <div className="simpul"><b>skema {d.metadata.current_schema_id}</b>
                  {d.metadata.skema.map((f: any) => <div key={f.id} className="kecil">{f.id}. {f.nama}: {f.tipe}</div>)}</div>
                <div className="simpul"><b>partition spec</b>
                  {d.metadata.spec.map((s: any) => <div key={s.spec_id} className="kecil">spec {s.spec_id}{s.spec_id === d.metadata.default_spec_id ? " (default)" : ""}: {s.field.join(", ") || "tanpa partisi"}</div>)}</div>
                <div className="simpul"><b>metadata-log</b>
                  {d.metadata.metadata_log.map((m: string) => <div key={m} className="kecil">{m.slice(0, 14)}…</div>)}</div>
              </div>
              <div className="kolom">
                <h3>Manifest list</h3>
                {d.snapshot && (
                  <div className="simpul baru">
                    <code>{d.snapshot.manifest_list.slice(0, 28)}…</code>
                    <div className="kecil">snapshot {d.snapshot.snapshot_id}</div>
                    <div className="kecil">parent {d.snapshot.parent_id ?? "-"}</div>
                    <div className="kecil">operasi <b>{d.snapshot.operasi}</b>, skema {d.snapshot.schema_id}</div>
                    <div className="kecil">{Object.entries(d.snapshot.ringkasan).filter(([k]) => /added|deleted|total-records|total-data/.test(k)).map(([k, v]) => `${k}=${v}`).join(" · ")}</div>
                  </div>)}
              </div>
              <div className="kolom">
                <h3>Manifest ({d.manifest.length})</h3>
                {d.manifest.map((m: any) => (
                  <div key={m.path} className={`simpul ${m.baru ? "baru" : ""}`}>
                    <code>{m.path.slice(0, 22)}…</code>
                    <div className="kecil">{m.isi} · spec {m.spec_id}</div>
                    <div className="kecil">added {m.added} · existing {m.existing} · deleted {m.deleted}</div>
                  </div>))}
              </div>
              <div className="kolom">
                <h3>Data file ({d.total_berkas_manifest})</h3>
                {d.manifest.map((m: any) => m.berkas.map((f: any) => (
                  <div key={m.path + f.path} className={`simpul ${f.baru ? "baru" : ""} ${f.status === "DELETED" ? "hapus" : ""}`}>
                    <code>{f.path.slice(0, 26)}…</code> <span className="kecil">{f.status}</span>
                    <div className="kecil">{rupiah(f.baris)} baris · {byte(f.byte)} · spec {f.spec_id}</div>
                    <div className="kecil">partisi {Object.entries(f.partisi).map(([k, v]) => `${k}=${v}`).join(", ")}</div>
                    {f.statistik.nominal && <div className="kecil">nominal min {rupiah(f.statistik.nominal[0])} maks {rupiah(f.statistik.nominal[1])}</div>}
                  </div>)))}
              </div>
            </div>
          </div>

          <div className="kartu">
            <h2>Objek di RustFS untuk tabel ini</h2>
            <p>{d.objek.total} objek ({byte(d.objek.byte)}): {d.objek.data} data file, {d.objek.metadata_json} metadata.json.
              {" "}Data file yang dirujuk snapshot yang masih ada: {d.objek.data_dirujuk_snapshot_mana_pun}; snapshot kini hanya memakai {d.total_berkas_manifest}.</p>
            <p className="redup">Berkas lama tidak dihapus saat DELETE, MERGE, atau compaction: snapshot lama masih merujuknya (itulah yang memungkinkan time travel).
              Berkas lama baru benar-benar hilang sesudah expire snapshots.</p>
          </div>
        </>)}
    </>
  );
}
