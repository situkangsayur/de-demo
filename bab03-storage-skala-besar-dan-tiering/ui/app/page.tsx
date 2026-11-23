"use client";
import { useEffect, useState } from "react";
import { ambil } from "@/components/data";
import { GrafikBatang, ukuran, WARNA } from "@/components/grafik";

type Daftar = { bucket: string; prefix: string; folder: string[]; objek: { key: string; ukuran: number; kelas: string; diubah: string }[]; terpotong: boolean };
type Langkah = { operasi: string; http: string; ms: number; hasil: unknown; ok: boolean };
type Multi = { key: string; byte: number; part: { PartNumber: number; byte: number; mulai_ms: number; selesai_ms: number }[]; total_ms: number; etag: string; catatan: string };
type Ganti = { objek: number; request: number; ms: number; catatan: string };

export default function Halaman() {
  const [prefix, setPrefix] = useState("");
  const [d, setD] = useState<Daftar | null>(null);
  const [langkah, setLangkah] = useState<Langkah[] | null>(null);
  const [multi, setMulti] = useState<Multi | null>(null);
  const [ganti, setGanti] = useState<Ganti | null>(null);
  const [sibuk, setSibuk] = useState("");
  const [konsol, setKonsol] = useState("");
  useEffect(() => {
    if (new URLSearchParams(window.location.search).get("jalan")) ambil<{ langkah: Langkah[] }>("/api/objek/dasar", { method: "POST" }).then((v) => setLangkah(v.langkah));
  }, []);
  useEffect(() => { setKonsol(`${window.location.protocol}//${window.location.hostname}:52304/rustfs/console/`); }, []);
  useEffect(() => { ambil<Daftar>(`/api/objek/daftar?prefix=${encodeURIComponent(prefix)}`).then(setD); }, [prefix, langkah, multi, ganti]);
  const naik = prefix.split("/").filter(Boolean).slice(0, -1).join("/");
  async function jalan<T>(nama: string, path: string, body: object, set: (v: T) => void) {
    setSibuk(nama);
    try { set(await ambil<T>(path, { method: "POST", body: JSON.stringify(body) })); } finally { setSibuk(""); }
  }
  return (
    <>
      <h1>Object storage: bucket, key, dan HTTP</h1>
      <p className="redup">Lab ini menjalankan RustFS sendiri (S3-compatible, sama dengan platform). Setiap tombol mengirim permintaan S3 sungguhan.
        Console RustFS: <a href={konsol} target="_blank" rel="noreferrer">{konsol}</a> (login dengan S3_ACCESS_KEY/S3_SECRET_KEY di .env).</p>
      <div className="dua">
        <section className="kartu">
          <h2>Isi bucket {d?.bucket}</h2>
          <div className="baris kecil"><code>s3://{d?.bucket}/{prefix}</code>
            {prefix && <button onClick={() => setPrefix(naik ? naik + "/" : "")}>naik satu tingkat</button>}</div>
          <div className="tabel-wadah"><table><tbody>
            {d?.folder.map((f) => <tr key={f}><td><a href="#" onClick={(e) => { e.preventDefault(); setPrefix(f); }}>{f.slice(prefix.length)}</a></td><td className="redup">awalan (CommonPrefix)</td><td /></tr>)}
            {d?.objek.map((o) => <tr key={o.key}><td>{o.key.slice(prefix.length)}</td><td className="angka">{ukuran(o.ukuran)}</td><td className="kecil redup">{o.kelas}</td></tr>)}
          </tbody></table></div>
          <p className="kecil redup">"Folder" hanyalah awalan key yang dikelompokkan oleh LIST dengan delimiter "/". Tidak ada objek direktori.</p>
        </section>
        <section className="kartu">
          <h2>Operasi dasar S3</h2>
          <button className="utama" disabled={!!sibuk} onClick={() => jalan("dasar", "/api/objek/dasar", {}, (v: { langkah: Langkah[] }) => setLangkah(v.langkah))}>
            Jalankan PUT, HEAD, GET range, LIST</button>
          {langkah && <>
            <GrafikBatang format={(v) => `${v} ms`} lebarLabel={200} data={langkah.map((l) => ({ label: l.operasi, nilai: l.ms, warna: l.ok ? WARNA[0] : WARNA[2] }))} />
            {langkah.map((l) => <details key={l.operasi}><summary><code>{l.http}</code> {l.ok ? "" : "(galat)"}</summary><pre>{JSON.stringify(l.hasil, null, 2)}</pre></details>)}
          </>}
        </section>
      </div>
      <div className="dua">
        <section className="kartu">
          <h2>Multipart upload</h2>
          <p className="kecil">Objek 24 MB dikirim dalam bagian 5 MB, empat bagian paralel. Bila satu bagian gagal, hanya bagian itu yang diulang.</p>
          <button disabled={!!sibuk} onClick={() => jalan("multi", "/api/objek/multipart", { mb: 24, part_mb: 5 }, setMulti)}>{sibuk === "multi" ? "Mengirim…" : "Unggah 24 MB"}</button>
          {multi && <>
            <svg viewBox={`0 0 600 ${multi.part.length * 22 + 10}`} width="100%" style={{ maxWidth: 760 }} role="img">
              {multi.part.map((p, i) => {
                const skala = 520 / multi.total_ms;
                return <g key={p.PartNumber} transform={`translate(0 ${i * 22})`}>
                  <text x={0} y={15}>bagian {p.PartNumber}</text>
                  <rect x={70 + p.mulai_ms * skala} y={4} width={Math.max(2, (p.selesai_ms - p.mulai_ms) * skala)} height={14} rx={3} fill={WARNA[i % 4]} />
                </g>;
              })}
            </svg>
            <p className="kecil">Total {multi.total_ms} ms untuk {ukuran(multi.byte)}. ETag <code>{multi.etag}</code>: {multi.catatan}</p>
          </>}
        </section>
        <section className="kartu">
          <h2>"Ganti nama direktori" yang tidak murah</h2>
          <p className="kecil">Buat 200 objek di <code>rename/lama/</code>, lalu pindahkan ke <code>rename/baru/</code>. Object storage tidak punya rename:
            setiap objek disalin lalu dihapus.</p>
          <button disabled={!!sibuk} onClick={() => jalan("ganti", "/api/objek/ganti-nama", { jumlah: 200 }, setGanti)}>{sibuk === "ganti" ? "Memindahkan…" : "Pindahkan 200 objek"}</button>
          {ganti && <p><b>{ganti.objek}</b> objek, <b>{ganti.request}</b> permintaan HTTP, <b>{(ganti.ms / 1000).toFixed(1)} detik</b>. {ganti.catatan}
            Sejuta objek berarti dua juta permintaan; ini salah satu alasan table format seperti Iceberg (Bab 4) tidak pernah me-rename berkas.</p>}
        </section>
      </div>
      <section className="kartu"><h2>Block, file, dan object storage</h2>
        <table><thead><tr><th>Jenis</th><th>Antarmuka</th><th>Contoh</th><th>Dipakai untuk</th></tr></thead><tbody>
          <tr><td>Block</td><td>volume mentah, sistem file di atasnya</td><td>disk EBS, iSCSI, LVM</td><td>database OLTP (PostgreSQL aplikasi)</td></tr>
          <tr><td>File</td><td>hierarki direktori, POSIX</td><td>NFS, HDFS</td><td>berbagi berkas, Hadoop on-premise</td></tr>
          <tr><td>Object</td><td>HTTP: PUT, GET, LIST, DELETE per key</td><td>S3, RustFS, MinIO, Ceph RGW</td><td>data lake, bronze/silver/gold platform</td></tr>
        </tbody></table></section>
    </>
  );
}
