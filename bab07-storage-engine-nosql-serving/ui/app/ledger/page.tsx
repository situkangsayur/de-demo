"use client";
import { useState } from "react";
import { api, useApi, angka, Galat } from "@/components/lib";

type E = { id: number; akad: string; nasabah: string; nilai: number; waktu: string; hash_prev: string; hash: string; hash_hitung: string; isi_cocok: boolean; rantai_ok: boolean };
type Status = { entri: E[]; rantai_utuh: boolean; jangkar: string; hash_terakhir: string; cocok_jangkar: boolean };

const pendek = (h: string) => (h ? `${h.slice(0, 8)}…${h.slice(-4)}` : "–");

export default function Ledger() {
  const st = useApi<Status>("ledger");
  const [id, setId] = useState(2);
  const [nilai, setNilai] = useState(1_000_000);
  const [galat, setGalat] = useState("");
  async function aksi(a: string, body: unknown = {}) {
    setGalat("");
    try { st.setData(await api<Status>(`ledger/${a}`, body)); } catch (e) { setGalat(String(e)); }
  }
  const d = st.data;
  return (
    <>
      <h1>Immutable ledger: hash chain jurnal akad</h1>
      <p className="redup">Setiap entri menyimpan hash entri sebelumnya: <code>hash_n = SHA-256(hash_n-1 + JSON(entri))</code>. Admin database bisa mengubah satu baris, tetapi perubahan itu ketahuan (tamper-evident). Rantai di dalam database bisa dihitung ulang oleh penyerang, sehingga hash terakhir perlu dijangkarkan di luar database.</p>
      <Galat teks={st.galat || galat} />
      <div className="kartu">
        <div className="baris">
          <button onClick={() => aksi("reset")}>Reset 6 akad contoh</button>
          <button className="utama" onClick={() => aksi("tambah")}>Tambah entri sah</button>
          <span style={{ marginLeft: 12 }}>Sunting diam-diam: entri</span>
          <input type="number" value={id} min={1} onChange={e => setId(Number(e.target.value))} style={{ width: 60 }} />
          <span>nilai</span><input type="number" value={nilai} onChange={e => setNilai(Number(e.target.value))} style={{ width: 130 }} />
          <button className="bahaya" onClick={() => aksi("sunting", { id, nilai })}>UPDATE langsung di tabel</button>
          <button className="bahaya" onClick={() => aksi("tutupi")}>Hitung ulang seluruh rantai (menutupi)</button>
        </div>
        {d && (
          <div className="baris" style={{ marginTop: 10 }}>
            <span className={`lencana ${d.rantai_utuh ? "ok" : "gagal"}`}>{d.rantai_utuh ? "rantai di database utuh" : "rantai di database RUSAK"}</span>
            <span className={`lencana ${d.cocok_jangkar ? "ok" : "gagal"}`}>{d.cocok_jangkar ? "hash terakhir = jangkar" : "hash terakhir ≠ jangkar"}</span>
            <span className="redup">jangkar (di luar DB, Redis): <code>{pendek(d.jangkar)}</code> · hash terakhir di DB: <code>{pendek(d.hash_terakhir)}</code></span>
          </div>
        )}
      </div>
      <div className="kartu" style={{ overflowX: "auto" }}>
        <div style={{ display: "flex", gap: 0, alignItems: "center", minWidth: "fit-content" }}>
          {(d?.entri || []).map((e, i) => (
            <div key={e.id} style={{ display: "flex", alignItems: "center" }}>
              {i > 0 && <div style={{ width: 28, textAlign: "center", color: e.rantai_ok ? "var(--hijau)" : "var(--merah)", fontSize: 20 }}>{e.rantai_ok ? "→" : "✕"}</div>}
              <div className={`blok-rantai ${e.isi_cocok && e.rantai_ok ? "ok" : "rusak"}`}>
                <b>#{e.id}</b> {e.akad}<br />
                <span className="redup">{e.nasabah}</span> · <b>Rp{angka(e.nilai)}</b><br />
                prev <code>{pendek(e.hash_prev)}</code><br />
                hash <code>{pendek(e.hash)}</code><br />
                {!e.isi_cocok && <span className="galat">isi berubah: hitung ulang = {pendek(e.hash_hitung)}</span>}
                {e.isi_cocok && !e.rantai_ok && <span className="galat">prev tidak sama dengan hash entri sebelumnya</span>}
              </div>
            </div>
          ))}
        </div>
      </div>
      <div className="kartu">
        <h2>Skenario</h2>
        <ol>
          <li>Reset, lalu sunting entri 2 menjadi Rp1.000.000. Entri 2 merah karena hash yang dihitung ulang tidak sama dengan hash tersimpan.</li>
          <li>Klik &quot;hitung ulang seluruh rantai&quot;. Rantai di database tampak utuh lagi, tetapi hash terakhir tidak sama dengan jangkar yang dicatat di luar database saat entri sah ditambahkan.</li>
          <li>Diskusi: di mana jangkar sebaiknya disimpan (WORM/object lock, ledger database seperti immudb, Hyperledger Fabric, atau publikasi berkala ke pihak ketiga), dan bagaimana hak dihapus UU PDP bila data pribadi ikut di-hash.</li>
        </ol>
      </div>
    </>
  );
}
