"use client";
import { useEffect, useState } from "react";
import { ambil, usePoll } from "@/components/data";
import { GrafikBatang, rupiah, WARNA } from "@/components/grafik";

type Hitung = { masuk: number; keluar: number; tertangkap: Record<string, number> };
type Status = {
  konfig: { flush_detik: number; batch_detik: number; generator_aktif: boolean; generator_laju: number; generator_persen_kotor: number };
  hitung: Record<string, Hitung>;
  antre_nats: number; buffer_ingestor: number; berkas_bronze: number; baris_oltp: number; baris_bronze: number;
  baris_silver: number; baris_karantina: number; sel_gold: number; flush_berikut_detik: number; batch_berikut_detik: number;
  jejak_terbaru: { trx_id: string; status: string; asal: string }[] | null;
};
type Detail = {
  jejak: { trx_id: string; status: string; catatan: string[] | null; waktu: Record<string, string>; asal: string };
  latensi_ms: Record<string, number>; total_ms?: number;
  oltp?: unknown; event?: unknown[]; bronze?: unknown[]; silver?: unknown; gold?: unknown; karantina?: unknown;
};
type Dash = { kesegaran_detik: number; hari_ini: Record<string, number>; jumlah_hari_ini: Record<string, number>; per_kanal: Record<string, number> };

// Tahap, posisi dalam data engineering lifecycle (Reis dan Housley), peran, dan latensi khas di platform.
const TAHAP: { id: string; nama: string; siklus: string; peran: string; platform: string }[] = [
  { id: "aplikasi", nama: "Aplikasi bank (OLTP PostgreSQL)", siklus: "Generation", peran: "software engineer aplikasi", platform: "milidetik" },
  { id: "event_app", nama: "Event aplikasi → NATS (stream BANK)", siklus: "Ingestion", peran: "data engineer", platform: "milidetik" },
  { id: "event_cdc", nama: "CDC: WAL → Debezium → NATS (stream CDC)", siklus: "Ingestion", peran: "data engineer", platform: "±1 detik" },
  { id: "bronze", nama: "Bronze: ingestor menulis berkas", siklus: "Storage", peran: "data engineer", platform: "detik (micro-batch)" },
  { id: "silver", nama: "Silver: bersih, tipe benar", siklus: "Transformation", peran: "data engineer", platform: "jam (DAG harian Airflow)" },
  { id: "gold", nama: "Gold: agregat star schema", siklus: "Transformation", peran: "analytics engineer", platform: "jam (DAG harian)" },
  { id: "dashboard", nama: "Dashboard direksi", siklus: "Serving", peran: "data analyst, data scientist", platform: "menit (refresh)" },
];

function fmtMs(ms: number | undefined): string {
  if (ms === undefined) return "…";
  if (ms < 1) return `${(ms * 1000).toFixed(0)} µs`;
  if (ms < 1000) return `${ms.toFixed(1)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

export default function Halaman() {
  const { data: st, galat } = usePoll<Status>("/api/status", 1500);
  const { data: dash } = usePoll<Dash>("/api/dashboard", 2000);
  const [dipilih, setDipilih] = useState<string | null>(null);
  const { data: det } = usePoll<Detail>(dipilih ? `/api/jejak/${dipilih}` : null, 1500);
  const [form, setForm] = useState({ rekening_id: 1, kanal: "mobile", jenis: "transfer_keluar", nominal: "250000", geser_menit: 0,
    kirim_ulang: "", ulang_publish: false, ack_hilang: false });
  const [hasil, setHasil] = useState("");
  // ?jejak=TRX... membuka langsung satu jejak (berguna untuk dibagikan ke mahasiswa)
  useEffect(() => { const j = new URLSearchParams(window.location.search).get("jejak"); if (j) setDipilih(j); }, []);

  async function kirim(ubah?: Partial<typeof form>) {
    const body = { ...form, ...ubah };
    const h = await ambil<{ status: number; pesan: string; trx_id: string[] }>("/api/transaksi", { method: "POST", body: JSON.stringify(body) });
    setHasil(`HTTP ${h.status}: ${h.pesan} (${(h.trx_id || []).join(", ")})`);
    if (h.trx_id?.length) setDipilih(h.trx_id[h.trx_id.length - 1]);
  }
  async function konfig(ubah: Partial<Status["konfig"]>) {
    if (!st) return;
    await ambil("/api/konfig", { method: "POST", body: JSON.stringify({ ...st.konfig, ...ubah }) });
  }

  const tahapAktif = det ? Object.keys(det.jejak.waktu) : [];
  return (
    <>
      <h1>Satu transaksi, tujuh tahap</h1>
      <p className="redup">Kirim satu transaksi dari aplikasi, lalu ikuti perjalanannya sampai muncul di dashboard. Tiap kotak menghitung baris
        yang masuk dan keluar, serta kesalahan yang tertangkap di tahap itu. Semua tahap berjalan sungguhan di API lab (skala kecil, di memori).</p>
      {galat && <p className="galat">{galat}</p>}

      <section className="kartu">
        <div className="baris" style={{ justifyContent: "space-between" }}>
          <h2>Peta siklus hidup data</h2>
          {st && <span className="kecil redup">antrean NATS {st.antre_nats} · buffer ingestor {st.buffer_ingestor} ·
            flush berikut {st.flush_berikut_detik.toFixed(0)} s · batch silver/gold berikut {st.batch_berikut_detik.toFixed(0)} s</span>}
        </div>
        <div className="peta">
          {TAHAP.map((t) => {
            const h = st?.hitung[t.id];
            const tangkap = Object.entries(h?.tertangkap || {});
            return (
              <div key={t.id} className={`tahap ${tahapAktif.includes(t.id) ? "aktif" : ""}`}>
                <div className="siklus">{t.siklus}</div>
                <h3>{t.nama}</h3>
                <div className="peran">peran: {t.peran}</div>
                <div className="angka-tahap">{t.id === "gold" ? `${st?.sel_gold ?? 0} sel` : `${h?.masuk ?? 0} → ${h?.keluar ?? 0}`}</div>
                <div className="kecil redup">{t.id === "gold" ? "dihitung ulang dari silver" : "masuk → lolos"}</div>
                <div className="kecil redup">latensi di platform: {t.platform}</div>
                {det?.latensi_ms[t.id] !== undefined && <div className="kecil"><b>jejak ini: {fmtMs(det.latensi_ms[t.id])}</b></div>}
                <ul className="tangkap">
                  {tangkap.map(([k, v]) => (
                    <li key={k} className={k.includes("digabung") || k.includes("terlambat") || k.includes("ditandai") || k.includes("tidak dicatat") ? "info" : ""}>
                      {v}× {k}
                    </li>
                  ))}
                </ul>
                <div className="arus" style={{ opacity: tahapAktif.includes(t.id) ? 1 : 0.25 }} />
              </div>
            );
          })}
        </div>
        <div className="undercurrent"><b>Undercurrents</b> menyelimuti semua tahap: security (akses API, PII), data management (katalog,
          kualitas), DataOps (log dan uji), data architecture, orchestration (jadwal flush dan batch), software engineering.</div>
      </section>

      <div className="kisi">
        <section className="kartu">
          <h2>Kirim transaksi dari aplikasi</h2>
          <div className="formulir">
            <label className="isian">rekening
              <select value={form.rekening_id} onChange={(e) => setForm({ ...form, rekening_id: +e.target.value })}>
                {Array.from({ length: 20 }, (_, i) => <option key={i} value={i + 1}>{`71${String(i + 1).padStart(8, "0")}`}</option>)}
              </select></label>
            <label className="isian">kanal (seperti dikirim sumber)
              <select value={form.kanal} onChange={(e) => setForm({ ...form, kanal: e.target.value })}>
                {["mobile", "Mobile Banking", "m-banking", "atm", "ATM", "teller ", "web", "WEB", "ecommerce", "qris"].map((k) =>
                  <option key={k} value={k}>{JSON.stringify(k)}</option>)}
              </select></label>
            <label className="isian">jenis
              <select value={form.jenis} onChange={(e) => setForm({ ...form, jenis: e.target.value })}>
                {["transfer_keluar", "transfer_masuk", "setor", "tarik", "bayar_belanja", "angsuran"].map((k) => <option key={k}>{k}</option>)}
              </select></label>
            <label className="isian">nominal (kosongkan untuk uji)
              <input value={form.nominal} onChange={(e) => setForm({ ...form, nominal: e.target.value })} /></label>
            <label className="isian">occurred_at
              <select value={form.geser_menit} onChange={(e) => setForm({ ...form, geser_menit: +e.target.value })}>
                <option value={0}>sekarang</option>
                <option value={-360}>6 jam lalu (perangkat offline)</option>
                <option value={-1560}>26 jam lalu (kemarin)</option>
                <option value={30}>30 menit di masa depan</option>
              </select></label>
            <label className="isian">request dikirim dua kali
              <select value={form.kirim_ulang} onChange={(e) => setForm({ ...form, kirim_ulang: e.target.value })}>
                <option value="">tidak</option>
                <option value="tanpa_kunci">ya, tanpa Idempotency-Key</option>
                <option value="dengan_kunci">ya, dengan Idempotency-Key</option>
              </select></label>
          </div>
          <div className="baris" style={{ marginTop: 10 }}>
            <label className="kecil"><input type="checkbox" checked={form.ulang_publish} onChange={(e) => setForm({ ...form, ulang_publish: e.target.checked })} /> publish event diulang (Nats-Msg-Id sama)</label>
            <label className="kecil"><input type="checkbox" checked={form.ack_hilang} onChange={(e) => setForm({ ...form, ack_hilang: e.target.checked })} /> ack ingestor hilang (redelivery)</label>
          </div>
          <div className="baris" style={{ marginTop: 10 }}>
            <button className="utama" onClick={() => kirim()}>Kirim</button>
            <button onClick={() => ambil("/api/batch", { method: "POST" })}>Jalankan batch sekarang</button>
            <button className="bahaya" onClick={() => { setDipilih(null); ambil("/api/reset", { method: "POST" }); }}>Reset</button>
          </div>
          {hasil && <p className="kecil">{hasil}</p>}
          <h3>Generator latar</h3>
          {st && <div className="baris kecil">
            <button className={st.konfig.generator_aktif ? "utama" : ""} onClick={() => konfig({ generator_aktif: !st.konfig.generator_aktif })}>
              {st.konfig.generator_aktif ? "Generator menyala" : "Nyalakan generator"}</button>
            laju <select value={st.konfig.generator_laju} onChange={(e) => konfig({ generator_laju: +e.target.value })}>
              {[1, 2, 5, 10, 20].map((v) => <option key={v} value={v}>{v} trx/detik</option>)}</select>
            kotor <select value={st.konfig.generator_persen_kotor} onChange={(e) => konfig({ generator_persen_kotor: +e.target.value })}>
              {[0, 8, 20, 50].map((v) => <option key={v} value={v}>{v}%</option>)}</select>
            batch tiap <select value={st.konfig.batch_detik} onChange={(e) => konfig({ batch_detik: +e.target.value })}>
              {[5, 15, 30, 60].map((v) => <option key={v} value={v}>{v} s</option>)}</select>
          </div>}
        </section>

        <section className="kartu daftar-jejak">
          <h2>Jejak terbaru</h2>
          <p className="kecil redup">Pilih satu transaksi untuk melihat wujudnya di setiap tahap.</p>
          {(st?.jejak_terbaru || []).map((j) => (
            <button key={j.trx_id} className={dipilih === j.trx_id ? "utama" : ""} onClick={() => setDipilih(j.trx_id)}>
              {j.trx_id} · <span className={`lencana ${j.status === "selesai" ? "ok" : j.status === "berjalan" ? "netral" : j.status === "karantina" || j.status === "ditolak" ? "gagal" : "warn"}`}>{j.status}</span> <span className="redup">{j.asal}</span>
            </button>
          ))}
        </section>
      </div>

      {det && <Jejak det={det} />}

      {dash && <section className="kartu">
        <h2>Dashboard mini (gold) · kesegaran {dash.kesegaran_detik.toFixed(0)} detik</h2>
        <GrafikBatang format={rupiah} data={Object.entries(dash.per_kanal).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ label: k, nilai: v }))} />
        <p className="kecil redup">Perbandingan hot path, bronze mentah, dan gold ada di halaman Dashboard.</p>
      </section>}
    </>
  );
}

function Jejak({ det }: { det: Detail }) {
  const urut = TAHAP.map((t) => t.id).filter((t) => det.latensi_ms[t] !== undefined);
  // skala logaritmik: milidetik dan detik bisa tampil berdampingan
  const mentah = (ms: number) => 30 + Math.log10(1 + ms * 10) * 60;
  const jumlah = urut.reduce((a, t) => a + mentah(det.latensi_ms[t]), 0) || 1;
  const lebar = (ms: number) => (mentah(ms) / jumlah) * 900;
  let x = 0;
  return (
    <section className="kartu">
      <div className="baris" style={{ justifyContent: "space-between" }}>
        <h2>Jejak {det.jejak.trx_id}</h2>
        <span>status <b>{det.jejak.status}</b>{det.total_ms !== undefined && <> · ujung ke ujung <b>{fmtMs(det.total_ms)}</b></>}</span>
      </div>
      <svg viewBox="0 0 900 56" width="100%" role="img">
        {urut.map((t, i) => {
          const w = lebar(det.latensi_ms[t]);
          const el = (
            <g key={t}>
              <rect x={x} y={8} width={w - 2} height={24} rx={4} fill={WARNA[i % WARNA.length]} />
              <text x={x + 4} y={48}>{t}</text>
              <text x={x + 4} y={24} style={{ fill: "#fff" }}>{fmtMs(det.latensi_ms[t])}</text>
            </g>
          );
          x += w;
          return el;
        })}
      </svg>
      <p className="kecil redup">Panjang segmen memakai skala logaritmik. Latensi tiap tahap dihitung dari tahap sebelumnya (event dari commit
        aplikasi, bronze dari event diterima, dan seterusnya).</p>
      {det.jejak.catatan && <ul>{det.jejak.catatan.map((c, i) => <li key={i}>{c}</li>)}</ul>}
      <div className="wujud">
        <div><h3>1. Baris OLTP (tabel transaksi)</h3><pre>{JSON.stringify(det.oltp ?? "tidak tercatat", null, 2)}</pre></div>
        <div><h3>2. Pesan NATS (BANK dan CDC)</h3><pre>{JSON.stringify(det.event ?? [], null, 2)}</pre></div>
        <div><h3>3. Baris bronze (8 kolom metadata)</h3><pre>{JSON.stringify(det.bronze ?? [], null, 2)}</pre></div>
        <div><h3>4. Baris silver{det.karantina ? " (dikarantina)" : ""}</h3><pre>{JSON.stringify(det.silver ?? det.karantina ?? "belum", null, 2)}</pre></div>
        <div><h3>5. Sel gold yang memuat transaksi ini</h3><pre>{JSON.stringify(det.gold ?? "belum", null, 2)}</pre></div>
      </div>
    </section>
  );
}
