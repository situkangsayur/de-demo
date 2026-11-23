"use client";

// Generator lalu lintas: mode live (laju per detik) dan backfill (riwayat N hari), plus "kotoran"
// yang disengaja sesuai slide P08 UTS. Semua aksi memanggil API aplikasi lewat APISIX.
import { useEffect, useState } from "react";
import { api, fmt, jam, post } from "../lib";

type Dirt = { duplikat: number; kosong: number; terlambat: number; akad_invalid: number; profil: number };
type Config = { rate: number; workers: number; dirt: Dirt; spike: string; spike_mul: number };
type Status = {
  mode: "idle" | "live" | "backfill";
  config: Config;
  counts: Record<string, number>;
  errors: Record<string, number>;
  recent: { at: string; msg: string }[] | null;
  backfill?: { days: number; per_day: number; day: string; day_index: number; actions_done: number; actions_planned: number; selesai?: boolean };
  cabang: { id: unknown; kode: string; nama: string; kota: string }[] | null;
  nasabah_dikenal: number;
};

const dirtInfo: { k: keyof Dirt; label: string; teori: string }[] = [
  { k: "duplikat", label: "Duplikat", teori: "request dikirim ulang tanpa Idempotency-Key → dua baris; silver wajib dedup (W2)" },
  { k: "kosong", label: "Nilai kosong", teori: "email/no_hp nasabah kosong (cabang_id transaksi juga dikirim kosong, tetapi diisi aplikasi) → uji completeness (W8)" },
  { k: "terlambat", label: "Data terlambat", teori: "occurred_at mundur 1–72 jam → masuk partisi tanggal aslinya; backfill/reprocess (W7)" },
  { k: "akad_invalid", label: "Kode akad tidak valid", teori: "mrb, MRBH, XYZ, … → standardisasi + karantina di silver (W2, W8)" },
  { k: "profil", label: "Perubahan profil", teori: "kota/segmen/no_hp berubah → dim_nasabah SCD tipe 2 (W4)" },
];

export default function Generator() {
  const [st, setSt] = useState<Status | null>(null);
  const [cfg, setCfg] = useState<Config | null>(null);
  const [days, setDays] = useState(30);
  const [perDay, setPerDay] = useState(200);
  const [shop, setShop] = useState(true);
  const [msg, setMsg] = useState("");

  const refresh = async () => {
    try {
      const s = await api<Status>("gen/status");
      setSt(s);
      setCfg((c) => c ?? s.config);
    } catch (e) {
      setMsg(String(e));
    }
  };
  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 1500);
    return () => clearInterval(t);
  }, []);

  const act = async (fn: () => Promise<unknown>) => {
    try {
      await fn();
      setMsg("");
    } catch (e) {
      setMsg(String(e));
    }
    refresh();
  };
  const save = (c: Config) => {
    setCfg(c);
    act(() => post("gen/config", c));
  };

  const counts = Object.entries(st?.counts ?? {}).sort();
  const errs = Object.entries(st?.errors ?? {}).sort();
  const bf = st?.backfill;

  return (
    <>
      <h1>Generator lalu lintas</h1>
      <p className="muted">
        Klien sintetis memanggil API aplikasi (bank + toko) lewat APISIX dari {st?.cabang?.length ?? "?"} cabang dan kanal mobile/atm/web/teller. Setiap aksi menghasilkan event di outbox
        (stream BANK/SHOP) dan perubahan WAL (stream CDC). Lihat hasilnya di halaman <a href="/">Aliran data</a>.
      </p>
      {msg && <p className="pill bad">{msg}</p>}

      <div className="grid g3" style={{ marginBottom: 12 }}>
        <div className="card">
          <h2>
            Mode live <span className={`pill ${st?.mode === "idle" ? "" : "ok"}`}>{st?.mode ?? "?"}</span>
          </h2>
          {cfg && (
            <>
              <label>
                Laju: <b>{cfg.rate}</b> aksi/detik
                <input type="range" min={0.5} max={50} step={0.5} value={cfg.rate} onChange={(e) => save({ ...cfg, rate: Number(e.target.value) })} style={{ width: "100%" }} />
              </label>
              <label>
                Request paralel: <b>{cfg.workers}</b>
                <input type="range" min={1} max={16} value={cfg.workers} onChange={(e) => save({ ...cfg, workers: Number(e.target.value) })} style={{ width: "100%" }} />
              </label>
              <label>
                Lonjakan tidak wajar di cabang{" "}
                <select value={cfg.spike} onChange={(e) => save({ ...cfg, spike: e.target.value })}>
                  <option value="">(tidak ada)</option>
                  {(st?.cabang ?? []).map((c) => (
                    <option key={c.kode} value={c.kode}>
                      {c.kode} · {c.nama}
                    </option>
                  ))}
                </select>
              </label>
            </>
          )}
          <div className="row" style={{ marginTop: 8 }}>
            <button className="primary" disabled={st?.mode !== "idle"} onClick={() => act(() => post("gen/start"))}>
              Mulai
            </button>
            <button className="danger" disabled={st?.mode === "idle"} onClick={() => act(() => post("gen/stop"))}>
              Hentikan
            </button>
            <button onClick={() => act(() => post("gen/reset-counts"))}>Reset hitungan</button>
          </div>
          <p className="small muted">Nasabah dikenal generator: {fmt(st?.nasabah_dikenal)}. Sekitar 20% nasabah baru sengaja tidak pernah bertransaksi (pertanyaan direksi no. 3).</p>
        </div>

        <div className="card">
          <h2>&quot;Kotoran&quot; yang disengaja</h2>
          {cfg &&
            dirtInfo.map((d) => (
              <label key={d.k}>
                {d.label}: <b>{Math.round(cfg.dirt[d.k] * 100)}%</b>
                <input
                  type="range"
                  min={0}
                  max={0.5}
                  step={0.01}
                  value={cfg.dirt[d.k]}
                  onChange={(e) => save({ ...cfg, dirt: { ...cfg.dirt, [d.k]: Number(e.target.value) } })}
                  style={{ width: "100%" }}
                />
                <span className="small muted">{d.teori}</span>
              </label>
            ))}
        </div>

        <div className="card">
          <h2>Backfill riwayat</h2>
          <p className="small muted">
            Membangkitkan riwayat N hari ke belakang dengan <code>occurred_at</code> historis yang berurutan (nasabah → rekening → transaksi/akad → perubahan profil), pola jam, akhir pekan
            lebih sepi, akhir bulan lebih ramai, dua hari lonjakan di satu cabang. Mulai kecil; 30 hari × 200 aksi ≈ 6.000 aksi.
          </p>
          <label>
            Hari: <input type="number" min={1} max={800} value={days} onChange={(e) => setDays(Number(e.target.value))} style={{ width: 90 }} /> <span className="small muted">(13 bulan ≈ 395)</span>
          </label>
          <label>
            Aksi per hari: <input type="number" min={10} max={20000} value={perDay} onChange={(e) => setPerDay(Number(e.target.value))} style={{ width: 90 }} />
          </label>
          <label className="row">
            <input type="checkbox" checked={shop} onChange={(e) => setShop(e.target.checked)} /> sertakan belanja toko (checkout dengan waktu historis)
          </label>
          <button className="primary" disabled={st?.mode !== "idle"} onClick={() => act(() => post("gen/backfill", { days, per_day: perDay, with_shop: shop }))}>
            Jalankan backfill
          </button>
          {bf && (
            <div style={{ marginTop: 8 }}>
              <div className="small">
                Hari {bf.day_index}/{bf.days} ({bf.day}) · {fmt(bf.actions_done)} aksi {bf.selesai ? "· selesai" : ""}
              </div>
              <progress max={bf.days} value={bf.day_index} style={{ width: "100%" }} />
            </div>
          )}
        </div>
      </div>

      <div className="grid g3">
        <div className="card">
          <h2>Aksi berhasil</h2>
          <table>
            <tbody>
              {counts.map(([k, v]) => (
                <tr key={k}>
                  <td>{k.startsWith("kotor") ? <span className="pill warn">{k}</span> : k}</td>
                  <td className="num">{fmt(v)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="card">
          <h2>Ditolak / gagal</h2>
          <table>
            <tbody>
              {errs.map(([k, v]) => (
                <tr key={k}>
                  <td>{k}</td>
                  <td className="num">{fmt(v)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="small muted">Penolakan bisnis (mis. saldo tidak cukup, HTTP 409) wajar dan juga terjadi di bank sungguhan.</p>
        </div>
        <div className="card">
          <h2>Catatan</h2>
          <div className="feed">
            {[...(st?.recent ?? [])].reverse().map((r, i) => (
              <div key={i}>
                <span className="muted">{jam(r.at)}</span> {r.msg}
              </div>
            ))}
          </div>
        </div>
      </div>
    </>
  );
}
