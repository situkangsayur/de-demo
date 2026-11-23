package main

// Generator lalu lintas: memanggil API aplikasi (bank + toko) lewat APISIX seperti klien sungguhan
// dari banyak cabang dan kanal, lalu menyisipkan "kotoran" yang disengaja (slide P08):
// duplikat (request dikirim ulang tanpa Idempotency-Key), nilai kosong, occurred_at mundur
// (data terlambat), kode akad tidak valid, dan perubahan profil nasabah (bahan SCD-2).
// Mode backfill membangkitkan riwayat N hari dengan occurred_at historis.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DirtConfig: peluang (0..1) setiap jenis kotoran per aksi yang relevan.
type DirtConfig struct {
	Duplikat    float64 `json:"duplikat"`     // kirim ulang request yang sama tanpa Idempotency-Key
	Kosong      float64 `json:"kosong"`       // kanal/cabang/email/no_hp dikosongkan
	Terlambat   float64 `json:"terlambat"`    // occurred_at mundur 1-72 jam
	AkadInvalid float64 `json:"akad_invalid"` // kode akad di luar MRB/MSY/MDH/IJR
	Profil      float64 `json:"profil"`       // bobot tambahan perubahan profil nasabah (SCD-2)
}

// GenConfig dapat diubah dari UI.
type GenConfig struct {
	Rate     float64    `json:"rate"`      // aksi per detik pada mode live
	Workers  int        `json:"workers"`   // request paralel
	Dirt     DirtConfig `json:"dirt"`      // peluang kotoran
	Spike    string     `json:"spike"`     // kode cabang yang sedang "melonjak" (kosong = tidak ada)
	SpikeMul float64    `json:"spike_mul"` // pengali volume cabang spike
}

func defaultConfig() GenConfig {
	return GenConfig{Rate: 4, Workers: 4, SpikeMul: 5,
		Dirt: DirtConfig{Duplikat: 0.03, Kosong: 0.03, Terlambat: 0.05, AkadInvalid: 0.08, Profil: 0.05}}
}

type cabang struct {
	ID   any    `json:"id"`
	Kode string `json:"kode"`
	Nama string `json:"nama"`
	Kota string `json:"kota"`
}

type rekening struct {
	ID    any
	No    string
	Saldo int64 // perkiraan lokal; aplikasi tetap sumber kebenaran
}

type nasabah struct {
	ID      any
	CIF     string
	Kota    string
	Segmen  string
	Cabang  int  // indeks ke g.cabang
	Dormant bool // nasabah baru yang sengaja tidak bertransaksi (pertanyaan direksi no. 3)
	Rek     []*rekening
}

type akad struct {
	ID      any
	Status  string
	Nasabah *nasabah
}

type product struct {
	ID    string `json:"id"`
	Price int    `json:"price"`
}

type logLine struct {
	At  string `json:"at"`
	Msg string `json:"msg"`
}

type generator struct {
	log *slog.Logger
	gw  *gateway

	mu       sync.Mutex
	cfg      GenConfig
	cabang   []cabang
	nasabah  []*nasabah
	akad     []*akad
	products []product
	counts   map[string]int64
	errs     map[string]int64
	recent   []logLine
	mode     string // idle | live | backfill
	backfill map[string]any
	cancel   context.CancelFunc
	rng      *rand.Rand
	// lastRestock membatasi frekuensi restock stok toko
	lastRestock time.Time
	seq         atomic.Int64
}

func newGenerator(log *slog.Logger, gw *gateway) *generator {
	return &generator{log: log, gw: gw, cfg: defaultConfig(), counts: map[string]int64{}, errs: map[string]int64{},
		mode: "idle", rng: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 42))}
}

func (g *generator) note(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	g.mu.Lock()
	g.recent = append(g.recent, logLine{At: time.Now().UTC().Format(time.RFC3339), Msg: msg})
	if len(g.recent) > 60 {
		g.recent = g.recent[len(g.recent)-60:]
	}
	g.mu.Unlock()
}

func (g *generator) count(k string) {
	g.mu.Lock()
	g.counts[k]++
	g.mu.Unlock()
}

func (g *generator) fail(action string, err error) {
	k := action + ": "
	var ae *apiErr
	switch {
	case errors.As(err, &ae) && (ae.Status == 400 || ae.Status == 409 || ae.Status == 422):
		k += fmt.Sprintf("ditolak aplikasi (HTTP %d)", ae.Status)
	case errors.As(err, &ae):
		k += fmt.Sprintf("HTTP %d", ae.Status)
	default:
		k += "jaringan/timeout"
	}
	g.mu.Lock()
	g.errs[k]++
	n := g.errs[k]
	g.mu.Unlock()
	if n <= 3 || n%100 == 0 {
		g.log.Warn("aksi generator gagal", "action", action, "err", err.Error(), "count", n)
		g.note("%s gagal: %v", action, err)
	}
}

// ---------------------------------------------------------------- acak

func (g *generator) f() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rng.Float64()
}

func (g *generator) intn(n int) int {
	if n <= 0 {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rng.IntN(n)
}

func (g *generator) norm() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rng.NormFloat64()
}

// weighted memilih kunci berdasarkan bobot.
func (g *generator) weighted(w map[string]float64) string {
	keys := make([]string, 0, len(w))
	total := 0.0
	for k, v := range w {
		keys = append(keys, k)
		total += v
	}
	sort.Strings(keys)
	x := g.f() * total
	for _, k := range keys {
		x -= w[k]
		if x <= 0 {
			return k
		}
	}
	return keys[len(keys)-1]
}

// lognormal: nominal transaksi bersifat right-skewed (banyak kecil, sedikit sangat besar).
func (g *generator) lognormal(median float64, sigma float64, round int64) int64 {
	v := int64(median * math.Exp(sigma*g.norm()))
	if round > 1 {
		v = (v/round + 1) * round
	}
	return max(v, round)
}

var kanalBobot = map[string]float64{"mobile": 0.52, "atm": 0.2, "web": 0.13, "teller": 0.15}

// jamBobot: distribusi jam transaksi WIB (sepi dini hari, puncak siang dan malam).
var jamBobot = []float64{0.3, 0.2, 0.1, 0.1, 0.2, 0.6, 1.5, 3, 4.5, 5.5, 6, 6.2, 6.8, 6, 5.5, 5.5, 5.2, 5, 5.4, 6, 5.5, 4, 2.5, 1}

var wib = time.FixedZone("WIB", 7*3600)

// waktuDalamHari memilih waktu acak pada tanggal day (WIB) mengikuti jamBobot.
func (g *generator) waktuDalamHari(day time.Time) time.Time {
	w := map[string]float64{}
	for h, b := range jamBobot {
		w[fmt.Sprintf("%02d", h)] = b
	}
	var h int
	fmt.Sscanf(g.weighted(w), "%d", &h)
	d := time.Date(day.Year(), day.Month(), day.Day(), h, g.intn(60), g.intn(60), 0, wib)
	if now := time.Now(); d.After(now) {
		d = now.Add(-time.Duration(g.intn(600)) * time.Second)
	}
	return d
}

// ---------------------------------------------------------------- data sintetis

var namaDepan = []string{"Ahmad", "Siti", "Muhammad", "Nur", "Dewi", "Rizky", "Fajar", "Aisyah", "Budi", "Putri",
	"Hana", "Ilham", "Fitri", "Yusuf", "Zahra", "Agus", "Rina", "Hendra", "Laila", "Rafi", "Salma", "Dimas", "Annisa", "Bayu"}
var namaBelakang = []string{"Hidayat", "Rahmawati", "Saputra", "Nurhaliza", "Pratama", "Lestari", "Firmansyah",
	"Kusuma", "Maulana", "Wulandari", "Ramadhan", "Syahputra", "Fauziah", "Hakim", "Permata", "Santoso", "Utami"}
var segmenBobot = map[string]float64{"ritel": 0.78, "prioritas": 0.07, "umkm": 0.15}
var kodeAkadValid = []string{"MRB", "MSY", "MDH", "IJR"}

// kodeAkadKotor: variasi yang sering muncul di sistem lama (huruf kecil, nama lengkap, salah ketik).
var kodeAkadKotor = []string{"mrb", "MRBH", "Murabahah", "XYZ", "", "IJR ", "MSY-1", "mdh"}

func (g *generator) digits(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte(byte('0' + g.intn(10)))
	}
	return b.String()
}

func (g *generator) profilBaru(cab cabang) map[string]any {
	nama := namaDepan[g.intn(len(namaDepan))] + " " + namaBelakang[g.intn(len(namaBelakang))]
	email := strings.ToLower(strings.ReplaceAll(nama, " ", ".")) + g.digits(3) + "@contoh.id"
	return map[string]any{
		"nama":      nama,
		"nik":       "32" + g.digits(14), // sintetis, bukan NIK sungguhan
		"no_hp":     "08" + g.digits(10),
		"email":     email,
		"kota":      cab.Kota,
		"segmen":    g.weighted(segmenBobot),
		"cabang_id": cab.ID,
	}
}

var kotaPindah = []string{"Bogor", "Depok", "Bekasi", "Tangerang", "Bandung", "Surabaya", "Yogyakarta", "Semarang",
	"Medan", "Makassar", "Palembang", "Malang", "Padang", "Balikpapan"}

// ---------------------------------------------------------------- bootstrap

// asMap mengambil objek entitas dari jawaban API: {..} langsung atau dibungkus satu kunci.
func asMap(v map[string]any, key string) map[string]any {
	if v == nil {
		return nil
	}
	if inner, ok := v[key].(map[string]any); ok {
		return inner
	}
	return v
}

func asList(v any, key string) []map[string]any {
	var arr []any
	switch t := v.(type) {
	case []any:
		arr = t
	case map[string]any:
		for _, k := range []string{key, "items", "data"} {
			if a, ok := t[k].([]any); ok {
				arr = a
				break
			}
		}
	}
	out := []map[string]any{}
	for _, x := range arr {
		if m, ok := x.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func str(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		if t == math.Trunc(t) {
			return fmt.Sprintf("%.0f", t)
		}
		return fmt.Sprint(t)
	default:
		return fmt.Sprint(t)
	}
}

func num(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case string:
		var n int64
		fmt.Sscan(t, &n)
		return n
	}
	return 0
}

// bootstrap memuat cabang, nasabah, rekening, akad, dan produk yang sudah ada di aplikasi.
func (g *generator) bootstrap(ctx context.Context) error {
	var cabRaw any
	if err := g.gw.call(ctx, http.MethodGet, pathCabang, nil, "", &cabRaw); err != nil {
		return fmt.Errorf("daftar cabang: %w", err)
	}
	cabs := []cabang{}
	for _, m := range asList(cabRaw, "cabang") {
		cabs = append(cabs, cabang{ID: m["id"], Kode: str(m["kode"]), Nama: str(m["nama"]), Kota: str(m["kota"])})
	}
	if len(cabs) == 0 {
		return errors.New("aplikasi belum punya cabang")
	}
	var prodRaw any
	prods := []product{}
	if err := g.gw.call(ctx, http.MethodGet, pathProducts, nil, "", &prodRaw); err == nil {
		for _, m := range asList(prodRaw, "products") {
			prods = append(prods, product{ID: str(m["id"]), Price: int(num(m["price"]))})
		}
	}
	cabIdx := map[string]int{}
	for i, c := range cabs {
		cabIdx[str(c.ID)] = i
	}
	byID := map[string]*nasabah{}
	nas := []*nasabah{}
	var nasRaw any
	if err := g.gw.call(ctx, http.MethodGet, pathNasabah+"?limit=500", nil, "", &nasRaw); err == nil {
		for _, m := range asList(nasRaw, "nasabah") {
			n := &nasabah{ID: m["id"], CIF: str(m["cif"]), Kota: str(m["kota"]), Segmen: str(m["segmen"]), Cabang: cabIdx[str(m["cabang_id"])]}
			byID[str(n.ID)] = n
			nas = append(nas, n)
		}
	}
	var rekRaw any
	if err := g.gw.call(ctx, http.MethodGet, pathRekening+"?limit=500", nil, "", &rekRaw); err == nil {
		for _, m := range asList(rekRaw, "rekening") {
			if n := byID[str(m["nasabah_id"])]; n != nil && str(m["status"]) == "aktif" {
				n.Rek = append(n.Rek, &rekening{ID: m["id"], No: str(m["no_rekening"]), Saldo: num(m["saldo"])})
			}
		}
	}
	aks := []*akad{}
	var akRaw any
	if err := g.gw.call(ctx, http.MethodGet, pathAkad+"?limit=500", nil, "", &akRaw); err == nil {
		for _, m := range asList(akRaw, "akad") {
			if n := byID[str(m["nasabah_id"])]; n != nil {
				aks = append(aks, &akad{ID: m["id"], Status: str(m["status"]), Nasabah: n})
			}
		}
	}
	g.mu.Lock()
	g.cabang, g.products, g.nasabah, g.akad = cabs, prods, nas, aks
	g.mu.Unlock()
	g.note("bootstrap: %d cabang, %d nasabah, %d akad, %d produk", len(cabs), len(nas), len(aks), len(prods))
	return nil
}

// ---------------------------------------------------------------- aksi

// opts membawa konteks waktu satu aksi: live (sekarang) atau backfill (hari tertentu).
type opts struct {
	day      time.Time // nol = live
	cabSpike int       // -1 = tidak ada
}

func (g *generator) occurred(o opts, allowLate bool) (string, bool) {
	var t time.Time
	if o.day.IsZero() {
		t = time.Now()
	} else {
		t = g.waktuDalamHari(o.day)
	}
	late := false
	g.mu.Lock()
	p := g.cfg.Dirt.Terlambat
	g.mu.Unlock()
	if allowLate && g.f() < p {
		t = t.Add(-time.Duration(1+g.intn(71)) * time.Hour) // perangkat offline, tersinkron belakangan
		late = true
	}
	return t.In(wib).Format(time.RFC3339), late
}

func (g *generator) dirt() DirtConfig {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cfg.Dirt
}

func (g *generator) pickNasabah(active bool) *nasabah {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.nasabah) == 0 {
		return nil
	}
	for i := 0; i < 20; i++ {
		n := g.nasabah[g.rng.IntN(len(g.nasabah))]
		if active && (n.Dormant || len(n.Rek) == 0) {
			continue
		}
		return n
	}
	return nil
}

func (g *generator) cabangFor(n *nasabah, o opts) cabang {
	g.mu.Lock()
	defer g.mu.Unlock()
	if o.cabSpike >= 0 && g.rng.Float64() < 0.5 {
		return g.cabang[o.cabSpike]
	}
	if n != nil && g.rng.Float64() < 0.8 {
		return g.cabang[n.Cabang%len(g.cabang)] // kebanyakan bertransaksi di cabang asal
	}
	return g.cabang[g.rng.IntN(len(g.cabang))]
}

func (g *generator) nasabahBaru(ctx context.Context, o opts) {
	g.mu.Lock()
	cab := g.cabang[g.rng.IntN(len(g.cabang))]
	ci := 0
	for i := range g.cabang {
		if str(g.cabang[i].ID) == str(cab.ID) {
			ci = i
		}
	}
	g.mu.Unlock()
	body := g.profilBaru(cab)
	body["occurred_at"], _ = g.occurred(o, false)
	if g.f() < math.Min(1, 3*g.dirt().Kosong) {
		if g.f() < 0.5 {
			body["email"] = ""
		} else {
			delete(body, "no_hp")
		}
		g.count("kotor: nilai kosong (nasabah)")
	}
	var res map[string]any
	if err := g.gw.call(ctx, http.MethodPost, pathNasabah, body, "", &res); err != nil {
		g.fail("nasabah baru", err)
		return
	}
	m := asMap(res, "nasabah")
	n := &nasabah{ID: m["id"], CIF: str(m["cif"]), Kota: str(body["kota"]), Segmen: str(body["segmen"]), Cabang: ci,
		Dormant: g.f() < 0.2}
	g.count("nasabah baru")
	// buka rekening tabungan dengan setoran awal (nasabah dormant: setoran kecil, lalu tidak aktif)
	setoran := g.lognormal(2_000_000, 0.8, 50_000)
	if n.Dormant {
		setoran = 100_000
	}
	g.bukaRekening(ctx, n, "tabungan_wadiah", setoran, cab, o)
	g.mu.Lock()
	g.nasabah = append(g.nasabah, n)
	g.mu.Unlock()
}

func (g *generator) bukaRekening(ctx context.Context, n *nasabah, jenis string, setoran int64, cab cabang, o opts) *rekening {
	occ, _ := g.occurred(o, false)
	var res map[string]any
	body := map[string]any{"nasabah_id": n.ID, "jenis": jenis, "setoran_awal": setoran, "cabang_id": cab.ID, "kanal": "teller", "occurred_at": occ}
	if err := g.gw.call(ctx, http.MethodPost, pathRekening, body, "", &res); err != nil {
		g.fail("buka rekening", err)
		return nil
	}
	m := asMap(res, "rekening")
	r := &rekening{ID: m["id"], No: str(m["no_rekening"]), Saldo: setoran}
	g.mu.Lock()
	n.Rek = append(n.Rek, r)
	g.mu.Unlock()
	g.count("buka rekening")
	return r
}

// transaksi mengirim satu transaksi. allowDirt=false untuk transaksi pendukung (setoran awal).
func (g *generator) transaksi(ctx context.Context, n *nasabah, r *rekening, jenis string, nominal int64, kanal string, cab cabang, o opts, allowDirt bool) {
	occ, late := g.occurred(o, allowDirt)
	body := map[string]any{"rekening_id": r.ID, "no_rekening": r.No, "cabang_id": cab.ID, "kanal": kanal,
		"jenis": jenis, "nominal": nominal, "occurred_at": occ}
	d := g.dirt()
	if allowDirt && g.f() < d.Kosong {
		// cabang_id dikirim kosong (transaksi mobile tanpa cabang). Aplikasi mengisinya dengan cabang
		// pembuka rekening, jadi kolom di OLTP tidak null; nilai kosong yang sampai ke bronze berasal
		// dari email/no_hp nasabah. kanal NOT NULL di aplikasi sehingga tidak dikosongkan.
		body["cabang_id"] = nil
		g.count("cabang_id dikosongkan (diisi aplikasi)")
	}
	if late {
		g.count("kotor: data terlambat")
	}
	dup := allowDirt && g.f() < d.Duplikat
	key := ""
	if !dup {
		key = fmt.Sprintf("gen-%d-%s", g.seq.Add(1), newID()[:8]) // request normal membawa Idempotency-Key
	}
	if err := g.gw.call(ctx, http.MethodPost, pathTransaksi, body, key, nil); err != nil {
		g.fail("transaksi "+jenis, err)
		return
	}
	g.count("transaksi " + jenis)
	g.adjustSaldo(r, jenis, nominal)
	if dup {
		// klien (mis. ATM) timeout lalu mengirim ulang request yang sama tanpa Idempotency-Key
		if err := g.gw.call(ctx, http.MethodPost, pathTransaksi, body, "", nil); err == nil {
			g.count("kotor: duplikat (transaksi)")
			g.adjustSaldo(r, jenis, nominal)
		}
	}
}

func (g *generator) adjustSaldo(r *rekening, jenis string, nominal int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch jenis {
	case "setor", "transfer_masuk":
		r.Saldo += nominal
	default:
		r.Saldo -= nominal
	}
}

func (g *generator) transaksiAcak(ctx context.Context, o opts) {
	n := g.pickNasabah(true)
	if n == nil {
		g.nasabahBaru(ctx, o)
		return
	}
	g.mu.Lock()
	r := n.Rek[g.rng.IntN(len(n.Rek))]
	saldo := r.Saldo
	g.mu.Unlock()
	cab := g.cabangFor(n, o)
	kanal := g.weighted(kanalBobot)
	jenis := g.weighted(map[string]float64{"setor": 0.32, "tarik": 0.33, "transfer": 0.35})
	if saldo < 200_000 {
		jenis = "setor"
	}
	switch jenis {
	case "setor":
		g.transaksi(ctx, n, r, "setor", g.lognormal(750_000, 0.9, 10_000), kanal, cab, o, true)
	case "tarik":
		round := int64(10_000)
		if kanal == "atm" {
			round = 50_000
		}
		nom := min(g.lognormal(400_000, 0.7, round), max(saldo/2, round))
		g.transaksi(ctx, n, r, "tarik", nom, kanal, cab, o, true)
	case "transfer":
		dst := g.pickNasabah(true)
		if dst == nil || dst == n {
			return
		}
		g.mu.Lock()
		dr := dst.Rek[0]
		g.mu.Unlock()
		nom := min(g.lognormal(500_000, 1.0, 1_000), max(saldo/2, 1_000))
		occ, late := g.occurred(o, true)
		if late {
			g.count("kotor: data terlambat")
		}
		body := map[string]any{"dari": r.No, "ke": dr.No, "nominal": nom, "kanal": kanal, "cabang_id": cab.ID, "occurred_at": occ}
		if err := g.gw.call(ctx, http.MethodPost, pathTransfer, body, "gen-"+newID(), nil); err != nil {
			g.fail("transfer", err)
			return
		}
		g.count("transfer")
		g.adjustSaldo(r, "transfer_keluar", nom)
		g.adjustSaldo(dr, "transfer_masuk", nom)
	}
}

func (g *generator) ubahProfil(ctx context.Context, o opts) {
	n := g.pickNasabah(false)
	if n == nil {
		return
	}
	body := map[string]any{}
	switch g.intn(3) {
	case 0:
		body["kota"] = kotaPindah[g.intn(len(kotaPindah))]
	case 1:
		body["segmen"] = g.weighted(segmenBobot)
	default:
		body["kota"] = kotaPindah[g.intn(len(kotaPindah))]
		body["no_hp"] = "08" + g.digits(10)
	}
	body["occurred_at"], _ = g.occurred(o, false)
	if err := g.gw.call(ctx, http.MethodPatch, fmt.Sprintf("%s/%s", pathNasabah, str(n.ID)), body, "", nil); err != nil {
		g.fail("ubah profil", err)
		return
	}
	g.mu.Lock()
	if k, ok := body["kota"].(string); ok {
		n.Kota = k
	}
	g.mu.Unlock()
	g.count("ubah profil (SCD-2)")
}

func (g *generator) ajukanAkad(ctx context.Context, o opts) {
	n := g.pickNasabah(true)
	if n == nil {
		return
	}
	kode := kodeAkadValid[g.intn(len(kodeAkadValid))]
	if g.f() < g.dirt().AkadInvalid {
		kode = kodeAkadKotor[g.intn(len(kodeAkadKotor))]
		g.count("kotor: kode akad tidak valid")
	}
	pokok := g.lognormal(25_000_000, 0.9, 1_000_000)
	tenor := []int{6, 12, 12, 24, 36, 60}[g.intn(6)]
	margin := pokok * int64(tenor) / 12 * int64(8+g.intn(6)) / 100
	occ, _ := g.occurred(o, false)
	var res map[string]any
	g.mu.Lock()
	rekID := n.Rek[0].ID
	g.mu.Unlock()
	body := map[string]any{"nasabah_id": n.ID, "rekening_id": rekID, "kode_akad": kode, "pokok": pokok, "margin": margin, "tenor_bulan": tenor, "occurred_at": occ}
	if err := g.gw.call(ctx, http.MethodPost, pathAkad, body, "", &res); err != nil {
		g.fail("ajukan akad", err)
		return
	}
	m := asMap(res, "akad")
	g.mu.Lock()
	g.akad = append(g.akad, &akad{ID: m["id"], Status: "diajukan", Nasabah: n})
	g.mu.Unlock()
	g.count("ajukan akad")
}

// prosesAkad memajukan satu akad: diajukan -> disetujui/ditolak, disetujui -> cair, cair -> angsuran.
func (g *generator) prosesAkad(ctx context.Context, o opts) {
	g.mu.Lock()
	var cand []*akad
	for _, a := range g.akad {
		if a.Status == "diajukan" || a.Status == "disetujui" || a.Status == "cair" {
			cand = append(cand, a)
		}
	}
	if len(cand) == 0 {
		g.mu.Unlock()
		return
	}
	a := cand[g.rng.IntN(len(cand))]
	g.mu.Unlock()
	aksi, next := "", ""
	switch a.Status {
	case "diajukan":
		aksi, next = "setujui", "disetujui"
		if g.f() < 0.15 {
			aksi, next = "tolak", "ditolak"
		}
	case "disetujui":
		aksi, next = "cairkan", "cair"
	case "cair":
		aksi, next = "angsuran", "cair"
	}
	occ, _ := g.occurred(o, false)
	var res map[string]any
	cab := g.cabangFor(a.Nasabah, o)
	body := map[string]any{"occurred_at": occ, "kanal": "teller", "cabang_id": cab.ID}
	if aksi == "angsuran" {
		body["kanal"] = g.weighted(map[string]float64{"mobile": 0.6, "teller": 0.2, "web": 0.2})
	}
	if err := g.gw.call(ctx, http.MethodPost, fmt.Sprintf("%s/%s/%s", pathAkad, str(a.ID), aksi), body, "", &res); err != nil {
		g.fail("akad "+aksi, err)
		var ae *apiErr
		if errors.As(err, &ae) && ae.Status == 409 {
			g.mu.Lock()
			a.Status = "?" // status lokal tidak sinkron; abaikan akad ini
			g.mu.Unlock()
		}
		return
	}
	if m := asMap(res, "akad"); m != nil && str(m["status"]) != "" {
		next = str(m["status"])
	}
	g.mu.Lock()
	a.Status = next
	g.mu.Unlock()
	g.count("akad " + aksi)
}

// belanja: checkout toko dibayar dari rekening bank (payment service memanggil bank).
func (g *generator) belanja(ctx context.Context, o opts) {
	n := g.pickNasabah(true)
	g.mu.Lock()
	np := len(g.products)
	g.mu.Unlock()
	if n == nil || np == 0 {
		return
	}
	items := []map[string]any{}
	for i := 0; i < 1+g.intn(3); i++ {
		g.mu.Lock()
		p := g.products[g.rng.IntN(len(g.products))]
		g.mu.Unlock()
		items = append(items, map[string]any{"product_id": p.ID, "qty": 1 + g.intn(2)})
	}
	g.mu.Lock()
	r := n.Rek[0]
	g.mu.Unlock()
	body := map[string]any{"customer": n.CIF, "items": items, "no_rekening": r.No}
	dup := g.f() < g.dirt().Duplikat
	key := ""
	if !dup {
		key = "gen-" + newID()
	}
	if err := g.gw.call(ctx, http.MethodPost, pathOrders, body, key, nil); err != nil {
		var ae *apiErr
		if errors.As(err, &ae) && ae.Status == 409 && strings.Contains(ae.Body, "stok") {
			g.restock(ctx)
		}
		g.fail("belanja", err)
		return
	}
	g.count("belanja (order)")
	if dup {
		if err := g.gw.call(ctx, http.MethodPost, pathOrders, body, "", nil); err == nil {
			g.count("kotor: duplikat (order)")
		}
	}
}

// restock: stok demo toko hanya 10 per produk, sehingga lalu lintas berjam-jam menghabiskannya.
// Generator berperan sebagai pemasok dan memanggil endpoint admin inventory bab05 (jaringan
// internal, tidak lewat APISIX), paling sering sekali per 30 detik.
func (g *generator) restock(ctx context.Context) {
	g.mu.Lock()
	if time.Since(g.lastRestock) < 30*time.Second {
		g.mu.Unlock()
		return
	}
	g.lastRestock = time.Now()
	g.mu.Unlock()
	url := env("INVENTORY_ADMIN_URL", "http://inventory:8080") + "/admin/reset-stock"
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	res, err := g.gw.http.Do(req)
	if err != nil {
		g.note("restock gagal: %v", err)
		return
	}
	res.Body.Close()
	g.count("restock toko (pemasok)")
	g.note("stok toko habis: restock lewat %s (HTTP %d)", url, res.StatusCode)
}

// step menjalankan satu aksi acak sesuai bobot campuran lalu lintas.
func (g *generator) step(ctx context.Context, o opts, withShop bool) {
	d := g.dirt()
	w := map[string]float64{"transaksi": 70, "nasabah": 4, "profil": 2 + 60*d.Profil, "akad_ajukan": 3, "akad_proses": 4}
	if withShop {
		w["belanja"] = 10
	}
	switch g.weighted(w) {
	case "transaksi":
		g.transaksiAcak(ctx, o)
	case "nasabah":
		g.nasabahBaru(ctx, o)
	case "profil":
		g.ubahProfil(ctx, o)
	case "akad_ajukan":
		g.ajukanAkad(ctx, o)
	case "akad_proses":
		g.prosesAkad(ctx, o)
	case "belanja":
		g.belanja(ctx, o)
	}
}

func (g *generator) spikeIndex() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, c := range g.cabang {
		if g.cfg.Spike != "" && (c.Kode == g.cfg.Spike || str(c.ID) == g.cfg.Spike) {
			return i
		}
	}
	return -1
}

// ensureSeed memastikan ada cukup nasabah aktif sebelum lalu lintas dimulai.
func (g *generator) ensureSeed(ctx context.Context, o opts, n int) {
	g.mu.Lock()
	have := len(g.nasabah)
	g.mu.Unlock()
	for i := have; i < n && ctx.Err() == nil; i++ {
		g.nasabahBaru(ctx, o)
	}
}

// runLive: aksi dengan laju cfg.Rate per detik memakai cfg.Workers goroutine.
func (g *generator) runLive(ctx context.Context) {
	g.ensureSeed(ctx, opts{cabSpike: -1}, 30)
	g.mu.Lock()
	workers := max(1, min(g.cfg.Workers, 16))
	g.mu.Unlock()
	jobs := make(chan struct{}, workers*2)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				g.step(ctx, opts{cabSpike: g.spikeIndex()}, true)
			}
		}()
	}
	defer func() { close(jobs); wg.Wait() }()
	for ctx.Err() == nil {
		g.mu.Lock()
		rate := math.Max(0.1, math.Min(g.cfg.Rate, 200))
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(float64(time.Second) / rate)):
		}
		select {
		case jobs <- struct{}{}:
		default: // aplikasi lebih lambat dari laju yang diminta: lewati satu tick
		}
	}
}

// runBackfill: riwayat days hari ke belakang, perDay aksi per hari, dengan 2 hari lonjakan
// tidak wajar pada satu cabang (bahan pertanyaan direksi no. 4).
func (g *generator) runBackfill(ctx context.Context, days, perDay int, withShop bool) {
	today := time.Now().In(wib)
	start := today.AddDate(0, 0, -days)
	g.ensureSeed(ctx, opts{day: start, cabSpike: -1}, 30)
	g.mu.Lock()
	ncab := len(g.cabang)
	workers := max(1, min(g.cfg.Workers*2, 16))
	g.mu.Unlock()
	spikeDays := map[int]int{}
	for i := 0; i < 2 && days > 3; i++ {
		spikeDays[1+g.intn(days-1)] = g.intn(ncab)
	}
	type job struct{ o opts }
	jobs := make(chan job, workers*4)
	var wg sync.WaitGroup
	var done atomic.Int64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				g.step(ctx, j.o, withShop)
				done.Add(1)
			}
		}()
	}
	total := days * perDay
	for d := days; d >= 1 && ctx.Err() == nil; d-- {
		day := today.AddDate(0, 0, -d)
		idx := days - d
		sp, isSpike := spikeDays[idx]
		n := perDay
		if !isSpike {
			sp = -1
		} else {
			n = perDay * 2
			g.mu.Lock()
			kode := g.cabang[sp].Kode
			g.mu.Unlock()
			g.note("backfill %s: lonjakan tidak wajar di cabang %s", day.Format("2006-01-02"), kode)
		}
		// variasi harian: akhir pekan lebih sepi, awal bulan (gajian) lebih ramai
		mul := 1.0
		if wd := day.Weekday(); wd == time.Saturday || wd == time.Sunday {
			mul = 0.7
		}
		if day.Day() >= 25 || day.Day() <= 2 {
			mul *= 1.3
		}
		n = int(float64(n) * mul)
		// pertambahan nasabah baru per hari agar "nasabah baru belum bertransaksi" bermakna
		for i := 0; i < max(1, perDay/40) && ctx.Err() == nil; i++ {
			jobs <- job{opts{day: day, cabSpike: -1}}
		}
		for i := 0; i < n && ctx.Err() == nil; i++ {
			jobs <- job{opts{day: day, cabSpike: sp}}
		}
		g.mu.Lock()
		g.backfill = map[string]any{"days": days, "per_day": perDay, "day": day.Format("2006-01-02"),
			"day_index": idx + 1, "actions_done": done.Load(), "actions_planned": total}
		g.mu.Unlock()
	}
	close(jobs)
	wg.Wait()
	g.mu.Lock()
	g.backfill["actions_done"] = done.Load()
	g.backfill["selesai"] = ctx.Err() == nil
	g.mu.Unlock()
	g.note("backfill selesai: %d hari, %d aksi", days, done.Load())
}

// ---------------------------------------------------------------- HTTP

func (g *generator) start(mode string, run func(ctx context.Context)) error {
	g.mu.Lock()
	if g.mode != "idle" {
		g.mu.Unlock()
		return fmt.Errorf("generator sedang %s; hentikan dulu", g.mode)
	}
	ctx, cancel := context.WithCancel(context.Background())
	g.mode, g.cancel = mode, cancel
	g.mu.Unlock()
	go func() {
		defer func() {
			g.mu.Lock()
			g.mode, g.cancel = "idle", nil
			g.mu.Unlock()
		}()
		bctx, bc := context.WithTimeout(ctx, 30*time.Second)
		err := g.bootstrap(bctx)
		bc()
		if err != nil {
			g.note("tidak dapat mulai: %v", err)
			g.log.Error("bootstrap generator gagal", "err", err)
			return
		}
		g.log.Info("generator mulai", "mode", mode)
		run(ctx)
		g.log.Info("generator berhenti", "mode", mode)
	}()
	return nil
}

func (g *generator) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/gen/status", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		defer g.mu.Unlock()
		writeJSON(w, 200, map[string]any{"mode": g.mode, "config": g.cfg, "counts": g.counts, "errors": g.errs,
			"recent": g.recent, "backfill": g.backfill, "cabang": g.cabang, "nasabah_dikenal": len(g.nasabah)})
	})
	mux.HandleFunc("POST /api/gen/config", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		c := g.cfg
		g.mu.Unlock()
		if err := readJSON(r, &c); err != nil {
			writeJSON(w, 400, map[string]any{"error": err.Error()})
			return
		}
		c.Rate = math.Max(0.1, math.Min(c.Rate, 200))
		c.Workers = max(1, min(c.Workers, 16))
		clamp := func(p *float64) { *p = math.Max(0, math.Min(*p, 1)) }
		clamp(&c.Dirt.Duplikat)
		clamp(&c.Dirt.Kosong)
		clamp(&c.Dirt.Terlambat)
		clamp(&c.Dirt.AkadInvalid)
		clamp(&c.Dirt.Profil)
		g.mu.Lock()
		g.cfg = c
		g.mu.Unlock()
		g.note("konfigurasi diubah: rate %.1f/s, spike %q", c.Rate, c.Spike)
		writeJSON(w, 200, c)
	})
	mux.HandleFunc("POST /api/gen/start", func(w http.ResponseWriter, r *http.Request) {
		if err := g.start("live", g.runLive); err != nil {
			writeJSON(w, 409, map[string]any{"error": err.Error()})
			return
		}
		g.note("mode live dimulai")
		writeJSON(w, 202, map[string]any{"mode": "live"})
	})
	mux.HandleFunc("POST /api/gen/backfill", func(w http.ResponseWriter, r *http.Request) {
		in := struct {
			Days     int   `json:"days"`
			PerDay   int   `json:"per_day"`
			WithShop *bool `json:"with_shop"`
		}{Days: 30, PerDay: 200}
		if err := readJSON(r, &in); err != nil {
			writeJSON(w, 400, map[string]any{"error": err.Error()})
			return
		}
		in.Days = max(1, min(in.Days, 800))
		in.PerDay = max(10, min(in.PerDay, 20000))
		shop := in.WithShop == nil || *in.WithShop
		if err := g.start("backfill", func(ctx context.Context) { g.runBackfill(ctx, in.Days, in.PerDay, shop) }); err != nil {
			writeJSON(w, 409, map[string]any{"error": err.Error()})
			return
		}
		g.note("backfill dimulai: %d hari x %d aksi", in.Days, in.PerDay)
		writeJSON(w, 202, map[string]any{"mode": "backfill", "days": in.Days, "per_day": in.PerDay})
	})
	mux.HandleFunc("POST /api/gen/stop", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		if g.cancel != nil {
			g.cancel()
		}
		g.mu.Unlock()
		g.note("generator dihentikan")
		writeJSON(w, 200, map[string]any{"stopping": true})
	})
	mux.HandleFunc("POST /api/gen/reset-counts", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		g.counts, g.errs, g.recent = map[string]int64{}, map[string]int64{}, nil
		g.mu.Unlock()
		writeJSON(w, 200, map[string]any{"ok": true})
	})
}
