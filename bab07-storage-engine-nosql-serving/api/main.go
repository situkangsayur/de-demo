// API lab Bab 7: storage engine, NoSQL, dan serving. Memuat data bank syariah ke PostgreSQL, ClickHouse,
// Redis, dan OpenSearch (2 node), lalu menyediakan benchmark pola akses, eksperimen amplifikasi tulis,
// perbandingan row vs columnar, serving fitur_nasabah, pencarian teks, dan jurnal hash chain.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Konfig struct {
	PGURL, RedisAddr, CHURL, CHUser, CHPass, OSURL string
	NNasabah, NTrx, Hari                           int
}

type App struct {
	s     *Stores
	d     *Data
	prog  *Progres
	siap  bool
	mu    sync.Mutex
	bench []Hasil
	sink  *HasilSinkron
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return d
}

func logInfo(msg string, kv ...any) { slog.Info(msg, kv...) }

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Key = "time"
			}
			return a
		},
	})).With("service", "bab07-api"))
	cfg := Konfig{
		PGURL:     fmt.Sprintf("postgres://lab:%s@postgres:5432/bank?sslmode=disable", os.Getenv("PG_PASSWORD")),
		RedisAddr: env("REDIS_ADDR", "redis:6379"),
		CHURL:     env("CLICKHOUSE_URL", "http://clickhouse:8123"), CHUser: env("CLICKHOUSE_USER", "lab"), CHPass: os.Getenv("CLICKHOUSE_PASSWORD"),
		OSURL:    env("OPENSEARCH_URL", "http://os1:9200"),
		NNasabah: envInt("JUMLAH_NASABAH", 5000), NTrx: envInt("JUMLAH_TRANSAKSI", 400000), Hari: envInt("HARI", 90),
	}
	akhir := time.Now().UTC().Truncate(24 * time.Hour) // tengah malam UTC hari ini: deterministik per hari, tanpa waktu masa depan
	app := &App{prog: &Progres{Tahap: "menunggu store", Jumlah: map[string]int{}, Throughput: map[string]float64{}}}
	app.d = Bangkitkan(cfg.NNasabah, cfg.NTrx, cfg.Hari, akhir)
	app.prog.Jumlah = map[string]int{"nasabah": len(app.d.Nasabah), "rekening": len(app.d.Rekening), "transaksi": len(app.d.Trx)}
	go app.siapkan(cfg)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) { tulis(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		app.mu.Lock()
		siap := app.siap
		app.mu.Unlock()
		tulis(w, 200, map[string]any{"siap": siap, "muat": app.prog.salin()})
	})
	perlu := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			app.mu.Lock()
			siap := app.siap
			app.mu.Unlock()
			if !siap {
				tulis(w, 503, map[string]string{"error": "data masih dimuat: " + app.prog.salin().Tahap})
				return
			}
			h(w, r)
		}
	}
	mux.HandleFunc("POST /api/bench", perlu(app.hBench))
	mux.HandleFunc("GET /api/bench", perlu(func(w http.ResponseWriter, r *http.Request) {
		app.mu.Lock()
		defer app.mu.Unlock()
		tulis(w, 200, map[string]any{"hasil": app.bench, "throughput_bulk": app.prog.salin().Throughput})
	}))
	mux.HandleFunc("POST /api/engine/tulis", perlu(app.hTulis))
	mux.HandleFunc("GET /api/engine/sampel", perlu(func(w http.ResponseWriter, r *http.Request) { tulis(w, 200, app.Sampel(r.Context())) }))
	mux.HandleFunc("POST /api/engine/reset", perlu(func(w http.ResponseWriter, r *http.Request) { balas(w, nil, app.ResetTulis(r.Context())) }))
	mux.HandleFunc("POST /api/engine/optimize", perlu(func(w http.ResponseWriter, r *http.Request) { balas(w, nil, app.Optimize(r.Context())) }))
	mux.HandleFunc("GET /api/engine/btree", perlu(func(w http.ResponseWriter, r *http.Request) { v, err := app.BTree(r.Context()); balas(w, v, err) }))
	mux.HandleFunc("GET /api/kolom/ukuran", perlu(func(w http.ResponseWriter, r *http.Request) { v, err := app.Ukuran(r.Context()); balas(w, v, err) }))
	mux.HandleFunc("POST /api/kolom/banding", perlu(func(w http.ResponseWriter, r *http.Request) {
		var q struct {
			Jenis string `json:"jenis"`
		}
		_ = json.NewDecoder(r.Body).Decode(&q)
		v, err := app.Banding(r.Context(), q.Jenis)
		balas(w, v, err)
	}))
	mux.HandleFunc("POST /api/fitur/sinkron", perlu(func(w http.ResponseWriter, r *http.Request) {
		v, err := app.HitungDanSinkron(r.Context())
		if err == nil {
			app.mu.Lock()
			app.sink = &v
			app.mu.Unlock()
		}
		balas(w, v, err)
	}))
	mux.HandleFunc("GET /api/fitur/info", perlu(func(w http.ResponseWriter, r *http.Request) {
		versi, _ := app.s.RD.Get(r.Context(), "fitur:versi").Result()
		ttl, _ := app.s.RD.TTL(r.Context(), "fitur:7").Result()
		app.mu.Lock()
		defer app.mu.Unlock()
		tulis(w, 200, map[string]any{"versi": versi, "ttl_detik": ttl.Seconds(), "sinkron_terakhir": app.sink, "jumlah_nasabah": len(app.d.Nasabah)})
	}))
	mux.HandleFunc("GET /api/fitur/{id}", perlu(func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil {
			balas(w, nil, err)
			return
		}
		tulis(w, 200, map[string]any{"nasabah_id": id, "hasil": app.Lookup(r.Context(), id)})
	}))
	mux.HandleFunc("GET /api/search/cluster", perlu(func(w http.ResponseWriter, r *http.Request) { v, err := app.Cluster(r.Context()); balas(w, v, err) }))
	mux.HandleFunc("GET /api/search/cari", perlu(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		if q == "" {
			q = "zakat wakaf"
		}
		v, err := app.Cari(r.Context(), q)
		balas(w, v, err)
	}))
	mux.HandleFunc("GET /api/ledger", perlu(func(w http.ResponseWriter, r *http.Request) {
		v, err := app.LedgerStatus(r.Context())
		balas(w, v, err)
	}))
	mux.HandleFunc("POST /api/ledger/{aksi}", perlu(app.hLedger))

	addr := ":" + env("PORT", "8080")
	slog.Info("api siap menerima request", "addr", addr)
	if err := http.ListenAndServe(addr, logReq(mux)); err != nil {
		slog.Error("server berhenti", "err", err.Error())
	}
}

func (a *App) siapkan(cfg Konfig) {
	ctx := context.Background()
	s, err := Hubungkan(ctx, cfg)
	if err != nil {
		a.prog.mu.Lock()
		a.prog.Galat = err.Error()
		a.prog.mu.Unlock()
		slog.Error("store tidak siap", "err", err.Error())
		return
	}
	a.s = s
	if err := a.Muat(ctx); err != nil {
		a.prog.mu.Lock()
		a.prog.Galat = err.Error()
		a.prog.mu.Unlock()
		slog.Error("muat data gagal", "err", err.Error())
		return
	}
	a.prog.set("menghitung fitur_nasabah dan menyalin ke Redis", 96)
	if v, err := a.HitungDanSinkron(ctx); err == nil {
		a.sink = &v
	} else {
		slog.Error("sinkron fitur gagal", "err", err.Error())
	}
	if st, _ := a.LedgerStatus(ctx); st == nil || len(st["entri"].([]Entri)) == 0 {
		_ = a.LedgerReset(ctx)
	}
	a.prog.mu.Lock()
	a.prog.Selesai, a.prog.Tahap, a.prog.Persen = true, "siap", 100
	a.prog.mu.Unlock()
	a.mu.Lock()
	a.siap = true
	a.mu.Unlock()
	slog.Info("data siap", "transaksi", len(a.d.Trx))
}

func (a *App) hBench(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Pola string `json:"pola"`
		N    int    `json:"n"`
	}
	_ = json.NewDecoder(r.Body).Decode(&q)
	if q.N <= 0 || q.N > 2000 {
		q.N = 200
	}
	if q.Pola == "" {
		q.Pola = "semua"
	}
	h := a.Bench(r.Context(), q.Pola, q.N)
	a.mu.Lock()
	// simpan hasil per pola (pola lain dari run sebelumnya tetap ada)
	var sisa []Hasil
	for _, x := range a.bench {
		if q.Pola != "semua" && x.Pola != q.Pola {
			sisa = append(sisa, x)
		}
	}
	a.bench = append(sisa, h...)
	a.mu.Unlock()
	tulis(w, 200, map[string]any{"hasil": h})
}

func (a *App) hTulis(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Store string `json:"store"`
		N     int    `json:"n"`
	}
	_ = json.NewDecoder(r.Body).Decode(&q)
	if q.N <= 0 || q.N > 2000 {
		q.N = 300
	}
	v, err := a.Tulis(r.Context(), q.Store, q.N)
	balas(w, v, err)
}

func (a *App) hLedger(w http.ResponseWriter, r *http.Request) {
	var q struct {
		ID      int    `json:"id"`
		Nilai   int64  `json:"nilai"`
		Akad    string `json:"akad"`
		Nasabah string `json:"nasabah"`
	}
	_ = json.NewDecoder(r.Body).Decode(&q)
	var err error
	switch r.PathValue("aksi") {
	case "reset":
		err = a.LedgerReset(r.Context())
	case "tambah":
		if q.Akad == "" {
			q.Akad, q.Nasabah, q.Nilai = fmt.Sprintf("MRB-%04d murabahah", time.Now().Unix()%10000), "CIF0000999", 75_000_000
		}
		err = a.LedgerTambah(r.Context(), q.Akad, q.Nasabah, q.Nilai)
	case "sunting":
		err = a.LedgerSunting(r.Context(), q.ID, q.Nilai)
	case "tutupi":
		err = a.LedgerTutupi(r.Context())
	default:
		err = fmt.Errorf("aksi tidak dikenal")
	}
	if err != nil {
		balas(w, nil, err)
		return
	}
	v, err := a.LedgerStatus(r.Context())
	balas(w, v, err)
}

func tulis(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func balas(w http.ResponseWriter, v any, err error) {
	if err != nil {
		slog.Warn("galat", "err", err.Error())
		tulis(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if v == nil {
		v = map[string]string{"status": "ok"}
	}
	tulis(w, 200, v)
}

func logReq(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 6)
		_, _ = rand.Read(b)
		id := hex.EncodeToString(b)
		w.Header().Set("X-Request-Id", id)
		t := time.Now()
		h.ServeHTTP(w, r)
		if r.URL.Path != "/api/health" && r.URL.Path != "/api/engine/sampel" && r.URL.Path != "/api/status" {
			slog.Info("request", "request_id", id, "method", r.Method, "path", r.URL.Path, "ms", time.Since(t).Milliseconds())
		}
	})
}
