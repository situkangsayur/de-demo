package main

// API lab Bab 1: menjalankan pipeline mini dan menyajikan status, jejak satu transaksi, dan dashboard.
// Log JSON satu baris (slog) dengan field service dan request_id agar terbaca Alloy/Loki.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var logger *slog.Logger

func main() {
	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.MessageKey {
				a.Key = "msg"
			}
			if a.Key == slog.LevelKey {
				a.Value = slog.StringValue(strings.ToLower(a.Value.String()))
			}
			return a
		}})).With("service", "bab01-api")
	p := BaruPipeline(20261123)
	if v, err := strconv.ParseFloat(os.Getenv("FLUSH_DETIK"), 64); err == nil {
		p.cfg.FlushDetik = v
	}
	if v, err := strconv.ParseFloat(os.Getenv("BATCH_DETIK"), 64); err == nil {
		p.cfg.BatchDetik = v
	}
	p.backfill(time.Now())
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go jalankanDetak(ctx, p)
	go jalankanGenerator(ctx, p)

	srv := &http.Server{Addr: ":8080", Handler: catat(rute(p)), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	logger.Info("api siap", "addr", srv.Addr, "flush_detik", p.cfg.FlushDetik, "batch_detik", p.cfg.BatchDetik)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server berhenti", "err", err.Error())
		os.Exit(1)
	}
}

func jalankanDetak(ctx context.Context, p *Pipeline) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			p.Detak(now)
		}
	}
}

func jalankanGenerator(ctx context.Context, p *Pipeline) {
	r := rand.New(rand.NewSource(7))
	for {
		p.mu.Lock()
		aktif, laju := p.cfg.GenAktif, p.cfg.GenLaju
		p.mu.Unlock()
		tunggu := time.Second
		if aktif && laju > 0 {
			tunggu = time.Duration(float64(time.Second) / laju * (0.5 + r.Float64()))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(tunggu):
		}
		if aktif {
			p.Kirim(p.PermintaanAcak(), "generator")
		}
	}
}

func rute(p *Pipeline) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { tulis(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) { tulis(w, 200, p.Status(time.Now())) })
	mux.HandleFunc("GET /api/dashboard", func(w http.ResponseWriter, r *http.Request) { tulis(w, 200, p.Dashboard(time.Now())) })
	mux.HandleFunc("GET /api/referensi", func(w http.ResponseWriter, r *http.Request) {
		tulis(w, 200, map[string]any{"cabang": DataCabang, "rekening": DataRekening[:20]})
	})
	mux.HandleFunc("POST /api/transaksi", func(w http.ResponseWriter, r *http.Request) {
		var q Permintaan
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&q); err != nil {
			tulis(w, 400, map[string]string{"error": "body JSON tidak valid"})
			return
		}
		h := p.Kirim(q, "kelas")
		logger.Info("transaksi dikirim", "request_id", r.Header.Get("X-Request-Id"), "status", h.Status, "trx_id", strings.Join(h.TrxID, ","),
			"kanal", q.Kanal, "kirim_ulang", q.KirimUlang)
		tulis(w, h.Status, h)
	})
	mux.HandleFunc("GET /api/jejak/{id}", func(w http.ResponseWriter, r *http.Request) {
		d, ok := p.DetailJejak(r.PathValue("id"))
		if !ok {
			tulis(w, 404, map[string]string{"error": "jejak tidak ditemukan"})
			return
		}
		tulis(w, 200, d)
	})
	mux.HandleFunc("POST /api/konfig", func(w http.ResponseWriter, r *http.Request) {
		var k Konfig
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<14)).Decode(&k); err != nil {
			tulis(w, 400, map[string]string{"error": "body JSON tidak valid"})
			return
		}
		p.SetKonfig(k)
		logger.Info("konfigurasi diubah", "generator_aktif", k.GenAktif, "laju", k.GenLaju, "persen_kotor", k.GenKotorPct)
		tulis(w, 200, p.Status(time.Now()).Konfig)
	})
	mux.HandleFunc("POST /api/batch", func(w http.ResponseWriter, r *http.Request) {
		// jalankan flush + batch sekarang (seperti "Trigger DAG" di Airflow)
		now := time.Now()
		p.mu.Lock()
		p.antarPesan(now)
		p.flush(now)
		p.batch(now)
		p.mu.Unlock()
		tulis(w, 200, map[string]string{"status": "batch dijalankan"})
	})
	mux.HandleFunc("POST /api/reset", func(w http.ResponseWriter, r *http.Request) {
		p.Reset()
		logger.Info("state direset")
		tulis(w, 200, map[string]string{"status": "direset"})
	})
	mux.HandleFunc("GET /api/platform", func(w http.ResponseWriter, r *http.Request) { tulis(w, 200, platform(r.Context())) })
	return mux
}

// platform: bila PLATFORM_FLOW_URL diisi dan platform menyala, ambil statistik aliran data nyata.
func platform(ctx context.Context) map[string]any {
	base := strings.TrimRight(os.Getenv("PLATFORM_FLOW_URL"), "/")
	if base == "" {
		return map[string]any{"aktif": false, "pesan": "PLATFORM_FLOW_URL kosong: lab berjalan sendiri"}
	}
	c, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(c, "GET", base+"/api/flow/stats", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return map[string]any{"aktif": false, "pesan": "platform tidak terjangkau: " + err.Error()}
	}
	defer resp.Body.Close()
	var v map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return map[string]any{"aktif": false, "pesan": fmt.Sprintf("respons platform tidak terbaca (HTTP %d)", resp.StatusCode)}
	}
	return map[string]any{"aktif": true, "url": base, "stats": v}
}

func tulis(w http.ResponseWriter, kode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(kode)
	_ = json.NewEncoder(w).Encode(v)
}

type perekam struct {
	http.ResponseWriter
	kode int
}

func (r *perekam) WriteHeader(k int) { r.kode = k; r.ResponseWriter.WriteHeader(k) }

func catat(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mulai := time.Now()
		rid := r.Header.Get("X-Request-Id")
		if rid == "" {
			rid = fmt.Sprintf("%x", mulai.UnixNano())
			r.Header.Set("X-Request-Id", rid)
		}
		rec := &perekam{ResponseWriter: w, kode: 200}
		h.ServeHTTP(rec, r)
		// status/dashboard dipanggil UI tiap 2 detik: tidak dicatat agar log tidak banjir
		if r.URL.Path == "/healthz" || (r.Method == "GET" && rec.kode < 400) {
			return
		}
		logger.Info("http", "request_id", rid, "method", r.Method, "path", r.URL.Path, "kode", rec.kode,
			"durasi_ms", time.Since(mulai).Milliseconds())
	})
}
