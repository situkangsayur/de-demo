// Command dash-api adalah API dashboard direksi bank syariah digital (platform end-to-end DE).
// Setiap pertanyaan bisnis dijawab dari dua sumber gold yang dibangun dua jalur berbeda:
//   - warehouse: ClickHouse database gold (jalur ELT, dbt)
//   - lake: DuckDB atas Iceberg lake.gold.* (jalur ETL, Polars/DuckDB), lewat lake-engine
//
// API juga meneruskan status Airflow (REST API v2), hasil data quality, time travel Iceberg,
// tiering/cost, lineage, dan aksi tiga uji demo (Q1-Q3).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "dash-api")

type ctxKey string

func main() {
	addr := envOr("ADDR", ":8080")
	app := &App{
		ch:      NewClickHouse(envOr("CLICKHOUSE_URL", "http://clickhouse:8123"), os.Getenv("CLICKHOUSE_USER"), os.Getenv("CLICKHOUSE_PASSWORD")),
		lake:    NewLake(envOr("LAKE_ENGINE_URL", "http://lake-engine:8090")),
		airflow: NewAirflow(envOr("AIRFLOW_URL", "http://airflow-apiserver:8080"), os.Getenv("AIRFLOW_USER"), os.Getenv("AIRFLOW_PASSWORD")),
	}
	mux := http.NewServeMux()
	app.routes(mux)
	srv := &http.Server{Addr: addr, Handler: withRequestID(mux), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	logger.Info("dash-api siap", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server berhenti", "err", err)
		os.Exit(1)
	}
}

func (a *App) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "dash-api"})
	})
	mux.HandleFunc("GET /api/ringkas", a.handleRingkas)
	mux.HandleFunc("GET /api/pertanyaan", a.handleDaftarPertanyaan)
	mux.HandleFunc("GET /api/pertanyaan/{no}", a.handlePertanyaan)
	mux.HandleFunc("GET /api/bandingkan/{no}", a.handleBandingkan)
	mux.HandleFunc("GET /api/fitur", a.handleFitur)
	mux.HandleFunc("GET /api/airflow/runs", a.handleRuns)
	mux.HandleFunc("GET /api/airflow/runs/{run_id}/tasks", a.handleTasks)
	mux.HandleFunc("POST /api/airflow/trigger", a.handleTrigger)
	mux.HandleFunc("POST /api/airflow/backfill", a.handleBackfill)
	mux.HandleFunc("GET /api/dq", a.handleDQ)
	mux.HandleFunc("GET /api/time-travel", a.handleTimeTravel)
	mux.HandleFunc("GET /api/time-travel/asof", a.handleAsOf)
	mux.HandleFunc("GET /api/tiering", a.handleTiering)
	mux.HandleFunc("GET /api/lineage", a.handleLineage)
	mux.HandleFunc("GET /api/uji/q1", a.handleUjiQ1)
	mux.HandleFunc("GET /api/uji/status", a.handleUjiStatus)
	mux.HandleFunc("POST /api/uji/sabotase", a.handleSabotase)
}

// ---------------------------------------------------------------- util HTTP

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-ID")
		if rid == "" {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			rid = hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-ID", rid)
		t0 := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		next.ServeHTTP(sw, r.WithContext(context.WithValue(r.Context(), ctxKey("rid"), rid)))
		if r.URL.Path != "/api/health" {
			logger.Info("request", "request_id", rid, "method", r.Method, "path", r.URL.Path,
				"status", sw.code, "ms", time.Since(t0).Milliseconds())
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, r *http.Request, code int, err error) {
	logger.Error("gagal", "request_id", r.Context().Value(ctxKey("rid")), "path", r.URL.Path, "err", err.Error())
	writeJSON(w, code, map[string]any{"error": err.Error()})
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func rid(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKey("rid")).(string); ok {
		return v
	}
	return "-"
}
