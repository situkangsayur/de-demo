// API lab Bab 6: generator bronze, sumber "core banking" yang bisa dibuat flaky, kendali demo
// (tahan data, sabotase), jembatan ke Airflow REST API, hasil data quality dan gold dari ClickHouse,
// lineage dari manifest dbt, serta dokumentasi dbt statis di /dbt-docs/.
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
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const dagID = "trx_harian"

type App struct {
	gen     *Generator
	af      *Airflow
	ch      *ClickHouse
	dbtDir  string
	hari    int
	mu      sync.Mutex
	flaky   bool
	gagalN  int
	panggil map[string]int
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Key = "time"
			}
			return a
		},
	})).With("service", "bab06-api"))
	hari, _ := strconv.Atoi(env("HARI", "10"))
	app := &App{
		gen:     &Generator{Dir: env("BRONZE_DIR", "/data/bronze")},
		af:      NewAirflow(env("AIRFLOW_URL", "http://airflow:8080"), env("AIRFLOW_USER", "admin"), os.Getenv("AIRFLOW_PASSWORD")),
		ch:      NewClickHouse(env("CLICKHOUSE_URL", "http://clickhouse:8123"), env("CLICKHOUSE_USER", "lab"), os.Getenv("CLICKHOUSE_PASSWORD")),
		dbtDir:  env("DBT_TARGET_DIR", "/opt/lab/dbt_target"),
		hari:    hari,
		gagalN:  2,
		panggil: map[string]int{},
	}
	if err := app.siapkanBronze(); err != nil {
		slog.Error("gagal membangkitkan bronze", "err", err.Error())
		os.Exit(1)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) { tulis(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /api/status", app.status)
	mux.HandleFunc("POST /api/kendali", app.kendali)
	mux.HandleFunc("GET /api/core-banking/kontrol", app.kontrol)
	mux.HandleFunc("GET /api/dag", app.dag)
	mux.HandleFunc("GET /api/runs", app.runs)
	mux.HandleFunc("GET /api/run/tries", app.tries)
	mux.HandleFunc("POST /api/jalankan", app.jalankan)
	mux.HandleFunc("POST /api/backfill", app.backfill)
	mux.HandleFunc("GET /api/gold", app.gold)
	mux.HandleFunc("GET /api/dq", app.dq)
	mux.HandleFunc("GET /api/lineage", app.lineage)
	mux.HandleFunc("GET /api/sla", app.sla)
	mux.Handle("GET /dbt-docs/", http.StripPrefix("/dbt-docs/", http.FileServer(http.Dir(filepath.Join(app.dbtDir, "target")))))
	addr := ":" + env("PORT", "8080")
	slog.Info("api siap", "addr", addr, "hari", hari)
	if err := http.ListenAndServe(addr, logReq(mux)); err != nil {
		slog.Error("server berhenti", "err", err.Error())
	}
}

func logReq(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 6)
		_, _ = rand.Read(b)
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-Id", id)
		t := time.Now()
		h.ServeHTTP(w, r)
		if r.URL.Path != "/api/health" {
			slog.Info("request", "request_id", id, "method", r.Method, "path", r.URL.Path, "ms", time.Since(t).Milliseconds())
		}
	})
}

func tulis(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func galat(w http.ResponseWriter, code int, err error) {
	slog.Warn("galat", "code", code, "err", err.Error())
	tulis(w, code, map[string]string{"error": err.Error()})
}

// daftarTgl: HARI tanggal terakhir s.d. kemarin (WIB), urut naik.
func (a *App) daftarTgl() []string {
	kemarin := time.Now().In(zonaWIB).AddDate(0, 0, -1)
	var out []string
	for i := a.hari - 1; i >= 0; i-- {
		out = append(out, kemarin.AddDate(0, 0, -i).Format("2006-01-02"))
	}
	return out
}

func (a *App) siapkanBronze() error {
	if err := a.gen.Snapshot(time.Now()); err != nil {
		return err
	}
	for _, t := range a.daftarTgl() {
		if _, err := os.Stat(a.gen.partisi("transaksi", t)); err == nil {
			continue // partisi sudah ada (restart): jangan timpa status tahan/sabotase
		}
		k, err := a.gen.Hari(t, false)
		if err != nil {
			return err
		}
		slog.Info("bronze dibangkitkan", "tgl", t, "jumlah_trx", k.JumlahTrx)
	}
	return nil
}

func (a *App) status(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	flaky := a.flaky
	a.mu.Unlock()
	errAF := a.af.Call(r.Context(), http.MethodGet, "/api/v2/version", nil, nil)
	tulis(w, 200, map[string]any{
		"hari": a.gen.Status(a.daftarTgl()), "flaky": flaky, "gagal_pertama": a.gagalN,
		"airflow_ok": errAF == nil, "sla_menit": env("SLA_MENIT", "5"),
	})
}

type kendaliReq struct {
	Aksi string `json:"aksi"`
	Tgl  string `json:"tgl"`
}

func validTgl(t string) bool {
	_, err := time.Parse("2006-01-02", t)
	return err == nil
}

func (a *App) kendali(w http.ResponseWriter, r *http.Request) {
	var q kendaliReq
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		galat(w, 400, err)
		return
	}
	if q.Aksi != "flaky_on" && q.Aksi != "flaky_off" && !validTgl(q.Tgl) {
		galat(w, 400, fmt.Errorf("tgl tidak valid: %q", q.Tgl))
		return
	}
	var err error
	hasil := map[string]any{"aksi": q.Aksi, "tgl": q.Tgl}
	switch q.Aksi {
	case "flaky_on", "flaky_off":
		a.mu.Lock()
		a.flaky = q.Aksi == "flaky_on"
		a.panggil = map[string]int{}
		a.mu.Unlock()
	case "tahan":
		err = a.gen.Tahan(q.Tgl, true)
	case "kirim":
		err = a.gen.Tahan(q.Tgl, false)
	case "sabotase":
		var n int
		n, err = a.gen.Sabotase(q.Tgl)
		hasil["baris_rusak"] = n
	case "pulihkan":
		err = a.gen.Pulihkan(q.Tgl)
	default:
		err = fmt.Errorf("aksi tidak dikenal: %s", q.Aksi)
	}
	if err != nil {
		galat(w, 400, err)
		return
	}
	slog.Info("kendali demo", "aksi", q.Aksi, "tgl", q.Tgl)
	tulis(w, 200, hasil)
}

// kontrol mensimulasikan API laporan core banking. Mode flaky: N panggilan pertama per tanggal -> 503.
func (a *App) kontrol(w http.ResponseWriter, r *http.Request) {
	tgl := r.URL.Query().Get("tgl")
	if !validTgl(tgl) {
		galat(w, 400, fmt.Errorf("tgl tidak valid"))
		return
	}
	a.mu.Lock()
	a.panggil[tgl]++
	ke, flaky := a.panggil[tgl], a.flaky
	a.mu.Unlock()
	if flaky && ke <= a.gagalN {
		slog.Warn("core banking sibuk (simulasi)", "tgl", tgl, "panggilan_ke", ke)
		tulis(w, 503, map[string]any{"error": "core banking sedang sibuk", "panggilan_ke": ke})
		return
	}
	b, err := os.ReadFile(filepath.Join(filepath.Dir(a.gen.Dir), "sumber", "kontrol-"+tgl+".json"))
	if err != nil {
		galat(w, 404, fmt.Errorf("laporan kontrol %s belum ada", tgl))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func (a *App) dag(w http.ResponseWriter, r *http.Request) {
	var out struct {
		Tasks []struct {
			TaskID      string   `json:"task_id"`
			Downstream  []string `json:"downstream_task_ids"`
			Operator    string   `json:"operator_name"`
			Retries     *float64 `json:"retries"`
			TriggerRule string   `json:"trigger_rule"`
		} `json:"tasks"`
	}
	if err := a.af.Call(r.Context(), http.MethodGet, "/api/v2/dags/"+dagID+"/tasks", nil, &out); err != nil {
		galat(w, 502, err)
		return
	}
	tulis(w, 200, out)
}

type runRingkas struct {
	DagRun
	Tgl   string                  `json:"tgl"`
	Tasks map[string]TaskInstance `json:"tasks"`
}

func (a *App) runs(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 50 {
		limit = 15
	}
	runs, err := a.af.Runs(r.Context(), dagID, limit)
	if err != nil {
		galat(w, 502, err)
		return
	}
	out := make([]runRingkas, len(runs))
	var wg sync.WaitGroup
	for i, run := range runs {
		out[i] = runRingkas{DagRun: run, Tgl: TglDari(run.LogicalDate), Tasks: map[string]TaskInstance{}}
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			tis, err := a.af.TaskInstances(r.Context(), dagID, id)
			if err != nil {
				return
			}
			for _, ti := range tis {
				out[i].Tasks[ti.TaskID] = ti
			}
		}(i, run.DagRunID)
	}
	wg.Wait()
	tulis(w, 200, map[string]any{"runs": out})
}

// tries: semua percobaan setiap task dalam satu run (untuk Gantt retry dan backoff).
func (a *App) tries(w http.ResponseWriter, r *http.Request) {
	run := r.URL.Query().Get("run_id")
	tis, err := a.af.TaskInstances(r.Context(), dagID, run)
	if err != nil {
		galat(w, 502, err)
		return
	}
	hasil := map[string][]TaskInstance{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ti := range tis {
		wg.Add(1)
		go func(task string) {
			defer wg.Done()
			t, err := a.af.Tries(r.Context(), dagID, run, task)
			if err != nil {
				return
			}
			mu.Lock()
			hasil[task] = t
			mu.Unlock()
		}(ti.TaskID)
	}
	wg.Wait()
	tulis(w, 200, map[string]any{"run_id": run, "tries": hasil})
}

type tglReq struct {
	Tgl    string `json:"tgl"`
	Dari   string `json:"dari"`
	Sampai string `json:"sampai"`
}

// jalankan: bila run untuk tanggal itu sudah ada -> clear (jalankan ulang, bukti idempotency);
// bila belum -> trigger run baru dengan logical_date = tgl 00.00 WIB.
func (a *App) jalankan(w http.ResponseWriter, r *http.Request) {
	var q tglReq
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil || !validTgl(q.Tgl) {
		galat(w, 400, fmt.Errorf("tgl tidak valid"))
		return
	}
	runs, err := a.af.Runs(r.Context(), dagID, 100)
	if err != nil {
		galat(w, 502, err)
		return
	}
	for _, run := range runs {
		if TglDari(run.LogicalDate) != q.Tgl {
			continue
		}
		if run.State == "running" || run.State == "queued" {
			galat(w, 409, fmt.Errorf("run %s masih %s", run.DagRunID, run.State))
			return
		}
		body := map[string]any{"dry_run": false, "dag_run_id": run.DagRunID, "only_failed": false, "reset_dag_runs": true}
		if err := a.af.Call(r.Context(), http.MethodPost, "/api/v2/dags/"+dagID+"/clearTaskInstances", body, nil); err != nil {
			galat(w, 502, err)
			return
		}
		slog.Info("run di-clear (jalankan ulang)", "tgl", q.Tgl, "run_id", run.DagRunID)
		tulis(w, 200, map[string]string{"aksi": "clear", "dag_run_id": run.DagRunID})
		return
	}
	id := fmt.Sprintf("manual__%s__%d", q.Tgl, time.Now().Unix())
	body := map[string]any{"dag_run_id": id, "logical_date": q.Tgl + "T00:00:00+07:00", "conf": map[string]any{}}
	if err := a.af.Call(r.Context(), http.MethodPost, "/api/v2/dags/"+dagID+"/dagRuns", body, nil); err != nil {
		galat(w, 502, err)
		return
	}
	slog.Info("run baru dipicu", "tgl", q.Tgl, "run_id", id)
	tulis(w, 200, map[string]string{"aksi": "trigger", "dag_run_id": id})
}

func (a *App) backfill(w http.ResponseWriter, r *http.Request) {
	var q tglReq
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil || !validTgl(q.Dari) || !validTgl(q.Sampai) {
		galat(w, 400, fmt.Errorf("dari/sampai tidak valid"))
		return
	}
	body := map[string]any{
		"dag_id": dagID, "from_date": q.Dari + "T00:00:00+07:00", "to_date": q.Sampai + "T00:00:00+07:00",
		"run_backwards": false, "dag_run_conf": map[string]any{}, "reprocess_behavior": "completed", "max_active_runs": 1,
	}
	var out map[string]any
	if err := a.af.Call(r.Context(), http.MethodPost, "/api/v2/backfills", body, &out); err != nil {
		galat(w, 502, err)
		return
	}
	slog.Info("backfill dibuat", "dari", q.Dari, "sampai", q.Sampai)
	tulis(w, 200, out)
}

func (a *App) tabelAda(ctx context.Context, db, t string) bool {
	rows, err := a.ch.Query(ctx, fmt.Sprintf("SELECT count() AS n FROM system.tables WHERE database='%s' AND name='%s'", db, t))
	return err == nil && len(rows) == 1 && num(rows[0]["n"]) > 0
}

func num(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}

// gold: per tanggal, bandingkan kontrol core banking, gold (idempotent + gerbang) dan gold_naif.
func (a *App) gold(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	per := map[string]map[string]any{}
	ambil := func(sql string) error {
		rows, err := a.ch.Query(ctx, sql)
		if err != nil {
			return err
		}
		for _, row := range rows {
			t := fmt.Sprint(row["tgl"])
			if per[t] == nil {
				per[t] = map[string]any{"tgl": t}
			}
			for k, v := range row {
				per[t][k] = v
			}
		}
		return nil
	}
	sqls := []string{
		"SELECT toString(dt) AS tgl, jumlah_trx AS kontrol_trx, total_nominal AS kontrol_nominal FROM raw.kontrol",
		"SELECT toString(tgl) AS tgl, sum(jumlah_trx) AS naif_trx, sum(total_nominal) AS naif_nominal, intDiv(count(), uniqExact(cabang_id)) AS naif_insert FROM gold.harian_cabang_naif GROUP BY tgl",
		"SELECT toString(tgl) AS tgl, count() AS publikasi, toString(max(waktu)) AS publikasi_terakhir FROM meta.publikasi WHERE tabel='gold.harian_cabang' GROUP BY tgl",
	}
	if a.tabelAda(ctx, "gold", "harian_cabang") {
		sqls = append(sqls,
			"SELECT toString(tgl) AS tgl, sum(jumlah_trx) AS gold_trx, sum(total_nominal) AS gold_nominal, "+
				"lower(hex(groupBitXor(cityHash64(cabang_id, jumlah_trx, total_nominal)))) AS gold_sidik FROM gold.harian_cabang GROUP BY tgl",
			"SELECT partition AS tgl, toString(max(modification_time)) AS gold_part_diubah, count() AS gold_part FROM system.parts "+
				"WHERE database='gold' AND table='harian_cabang' AND active GROUP BY partition")
	}
	for _, s := range sqls {
		if err := ambil(s); err != nil {
			galat(w, 502, err)
			return
		}
	}
	var out []map[string]any
	for _, v := range per {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["tgl"].(string) < out[j]["tgl"].(string) })
	var cabang []map[string]any
	if a.tabelAda(ctx, "gold", "harian_cabang") {
		cabang, _ = a.ch.Query(ctx, "SELECT toString(tgl) AS tgl, cabang_id, kode_cabang, jumlah_trx, total_nominal FROM gold.harian_cabang ORDER BY tgl, cabang_id")
	}
	tulis(w, 200, map[string]any{"per_tgl": out, "cabang": cabang})
}

func (a *App) dq(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	runs, err := a.ch.Query(ctx, "SELECT run_id, toString(tgl) AS tgl, toString(max(waktu)) AS terakhir, countIf(status='pass') AS lulus, "+
		"countIf(status IN ('fail','error')) AS gagal, countIf(status='warn') AS peringatan FROM meta.dq_hasil GROUP BY run_id, tgl ORDER BY max(waktu) DESC LIMIT 30")
	if err != nil {
		galat(w, 502, err)
		return
	}
	run := r.URL.Query().Get("run_id")
	if run == "" && len(runs) > 0 {
		run = fmt.Sprint(runs[0]["run_id"])
	}
	run = strings.ReplaceAll(run, "'", "")
	detail, err := a.ch.Query(ctx, "SELECT nama, model, kolom, dimensi, keparahan, status, failures, pesan, toString(waktu) AS dicatat "+
		"FROM meta.dq_hasil WHERE run_id = '"+run+"' AND waktu = (SELECT max(waktu) FROM meta.dq_hasil WHERE run_id = '"+run+"') ORDER BY dimensi, nama")
	if err != nil {
		galat(w, 502, err)
		return
	}
	tulis(w, 200, map[string]any{"runs": runs, "run_id": run, "hasil": detail})
}

func (a *App) sla(w http.ResponseWriter, r *http.Request) {
	rows, err := a.ch.Query(r.Context(), "SELECT run_id, tgl, batas, toString(dicatat) AS dicatat FROM meta.sla_terlewat ORDER BY dicatat DESC LIMIT 20")
	if err != nil {
		galat(w, 502, err)
		return
	}
	tulis(w, 200, map[string]any{"terlewat": rows, "sla_menit": env("SLA_MENIT", "5")})
}
