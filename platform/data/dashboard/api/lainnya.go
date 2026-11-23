package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const dagID = "bank_harian"

func (a *App) chRows(r *http.Request, sql string) (map[string]any, error) {
	h, err := a.ch.Query(r.Context(), sql)
	if err != nil {
		return nil, err
	}
	return map[string]any{"kolom": h.Kolom, "baris": h.Baris, "ms": h.LatencyMs}, nil
}

// handleRingkas: angka singkat untuk beranda (tanggal tersedia, jumlah baris per sumber).
func (a *App) handleRingkas(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{}
	if h, err := a.ch.Query(r.Context(), `SELECT min(tgl), max(tgl), count() FROM gold.fakta_transaksi`); err == nil && len(h.Baris) > 0 {
		out["warehouse"] = h.Baris[0]
	} else if err != nil {
		out["warehouse_error"] = err.Error()
	}
	if h, err := a.lake.Query(r.Context(), `SELECT min(tgl), max(tgl), count(*) FROM gold.fakta_transaksi`); err == nil && len(h.Baris) > 0 {
		out["lake"] = h.Baris[0]
	} else if err != nil {
		out["lake_error"] = err.Error()
	}
	wib := time.FixedZone("WIB", 7*3600)
	out["kemarin"] = time.Now().In(wib).AddDate(0, 0, -1).Format("2006-01-02")
	writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------- Airflow

func (a *App) handleRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := a.airflow.Call(r.Context(), http.MethodGet, "/dags/"+dagID+"/dagRuns?order_by=-run_after&limit=30", nil)
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	dag, _ := a.airflow.Call(r.Context(), http.MethodGet, "/dags/"+dagID, nil)
	bf, _ := a.airflow.Call(r.Context(), http.MethodGet, "/backfills?dag_id="+dagID+"&limit=10&order_by=-id", nil)
	writeJSON(w, http.StatusOK, map[string]any{"dag": dag, "runs": runs, "backfills": bf})
}

func (a *App) handleTasks(w http.ResponseWriter, r *http.Request) {
	id := url.PathEscape(r.PathValue("run_id"))
	ti, err := a.airflow.Call(r.Context(), http.MethodGet, "/dags/"+dagID+"/dagRuns/"+id+"/taskInstances?limit=100", nil)
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, ti)
}

func (a *App) handleTrigger(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Tgl   string `json:"tgl"`
		Penuh bool   `json:"penuh"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || !reTgl.MatchString(in.Tgl) {
		writeErr(w, r, http.StatusBadRequest, fmt.Errorf("body {\"tgl\": \"YYYY-MM-DD\"}"))
		return
	}
	runID := fmt.Sprintf("dashboard-%s-%d", in.Tgl, time.Now().Unix())
	out, err := a.airflow.Call(r.Context(), http.MethodPost, "/dags/"+dagID+"/dagRuns", map[string]any{
		"dag_run_id": runID, "logical_date": nil, "conf": map[string]any{"tgl": in.Tgl, "penuh": in.Penuh},
		"note": "dipicu dari dashboard"})
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// handleBackfill: backfill Airflow 3 (dikelola scheduler). Run untuk tanggal yang sudah selesai
// dijalankan ulang (reprocess_behavior=completed), satu per satu.
func (a *App) handleBackfill(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Dari   string `json:"dari"`
		Sampai string `json:"sampai"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || !reTgl.MatchString(in.Dari) || !reTgl.MatchString(in.Sampai) {
		writeErr(w, r, http.StatusBadRequest, fmt.Errorf("body {\"dari\": \"YYYY-MM-DD\", \"sampai\": \"YYYY-MM-DD\"}"))
		return
	}
	// Batas bawah backfill tidak selalu inklusif terhadap logical date (tgl D = D 00.00 WIB):
	// cek dengan dry run, geser satu hari bila tanggal pertama tidak ikut.
	wib := time.FixedZone("WIB", 7*3600)
	d0, _ := time.ParseInLocation("2006-01-02", in.Dari, wib)
	isi := func(dari time.Time) map[string]any {
		return map[string]any{"dag_id": dagID, "from_date": dari.Format(time.RFC3339), "to_date": in.Sampai + "T00:00:00+07:00",
			"reprocess_behavior": "completed", "max_active_runs": 1, "run_backwards": false, "dag_run_conf": map[string]any{}}
	}
	body := isi(d0)
	if dr, err := a.airflow.Call(r.Context(), http.MethodPost, "/backfills/dry_run", body); err == nil {
		ada := false
		if m, ok := dr.(map[string]any); ok {
			if l, ok := m["backfills"].([]any); ok {
				for _, x := range l {
					if e, ok := x.(map[string]any); ok {
						if t, err := time.Parse(time.RFC3339, fmt.Sprint(e["logical_date"])); err == nil && t.Equal(d0) {
							ada = true
						}
					}
				}
			}
		}
		if !ada {
			body = isi(d0.AddDate(0, 0, -1))
		}
	}
	out, err := a.airflow.Call(r.Context(), http.MethodPost, "/backfills", body)
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------- data quality

func (a *App) handleDQ(w http.ResponseWriter, r *http.Request) {
	run := r.URL.Query().Get("run_id")
	filter := "run_id = (SELECT argMax(run_id, waktu) FROM meta.dq_hasil)"
	if run != "" {
		filter = "run_id = '" + strings.ReplaceAll(run, "'", "") + "'"
	}
	hasil, err := a.chRows(r, `SELECT formatDateTime(waktu, '%Y-%m-%d %H:%i:%S') AS waktu, run_id, jalur, lapisan, tes, dimensi, aturan, status,
       keparahan, gagal, total, nilai, contoh, tgl_awal, tgl_akhir
FROM meta.dq_hasil WHERE `+filter+` ORDER BY jalur, lapisan, dimensi, tes`)
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	riwayat, _ := a.chRows(r, `SELECT run_id, formatDateTime(max(waktu), '%Y-%m-%d %H:%i:%S') AS terakhir,
       countIf(status = 'lulus') AS lulus, countIf(status = 'gagal') AS gagal,
       countIf(status = 'peringatan') AS peringatan, countIf(status = 'dilewati') AS dilewati
FROM meta.dq_hasil GROUP BY run_id ORDER BY max(waktu) DESC LIMIT 20`)
	dimensi, _ := a.chRows(r, `SELECT dimensi, jalur, count() AS jumlah_tes FROM meta.dq_hasil
WHERE `+filter+` GROUP BY dimensi, jalur ORDER BY dimensi, jalur`)
	writeJSON(w, http.StatusOK, map[string]any{"hasil": hasil, "riwayat": riwayat, "dimensi": dimensi})
}

// ---------------------------------------------------------------- time travel, tiering, lineage

func (a *App) handleTimeTravel(w http.ResponseWriter, r *http.Request) {
	tabel := r.URL.Query().Get("tabel")
	if tabel == "" {
		tabel = "gold.fakta_transaksi"
	}
	out, err := a.lake.Get(r.Context(), "/snapshots?tabel="+url.QueryEscape(tabel))
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *App) handleAsOf(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	tabel := q.Get("tabel")
	if tabel == "" {
		tabel = "gold.fakta_transaksi"
	}
	out, err := a.lake.Get(r.Context(), "/asof?tabel="+url.QueryEscape(tabel)+"&waktu="+url.QueryEscape(q.Get("waktu")))
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *App) handleTiering(w http.ResponseWriter, r *http.Request) {
	geser, _ := strconv.Atoi(r.URL.Query().Get("geser_hari"))
	lk, err := a.lake.Get(r.Context(), fmt.Sprintf("/tiering?geser_hari=%d", geser))
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	cost, _ := a.chRows(r, `SELECT skenario, laju, bulan, warm_gb, cold_gb, archive_gb, simpan_usd, ambil_usd, egress_usd, total_usd, kumulatif_usd
FROM meta.cost_bulanan ORDER BY laju, skenario, bulan`)
	harga, _ := a.chRows(r, `SELECT kunci, deskripsi, usagetype_aws, usd, tanggal_akses FROM meta.cost_harga ORDER BY usd DESC`)
	status, _ := a.chRows(r, `SELECT formatDateTime(waktu, '%Y-%m-%d %H:%i:%S'), mode, pesan FROM meta.tiering_status ORDER BY waktu DESC LIMIT 1`)
	writeJSON(w, http.StatusOK, map[string]any{"lake": lk, "cost": cost, "harga": harga, "status": status})
}

func (a *App) handleLineage(w http.ResponseWriter, r *http.Request) {
	lk, err := a.lake.Get(r.Context(), "/lapisan")
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	wh, _ := a.chRows(r, `SELECT database, name, engine, total_rows, total_bytes
FROM system.tables WHERE database IN ('raw', 'stg', 'gold') AND name NOT LIKE '%muat%' ORDER BY database, name`)
	writeJSON(w, http.StatusOK, map[string]any{"lake": lk, "warehouse": wh})
}

// ---------------------------------------------------------------- tiga uji demo (Q1-Q3)

// handleUjiQ1: query pemeriksaan slide P08 (checks/q1_idempoten.sql) dari kedua sumber.
func (a *App) handleUjiQ1(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	dari, sampai := q.Get("dari"), q.Get("sampai")
	if !reTgl.MatchString(dari) || !reTgl.MatchString(sampai) {
		writeErr(w, r, http.StatusBadRequest, fmt.Errorf("?dari=YYYY-MM-DD&sampai=YYYY-MM-DD"))
		return
	}
	sqlWH := fmt.Sprintf(`SELECT tgl, count() AS baris, sum(nominal) AS total, uniqExact(trx_id) AS trx_unik
FROM gold.fakta_transaksi WHERE tgl BETWEEN toDate('%s') AND toDate('%s') GROUP BY tgl ORDER BY tgl`, dari, sampai)
	sqlLK := fmt.Sprintf(`SELECT tgl, count(*) AS baris, sum(nominal) AS total, count(DISTINCT trx_id) AS trx_unik
FROM gold.fakta_transaksi WHERE tgl BETWEEN DATE '%s' AND DATE '%s' GROUP BY tgl ORDER BY tgl`, dari, sampai)
	wh, err1 := a.ch.Query(r.Context(), sqlWH)
	lk, err2 := a.lake.Query(r.Context(), sqlLK)
	out := map[string]any{"warehouse": wh, "lake": lk}
	if err1 != nil {
		out["warehouse_error"] = err1.Error()
	}
	if err2 != nil {
		out["lake_error"] = err2.Error()
	}
	writeJSON(w, http.StatusOK, out)
}

// handleUjiStatus: bukti Q2 (gold tidak berubah saat DQ gagal) dan kondisi sabotase.
func (a *App) handleUjiStatus(w http.ResponseWriter, r *http.Request) {
	lk, err := a.lake.Get(r.Context(), "/lapisan")
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	wh, _ := a.chRows(r, `SELECT table, formatDateTime(max(modification_time), '%Y-%m-%d %H:%i:%S') AS diubah, sum(rows) AS baris
FROM system.parts WHERE database = 'gold' AND active GROUP BY table ORDER BY table`)
	dq, _ := a.chRows(r, `SELECT formatDateTime(waktu, '%Y-%m-%d %H:%i:%S'), run_id, jalur, tes, dimensi, aturan, gagal, contoh
FROM meta.dq_hasil WHERE status = 'gagal' ORDER BY waktu DESC LIMIT 12`)
	writeJSON(w, http.StatusOK, map[string]any{"lake": lk, "warehouse_gold": wh, "dq_gagal_terakhir": dq})
}

func (a *App) handleSabotase(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Aksi string `json:"aksi"`
		Tgl  string `json:"tgl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || !reTgl.MatchString(in.Tgl) ||
		(in.Aksi != "sisipkan" && in.Aksi != "pulihkan") {
		writeErr(w, r, http.StatusBadRequest, fmt.Errorf("body {\"aksi\": \"sisipkan|pulihkan\", \"tgl\": \"YYYY-MM-DD\"}"))
		return
	}
	out, err := a.lake.Post(r.Context(), "/sabotase", in)
	if err != nil {
		writeErr(w, r, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
