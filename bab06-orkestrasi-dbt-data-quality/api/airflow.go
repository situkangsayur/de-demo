package main

// Klien REST API v2 Airflow 3 (token JWT dari /auth/token, simple auth manager).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type Airflow struct {
	base, user, pass string
	hc               *http.Client
	mu               sync.Mutex
	token            string
	tokenAt          time.Time
}

func NewAirflow(base, user, pass string) *Airflow {
	return &Airflow{base: base, user: user, pass: pass, hc: &http.Client{Timeout: 20 * time.Second}}
}

func (a *Airflow) bearer(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token != "" && time.Since(a.tokenAt) < 20*time.Minute {
		return a.token, nil
	}
	b, _ := json.Marshal(map[string]string{"username": a.user, "password": a.pass})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, a.base+"/auth/token", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("airflow auth %d: %s", resp.StatusCode, body)
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	a.token, a.tokenAt = out.AccessToken, time.Now()
	return a.token, nil
}

// Call memanggil endpoint dan men-decode JSON ke out (boleh nil).
func (a *Airflow) Call(ctx context.Context, method, path string, in, out any) error {
	tok, err := a.bearer(ctx)
	if err != nil {
		return err
	}
	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, a.base+path, body)
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		a.mu.Lock()
		a.token = ""
		a.mu.Unlock()
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("airflow %s %s -> %d: %s", method, path, resp.StatusCode, truncate(string(data), 400))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

type DagRun struct {
	DagRunID    string  `json:"dag_run_id"`
	LogicalDate *string `json:"logical_date"`
	State       string  `json:"state"`
	RunType     string  `json:"run_type"`
	QueuedAt    *string `json:"queued_at"`
	StartDate   *string `json:"start_date"`
	EndDate     *string `json:"end_date"`
}

type TaskInstance struct {
	TaskID     string   `json:"task_id"`
	State      *string  `json:"state"`
	TryNumber  int      `json:"try_number"`
	MaxTries   int      `json:"max_tries"`
	StartDate  *string  `json:"start_date"`
	EndDate    *string  `json:"end_date"`
	Duration   *float64 `json:"duration"`
	RunID      string   `json:"dag_run_id"`
	Operator   *string  `json:"operator"`
	NextMethod *string  `json:"next_method"`
}

var zonaWIB = time.FixedZone("WIB", 7*3600)

// TglDari mengubah logical_date (UTC) menjadi tanggal logis WIB (ds).
func TglDari(ld *string) string {
	if ld == nil {
		return ""
	}
	t, err := time.Parse(time.RFC3339Nano, *ld)
	if err != nil {
		return ""
	}
	return t.In(zonaWIB).Format("2006-01-02")
}

func (a *Airflow) Runs(ctx context.Context, dag string, limit int) ([]DagRun, error) {
	var out struct {
		DagRuns []DagRun `json:"dag_runs"`
	}
	q := url.Values{"limit": {fmt.Sprint(limit)}, "order_by": {"-run_after"}}
	err := a.Call(ctx, http.MethodGet, "/api/v2/dags/"+dag+"/dagRuns?"+q.Encode(), nil, &out)
	return out.DagRuns, err
}

func (a *Airflow) TaskInstances(ctx context.Context, dag, run string) ([]TaskInstance, error) {
	var out struct {
		TaskInstances []TaskInstance `json:"task_instances"`
	}
	err := a.Call(ctx, http.MethodGet, "/api/v2/dags/"+dag+"/dagRuns/"+url.PathEscape(run)+"/taskInstances?limit=100", nil, &out)
	return out.TaskInstances, err
}

func (a *Airflow) Tries(ctx context.Context, dag, run, task string) ([]TaskInstance, error) {
	var out struct {
		TaskInstances []TaskInstance `json:"task_instances"`
	}
	err := a.Call(ctx, http.MethodGet, "/api/v2/dags/"+dag+"/dagRuns/"+url.PathEscape(run)+"/taskInstances/"+task+"/tries", nil, &out)
	return out.TaskInstances, err
}
