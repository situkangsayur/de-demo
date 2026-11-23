package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Hasil adalah bentuk hasil query yang sama untuk kedua sumber.
type Hasil struct {
	Sumber    string   `json:"sumber"`
	Mesin     string   `json:"mesin"`
	SQL       string   `json:"sql"`
	Kolom     []string `json:"kolom"`
	Baris     [][]any  `json:"baris"`
	LatencyMs float64  `json:"latency_ms"`
	EngineMs  float64  `json:"engine_ms"`
	Error     string   `json:"error,omitempty"`
}

// ---------------------------------------------------------------- ClickHouse (HTTP interface)

type ClickHouse struct {
	base, user, pass string
	hc               *http.Client
}

func NewClickHouse(base, user, pass string) *ClickHouse {
	return &ClickHouse{base: base, user: user, pass: pass, hc: &http.Client{Timeout: 60 * time.Second}}
}

func (c *ClickHouse) Query(ctx context.Context, sql string) (*Hasil, error) {
	q := url.Values{}
	q.Set("default_format", "JSONCompact")
	q.Set("output_format_json_quote_64bit_integers", "0")
	q.Set("output_format_json_quote_decimals", "0")
	q.Set("join_use_nulls", "1")
	q.Set("readonly", "2")
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/?"+q.Encode(), strings.NewReader(sql))
	req.Header.Set("X-ClickHouse-User", c.user)
	req.Header.Set("X-ClickHouse-Key", c.pass)
	req.Header.Set("X-Request-ID", rid(ctx))
	t0 := time.Now()
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("clickhouse %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		Meta []struct {
			Name string `json:"name"`
		} `json:"meta"`
		Data       [][]any `json:"data"`
		Statistics struct {
			Elapsed float64 `json:"elapsed"`
		} `json:"statistics"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	h := &Hasil{Sumber: "warehouse", Mesin: "ClickHouse 24.8 (gold, jalur ELT)", SQL: sql, Baris: out.Data,
		LatencyMs: float64(time.Since(t0).Microseconds()) / 1000, EngineMs: out.Statistics.Elapsed * 1000}
	for _, m := range out.Meta {
		h.Kolom = append(h.Kolom, m.Name)
	}
	if h.Baris == nil {
		h.Baris = [][]any{}
	}
	return h, nil
}

// ---------------------------------------------------------------- lake-engine (DuckDB atas Iceberg)

type Lake struct {
	base string
	hc   *http.Client
}

func NewLake(base string) *Lake {
	return &Lake{base: base, hc: &http.Client{Timeout: 120 * time.Second}}
}

func (l *Lake) do(ctx context.Context, method, path string, in any, out any) error {
	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, l.base+path, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", rid(ctx))
	resp, err := l.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("lake-engine %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return dec.Decode(out)
}

func (l *Lake) Query(ctx context.Context, sql string) (*Hasil, error) {
	var out struct {
		Kolom []string `json:"kolom"`
		Baris [][]any  `json:"baris"`
		Ms    float64  `json:"ms"`
	}
	t0 := time.Now()
	if err := l.do(ctx, http.MethodPost, "/query", map[string]string{"sql": sql}, &out); err != nil {
		return nil, err
	}
	if out.Baris == nil {
		out.Baris = [][]any{}
	}
	return &Hasil{Sumber: "lake", Mesin: "DuckDB 2.0 atas Iceberg (lake.gold, jalur ETL)", SQL: sql, Kolom: out.Kolom,
		Baris: out.Baris, LatencyMs: float64(time.Since(t0).Microseconds()) / 1000, EngineMs: out.Ms}, nil
}

func (l *Lake) Get(ctx context.Context, path string) (map[string]any, error) {
	var out map[string]any
	err := l.do(ctx, http.MethodGet, path, nil, &out)
	return out, err
}

func (l *Lake) Post(ctx context.Context, path string, in any) (map[string]any, error) {
	var out map[string]any
	err := l.do(ctx, http.MethodPost, path, in, &out)
	return out, err
}

// ---------------------------------------------------------------- Airflow REST API v2

type Airflow struct {
	base, user, pass string
	hc               *http.Client
	mu               sync.Mutex
	token            string
	tokenAt          time.Time
}

func NewAirflow(base, user, pass string) *Airflow {
	return &Airflow{base: base, user: user, pass: pass, hc: &http.Client{Timeout: 30 * time.Second}}
}

func (a *Airflow) bearer(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token != "" && time.Since(a.tokenAt) < 10*time.Minute {
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
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("airflow auth %d: %s", resp.StatusCode, body)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	a.token, a.tokenAt = out.AccessToken, time.Now()
	return a.token, nil
}

func (a *Airflow) Call(ctx context.Context, method, path string, in any) (any, error) {
	tok, err := a.bearer(ctx)
	if err != nil {
		return nil, err
	}
	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, a.base+"/api/v2"+path, body)
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("airflow %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var out any
	if len(b) == 0 {
		return map[string]any{}, nil
	}
	err = json.Unmarshal(b, &out)
	return out, err
}
