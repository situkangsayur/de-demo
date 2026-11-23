package main

// Klien ClickHouse lewat HTTP interface (FORMAT JSON), hanya pustaka standar.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type ClickHouse struct {
	url, user, pass string
	hc              *http.Client
}

func NewClickHouse(url, user, pass string) *ClickHouse {
	return &ClickHouse{url: url, user: user, pass: pass, hc: &http.Client{Timeout: 30 * time.Second}}
}

// Query menjalankan SELECT dan mengembalikan baris sebagai map.
func (c *ClickHouse) Query(ctx context.Context, sql string) ([]map[string]any, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/?output_format_json_quote_64bit_integers=0",
		strings.NewReader(sql+" FORMAT JSON"))
	req.Header.Set("X-ClickHouse-User", c.user)
	req.Header.Set("X-ClickHouse-Key", c.pass)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse %d: %s", resp.StatusCode, truncate(string(b), 400))
	}
	var out struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

func (c *ClickHouse) Ping(ctx context.Context) error {
	_, err := c.Query(ctx, "SELECT 1")
	return err
}
