package main

// Koneksi ke empat store: PostgreSQL (B-tree, baris), ClickHouse (MergeTree, kolom), Redis (key-value
// di memori), OpenSearch (inverted index, 2 node). ClickHouse dan OpenSearch lewat HTTP (pustaka standar).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Stores struct {
	PG *pgxpool.Pool
	RD *redis.Client
	CH *ClickHouse
	OS *OpenSearch
}

type ClickHouse struct {
	url, user, pass string
	hc              *http.Client
}

// Ringkasan baca dari header X-ClickHouse-Summary.
type CHRingkas struct {
	ReadRows  string `json:"read_rows"`
	ReadBytes string `json:"read_bytes"`
}

func (c *ClickHouse) do(ctx context.Context, sql string, body io.Reader, statistik bool) ([]byte, CHRingkas, error) {
	q := "/?output_format_json_quote_64bit_integers=0"
	if statistik {
		q += "&wait_end_of_query=1"
	}
	var req *http.Request
	if body == nil {
		req, _ = http.NewRequestWithContext(ctx, http.MethodPost, c.url+q, strings.NewReader(sql))
	} else {
		req, _ = http.NewRequestWithContext(ctx, http.MethodPost, c.url+q+"&query="+urlEnc(sql), body)
	}
	req.Header.Set("X-ClickHouse-User", c.user)
	req.Header.Set("X-ClickHouse-Key", c.pass)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, CHRingkas{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var rs CHRingkas
	_ = json.Unmarshal([]byte(resp.Header.Get("X-ClickHouse-Summary")), &rs)
	if resp.StatusCode != http.StatusOK {
		return nil, rs, fmt.Errorf("clickhouse %d: %s", resp.StatusCode, potong(string(b), 300))
	}
	return b, rs, nil
}

func (c *ClickHouse) Exec(ctx context.Context, sql string) error {
	_, _, err := c.do(ctx, sql, nil, false)
	return err
}

func (c *ClickHouse) Insert(ctx context.Context, sql string, body io.Reader) error {
	_, _, err := c.do(ctx, sql, body, false)
	return err
}

func (c *ClickHouse) Query(ctx context.Context, sql string) ([]map[string]any, error) {
	b, _, err := c.do(ctx, sql+" FORMAT JSON", nil, false)
	if err != nil {
		return nil, err
	}
	var out struct {
		Data []map[string]any `json:"data"`
	}
	return out.Data, json.Unmarshal(b, &out)
}

// QueryStat menjalankan query dan mengembalikan ringkasan baca (baris dan byte yang dibaca engine).
func (c *ClickHouse) QueryStat(ctx context.Context, sql string) (CHRingkas, error) {
	_, rs, err := c.do(ctx, sql+" FORMAT Null", nil, true)
	return rs, err
}

type OpenSearch struct {
	url string
	hc  *http.Client
}

func (o *OpenSearch) Do(ctx context.Context, method, path string, in any, out any) error {
	var body io.Reader
	switch v := in.(type) {
	case nil:
	case []byte:
		body = bytes.NewReader(v)
	default:
		b, _ := json.Marshal(v)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, o.url+path, body)
	if strings.HasSuffix(path, "_bulk") {
		req.Header.Set("Content-Type", "application/x-ndjson")
	} else {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := o.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("opensearch %s %s -> %d: %s", method, path, resp.StatusCode, potong(string(b), 300))
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

func potong(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func urlEnc(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// Hubungkan menunggu semua store siap (OpenSearch 2 node butuh puluhan detik).
func Hubungkan(ctx context.Context, cfg Konfig) (*Stores, error) {
	s := &Stores{
		CH: &ClickHouse{url: cfg.CHURL, user: cfg.CHUser, pass: cfg.CHPass, hc: &http.Client{Timeout: 120 * time.Second}},
		OS: &OpenSearch{url: cfg.OSURL, hc: &http.Client{Timeout: 120 * time.Second}},
		RD: redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 8}),
	}
	pc, err := pgxpool.ParseConfig(cfg.PGURL)
	if err != nil {
		return nil, err
	}
	pc.MaxConns = 8
	batas := time.Now().Add(4 * time.Minute)
	for {
		var e error
		if s.PG == nil {
			if p, err := pgxpool.NewWithConfig(ctx, pc); err == nil && p.Ping(ctx) == nil {
				s.PG = p
			} else {
				e = fmt.Errorf("postgres belum siap")
				if p != nil {
					p.Close()
				}
			}
		}
		if e == nil {
			e = s.RD.Ping(ctx).Err()
		}
		if e == nil {
			e = s.CH.Exec(ctx, "SELECT 1")
		}
		if e == nil {
			var h struct {
				Status string `json:"status"`
				Nodes  int    `json:"number_of_nodes"`
			}
			e = s.OS.Do(ctx, http.MethodGet, "/_cluster/health?wait_for_status=yellow&timeout=5s", nil, &h)
			if e == nil && h.Nodes < 2 {
				e = fmt.Errorf("opensearch baru %d node", h.Nodes)
			}
		}
		if e == nil {
			return s, nil
		}
		if time.Now().After(batas) {
			return nil, e
		}
		logInfo("menunggu store", "err", e.Error())
		time.Sleep(3 * time.Second)
	}
}
