package main

// Search: cluster OpenSearch 2 node (shard dan replica), analyzer (token = kunci inverted index),
// posting list per token, dan perbandingan dengan PostgreSQL ILIKE / GIN.

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (a *App) Cluster(ctx context.Context) (map[string]any, error) {
	var health map[string]any
	var nodes, shards []map[string]any
	if err := a.s.OS.Do(ctx, http.MethodGet, "/_cluster/health", nil, &health); err != nil {
		return nil, err
	}
	if err := a.s.OS.Do(ctx, http.MethodGet, "/_cat/nodes?format=json&h=name,heap.percent,ram.percent,node.role,cluster_manager", nil, &nodes); err != nil {
		return nil, err
	}
	if err := a.s.OS.Do(ctx, http.MethodGet, "/_cat/shards/transaksi,rekening?format=json&h=index,shard,prirep,state,docs,store,node", nil, &shards); err != nil {
		return nil, err
	}
	var disk map[string]any
	_ = a.s.OS.Do(ctx, http.MethodGet, "/_cluster/settings?include_defaults=true&filter_path=*.cluster.routing.allocation.disk.threshold_enabled", nil, &disk)
	return map[string]any{"health": health, "nodes": nodes, "shards": shards, "disk_threshold": disk}, nil
}

type Token struct {
	Token   string `json:"token"`
	Posting int64  `json:"posting"` // jumlah dokumen yang memuat token
}

func (a *App) Cari(ctx context.Context, q string) (map[string]any, error) {
	s := a.s
	var an struct {
		Tokens []struct {
			Token string `json:"token"`
		} `json:"tokens"`
	}
	if err := s.OS.Do(ctx, http.MethodPost, "/transaksi/_analyze", map[string]any{"field": "keterangan", "text": q}, &an); err != nil {
		return nil, err
	}
	var tokens []Token
	for _, t := range an.Tokens {
		var c struct {
			Count int64 `json:"count"`
		}
		_ = s.OS.Do(ctx, http.MethodPost, "/transaksi/_count", map[string]any{"query": map[string]any{"term": map[string]any{"keterangan": t.Token}}}, &c)
		tokens = append(tokens, Token{t.Token, c.Count})
	}
	t := time.Now()
	var res struct {
		Took int64 `json:"took"`
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				ID        string              `json:"_id"`
				Score     float64             `json:"_score"`
				Source    map[string]any      `json:"_source"`
				Highlight map[string][]string `json:"highlight"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := s.OS.Do(ctx, http.MethodPost, "/transaksi/_search?track_total_hits=true", map[string]any{"size": 8,
		"query":     map[string]any{"match": map[string]any{"keterangan": map[string]any{"query": q, "operator": "and"}}},
		"highlight": map[string]any{"fields": map[string]any{"keterangan": map[string]any{}}}}, &res); err != nil {
		return nil, err
	}
	osMs := ms(t)
	kata := strings.Fields(strings.ToLower(q))
	var hasilPG []map[string]any
	if len(kata) > 0 {
		var conds []string
		var args []any
		for i, k := range kata {
			conds = append(conds, "keterangan ILIKE $"+strconv.Itoa(i+1))
			args = append(args, "%"+k+"%")
		}
		var n int64
		t = time.Now()
		if err := s.PG.QueryRow(ctx, "SELECT count(*) FROM transaksi WHERE "+strings.Join(conds, " AND "), args...).Scan(&n); err == nil {
			hasilPG = append(hasilPG, map[string]any{"cara": "PostgreSQL ILIKE (seq scan)", "jumlah": n, "ms": ms(t)})
		}
		t = time.Now()
		if err := s.PG.QueryRow(ctx, "SELECT count(*) FROM transaksi WHERE to_tsvector('simple', keterangan) @@ plainto_tsquery('simple', $1)", q).Scan(&n); err == nil {
			hasilPG = append(hasilPG, map[string]any{"cara": "PostgreSQL GIN full-text", "jumlah": n, "ms": ms(t)})
		}
	}
	return map[string]any{"q": q, "token": tokens, "opensearch": map[string]any{"took_ms": res.Took, "ms_klien": osMs,
		"total": res.Hits.Total.Value, "hits": res.Hits.Hits}, "pembanding": hasilPG}, nil
}
