package main

// Benchmark empat pola akses (slide P07) + lookup fitur, di setiap store yang masuk akal.
// Diukur dari API (satu klien, berurutan), dengan pemanasan, kunci acak, dan persentil p50/p95/p99.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"time"
)

type Hasil struct {
	Pola     string  `json:"pola"`
	Store    string  `json:"store"`
	Engine   string  `json:"engine"`
	N        int     `json:"n"`
	P50      float64 `json:"p50_ms"`
	P95      float64 `json:"p95_ms"`
	P99      float64 `json:"p99_ms"`
	Rata     float64 `json:"rata_ms"`
	OpsDetik float64 `json:"ops_per_detik"`
	Baca     string  `json:"baca,omitempty"`
	Query    string  `json:"query"`
	Catatan  string  `json:"catatan,omitempty"`
	Galat    string  `json:"galat,omitempty"`
}

// Persentil dengan metode nearest-rank pada data terurut.
func Persentil(urut []float64, p float64) float64 {
	if len(urut) == 0 {
		return 0
	}
	i := int(math.Ceil(p/100*float64(len(urut)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(urut) {
		i = len(urut) - 1
	}
	return urut[i]
}

func ukur(ctx context.Context, n, pemanasan int, fn func(context.Context, *rand.Rand) error) ([]float64, error) {
	r := rand.New(rand.NewSource(42))
	for i := 0; i < pemanasan; i++ {
		if err := fn(ctx, r); err != nil {
			return nil, err
		}
	}
	t := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		a := time.Now()
		if err := fn(ctx, r); err != nil {
			return nil, err
		}
		t = append(t, float64(time.Since(a).Microseconds())/1000)
	}
	sort.Float64s(t)
	return t, nil
}

func ringkas(h *Hasil, t []float64) {
	h.N = len(t)
	h.P50, h.P95, h.P99 = Persentil(t, 50), Persentil(t, 95), Persentil(t, 99)
	s := 0.0
	for _, x := range t {
		s += x
	}
	if len(t) > 0 {
		h.Rata = s / float64(len(t))
		h.OpsDetik = 1000 / h.Rata
	}
}

type uji struct {
	store, engine, query, catatan string
	fn                            func(context.Context, *rand.Rand) error
	baca                          func(context.Context) string
}

var pasanganKata = [][2]string{{"zakat", "wakaf"}, {"infak", "masjid"}, {"bayar", "zakat"}, {"tabungan", "haji"}, {"wakaf", "produktif"}, {"sedekah", "jumat"}}

func (a *App) daftarUji(pola string) []uji {
	s, d := a.s, a.d
	rek := func(r *rand.Rand) int64 { return d.Rekening[r.Intn(len(d.Rekening))].ID }
	nas := func(r *rand.Rand) int32 { return d.Nasabah[r.Intn(len(d.Nasabah))].ID }
	pgBaca := func(sql string, args ...any) func(context.Context) string {
		return func(ctx context.Context) string { return a.bacaPG(ctx, sql, args...) }
	}
	chBaca := func(sql string) func(context.Context) string {
		return func(ctx context.Context) string {
			rs, err := s.CH.QueryStat(ctx, sql)
			if err != nil {
				return ""
			}
			return fmt.Sprintf("%s baris, %s", rs.ReadRows, byteStr(rs.ReadBytes))
		}
	}
	dariAgregasi := d.Akhir.Add(-30 * 24 * time.Hour).UTC().Format("2006-01-02 15:04:05")
	switch pola {
	case "point":
		return []uji{
			{"PostgreSQL", "B-tree (PK)", "SELECT saldo FROM rekening WHERE id = $1", "",
				func(ctx context.Context, r *rand.Rand) error {
					var x int64
					return s.PG.QueryRow(ctx, "SELECT saldo FROM rekening WHERE id = $1", rek(r)).Scan(&x)
				}, pgBaca("SELECT saldo FROM rekening WHERE id = $1", d.Rekening[7].ID)},
			{"Redis", "hash table di memori", "GET saldo:<id>", "",
				func(ctx context.Context, r *rand.Rand) error {
					return s.RD.Get(ctx, fmt.Sprintf("saldo:%d", rek(r))).Err()
				}, nil},
			{"ClickHouse", "MergeTree (sparse index)", "SELECT saldo FROM rekening WHERE rekening_id = ?", "membaca satu granule (8.192 baris) untuk satu kunci",
				func(ctx context.Context, r *rand.Rand) error {
					_, err := s.CH.Query(ctx, fmt.Sprintf("SELECT saldo FROM rekening WHERE rekening_id = %d", rek(r)))
					return err
				}, chBaca(fmt.Sprintf("SELECT saldo FROM rekening WHERE rekening_id = %d", d.Rekening[7].ID))},
			{"OpenSearch", "GET dokumen per _id", "GET /rekening/_doc/<id>", "",
				func(ctx context.Context, r *rand.Rand) error {
					return s.OS.Do(ctx, http.MethodGet, fmt.Sprintf("/rekening/_doc/%d", rek(r)), nil, nil)
				}, nil},
		}
	case "range":
		q := "SELECT id, occurred_at, kanal, nominal FROM transaksi WHERE nasabah_id = $1 ORDER BY occurred_at DESC LIMIT 50"
		chq := "SELECT trx_id, waktu, kanal, nominal FROM %s WHERE nasabah_id = %d ORDER BY waktu DESC LIMIT 50"
		return []uji{
			{"PostgreSQL", "B-tree (nasabah_id, occurred_at)", q, "",
				func(ctx context.Context, r *rand.Rand) error {
					rows, err := s.PG.Query(ctx, q, nas(r))
					if err != nil {
						return err
					}
					for rows.Next() {
					}
					rows.Close()
					return rows.Err()
				}, pgBaca(q, int32(7))},
			{"Redis", "list per nasabah (sudah disiapkan)", "LRANGE riwayat:<nasabah> 0 49", "data disalin terlebih dulu; hanya cocok untuk query yang sudah diketahui",
				func(ctx context.Context, r *rand.Rand) error {
					return s.RD.LRange(ctx, fmt.Sprintf("riwayat:%d", nas(r)), 0, 49).Err()
				}, nil},
			{"ClickHouse", "ORDER BY (nasabah_id, waktu)", fmt.Sprintf(chq, "fakta_transaksi", 0), "",
				func(ctx context.Context, r *rand.Rand) error {
					_, err := s.CH.Query(ctx, fmt.Sprintf(chq, "fakta_transaksi", nas(r)))
					return err
				}, chBaca(fmt.Sprintf(chq, "fakta_transaksi", 7))},
			{"ClickHouse", "ORDER BY (cabang_id, waktu)", fmt.Sprintf(chq, "fakta_transaksi_cabang", 0), "ORDER BY tidak cocok dengan filter: semua granule dibaca",
				func(ctx context.Context, r *rand.Rand) error {
					_, err := s.CH.Query(ctx, fmt.Sprintf(chq, "fakta_transaksi_cabang", nas(r)))
					return err
				}, chBaca(fmt.Sprintf(chq, "fakta_transaksi_cabang", 7))},
			{"OpenSearch", "inverted index + doc values", "term nasabah_id, sort waktu desc, size 50", "",
				func(ctx context.Context, r *rand.Rand) error {
					return s.OS.Do(ctx, http.MethodPost, "/transaksi/_search", map[string]any{"size": 50,
						"query": map[string]any{"term": map[string]any{"nasabah_id": nas(r)}}, "sort": []any{map[string]string{"waktu": "desc"}}}, nil)
				}, nil},
		}
	case "agregasi":
		q := "SELECT cabang_id, count(*), sum(nominal) FROM transaksi WHERE occurred_at >= $1 GROUP BY cabang_id"
		chq := "SELECT cabang_id, count(), sum(nominal) FROM %s WHERE waktu >= '" + dariAgregasi + "' GROUP BY cabang_id"
		return []uji{
			{"PostgreSQL", "heap baris (seq scan)", q, "membaca semua kolom setiap baris",
				func(ctx context.Context, r *rand.Rand) error {
					rows, err := s.PG.Query(ctx, q, d.Akhir.Add(-30*24*time.Hour))
					if err != nil {
						return err
					}
					for rows.Next() {
					}
					rows.Close()
					return rows.Err()
				}, pgBaca(q, d.Akhir.Add(-30*24*time.Hour))},
			{"ClickHouse", "kolom + vectorized", fmt.Sprintf(chq, "fakta_transaksi_cabang"), "hanya kolom cabang_id, waktu, nominal yang dibaca",
				func(ctx context.Context, r *rand.Rand) error {
					_, err := s.CH.Query(ctx, fmt.Sprintf(chq, "fakta_transaksi_cabang"))
					return err
				}, chBaca(fmt.Sprintf(chq, "fakta_transaksi_cabang"))},
			{"OpenSearch", "aggregation doc values", "range waktu + terms cabang_id + sum nominal", "",
				func(ctx context.Context, r *rand.Rand) error {
					return s.OS.Do(ctx, http.MethodPost, "/transaksi/_search?request_cache=false", map[string]any{"size": 0,
						"query": map[string]any{"range": map[string]any{"waktu": map[string]any{"gte": d.Akhir.Add(-30 * 24 * time.Hour).Format(time.RFC3339)}}},
						"aggs": map[string]any{"cabang": map[string]any{"terms": map[string]any{"field": "cabang_id", "size": 10},
							"aggs": map[string]any{"total": map[string]any{"sum": map[string]string{"field": "nominal"}}}}}}, nil)
				}, nil},
			{"Redis", "-", "(tidak ada)", "tidak masuk akal: key-value tidak punya aggregation lintas key", nil, nil},
		}
	case "teks":
		qIlike := "SELECT count(*) FROM transaksi WHERE keterangan ILIKE $1 AND keterangan ILIKE $2"
		qFTS := "SELECT count(*) FROM transaksi WHERE to_tsvector('simple', keterangan) @@ to_tsquery('simple', $1)"
		return []uji{
			{"PostgreSQL", "ILIKE (seq scan)", qIlike, "memindai semua baris",
				func(ctx context.Context, r *rand.Rand) error {
					k := pasanganKata[r.Intn(len(pasanganKata))]
					var n int64
					return s.PG.QueryRow(ctx, qIlike, "%"+k[0]+"%", "%"+k[1]+"%").Scan(&n)
				}, pgBaca(qIlike, "%zakat%", "%wakaf%")},
			{"PostgreSQL", "GIN full-text (inverted index)", qFTS, "PostgreSQL berekstensi: inverted index juga ada di Postgres",
				func(ctx context.Context, r *rand.Rand) error {
					k := pasanganKata[r.Intn(len(pasanganKata))]
					var n int64
					return s.PG.QueryRow(ctx, qFTS, k[0]+" & "+k[1]).Scan(&n)
				}, pgBaca(qFTS, "zakat & wakaf")},
			{"ClickHouse", "hasToken (scan kolom)", "SELECT count() FROM fakta_transaksi WHERE hasToken(keterangan,'a') AND hasToken(keterangan,'b')", "tanpa skip index teks: kolom keterangan dipindai",
				func(ctx context.Context, r *rand.Rand) error {
					k := pasanganKata[r.Intn(len(pasanganKata))]
					_, err := s.CH.Query(ctx, fmt.Sprintf("SELECT count() FROM fakta_transaksi WHERE hasToken(keterangan,'%s') AND hasToken(keterangan,'%s')", k[0], k[1]))
					return err
				}, chBaca("SELECT count() FROM fakta_transaksi WHERE hasToken(keterangan,'zakat') AND hasToken(keterangan,'wakaf')")},
			{"OpenSearch", "inverted index (2 shard × 2 node)", "match keterangan operator=and", "",
				func(ctx context.Context, r *rand.Rand) error {
					k := pasanganKata[r.Intn(len(pasanganKata))]
					return s.OS.Do(ctx, http.MethodPost, "/transaksi/_count", map[string]any{"query": map[string]any{
						"match": map[string]any{"keterangan": map[string]any{"query": k[0] + " " + k[1], "operator": "and"}}}}, nil)
				}, nil},
			{"Redis", "-", "(tidak ada)", "tidak masuk akal tanpa modul pencarian", nil, nil},
		}
	case "fitur":
		return []uji{
			{"Redis", "hash per nasabah", "HGETALL fitur:<nasabah>", "serving store untuk hot path skor fraud/AI",
				func(ctx context.Context, r *rand.Rand) error {
					return s.RD.HGetAll(ctx, fmt.Sprintf("fitur:%d", nas(r))).Err()
				}, nil},
			{"PostgreSQL", "B-tree (PK)", "SELECT * FROM fitur_nasabah WHERE nasabah_id = $1", "",
				func(ctx context.Context, r *rand.Rand) error {
					rows, err := s.PG.Query(ctx, "SELECT * FROM fitur_nasabah WHERE nasabah_id = $1", nas(r))
					if err != nil {
						return err
					}
					for rows.Next() {
					}
					rows.Close()
					return rows.Err()
				}, nil},
			{"ClickHouse", "tabel fitur (ORDER BY nasabah_id)", "SELECT * FROM fitur_nasabah WHERE nasabah_id = ?", "",
				func(ctx context.Context, r *rand.Rand) error {
					_, err := s.CH.Query(ctx, fmt.Sprintf("SELECT * FROM fitur_nasabah WHERE nasabah_id = %d", nas(r)))
					return err
				}, nil},
			{"ClickHouse", "hitung saat diminta dari fakta", "SELECT count(), sum(nominal) ... FROM fakta_transaksi WHERE nasabah_id = ? AND waktu >= now()-30d", "tanpa materialisasi fitur",
				func(ctx context.Context, r *rand.Rand) error {
					_, err := s.CH.Query(ctx, fmt.Sprintf(sqlFiturLangsung, nas(r)))
					return err
				}, nil},
		}
	}
	return nil
}

func byteStr(s string) string {
	var n float64
	_, _ = fmt.Sscan(s, &n)
	u := []string{"B", "KB", "MB", "GB"}
	i := 0
	for n >= 1024 && i < 3 {
		n /= 1024
		i++
	}
	return fmt.Sprintf("%.1f %s", n, u[i])
}

// bacaPG: page (8 KB) yang disentuh menurut EXPLAIN (ANALYZE, BUFFERS).
func (a *App) bacaPG(ctx context.Context, sql string, args ...any) string {
	var plan string
	if err := a.s.PG.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+sql, args...).Scan(&plan); err != nil {
		return ""
	}
	var p []struct {
		Plan map[string]any `json:"Plan"`
	}
	if json.Unmarshal([]byte(plan), &p) != nil || len(p) == 0 {
		return ""
	}
	hit, read := num(p[0].Plan["Shared Hit Blocks"]), num(p[0].Plan["Shared Read Blocks"])
	jenis := fmt.Sprint(p[0].Plan["Node Type"])
	return fmt.Sprintf("%.0f page (%s), %s", hit+read, byteStr(fmt.Sprint((hit+read)*8192)), jenis)
}

func num(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}

func (a *App) Bench(ctx context.Context, pola string, n int) []Hasil {
	polaList := []string{pola}
	if pola == "semua" {
		polaList = []string{"point", "range", "agregasi", "teks", "fitur"}
	}
	var out []Hasil
	for _, p := range polaList {
		nn, warm := n, 20
		if p == "agregasi" || p == "teks" {
			nn, warm = max(10, n/10), 3
		}
		for _, u := range a.daftarUji(p) {
			h := Hasil{Pola: p, Store: u.store, Engine: u.engine, Query: u.query, Catatan: u.catatan}
			if u.fn == nil {
				out = append(out, h)
				continue
			}
			t, err := ukur(ctx, nn, warm, u.fn)
			if err != nil {
				h.Galat = err.Error()
			} else {
				ringkas(&h, t)
			}
			if u.baca != nil {
				h.Baca = u.baca(ctx)
			}
			out = append(out, h)
			logInfo("bench", "pola", p, "store", u.store, "engine", u.engine, "p50_ms", h.P50, "p95_ms", h.P95)
		}
	}
	return out
}
