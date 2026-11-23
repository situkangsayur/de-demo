package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type App struct {
	ch      *ClickHouse
	lake    *Lake
	airflow *Airflow
}

// Pertanyaan direksi (slide P08, jalur A). SQL ditulis untuk dua dialek; sebagian besar sama
// karena keduanya mengikuti SQL standar. Tabel bernama sama di kedua sumber: gold.<tabel>.
type Pertanyaan struct {
	No        int    `json:"no"`
	Judul     string `json:"judul"`
	Penjelas  string `json:"penjelas"`
	Warehouse string `json:"sql_warehouse"`
	Lake      string `json:"sql_lake"`
}

var daftarPertanyaan = []Pertanyaan{
	{
		No:       1,
		Judul:    "Nominal dan jumlah transaksi kemarin per cabang dan per kanal",
		Penjelas: "Grain fakta = satu transaksi; filter satu partisi tgl, dikelompokkan per cabang dan kanal.",
		Warehouse: `SELECT c.nama AS cabang, f.kanal_key AS kanal, count() AS jumlah_trx, sum(f.nominal) AS total_nominal
FROM gold.fakta_transaksi f
LEFT JOIN gold.dim_cabang c ON c.cabang_key = f.cabang_key
WHERE f.tgl = toDate('{tgl}')
GROUP BY cabang, kanal ORDER BY cabang, kanal`,
		Lake: `SELECT c.nama AS cabang, f.kanal_key AS kanal, count(*) AS jumlah_trx, sum(f.nominal) AS total_nominal
FROM gold.fakta_transaksi f
LEFT JOIN gold.dim_cabang c ON c.cabang_key = f.cabang_key
WHERE f.tgl = DATE '{tgl}'
GROUP BY cabang, kanal ORDER BY cabang, kanal`,
	},
	{
		No:       2,
		Judul:    "Pembiayaan cair per jenis akad, dan tren 30 hari",
		Penjelas: "fakta_pembiayaan (grain satu akad cair) x dim_akad, 30 hari s.d. tanggal acuan.",
		Warehouse: `SELECT p.tgl_cair AS tgl, a.nama_akad AS akad, count() AS jumlah_akad, sum(p.plafon) AS total_plafon
FROM gold.fakta_pembiayaan p
JOIN gold.dim_akad a ON a.akad_key = p.akad_key
WHERE p.tgl_cair BETWEEN toDate('{tgl}') - 29 AND toDate('{tgl}')
GROUP BY tgl, akad ORDER BY tgl, akad`,
		Lake: `SELECT p.tgl_cair AS tgl, a.nama_akad AS akad, count(*) AS jumlah_akad, sum(p.plafon) AS total_plafon
FROM gold.fakta_pembiayaan p
JOIN gold.dim_akad a ON a.akad_key = p.akad_key
WHERE p.tgl_cair BETWEEN DATE '{tgl}' - 29 AND DATE '{tgl}'
GROUP BY tgl, akad ORDER BY tgl, akad`,
	},
	{
		No:       3,
		Judul:    "Nasabah baru yang belum bertransaksi dalam 14 hari",
		Penjelas: "Nasabah (versi aktif SCD-2) yang mendaftar dalam 14 hari terakhir s.d. tanggal acuan dan belum punya satu pun transaksi.",
		Warehouse: `SELECT n.nasabah_id, n.nama_samaran, c.nama AS cabang, n.segmen, n.tgl_daftar,
       date_diff('day', n.tgl_daftar, toDate('{tgl}')) AS hari_sejak_daftar
FROM gold.dim_nasabah n
LEFT JOIN gold.dim_cabang c ON c.cabang_key = n.cabang_id
WHERE n.is_current AND n.tgl_daftar BETWEEN toDate('{tgl}') - 13 AND toDate('{tgl}')
  AND n.nasabah_id NOT IN (SELECT nasabah_id FROM gold.fakta_transaksi
                           WHERE tgl <= toDate('{tgl}') AND nasabah_id IS NOT NULL)
ORDER BY n.tgl_daftar, n.nasabah_id`,
		Lake: `SELECT n.nasabah_id, n.nama_samaran, c.nama AS cabang, n.segmen, n.tgl_daftar,
       date_diff('day', n.tgl_daftar, DATE '{tgl}') AS hari_sejak_daftar
FROM gold.dim_nasabah n
LEFT JOIN gold.dim_cabang c ON c.cabang_key = n.cabang_id
WHERE n.is_current AND n.tgl_daftar BETWEEN DATE '{tgl}' - 13 AND DATE '{tgl}'
  AND n.nasabah_id NOT IN (SELECT nasabah_id FROM gold.fakta_transaksi
                           WHERE tgl <= DATE '{tgl}' AND nasabah_id IS NOT NULL)
ORDER BY n.tgl_daftar, n.nasabah_id`,
	},
	{
		No:       4,
		Judul:    "Cabang dengan lonjakan transaksi tidak wajar dibanding rata-rata 4 pekan",
		Penjelas: "Jumlah transaksi pada tanggal acuan dibanding rata-rata harian 28 hari sebelumnya; lonjakan bila rasio >= 1,5 dan >= 30 transaksi.",
		Warehouse: `WITH h AS (SELECT cabang_key, count() AS n FROM gold.fakta_transaksi
           WHERE tgl = toDate('{tgl}') GROUP BY cabang_key),
     b AS (SELECT cabang_key, count() / 28 AS rata FROM gold.fakta_transaksi
           WHERE tgl BETWEEN toDate('{tgl}') - 28 AND toDate('{tgl}') - 1 GROUP BY cabang_key)
SELECT c.nama AS cabang, coalesce(h.n, 0) AS trx_tgl_acuan, round(coalesce(b.rata, 0), 2) AS rata_harian_4_pekan,
       round(coalesce(h.n, 0) / nullIf(coalesce(b.rata, 0), 0), 2) AS rasio,
       coalesce(h.n, 0) >= 1.5 * coalesce(b.rata, 0) AND coalesce(h.n, 0) >= 30 AS lonjakan
FROM gold.dim_cabang c
LEFT JOIN h ON h.cabang_key = c.cabang_key
LEFT JOIN b ON b.cabang_key = c.cabang_key
ORDER BY rasio DESC NULLS LAST, cabang`,
		Lake: `WITH h AS (SELECT cabang_key, count(*) AS n FROM gold.fakta_transaksi
           WHERE tgl = DATE '{tgl}' GROUP BY cabang_key),
     b AS (SELECT cabang_key, count(*) / 28 AS rata FROM gold.fakta_transaksi
           WHERE tgl BETWEEN DATE '{tgl}' - 28 AND DATE '{tgl}' - 1 GROUP BY cabang_key)
SELECT c.nama AS cabang, coalesce(h.n, 0) AS trx_tgl_acuan, round(coalesce(b.rata, 0), 2) AS rata_harian_4_pekan,
       round(coalesce(h.n, 0) / nullif(coalesce(b.rata, 0), 0), 2) AS rasio,
       coalesce(h.n, 0) >= 1.5 * coalesce(b.rata, 0) AND coalesce(h.n, 0) >= 30 AS lonjakan
FROM gold.dim_cabang c
LEFT JOIN h ON h.cabang_key = c.cabang_key
LEFT JOIN b ON b.cabang_key = c.cabang_key
ORDER BY rasio DESC NULLS LAST, cabang`,
	},
}

var reTgl = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// tglAcuan: parameter ?tgl=, bawaan kemarin (WIB).
func tglAcuan(r *http.Request) (string, error) {
	t := r.URL.Query().Get("tgl")
	if t == "" {
		wib := time.FixedZone("WIB", 7*3600)
		return time.Now().In(wib).AddDate(0, 0, -1).Format("2006-01-02"), nil
	}
	if !reTgl.MatchString(t) {
		return "", fmt.Errorf("tgl harus YYYY-MM-DD")
	}
	return t, nil
}

func ambilPertanyaan(r *http.Request) (Pertanyaan, error) {
	no, err := strconv.Atoi(r.PathValue("no"))
	if err != nil || no < 1 || no > len(daftarPertanyaan) {
		return Pertanyaan{}, fmt.Errorf("pertanyaan 1..%d", len(daftarPertanyaan))
	}
	return daftarPertanyaan[no-1], nil
}

func (a *App) jawab(ctx context.Context, p Pertanyaan, sumber, tgl string) *Hasil {
	var h *Hasil
	var err error
	if sumber == "lake" {
		h, err = a.lake.Query(ctx, strings.ReplaceAll(p.Lake, "{tgl}", tgl))
	} else {
		h, err = a.ch.Query(ctx, strings.ReplaceAll(p.Warehouse, "{tgl}", tgl))
	}
	if err != nil {
		return &Hasil{Sumber: sumber, Error: err.Error(), Kolom: []string{}, Baris: [][]any{}}
	}
	return h
}

func (a *App) handleDaftarPertanyaan(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, daftarPertanyaan)
}

func (a *App) handlePertanyaan(w http.ResponseWriter, r *http.Request) {
	p, err := ambilPertanyaan(r)
	if err != nil {
		writeErr(w, r, http.StatusBadRequest, err)
		return
	}
	tgl, err := tglAcuan(r)
	if err != nil {
		writeErr(w, r, http.StatusBadRequest, err)
		return
	}
	sumber := r.URL.Query().Get("sumber")
	if sumber != "lake" {
		sumber = "warehouse"
	}
	h := a.jawab(r.Context(), p, sumber, tgl)
	code := http.StatusOK
	if h.Error != "" {
		code = http.StatusBadGateway
	}
	writeJSON(w, code, map[string]any{"pertanyaan": p, "tgl": tgl, "hasil": h})
}

// handleBandingkan menjalankan pertanyaan yang sama di kedua sumber secara paralel, lalu
// membandingkan isi (setelah normalisasi angka) dan latency.
func (a *App) handleBandingkan(w http.ResponseWriter, r *http.Request) {
	p, err := ambilPertanyaan(r)
	if err != nil {
		writeErr(w, r, http.StatusBadRequest, err)
		return
	}
	tgl, err := tglAcuan(r)
	if err != nil {
		writeErr(w, r, http.StatusBadRequest, err)
		return
	}
	var wh, lk *Hasil
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); wh = a.jawab(r.Context(), p, "warehouse", tgl) }()
	go func() { defer wg.Done(); lk = a.jawab(r.Context(), p, "lake", tgl) }()
	wg.Wait()
	sama, beda := bandingkan(wh, lk)
	writeJSON(w, http.StatusOK, map[string]any{
		"pertanyaan": p, "tgl": tgl, "warehouse": wh, "lake": lk,
		"identik": sama && wh.Error == "" && lk.Error == "", "selisih": beda,
	})
}

// normal mengubah sel menjadi string yang sebanding lintas mesin: angka dibulatkan 2 desimal,
// boolean menjadi 1/0, tanggal tanpa jam.
func normal(v any) string {
	switch x := v.(type) {
	case nil:
		return "∅"
	case bool: // ClickHouse mengembalikan boolean sebagai UInt8 (1/0), DuckDB sebagai true/false
		if x {
			return "1.00"
		}
		return "0.00"
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return x.String()
		}
		return strconv.FormatFloat(math.Round(f*100)/100, 'f', 2, 64)
	case float64:
		return strconv.FormatFloat(math.Round(x*100)/100, 'f', 2, 64)
	case string:
		if len(x) >= 10 && reTgl.MatchString(x[:10]) && (len(x) == 10 || strings.HasSuffix(x, " 00:00:00") || strings.HasSuffix(x, "T00:00:00")) {
			return x[:10]
		}
		if f, err := strconv.ParseFloat(x, 64); err == nil && !reTgl.MatchString(x) {
			return strconv.FormatFloat(math.Round(f*100)/100, 'f', 2, 64)
		}
		return x
	default:
		return fmt.Sprint(x)
	}
}

func baris(h *Hasil) []string {
	out := make([]string, 0, len(h.Baris))
	for _, r := range h.Baris {
		s := make([]string, len(r))
		for i, v := range r {
			s[i] = normal(v)
		}
		out = append(out, strings.Join(s, " | "))
	}
	sort.Strings(out)
	return out
}

func bandingkan(a, b *Hasil) (bool, []string) {
	x, y := baris(a), baris(b)
	var beda []string
	setY := map[string]int{}
	for _, s := range y {
		setY[s]++
	}
	for _, s := range x {
		if setY[s] > 0 {
			setY[s]--
		} else {
			beda = append(beda, "hanya di warehouse: "+s)
		}
	}
	for s, n := range setY {
		for i := 0; i < n; i++ {
			beda = append(beda, "hanya di lake: "+s)
		}
	}
	if len(beda) > 20 {
		beda = beda[:20]
	}
	return len(beda) == 0, beda
}
