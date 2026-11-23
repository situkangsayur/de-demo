package main

// Eksperimen storage engine: tulis satu baris per operasi ke tiap store dan ukur amplifikasi tulis.
//   PostgreSQL (B-tree + heap + WAL): byte WAL per baris.
//   ClickHouse (MergeTree, keluarga LSM): setiap INSERT = part baru; merge di latar menulis ulang data.
//   Redis (AOF): log append-only, byte AOF per operasi.
//   OpenSearch (Lucene): segment immutable + merge, mirip LSM.

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

type HasilTulis struct {
	Store       string             `json:"store"`
	N           int                `json:"n"`
	Ms          float64            `json:"ms"`
	OpsDetik    float64            `json:"ops_per_detik"`
	ByteLogis   int64              `json:"byte_logis"`
	Metrik      map[string]float64 `json:"metrik"`
	Amplifikasi float64            `json:"amplifikasi_tulis"`
	Penjelasan  string             `json:"penjelasan"`
}

func ketAcak(r *rand.Rand) string { return keterangan[r.Intn(len(keterangan))] }

func (a *App) Tulis(ctx context.Context, store string, n int) (HasilTulis, error) {
	s := a.s
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	h := HasilTulis{Store: store, N: n, Metrik: map[string]float64{}}
	// byte logis per baris: rekening_id (8) + nominal (8) + waktu (8) + keterangan
	baris := make([][3]any, n)
	for i := range baris {
		k := ketAcak(r)
		baris[i] = [3]any{a.d.Rekening[r.Intn(len(a.d.Rekening))].ID, int64(10_000 * (1 + r.Intn(500))), k}
		h.ByteLogis += int64(24 + len(k))
	}
	t := time.Now()
	switch store {
	case "postgres":
		var lsn0, lsn1 string
		var uk0, uk1 int64
		_ = s.PG.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text, pg_total_relation_size('tulis_uji')").Scan(&lsn0, &uk0)
		t = time.Now()
		for _, b := range baris { // autocommit: setiap INSERT satu transaksi, WAL di-flush saat commit
			if _, err := s.PG.Exec(ctx, "INSERT INTO tulis_uji (rekening_id, nominal, keterangan) VALUES ($1, $2, $3)", b[0], b[1], b[2]); err != nil {
				return h, err
			}
		}
		h.Ms = ms(t)
		var wal float64
		_ = s.PG.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text, pg_total_relation_size('tulis_uji'), pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)", lsn0).Scan(&lsn1, &uk1, &wal)
		h.Metrik["byte_wal"] = wal
		h.Metrik["byte_wal_per_baris"] = wal / float64(n)
		h.Metrik["tabel_indeks_bertambah_byte"] = float64(uk1 - uk0)
		h.Amplifikasi = wal / float64(h.ByteLogis)
		h.Penjelasan = "Setiap INSERT menulis record WAL untuk heap dan untuk setiap indeks (PK + tulis_uji_rek), ditambah full-page image saat page pertama kali diubah sesudah checkpoint. Page heap/indeks 8 KB ditulis ulang di tempat (update in place)."
	case "clickhouse":
		_ = s.CH.Exec(ctx, "SYSTEM FLUSH LOGS")
		mulai := time.Now().UTC().Add(-time.Second).Format("2006-01-02 15:04:05")
		t = time.Now()
		for i, b := range baris { // anti-pattern yang disengaja: satu INSERT per baris = satu part per baris
			sql := fmt.Sprintf("INSERT INTO tulis_uji VALUES (%d, %d, %d, now64(3), '%s')", time.Now().UnixNano()+int64(i), b[0], b[1], b[2])
			if err := s.CH.Exec(ctx, sql); err != nil {
				return h, err
			}
		}
		h.Ms = ms(t)
		time.Sleep(1500 * time.Millisecond)
		_ = s.CH.Exec(ctx, "SYSTEM FLUSH LOGS")
		rows, err := s.CH.Query(ctx, "SELECT countIf(event_type='NewPart') AS part_baru, countIf(event_type='MergeParts') AS merge, "+
			"sumIf(size_in_bytes, event_type='NewPart') AS byte_part_baru, sumIf(size_in_bytes, event_type='MergeParts') AS byte_merge "+
			"FROM system.part_log WHERE database=currentDatabase() AND table='tulis_uji' AND event_time >= '"+mulai+"'")
		if err == nil && len(rows) == 1 {
			for k, v := range rows[0] {
				h.Metrik[k] = num(v)
			}
		}
		aktif, _ := s.CH.Query(ctx, "SELECT count() AS n FROM system.parts WHERE database=currentDatabase() AND table='tulis_uji' AND active")
		if len(aktif) == 1 {
			h.Metrik["part_aktif_sekarang"] = num(aktif[0]["n"])
		}
		if b := h.Metrik["byte_part_baru"]; b > 0 {
			h.Amplifikasi = (b + h.Metrik["byte_merge"]) / b
		}
		h.Penjelasan = "Setiap INSERT membuat part (direktori berisi berkas per kolom). Merge di latar menggabungkan part kecil menjadi besar dan menulis ulang datanya, mirip compaction LSM. Amplifikasi = (byte part baru + byte hasil merge) / byte part baru. Praktik benar: INSERT batch ribuan baris atau async_insert."
	case "redis":
		info0, _ := s.RD.Info(ctx, "persistence").Result()
		t = time.Now()
		for i, b := range baris {
			if err := s.RD.Set(ctx, fmt.Sprintf("tulis:%d:%d", time.Now().UnixNano(), i), fmt.Sprintf("%v|%v|%v", b[0], b[1], b[2]), time.Hour).Err(); err != nil {
				return h, err
			}
		}
		h.Ms = ms(t)
		time.Sleep(1100 * time.Millisecond) // appendfsync everysec
		info1, _ := s.RD.Info(ctx, "persistence").Result()
		aof := infoNum(info1, "aof_current_size") - infoNum(info0, "aof_current_size")
		h.Metrik["byte_aof"] = aof
		h.Metrik["byte_aof_per_operasi"] = aof / float64(n)
		h.Amplifikasi = aof / float64(h.ByteLogis)
		h.Penjelasan = "Data hidup di memori (hash table). Ketahanan dari AOF: setiap perintah ditambahkan di ujung log (append-only, ditulis berurutan), di-fsync tiap detik. AOF sesekali ditulis ulang (rewrite) agar tidak membesar."
	case "opensearch":
		_ = s.OS.Do(ctx, http.MethodPut, "/tulis_uji", map[string]any{"settings": map[string]any{"number_of_shards": 1, "number_of_replicas": 1, "refresh_interval": "200ms"}}, nil)
		seg0, byte0 := a.segmen(ctx), a.ukuranIndeks(ctx)
		t = time.Now()
		for _, b := range baris {
			if err := s.OS.Do(ctx, http.MethodPost, "/tulis_uji/_doc", map[string]any{"rekening_id": b[0], "nominal": b[1], "keterangan": b[2], "waktu": time.Now().UTC()}, nil); err != nil {
				return h, err
			}
		}
		h.Ms = ms(t)
		time.Sleep(time.Second)
		h.Metrik["segment_sebelum"] = seg0
		h.Metrik["segment_sesudah"] = a.segmen(ctx)
		h.Metrik["byte_store_translog"] = a.ukuranIndeks(ctx) - byte0
		h.Amplifikasi = h.Metrik["byte_store_translog"] / float64(h.ByteLogis)
		h.Penjelasan = "Lucene menulis dokumen ke buffer memori, lalu setiap refresh membentuk segment baru yang immutable; segment kecil digabung (merge) di latar. Strukturnya keluarga LSM. Replica menulis ulang dokumen yang sama di node kedua; translog (WAL Lucene) ikut dihitung."
	default:
		return h, fmt.Errorf("store tidak dikenal: %s", store)
	}
	if h.Ms > 0 {
		h.OpsDetik = float64(n) / (h.Ms / 1000)
	}
	logInfo("eksperimen tulis", "store", store, "n", n, "ms", h.Ms, "amplifikasi", h.Amplifikasi)
	return h, nil
}

func infoNum(info, kunci string) float64 {
	for _, l := range strings.Split(info, "\n") {
		if strings.HasPrefix(l, kunci+":") {
			var f float64
			_, _ = fmt.Sscan(strings.TrimSpace(strings.TrimPrefix(l, kunci+":")), &f)
			return f
		}
	}
	return 0
}

func (a *App) segmen(ctx context.Context) float64 {
	var out []map[string]any
	if err := a.s.OS.Do(ctx, http.MethodGet, "/_cat/segments/tulis_uji?format=json", nil, &out); err != nil {
		return 0
	}
	return float64(len(out))
}

// ukuranIndeks: byte segment + translog index tulis_uji di semua shard (primary dan replica).
func (a *App) ukuranIndeks(ctx context.Context) float64 {
	var st struct {
		All struct {
			Total struct {
				Store struct {
					Size float64 `json:"size_in_bytes"`
				} `json:"store"`
				Translog struct {
					Size float64 `json:"size_in_bytes"`
				} `json:"translog"`
			} `json:"total"`
		} `json:"_all"`
	}
	if err := a.s.OS.Do(ctx, http.MethodGet, "/tulis_uji/_stats/store,translog", nil, &st); err != nil {
		return 0
	}
	return st.All.Total.Store.Size + st.All.Total.Translog.Size
}

// Sampel jumlah part aktif ClickHouse dan segment OpenSearch (UI memanggil berulang untuk grafik merge).
func (a *App) Sampel(ctx context.Context) map[string]any {
	out := map[string]any{"t": time.Now().UnixMilli()}
	if r, err := a.s.CH.Query(ctx, "SELECT count() AS part, sum(rows) AS baris FROM system.parts WHERE database=currentDatabase() AND table='tulis_uji' AND active"); err == nil && len(r) == 1 {
		out["ch_part"], out["ch_baris"] = num(r[0]["part"]), num(r[0]["baris"])
	}
	if r, err := a.s.CH.Query(ctx, "SELECT count() AS n FROM system.merges WHERE database=currentDatabase() AND table='tulis_uji'"); err == nil && len(r) == 1 {
		out["ch_merge_berjalan"] = num(r[0]["n"])
	}
	out["os_segment"] = a.segmen(ctx)
	return out
}

func (a *App) ResetTulis(ctx context.Context) error {
	if _, err := a.s.PG.Exec(ctx, "TRUNCATE tulis_uji"); err != nil {
		return err
	}
	if err := a.s.CH.Exec(ctx, "TRUNCATE TABLE tulis_uji"); err != nil {
		return err
	}
	_ = a.s.OS.Do(ctx, http.MethodDelete, "/tulis_uji", nil, nil)
	keys, _ := a.s.RD.Keys(ctx, "tulis:*").Result()
	if len(keys) > 0 {
		_ = a.s.RD.Del(ctx, keys...).Err()
	}
	return nil
}

func (a *App) Optimize(ctx context.Context) error {
	return a.s.CH.Exec(ctx, "OPTIMIZE TABLE tulis_uji FINAL")
}

// BTree: kedalaman dan ukuran indeks B-tree PostgreSQL (pageinspect.bt_metap) dan page yang dibaca satu lookup.
func (a *App) BTree(ctx context.Context) ([]map[string]any, error) {
	var out []map[string]any
	for _, idx := range []string{"rekening_pkey", "transaksi_pkey", "trx_nasabah_waktu"} {
		var level, root int64
		var halaman, tuple int64
		err := a.s.PG.QueryRow(ctx, `SELECT m.level, m.root, pg_relation_size($1::regclass)/8192, c.reltuples::bigint
			FROM bt_metap($1) m, pg_class c WHERE c.oid = $1::regclass`, idx).Scan(&level, &root, &halaman, &tuple)
		if err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"indeks": idx, "level_akar": level, "kedalaman": level + 1, "page": halaman, "tuple": tuple,
			"kunci_per_page": float64(tuple) / float64(max(halaman, 1))})
	}
	return out, nil
}
