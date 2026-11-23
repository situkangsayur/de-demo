package main

// Row vs columnar: ukuran per kolom dan byte yang dibaca query yang sama di PostgreSQL (heap baris)
// dan ClickHouse (berkas per kolom, terkompresi).

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

func (a *App) Ukuran(ctx context.Context) (map[string]any, error) {
	pg := map[string]any{}
	var heap, idx, total, baris int64
	if err := a.s.PG.QueryRow(ctx, `SELECT pg_relation_size('transaksi'), pg_indexes_size('transaksi'),
		pg_total_relation_size('transaksi'), (SELECT reltuples::bigint FROM pg_class WHERE relname='transaksi')`).Scan(&heap, &idx, &total, &baris); err != nil {
		return nil, err
	}
	pg["heap_byte"], pg["indeks_byte"], pg["total_byte"], pg["baris"] = heap, idx, total, baris
	rows, err := a.s.PG.Query(ctx, "SELECT attname::text, avg_width FROM pg_stats WHERE tablename='transaksi' ORDER BY attname")
	if err != nil {
		return nil, err
	}
	var kol []map[string]any
	for rows.Next() {
		var n string
		var w int32
		if err := rows.Scan(&n, &w); err != nil {
			return nil, err
		}
		kol = append(kol, map[string]any{"kolom": n, "lebar_rata_byte": w})
	}
	rows.Close()
	pg["kolom"] = kol
	ch, err := a.s.CH.Query(ctx, `SELECT name AS kolom, type AS tipe, data_compressed_bytes AS terkompresi, data_uncompressed_bytes AS mentah
		FROM system.columns WHERE database=currentDatabase() AND table='fakta_transaksi' ORDER BY position`)
	if err != nil {
		return nil, err
	}
	return map[string]any{"postgres": pg, "clickhouse": ch}, nil
}

type Banding struct {
	Jenis string           `json:"jenis"`
	Judul string           `json:"judul"`
	Hasil []map[string]any `json:"hasil"`
}

var querBanding = map[string][3]string{ // judul, SQL PostgreSQL, SQL ClickHouse
	"agregasi":   {"Total per cabang 30 hari (3 kolom dari 10)", "SELECT cabang_id, count(*), sum(nominal) FROM transaksi WHERE occurred_at >= now() - interval '30 days' GROUP BY cabang_id", "SELECT cabang_id, count(), sum(nominal) FROM fakta_transaksi_cabang WHERE waktu >= now() - INTERVAL 30 DAY GROUP BY cabang_id"},
	"satu_kolom": {"Total nominal seluruh tabel (1 kolom)", "SELECT sum(nominal) FROM transaksi", "SELECT sum(nominal) FROM fakta_transaksi"},
	"baris_penuh": {"Semua kolom 50 transaksi terakhir satu nasabah", "SELECT * FROM transaksi WHERE nasabah_id = 7 ORDER BY occurred_at DESC LIMIT 50",
		"SELECT * FROM fakta_transaksi WHERE nasabah_id = 7 ORDER BY waktu DESC LIMIT 50"},
	"point_trx": {"Satu transaksi per id (kolom yang bukan ORDER BY di ClickHouse)", "SELECT * FROM transaksi WHERE id = 123456", "SELECT * FROM fakta_transaksi WHERE trx_id = 123456"},
}

func (a *App) Banding(ctx context.Context, jenis string) (Banding, error) {
	q, ok := querBanding[jenis]
	if !ok {
		return Banding{}, fmt.Errorf("jenis tidak dikenal")
	}
	b := Banding{Jenis: jenis, Judul: q[0]}
	// PostgreSQL: EXPLAIN ANALYZE BUFFERS memberi waktu eksekusi dan page yang disentuh
	var plan string
	t := time.Now()
	if err := a.s.PG.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+q[1]).Scan(&plan); err != nil {
		return b, err
	}
	dur := ms(t)
	var p []struct {
		Plan          map[string]any `json:"Plan"`
		ExecutionTime float64        `json:"Execution Time"`
	}
	_ = json.Unmarshal([]byte(plan), &p)
	pages := num(p[0].Plan["Shared Hit Blocks"]) + num(p[0].Plan["Shared Read Blocks"])
	b.Hasil = append(b.Hasil, map[string]any{"store": "PostgreSQL", "sql": q[1], "ms": p[0].ExecutionTime, "ms_klien": dur,
		"byte_dibaca": pages * 8192, "rincian": fmt.Sprintf("%.0f page 8 KB, node %v", pages, p[0].Plan["Node Type"])})
	t = time.Now()
	rs, err := a.s.CH.QueryStat(ctx, q[2])
	if err != nil {
		return b, err
	}
	var rb, rr float64
	_, _ = fmt.Sscan(rs.ReadBytes, &rb)
	_, _ = fmt.Sscan(rs.ReadRows, &rr)
	b.Hasil = append(b.Hasil, map[string]any{"store": "ClickHouse", "sql": q[2], "ms": ms(t), "byte_dibaca": rb,
		"rincian": fmt.Sprintf("%.0f baris dibaca (byte setelah dekompresi)", rr)})
	return b, nil
}
