package main

// gold.fitur_nasabah: dihitung di ClickHouse (point-in-time sampai tgl_fitur), lalu disalin ke serving store
// (Redis untuk hot path, PostgreSQL sebagai pembanding). Gold tetap sumber kebenaran; salinan bisa dibangun ulang.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const sqlFitur = `CREATE TABLE fitur_nasabah ENGINE = MergeTree ORDER BY nasabah_id AS
WITH toDate('%s') AS d
SELECT nasabah_id, d AS tgl_fitur,
  toUInt32(countIf(tgl > d - 7)) AS trx_7h,
  toInt64(sumIf(nominal, tgl > d - 7)) AS nominal_7h,
  toUInt32(countIf(tgl > d - 30)) AS trx_30h,
  toInt64(sumIf(nominal, tgl > d - 30)) AS nominal_30h,
  toInt32(dateDiff('day', max(tgl), d)) AS hari_sejak_trx_terakhir,
  round(countIf(kanal IN ('mobile', 'web', 'ecommerce') AND tgl > d - 30) / greatest(countIf(tgl > d - 30), 1), 3) AS rasio_digital_30h,
  toUInt32(countIf(jenis = 'bayar_belanja' AND tgl > d - 30)) AS belanja_ecom_30h,
  toInt64(sumIf(nominal, jenis = 'bayar_belanja' AND tgl > d - 30)) AS nominal_belanja_ecom_30h
FROM fakta_transaksi
WHERE tgl <= d
GROUP BY nasabah_id`

// Fitur satu nasabah dihitung langsung dari fakta (tanpa materialisasi), untuk pembanding latency.
const sqlFiturLangsung = `SELECT count() AS trx_30h, sum(nominal) AS nominal_30h,
  countIf(waktu >= now() - INTERVAL 7 DAY) AS trx_7h, dateDiff('day', max(waktu), now()) AS hari_sejak_trx_terakhir,
  round(countIf(kanal IN ('mobile', 'web', 'ecommerce')) / greatest(count(), 1), 3) AS rasio_digital_30h
FROM fakta_transaksi WHERE nasabah_id = %d AND waktu >= now() - INTERVAL 30 DAY`

type HasilSinkron struct {
	TglFitur  string  `json:"tgl_fitur"`
	Baris     int     `json:"baris"`
	HitungMs  float64 `json:"hitung_clickhouse_ms"`
	RedisMs   float64 `json:"salin_redis_ms"`
	PGMs      float64 `json:"salin_postgres_ms"`
	VersiLama string  `json:"versi_lama"`
}

func ms(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

func (a *App) HitungDanSinkron(ctx context.Context) (HasilSinkron, error) {
	s := a.s
	tgl := a.d.Akhir.Add(-24 * time.Hour).UTC().Format("2006-01-02")
	h := HasilSinkron{TglFitur: tgl}
	h.VersiLama, _ = s.RD.Get(ctx, "fitur:versi").Result()
	t := time.Now()
	if err := s.CH.Exec(ctx, "DROP TABLE IF EXISTS fitur_nasabah"); err != nil {
		return h, err
	}
	if err := s.CH.Exec(ctx, fmt.Sprintf(sqlFitur, tgl)); err != nil {
		return h, err
	}
	h.HitungMs = ms(t)
	b, _, err := s.CH.do(ctx, "SELECT * FROM fitur_nasabah FORMAT JSONEachRow", nil, false)
	if err != nil {
		return h, err
	}
	profil := map[int32]Nasabah{}
	for _, n := range a.d.Nasabah {
		profil[n.ID] = n
	}
	var rows []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			return h, err
		}
		n := profil[int32(num(m["nasabah_id"]))]
		m["segmen_saat_itu"], m["kota_saat_itu"] = n.Segmen, n.Kota
		rows = append(rows, m)
	}
	h.Baris = len(rows)
	// Redis: satu hash per nasabah, TTL 2 hari (salinan turunan, boleh hilang dan dibangun ulang)
	t = time.Now()
	pipe := s.RD.Pipeline()
	for _, m := range rows {
		key := fmt.Sprintf("fitur:%v", m["nasabah_id"])
		vals := make([]any, 0, len(m)*2)
		for k, v := range m {
			vals = append(vals, k, fmt.Sprint(v))
		}
		pipe.HSet(ctx, key, vals...)
		pipe.Expire(ctx, key, 48*time.Hour)
		if pipe.Len() > 4000 {
			if _, err := pipe.Exec(ctx); err != nil {
				return h, err
			}
		}
	}
	pipe.Set(ctx, "fitur:versi", tgl, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return h, err
	}
	h.RedisMs = ms(t)
	// PostgreSQL: ganti isi dalam satu transaksi (pembaca tidak melihat tabel setengah terisi)
	t = time.Now()
	tx, err := s.PG.Begin(ctx)
	if err != nil {
		return h, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "DELETE FROM fitur_nasabah"); err != nil {
		return h, err
	}
	kol := []string{"nasabah_id", "tgl_fitur", "trx_7h", "nominal_7h", "trx_30h", "nominal_30h", "hari_sejak_trx_terakhir",
		"rasio_digital_30h", "belanja_ecom_30h", "nominal_belanja_ecom_30h", "segmen_saat_itu", "kota_saat_itu"}
	d, _ := time.Parse("2006-01-02", tgl)
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"fitur_nasabah"}, kol, pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
		m := rows[i]
		return []any{int32(num(m["nasabah_id"])), d, int32(num(m["trx_7h"])), int64(num(m["nominal_7h"])), int32(num(m["trx_30h"])),
			int64(num(m["nominal_30h"])), int32(num(m["hari_sejak_trx_terakhir"])), num(m["rasio_digital_30h"]),
			int32(num(m["belanja_ecom_30h"])), int64(num(m["nominal_belanja_ecom_30h"])), m["segmen_saat_itu"], m["kota_saat_itu"]}, nil
	})); err != nil {
		return h, err
	}
	if err := tx.Commit(ctx); err != nil {
		return h, err
	}
	h.PGMs = ms(t)
	logInfo("fitur disinkronkan", "tgl_fitur", tgl, "baris", h.Baris, "redis_ms", h.RedisMs)
	return h, nil
}

type LookupFitur struct {
	Store string         `json:"store"`
	Cara  string         `json:"cara"`
	Ms    float64        `json:"ms"`
	Data  map[string]any `json:"data,omitempty"`
	Galat string         `json:"galat,omitempty"`
}

func (a *App) Lookup(ctx context.Context, id int) []LookupFitur {
	s := a.s
	var out []LookupFitur
	t := time.Now()
	m, err := s.RD.HGetAll(ctx, "fitur:"+strconv.Itoa(id)).Result()
	l := LookupFitur{Store: "Redis", Cara: "HGETALL fitur:" + strconv.Itoa(id), Ms: ms(t), Data: map[string]any{}}
	for k, v := range m {
		l.Data[k] = v
	}
	if err != nil {
		l.Galat = err.Error()
	}
	out = append(out, l)
	t = time.Now()
	rows, err := s.PG.Query(ctx, "SELECT * FROM fitur_nasabah WHERE nasabah_id = $1", id)
	l = LookupFitur{Store: "PostgreSQL", Cara: "SELECT * FROM fitur_nasabah WHERE nasabah_id = $1"}
	if err == nil {
		mp, e := pgx.CollectRows(rows, pgx.RowToMap)
		l.Ms = ms(t)
		if e == nil && len(mp) > 0 {
			l.Data = mp[0]
		}
	} else {
		l.Galat = err.Error()
	}
	out = append(out, l)
	t = time.Now()
	r1, err := s.CH.Query(ctx, fmt.Sprintf("SELECT * FROM fitur_nasabah WHERE nasabah_id = %d", id))
	l = LookupFitur{Store: "ClickHouse", Cara: "tabel fitur_nasabah", Ms: ms(t)}
	if err == nil && len(r1) > 0 {
		l.Data = r1[0]
	} else if err != nil {
		l.Galat = err.Error()
	}
	out = append(out, l)
	t = time.Now()
	r2, err := s.CH.Query(ctx, fmt.Sprintf(sqlFiturLangsung, id))
	l = LookupFitur{Store: "ClickHouse", Cara: "hitung langsung dari fakta_transaksi", Ms: ms(t)}
	if err == nil && len(r2) > 0 {
		l.Data = r2[0]
	} else if err != nil {
		l.Galat = err.Error()
	}
	return append(out, l)
}
