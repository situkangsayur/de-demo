package main

// Pembangkit data bank syariah (deterministik) dan pemuatan ke empat store. Throughput tulis bulk
// setiap store dicatat untuk tabel "kapan memilih".

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type Cabang struct {
	ID                        int
	Kode, Nama, Kota, Wilayah string
}

var daftarCabang = []Cabang{
	{1, "JKT01", "KC Jakarta Thamrin", "jakarta pusat", "Jabodetabek"},
	{2, "JKT02", "KC Jakarta Selatan", "jakarta selatan", "Jabodetabek"},
	{3, "BGR01", "KC Bogor", "bogor", "Jabodetabek"},
	{4, "BDG01", "KC Bandung", "bandung", "Jawa Barat"},
	{5, "SBY01", "KC Surabaya", "surabaya", "Jawa Timur"},
	{6, "MDN01", "KC Medan", "medan", "Sumatra"},
	{7, "MKS01", "KC Makassar", "makassar", "Sulawesi"},
	{8, "YGY01", "KC Yogyakarta", "yogyakarta", "Jawa Tengah-DIY"},
}
var bobotCabang = []int{18, 14, 9, 14, 13, 11, 9, 12}
var kanalList = []string{"mobile", "atm", "teller", "web", "ecommerce"}
var bobotKanal = []int{45, 18, 9, 13, 15}
var jenisPerKanal = map[string][]string{
	"mobile": {"transfer_keluar", "transfer_masuk", "angsuran"}, "atm": {"tarik", "tarik", "setor"},
	"teller": {"setor", "tarik", "angsuran"}, "web": {"transfer_keluar", "transfer_masuk"}, "ecommerce": {"bayar_belanja"},
}

// Keterangan bebas: bahan pencarian teks (zakat, wakaf, infak, ...).
var keterangan = []string{
	"transfer keluarga", "bayar listrik", "belanja bulanan", "gaji bulanan", "angsuran murabahah rumah",
	"biaya pendidikan anak", "zakat profesi", "infak masjid", "wakaf uang produktif", "sedekah jumat",
	"bayar kos", "pulsa dan paket data", "tiket kereta", "qurban idul adha", "zakat mal dan wakaf ramadhan",
	"setoran tabungan haji", "bayar zakat fitrah", "donasi bencana", "belanja daring toko", "pembayaran sekolah",
}
var bobotKet = []int{14, 10, 12, 8, 6, 5, 4, 5, 2, 4, 6, 8, 4, 1, 1, 2, 2, 2, 10, 4}

type Trx struct {
	ID, Rekening int64
	Nasabah      int32
	Cabang       int16
	Kanal, Jenis string
	Nominal      int64
	Waktu        time.Time
	Ket          string
}

type Rekening struct {
	ID      int64
	Nasabah int32
	Jenis   string
	Saldo   int64
	Cabang  int16
}

type Nasabah struct {
	ID     int32
	Kota   string
	Segmen string
	Cabang int16
}

type Data struct {
	Nasabah  []Nasabah
	Rekening []Rekening
	Trx      []Trx
	Akhir    time.Time
}

func pilih(r *rand.Rand, bobot []int) int {
	t := 0
	for _, b := range bobot {
		t += b
	}
	x := r.Intn(t)
	for i, b := range bobot {
		if x < b {
			return i
		}
		x -= b
	}
	return len(bobot) - 1
}

// Bangkitkan data N transaksi selama `hari` hari sampai `akhir`, urut waktu.
func Bangkitkan(nNasabah, nTrx, hari int, akhir time.Time) *Data {
	r := rand.New(rand.NewSource(20261123))
	d := &Data{Akhir: akhir}
	segmen := []string{"ritel", "ritel", "prioritas", "umkm"}
	for i := 1; i <= nNasabah; i++ {
		c := daftarCabang[pilih(r, bobotCabang)]
		d.Nasabah = append(d.Nasabah, Nasabah{ID: int32(i), Kota: c.Kota, Segmen: segmen[r.Intn(4)], Cabang: int16(c.ID)})
	}
	jenis := []string{"tabungan_wadiah", "tabungan_wadiah", "giro", "deposito_mudharabah"}
	var id int64 = 7100000000
	for _, n := range d.Nasabah {
		for k := 0; k < 1+r.Intn(2); k++ {
			id++
			d.Rekening = append(d.Rekening, Rekening{ID: id, Nasabah: n.ID, Jenis: jenis[r.Intn(4)],
				Saldo: int64(100_000 * (1 + r.Intn(2000))), Cabang: n.Cabang})
		}
	}
	awal := akhir.Add(-time.Duration(hari) * 24 * time.Hour)
	span := int64(akhir.Sub(awal) / time.Second)
	d.Trx = make([]Trx, nTrx)
	for i := 0; i < nTrx; i++ {
		rk := d.Rekening[r.Intn(len(d.Rekening))]
		kanal := kanalList[pilih(r, bobotKanal)]
		cab := rk.Cabang
		if r.Float64() < 0.2 {
			cab = int16(daftarCabang[pilih(r, bobotCabang)].ID)
		}
		d.Trx[i] = Trx{Rekening: rk.ID, Nasabah: rk.Nasabah, Cabang: cab, Kanal: kanal,
			Jenis: jenisPerKanal[kanal][r.Intn(len(jenisPerKanal[kanal]))], Nominal: int64(10_000 * (5 + r.Intn(500))),
			Waktu: awal.Add(time.Duration(r.Int63n(span)) * time.Second), Ket: keterangan[pilih(r, bobotKet)]}
	}
	sort.Slice(d.Trx, func(i, j int) bool { return d.Trx[i].Waktu.Before(d.Trx[j].Waktu) })
	for i := range d.Trx {
		d.Trx[i].ID = int64(i + 1)
	}
	return d
}

type Progres struct {
	mu         sync.Mutex
	Tahap      string             `json:"tahap"`
	Persen     int                `json:"persen"`
	Selesai    bool               `json:"selesai"`
	Galat      string             `json:"galat,omitempty"`
	Jumlah     map[string]int     `json:"jumlah"`
	Throughput map[string]float64 `json:"throughput_bulk"` // baris/detik saat muat bulk
}

func (p *Progres) set(tahap string, persen int) {
	p.mu.Lock()
	p.Tahap, p.Persen = tahap, persen
	p.mu.Unlock()
	logInfo("muat data", "tahap", tahap, "persen", persen)
}

func (p *Progres) catat(store string, baris int, dur time.Duration) {
	p.mu.Lock()
	p.Throughput[store] = float64(baris) / dur.Seconds()
	p.mu.Unlock()
}

func (p *Progres) salin() Progres {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Progres{Tahap: p.Tahap, Persen: p.Persen, Selesai: p.Selesai, Galat: p.Galat, Jumlah: p.Jumlah, Throughput: p.Throughput}
}

// Muat menulis data ke semua store. Bila PostgreSQL sudah berisi (lab_meta.dimuat), cukup baca ulang throughput.
func (a *App) Muat(ctx context.Context) error {
	p := a.prog
	var dimuat, tp string
	_ = a.s.PG.QueryRow(ctx, "SELECT nilai FROM lab_meta WHERE kunci='dimuat'").Scan(&dimuat)
	_ = a.s.PG.QueryRow(ctx, "SELECT nilai FROM lab_meta WHERE kunci='throughput'").Scan(&tp)
	if dimuat == strconv.Itoa(len(a.d.Trx)) {
		_ = json.Unmarshal([]byte(tp), &p.Throughput)
		p.set("data sudah ada (volume lama)", 100)
		return nil
	}
	steps := []struct {
		nama string
		fn   func(context.Context) error
	}{
		{"PostgreSQL (COPY)", a.muatPG}, {"ClickHouse (INSERT batch)", a.muatCH},
		{"Redis (pipeline)", a.muatRedis}, {"OpenSearch (_bulk)", a.muatOS},
	}
	for i, st := range steps {
		p.set(st.nama, i*25)
		if err := st.fn(ctx); err != nil {
			return fmt.Errorf("%s: %w", st.nama, err)
		}
	}
	b, _ := json.Marshal(p.Throughput)
	_, err := a.s.PG.Exec(ctx, `INSERT INTO lab_meta VALUES ('dimuat',$1),('throughput',$2)
		ON CONFLICT (kunci) DO UPDATE SET nilai=EXCLUDED.nilai`, strconv.Itoa(len(a.d.Trx)), string(b))
	return err
}

func (a *App) muatPG(ctx context.Context) error {
	d, pg := a.d, a.s.PG
	if _, err := pg.Exec(ctx, "TRUNCATE cabang, nasabah, rekening, transaksi, fitur_nasabah; DROP INDEX IF EXISTS trx_nasabah_waktu, trx_ket_fts"); err != nil {
		return err
	}
	var rows [][]any
	for _, c := range daftarCabang {
		rows = append(rows, []any{int16(c.ID), c.Kode, c.Nama, c.Kota, c.Wilayah})
	}
	if _, err := pg.CopyFrom(ctx, pgx.Identifier{"cabang"}, []string{"id", "kode", "nama", "kota", "wilayah"}, pgx.CopyFromRows(rows)); err != nil {
		return err
	}
	if _, err := pg.CopyFrom(ctx, pgx.Identifier{"nasabah"}, []string{"id", "cif", "nama", "kota", "segmen", "cabang_id"},
		pgx.CopyFromSlice(len(d.Nasabah), func(i int) ([]any, error) {
			n := d.Nasabah[i]
			return []any{n.ID, fmt.Sprintf("CIF%07d", n.ID), fmt.Sprintf("Nasabah %d", n.ID), n.Kota, n.Segmen, n.Cabang}, nil
		})); err != nil {
		return err
	}
	if _, err := pg.CopyFrom(ctx, pgx.Identifier{"rekening"}, []string{"id", "no_rekening", "nasabah_id", "jenis", "saldo", "status", "cabang_id"},
		pgx.CopyFromSlice(len(d.Rekening), func(i int) ([]any, error) {
			r := d.Rekening[i]
			return []any{r.ID, strconv.FormatInt(r.ID, 10), r.Nasabah, r.Jenis, r.Saldo, "aktif", r.Cabang}, nil
		})); err != nil {
		return err
	}
	t := time.Now()
	if _, err := pg.CopyFrom(ctx, pgx.Identifier{"transaksi"}, []string{"id", "rekening_id", "nasabah_id", "cabang_id", "kanal", "jenis", "nominal", "occurred_at", "keterangan"},
		pgx.CopyFromSlice(len(d.Trx), func(i int) ([]any, error) {
			x := d.Trx[i]
			return []any{x.ID, x.Rekening, x.Nasabah, x.Cabang, x.Kanal, x.Jenis, x.Nominal, x.Waktu, x.Ket}, nil
		})); err != nil {
		return err
	}
	for _, s := range []string{
		"CREATE INDEX trx_nasabah_waktu ON transaksi (nasabah_id, occurred_at DESC)",
		"CREATE INDEX trx_ket_fts ON transaksi USING gin (to_tsvector('simple', keterangan))",
		"ANALYZE",
	} {
		if _, err := pg.Exec(ctx, s); err != nil {
			return err
		}
	}
	a.prog.catat("postgres", len(d.Trx), time.Since(t))
	return nil
}

const ddlFakta = `CREATE TABLE IF NOT EXISTS %s (
  trx_id UInt64, tgl Date, waktu DateTime('UTC'), rekening_id UInt64, nasabah_id UInt32, cabang_id UInt16,
  kanal LowCardinality(String), jenis LowCardinality(String), nominal Int64, keterangan String
) ENGINE = MergeTree PARTITION BY toYYYYMM(tgl) ORDER BY %s
SETTINGS min_bytes_for_wide_part = 0` // part Wide: satu berkas per kolom (ukuran per kolom terlihat)

func (a *App) muatCH(ctx context.Context) error {
	ch, d := a.s.CH, a.d
	for _, s := range []string{
		"DROP TABLE IF EXISTS fakta_transaksi", "DROP TABLE IF EXISTS fakta_transaksi_cabang", "DROP TABLE IF EXISTS rekening",
		// dua salinan fakta dengan ORDER BY berbeda: memperlihatkan pengaruh sparse primary index
		fmt.Sprintf(ddlFakta, "fakta_transaksi", "(nasabah_id, waktu)"),
		fmt.Sprintf(ddlFakta, "fakta_transaksi_cabang", "(cabang_id, waktu)"),
		"CREATE TABLE rekening (rekening_id UInt64, nasabah_id UInt32, jenis LowCardinality(String), saldo Int64, cabang_id UInt16) ENGINE = MergeTree ORDER BY rekening_id SETTINGS min_bytes_for_wide_part = 0",
		"CREATE TABLE IF NOT EXISTS tulis_uji (id UInt64, rekening_id UInt64, nominal Int64, waktu DateTime64(3), keterangan String) ENGINE = MergeTree ORDER BY id",
	} {
		if err := ch.Exec(ctx, s); err != nil {
			return err
		}
	}
	var buf bytes.Buffer
	for _, r := range d.Rekening {
		fmt.Fprintf(&buf, "%d\t%d\t%s\t%d\t%d\n", r.ID, r.Nasabah, r.Jenis, r.Saldo, r.Cabang)
	}
	if err := ch.Insert(ctx, "INSERT INTO rekening FORMAT TabSeparated", &buf); err != nil {
		return err
	}
	t := time.Now()
	const batch = 100_000
	for i := 0; i < len(d.Trx); i += batch {
		buf.Reset()
		for _, x := range d.Trx[i:min(i+batch, len(d.Trx))] {
			fmt.Fprintf(&buf, "%d\t%s\t%s\t%d\t%d\t%d\t%s\t%s\t%d\t%s\n", x.ID, x.Waktu.UTC().Format("2006-01-02"),
				x.Waktu.UTC().Format("2006-01-02 15:04:05"), x.Rekening, x.Nasabah, x.Cabang, x.Kanal, x.Jenis, x.Nominal, x.Ket)
		}
		if err := ch.Insert(ctx, "INSERT INTO fakta_transaksi FORMAT TabSeparated", bytes.NewReader(buf.Bytes())); err != nil {
			return err
		}
	}
	a.prog.catat("clickhouse", len(d.Trx), time.Since(t))
	if err := ch.Exec(ctx, "INSERT INTO fakta_transaksi_cabang SELECT * FROM fakta_transaksi"); err != nil {
		return err
	}
	return ch.Exec(ctx, "OPTIMIZE TABLE fakta_transaksi FINAL")
}

func (a *App) muatRedis(ctx context.Context) error {
	rd, d := a.s.RD, a.d
	if err := rd.FlushDB(ctx).Err(); err != nil {
		return err
	}
	pipe := rd.Pipeline()
	for _, r := range d.Rekening {
		pipe.Set(ctx, fmt.Sprintf("saldo:%d", r.ID), r.Saldo, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	// riwayat 50 transaksi terakhir per nasabah: list (terbaru di kepala)
	per := map[int32][]Trx{}
	for _, x := range d.Trx {
		per[x.Nasabah] = append(per[x.Nasabah], x)
	}
	t, n := time.Now(), 0
	pipe = rd.Pipeline()
	for nid, xs := range per {
		if len(xs) > 50 {
			xs = xs[len(xs)-50:]
		}
		vals := make([]any, 0, len(xs))
		for i := len(xs) - 1; i >= 0; i-- {
			b, _ := json.Marshal(map[string]any{"id": xs[i].ID, "waktu": xs[i].Waktu.UTC().Format(time.RFC3339),
				"kanal": xs[i].Kanal, "jenis": xs[i].Jenis, "nominal": xs[i].Nominal})
			vals = append(vals, b)
		}
		key := fmt.Sprintf("riwayat:%d", nid)
		pipe.Del(ctx, key)
		pipe.RPush(ctx, key, vals...)
		n += len(vals)
		if pipe.Len() > 2000 {
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	a.prog.catat("redis", n, time.Since(t))
	return nil
}

func (a *App) muatOS(ctx context.Context) error {
	os, d := a.s.OS, a.d
	_ = os.Do(ctx, http.MethodDelete, "/transaksi", nil, nil)
	_ = os.Do(ctx, http.MethodDelete, "/rekening", nil, nil)
	pemetaan := map[string]any{
		"settings": map[string]any{"number_of_shards": 2, "number_of_replicas": 1, "refresh_interval": "-1"},
		"mappings": map[string]any{"properties": map[string]any{
			"trx_id": map[string]string{"type": "long"}, "rekening_id": map[string]string{"type": "long"},
			"nasabah_id": map[string]string{"type": "integer"}, "cabang_id": map[string]string{"type": "short"},
			"kanal": map[string]string{"type": "keyword"}, "jenis": map[string]string{"type": "keyword"},
			"nominal": map[string]string{"type": "long"}, "waktu": map[string]string{"type": "date"},
			"keterangan": map[string]string{"type": "text"},
		}},
	}
	if err := os.Do(ctx, http.MethodPut, "/transaksi", pemetaan, nil); err != nil {
		return err
	}
	if err := os.Do(ctx, http.MethodPut, "/rekening", map[string]any{"settings": map[string]any{"number_of_shards": 2, "number_of_replicas": 1}}, nil); err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, r := range d.Rekening {
		fmt.Fprintf(&buf, "{\"index\":{\"_index\":\"rekening\",\"_id\":\"%d\"}}\n{\"nasabah_id\":%d,\"jenis\":%q,\"saldo\":%d,\"cabang_id\":%d}\n",
			r.ID, r.Nasabah, r.Jenis, r.Saldo, r.Cabang)
	}
	if err := os.Do(ctx, http.MethodPost, "/_bulk?refresh=true", buf.Bytes(), nil); err != nil {
		return err
	}
	t := time.Now()
	const batch = 5000
	for i := 0; i < len(d.Trx); i += batch {
		buf.Reset()
		for _, x := range d.Trx[i:min(i+batch, len(d.Trx))] {
			fmt.Fprintf(&buf, "{\"index\":{\"_index\":\"transaksi\",\"_id\":\"%d\"}}\n{\"trx_id\":%d,\"rekening_id\":%d,\"nasabah_id\":%d,\"cabang_id\":%d,\"kanal\":%q,\"jenis\":%q,\"nominal\":%d,\"waktu\":%q,\"keterangan\":%q}\n",
				x.ID, x.ID, x.Rekening, x.Nasabah, x.Cabang, x.Kanal, x.Jenis, x.Nominal, x.Waktu.UTC().Format(time.RFC3339), x.Ket)
		}
		var res struct {
			Errors bool `json:"errors"`
		}
		if err := os.Do(ctx, http.MethodPost, "/_bulk", buf.Bytes(), &res); err != nil {
			return err
		}
		if res.Errors {
			return fmt.Errorf("bulk opensearch berisi error pada batch %d", i/batch)
		}
		if i%(batch*20) == 0 {
			a.prog.set(fmt.Sprintf("OpenSearch (_bulk) %d/%d", i, len(d.Trx)), 75+20*i/len(d.Trx))
		}
	}
	if err := os.Do(ctx, http.MethodPut, "/transaksi/_settings", map[string]any{"index": map[string]any{"refresh_interval": "1s"}}, nil); err != nil {
		return err
	}
	if err := os.Do(ctx, http.MethodPost, "/transaksi/_refresh", nil, nil); err != nil {
		return err
	}
	a.prog.catat("opensearch", len(d.Trx), time.Since(t))
	return os.Do(ctx, http.MethodPost, "/transaksi/_forcemerge?max_num_segments=1", nil, nil)
}
