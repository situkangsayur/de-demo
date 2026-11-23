package main

// Generator bronze sintetis berskema platform (KONTRAK-DATA.md, ops/SKEMA.md): entitas bank
// cabang, rekening (snapshot), transaksi dan akad_pembiayaan (per partisi dt), kolom metadata W1
// (_ingested_at, _source, _batch_id, _event_id, _event_type, _occurred_at, _ts_ms, _op), kolom data
// bertipe string. Deterministik per tanggal: membangkitkan ulang tanggal yang sama menghasilkan isi sama.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

type Cabang struct {
	ID      int
	Kode    string
	Nama    string
	Kota    string
	Wilayah string
}

// Sama dengan platform/data/dev/bangkitkan_bronze.py.
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

// Kode kanal mentah tidak seragam, seperti di aplikasi sumber; distandardkan di stg_transaksi.
var kanalMentah = map[string][]string{
	"mobile":    {"mobile", "Mobile Banking", "m-banking"},
	"atm":       {"ATM", "atm"},
	"teller":    {"teller", "teller "},
	"web":       {"web", "WEB", "internet banking"},
	"ecommerce": {"ecommerce", "e-commerce"},
}
var urutanKanal = []string{"mobile", "atm", "teller", "web", "ecommerce"}
var bobotKanal = []int{45, 18, 9, 13, 15}
var jenisPerKanal = map[string][]string{
	"mobile":    {"transfer_keluar", "transfer_masuk", "angsuran", "transfer_keluar"},
	"atm":       {"tarik", "tarik", "setor"},
	"teller":    {"setor", "tarik", "angsuran"},
	"web":       {"transfer_keluar", "transfer_masuk", "angsuran"},
	"ecommerce": {"bayar_belanja"},
}
var kodeAkad = []string{"MRB", "MRB", "MRB", "MSY", "MDH", "IJR", "IJR", "murabahah"}
var keteranganTrx = []string{"", "", "", "zakat profesi", "infak masjid", "wakaf uang", "belanja bulanan", "gaji", "sedekah"}

const jumlahRekening = 600

type Generator struct {
	Dir string // akar bronze, mis. /data/bronze
}

func pilihBerbobot(r *rand.Rand, bobot []int) int {
	total := 0
	for _, b := range bobot {
		total += b
	}
	x := r.Intn(total)
	for i, b := range bobot {
		if x < b {
			return i
		}
		x -= b
	}
	return len(bobot) - 1
}

func benih(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

func uuidDari(r *rand.Rand) string {
	b := make([]byte, 16)
	r.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type Rekening struct {
	ID, No, NasabahID, Jenis string
	CabangID                 int
}

// DaftarRekening mengembalikan snapshot rekening yang sama setiap kali (benih tetap).
func DaftarRekening() []Rekening {
	r := rand.New(rand.NewSource(benih("rekening")))
	jenis := []string{"tabungan_wadiah", "tabungan_wadiah", "giro", "deposito_mudharabah"}
	out := make([]Rekening, 0, jumlahRekening)
	for i := 1; i <= jumlahRekening; i++ {
		cab := daftarCabang[pilihBerbobot(r, bobotCabang)]
		out = append(out, Rekening{
			ID: uuidDari(r), No: fmt.Sprintf("71%08d", i), NasabahID: uuidDari(r),
			Jenis: jenis[r.Intn(len(jenis))], CabangID: cab.ID,
		})
	}
	return out
}

type baris map[string]any

func meta(r *rand.Rand, batch, tipe string, terjadi, masuk time.Time) baris {
	return baris{
		"_ingested_at": masuk.UTC().Format(time.RFC3339Nano), "_source": "app", "_batch_id": batch,
		"_event_id": uuidDari(r), "_event_type": tipe, "_occurred_at": terjadi.UTC().Format(time.RFC3339),
		"_ts_ms": fmt.Sprint(masuk.UnixMilli()), "_op": nil,
	}
}

// Kontrol = laporan core banking: jumlah transaksi unik dan total nominal satu hari.
type Kontrol struct {
	Tgl          string `json:"tgl"`
	JumlahTrx    int    `json:"jumlah_trx"`
	TotalNominal int64  `json:"total_nominal"`
}

func (g *Generator) partisi(entitas, tgl string) string {
	return filepath.Join(g.Dir, "app", entitas, "dt="+tgl)
}

func tulisJSONL(path string, rows []baris) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o775); err != nil {
		return err
	}
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// Snapshot menulis dimensi kecil (full load).
func (g *Generator) Snapshot(now time.Time) error {
	var cab []baris
	for _, c := range daftarCabang {
		cab = append(cab, baris{"id": fmt.Sprint(c.ID), "kode": c.Kode, "nama": c.Nama, "kota": c.Kota,
			"wilayah": c.Wilayah, "_ingested_at": now.UTC().Format(time.RFC3339)})
	}
	if err := tulisJSONL(filepath.Join(g.Dir, "app", "cabang", "snapshot.jsonl"), cab); err != nil {
		return err
	}
	var rek []baris
	for _, r := range DaftarRekening() {
		rek = append(rek, baris{"id": r.ID, "no_rekening": r.No, "nasabah_id": r.NasabahID, "jenis": r.Jenis,
			"cabang_id": fmt.Sprint(r.CabangID), "status": "aktif", "dibuka_at": "2025-01-02T03:00:00Z",
			"_ingested_at": now.UTC().Format(time.RFC3339)})
	}
	return tulisJSONL(filepath.Join(g.Dir, "app", "rekening", "snapshot.jsonl"), rek)
}

// Hari membangkitkan partisi satu tanggal (WIB = UTC untuk jam 01.00-15.00 UTC), menulis _SUCCESS bila tahan=false.
func (g *Generator) Hari(tgl string, tahan bool) (Kontrol, error) {
	d, err := time.Parse("2006-01-02", tgl)
	if err != nil {
		return Kontrol{}, err
	}
	r := rand.New(rand.NewSource(benih("trx" + tgl)))
	rek := DaftarRekening()
	n := 1200 + r.Intn(600)
	batch1 := d.Format("20060102") + "T160000Z-" + fmt.Sprintf("%06x", r.Intn(1<<24))
	batch2 := d.Format("20060102") + "T163000Z-" + fmt.Sprintf("%06x", r.Intn(1<<24))
	var part1, part2 []baris
	k := Kontrol{Tgl: tgl}
	for i := 0; i < n; i++ {
		rk := rek[r.Intn(len(rek))]
		cab := rk.CabangID
		if i < len(daftarCabang) { // jamin setiap cabang mengirim data
			cab = daftarCabang[i].ID
		} else if r.Float64() > 0.8 {
			cab = daftarCabang[pilihBerbobot(r, bobotCabang)].ID
		}
		kanal := urutanKanal[pilihBerbobot(r, bobotKanal)]
		jenis := jenisPerKanal[kanal][r.Intn(len(jenisPerKanal[kanal]))]
		nominal := int64(10_000 * (5 + r.Intn(500)))
		if kanal == "teller" {
			nominal *= 4
		}
		terjadi := d.Add(time.Hour + time.Duration(r.Intn(14*3600))*time.Second)
		dibuat := terjadi.Add(time.Duration(r.Intn(900)) * time.Millisecond)
		if r.Float64() < 0.02 { // data terlambat: occurred_at mundur 1-3 jam dari created_at
			terjadi = terjadi.Add(-time.Duration(60+r.Intn(120)) * time.Minute)
			if terjadi.Before(d) {
				terjadi = d.Add(time.Minute)
			}
		}
		masuk := dibuat.Add(time.Duration(1+r.Intn(30)) * time.Second)
		row := meta(r, batch1, "bank.transaksi.dibuat", terjadi, masuk)
		row["id"] = fmt.Sprintf("TRX%s%06d", d.Format("20060102"), i+1)
		row["rekening_id"] = rk.ID
		row["cabang_id"] = fmt.Sprint(cab)
		alias := kanalMentah[kanal]
		row["kanal"] = alias[r.Intn(len(alias))]
		row["jenis"] = jenis
		row["nominal"] = fmt.Sprint(nominal)
		row["occurred_at"] = terjadi.UTC().Format(time.RFC3339)
		row["created_at"] = dibuat.UTC().Format(time.RFC3339Nano)
		row["keterangan"] = keteranganTrx[r.Intn(len(keteranganTrx))]
		row["akad_id"] = nil
		k.JumlahTrx++
		k.TotalNominal += nominal
		if i%2 == 0 {
			part1 = append(part1, row)
		} else {
			row["_batch_id"] = batch2
			part2 = append(part2, row)
		}
		if r.Float64() < 0.005 { // at-least-once: event yang sama tertulis lagi di batch lain (_event_id sama)
			dup := baris{}
			for kk, v := range row {
				dup[kk] = v
			}
			dup["_batch_id"] = batch2
			part2 = append(part2, dup)
		}
	}
	dir := g.partisi("transaksi", tgl)
	if err := os.MkdirAll(dir, 0o775); err != nil {
		return k, err
	}
	if err := tulisJSONL(filepath.Join(dir, "part-"+batch1+".jsonl"), part1); err != nil {
		return k, err
	}
	if err := tulisJSONL(filepath.Join(dir, "part-"+batch2+".jsonl"), part2); err != nil {
		return k, err
	}
	// akad pembiayaan yang cair hari itu
	var akad []baris
	for i := 0; i < 3+r.Intn(6); i++ {
		rk := rek[r.Intn(len(rek))]
		cair := d.Add(time.Duration(2+r.Intn(10)) * time.Hour)
		row := meta(r, batch1, "bank.akad.dicairkan", cair, cair.Add(5*time.Second))
		pokok := int64(5_000_000 * (1 + r.Intn(40)))
		row["id"] = uuidDari(r)
		row["nasabah_id"] = rk.NasabahID
		row["rekening_id"] = rk.ID
		row["kode_akad"] = kodeAkad[r.Intn(len(kodeAkad))]
		row["pokok"] = fmt.Sprint(pokok)
		row["margin"] = fmt.Sprint(pokok / 10)
		row["tenor_bulan"] = fmt.Sprint(12 * (1 + r.Intn(3)))
		row["status"] = "cair"
		row["cair_at"] = cair.UTC().Format(time.RFC3339)
		akad = append(akad, row)
	}
	if err := tulisJSONL(filepath.Join(g.partisi("akad_pembiayaan", tgl), "part-"+batch1+".jsonl"), akad); err != nil {
		return k, err
	}
	kb, _ := json.Marshal(k)
	sumber := filepath.Join(filepath.Dir(g.Dir), "sumber")
	if err := os.MkdirAll(sumber, 0o775); err != nil {
		return k, err
	}
	if err := os.WriteFile(filepath.Join(sumber, "kontrol-"+tgl+".json"), kb, 0o664); err != nil {
		return k, err
	}
	if !tahan {
		return k, os.WriteFile(filepath.Join(dir, "_SUCCESS"), nil, 0o664)
	}
	return k, nil
}

// Sabotase menambahkan berkas rusak ke partisi tgl (slide P06, langkah 4): 50 trx_id ganda, kanal di luar
// domain, rekening kosong, nominal negatif, rekening yang tidak terdaftar, dan kode akad tidak valid.
func (g *Generator) Sabotase(tgl string) (int, error) {
	dir := g.partisi("transaksi", tgl)
	files, _ := filepath.Glob(filepath.Join(dir, "part-*T160000Z-*.jsonl"))
	if len(files) == 0 {
		return 0, fmt.Errorf("partisi %s belum ada", tgl)
	}
	b, err := os.ReadFile(files[0])
	if err != nil {
		return 0, err
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var asli []baris
	dec := json.NewDecoder(bytes.NewReader(b))
	for dec.More() && len(asli) < 54 {
		var x baris
		if err := dec.Decode(&x); err != nil {
			return 0, err
		}
		asli = append(asli, x)
	}
	var rusak []baris
	for i, x := range asli {
		x["_event_id"] = uuidDari(r) // event berbeda, trx_id sama: duplikat bisnis
		x["_batch_id"] = "sabotase"
		switch i {
		case 50:
			x["id"] = fmt.Sprintf("TRXSAB%s01", tgl)
			x["kanal"] = "satelit"
		case 51:
			x["id"] = fmt.Sprintf("TRXSAB%s02", tgl)
			x["rekening_id"] = nil
		case 52:
			x["id"] = fmt.Sprintf("TRXSAB%s03", tgl)
			x["nominal"] = "-250000"
		case 53:
			x["id"] = fmt.Sprintf("TRXSAB%s04", tgl)
			x["rekening_id"] = "REK-TIDAK-ADA"
		}
		rusak = append(rusak, x)
	}
	if err := tulisJSONL(filepath.Join(dir, "part-sabotase.jsonl"), rusak); err != nil {
		return 0, err
	}
	d, _ := time.Parse("2006-01-02", tgl)
	ak := meta(r, "sabotase", "bank.akad.dicairkan", d.Add(3*time.Hour), d.Add(3*time.Hour+time.Second))
	isi := baris{"id": uuidDari(r), "nasabah_id": "x", "rekening_id": "x", "kode_akad": "XYZ",
		"pokok": "10000000", "margin": "1000000", "tenor_bulan": "12", "status": "cair",
		"cair_at": d.Add(3 * time.Hour).UTC().Format(time.RFC3339)}
	for kk, v := range isi {
		ak[kk] = v
	}
	if err := tulisJSONL(filepath.Join(g.partisi("akad_pembiayaan", tgl), "part-sabotase.jsonl"), []baris{ak}); err != nil {
		return 0, err
	}
	return len(rusak) + 1, nil
}

func (g *Generator) Pulihkan(tgl string) error {
	for _, e := range []string{"transaksi", "akad_pembiayaan"} {
		p := filepath.Join(g.partisi(e, tgl), "part-sabotase.jsonl")
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (g *Generator) Tahan(tgl string, tahan bool) error {
	dir := g.partisi("transaksi", tgl)
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("partisi %s belum ada", tgl)
	}
	ok := filepath.Join(dir, "_SUCCESS")
	if tahan {
		if err := os.Remove(ok); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	return os.WriteFile(ok, nil, 0o664)
}

type StatusHari struct {
	Tgl      string `json:"tgl"`
	Siap     bool   `json:"siap"`
	Sabotase bool   `json:"sabotase"`
	Berkas   int    `json:"berkas"`
}

func (g *Generator) Status(tgls []string) []StatusHari {
	var out []StatusHari
	for _, t := range tgls {
		dir := g.partisi("transaksi", t)
		files, _ := filepath.Glob(filepath.Join(dir, "part-*.jsonl"))
		_, e1 := os.Stat(filepath.Join(dir, "_SUCCESS"))
		_, e2 := os.Stat(filepath.Join(dir, "part-sabotase.jsonl"))
		out = append(out, StatusHari{Tgl: t, Siap: e1 == nil, Sabotase: e2 == nil, Berkas: len(files)})
	}
	return out
}
