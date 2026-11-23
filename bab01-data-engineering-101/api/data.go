package main

// Data referensi kecil dengan skema platform (cabang, nasabah, rekening). Cabang sama dengan
// platform/data/dev/bangkitkan_bronze.py agar angka lab mudah dibandingkan dengan platform.

import (
	"fmt"
	"math/rand"
)

type Cabang struct {
	ID      int    `json:"id"`
	Kode    string `json:"kode"`
	Nama    string `json:"nama"`
	Kota    string `json:"kota"`
	Wilayah string `json:"wilayah"`
}

type Nasabah struct {
	ID       int    `json:"id"`
	CIF      string `json:"cif"`
	Nama     string `json:"nama"`
	Kota     string `json:"kota"`
	Segmen   string `json:"segmen"`
	CabangID int    `json:"cabang_id"`
}

type Rekening struct {
	ID         int     `json:"id"`
	NoRekening string  `json:"no_rekening"`
	NasabahID  int     `json:"nasabah_id"`
	CabangID   int     `json:"cabang_id"`
	Jenis      string  `json:"jenis"`
	SaldoAwal  float64 `json:"saldo_awal"`
	Nama       string  `json:"nama_nasabah"`
}

var DataCabang = []Cabang{
	{1, "JKT01", "KC Jakarta Thamrin", "jakarta pusat", "Jabodetabek"},
	{2, "JKT02", "KC Jakarta Selatan", "jakarta selatan", "Jabodetabek"},
	{3, "BGR01", "KC Bogor", "bogor", "Jabodetabek"},
	{4, "BDG01", "KC Bandung", "bandung", "Jawa Barat"},
	{5, "SBY01", "KC Surabaya", "surabaya", "Jawa Timur"},
	{6, "MDN01", "KC Medan", "medan", "Sumatra"},
	{7, "MKS01", "KC Makassar", "makassar", "Sulawesi"},
	{8, "YGY01", "KC Yogyakarta", "yogyakarta", "Jawa Tengah-DIY"},
}

var (
	DataNasabah  []Nasabah
	DataRekening []Rekening
)

func init() {
	r := rand.New(rand.NewSource(20261123))
	depan := []string{"Ahmad", "Siti", "Muhammad", "Nur", "Rizki", "Dewi", "Fajar", "Aisyah", "Budi", "Fatimah", "Yusuf", "Zahra"}
	belakang := []string{"Pratama", "Hidayat", "Saputra", "Lestari", "Nugroho", "Siregar", "Wibowo", "Hasibuan", "Maulana"}
	for i := 1; i <= 60; i++ {
		c := DataCabang[r.Intn(len(DataCabang))]
		n := Nasabah{ID: i, CIF: fmt.Sprintf("CIF%07d", i), Nama: depan[r.Intn(len(depan))] + " " + belakang[r.Intn(len(belakang))],
			Kota: c.Kota, Segmen: []string{"ritel", "ritel", "prioritas", "umkm"}[r.Intn(4)], CabangID: c.ID}
		DataNasabah = append(DataNasabah, n)
		DataRekening = append(DataRekening, Rekening{ID: i, NoRekening: fmt.Sprintf("71%08d", i), NasabahID: i, CabangID: c.ID,
			Jenis:     []string{"tabungan_wadiah", "tabungan_wadiah", "giro", "deposito_mudharabah"}[r.Intn(4)],
			SaldoAwal: float64(r.Intn(400)+100) * 100_000, Nama: n.Nama})
	}
}

func RekeningByID(id int) (Rekening, bool) {
	for _, r := range DataRekening {
		if r.ID == id {
			return r, true
		}
	}
	return Rekening{}, false
}

func NamaCabang(id int) string {
	for _, c := range DataCabang {
		if c.ID == id {
			return c.Nama
		}
	}
	return fmt.Sprintf("cabang %d", id)
}
