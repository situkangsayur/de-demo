package main

import (
	"testing"
	"time"
)

func TestPersentil(t *testing.T) {
	x := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if Persentil(x, 50) != 5 || Persentil(x, 95) != 10 || Persentil(x, 10) != 1 {
		t.Fatalf("persentil salah: %v %v %v", Persentil(x, 50), Persentil(x, 95), Persentil(x, 10))
	}
	if Persentil(nil, 50) != 0 {
		t.Fatal("kosong harus 0")
	}
}

func TestBangkitkanDeterministikDanUrut(t *testing.T) {
	akhir := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	a, b := Bangkitkan(100, 2000, 30, akhir), Bangkitkan(100, 2000, 30, akhir)
	if len(a.Trx) != 2000 || a.Trx[1999] != b.Trx[1999] {
		t.Fatal("tidak deterministik")
	}
	for i := 1; i < len(a.Trx); i++ {
		if a.Trx[i].Waktu.Before(a.Trx[i-1].Waktu) || a.Trx[i].ID != int64(i+1) {
			t.Fatal("transaksi harus urut waktu dengan id berurutan")
		}
	}
	if a.Trx[0].Waktu.Before(akhir.Add(-30*24*time.Hour)) || !a.Trx[1999].Waktu.Before(akhir) {
		t.Fatal("waktu di luar jendela")
	}
}

func TestHashChainMendeteksiSuntingan(t *testing.T) {
	prev := nol64
	var es []Entri
	for i, c := range contohAkad[:3] {
		e := Entri{ID: i + 1, Akad: c.akad, Nasabah: c.nasabah, Nilai: c.nilai, Waktu: time.Unix(1795400000, 0), HashPrev: prev}
		e.Hash = HashEntri(prev, e)
		prev = e.Hash
		es = append(es, e)
	}
	for _, e := range Verifikasi(es) {
		if !e.IsiCocok || !e.RantaiOK {
			t.Fatal("rantai asli harus utuh")
		}
	}
	es[1].Nilai = 1
	v := Verifikasi(es)
	if v[1].IsiCocok || !v[0].IsiCocok || !v[2].IsiCocok {
		t.Fatalf("suntingan entri 2 harus terdeteksi tepat di entri 2: %+v", v)
	}
}
