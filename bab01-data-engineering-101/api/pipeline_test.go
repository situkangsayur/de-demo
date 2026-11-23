package main

import (
	"testing"
	"time"
)

func jalankan(p *Pipeline, now time.Time) time.Time {
	now = now.Add(5 * time.Second)
	p.mu.Lock()
	p.antarPesan(now)
	p.flush(now)
	p.batch(now)
	p.mu.Unlock()
	return now
}

func TestStandarKanal(t *testing.T) {
	kasus := map[string]string{"Mobile Banking": "mobile", "teller ": "teller", "WEB": "web", "e-commerce": "ecommerce", "qris": ""}
	for in, harap := range kasus {
		if got := StandarKanal(in); got != harap {
			t.Errorf("StandarKanal(%q) = %q, harap %q", in, got, harap)
		}
	}
}

func TestTransaksiBersihSampaiGold(t *testing.T) {
	p := BaruPipeline(1)
	h := p.Kirim(Permintaan{RekeningID: 1, Kanal: "Mobile Banking", Jenis: "setor", Nominal: "150000"}, "kelas")
	if h.Status != 201 || len(h.TrxID) != 1 {
		t.Fatalf("hasil kirim: %+v", h)
	}
	jalankan(p, time.Now())
	s, ok := p.silver[h.TrxID[0]]
	if !ok {
		t.Fatal("transaksi tidak sampai silver")
	}
	if s.Kanal != "mobile" || len(s.Sumber) != 2 {
		t.Errorf("silver: kanal %q sumber %v (harap mobile, app+cdc)", s.Kanal, s.Sumber)
	}
	d := p.Dashboard(time.Now().Add(10 * time.Second))
	if d.Rekonsiliasi["cocok"] != true {
		t.Errorf("rekonsiliasi silver vs gold tidak cocok: %v", d.Rekonsiliasi)
	}
	if p.jejak[h.TrxID[0]].Status != "selesai" {
		t.Errorf("status jejak %q, harap selesai", p.jejak[h.TrxID[0]].Status)
	}
}

func TestKotoranTertangkap(t *testing.T) {
	p := BaruPipeline(2)
	if h := p.Kirim(Permintaan{RekeningID: 2, Kanal: "mobile", Nominal: "1000", GeserMenit: 30}, "kelas"); h.Status != 400 {
		t.Errorf("masa depan: status %d, harap 400", h.Status)
	}
	if h := p.Kirim(Permintaan{RekeningID: 2, Kanal: "mobile", Jenis: "tarik", Nominal: "999999999999"}, "kelas"); h.Status != 409 {
		t.Errorf("saldo kurang: status %d, harap 409", h.Status)
	}
	kosong := p.Kirim(Permintaan{RekeningID: 3, Kanal: "atm", Jenis: "setor", Nominal: ""}, "kelas")
	ganda := p.Kirim(Permintaan{RekeningID: 4, Kanal: "web", Jenis: "setor", Nominal: "50000", KirimUlang: "tanpa_kunci"}, "kelas")
	aman := p.Kirim(Permintaan{RekeningID: 5, Kanal: "web", Jenis: "setor", Nominal: "50000", KirimUlang: "dengan_kunci"}, "kelas")
	p.Kirim(Permintaan{RekeningID: 6, Kanal: "mobile", Jenis: "setor", Nominal: "70000", AckHilang: true, UlangPublish: true}, "kelas")
	jalankan(p, time.Now())
	if _, ok := p.silver[kosong.TrxID[0]]; ok || !p.karID[kosong.TrxID[0]] {
		t.Error("nominal kosong seharusnya dikarantina")
	}
	if len(ganda.TrxID) != 2 || len(p.silver[ganda.TrxID[1]].Flag) == 0 {
		t.Error("request ulang tanpa Idempotency-Key seharusnya ditandai dugaan duplikat bisnis")
	}
	if _, ok := p.silver[aman.TrxID[1]]; ok {
		t.Error("request ulang dengan Idempotency-Key tidak boleh menjadi transaksi kedua")
	}
	if p.hit["silver"].Tolak["duplikat event_id (redelivery)"] == 0 {
		t.Error("redelivery seharusnya terdeteksi di silver")
	}
	if p.hit["event_app"].Tolak["publish ulang, Nats-Msg-Id sama (dibuang JetStream)"] == 0 {
		t.Error("publish ulang seharusnya dibuang jendela duplikat")
	}
}

func TestDataTerlambatMasukPartisiAsli(t *testing.T) {
	p := BaruPipeline(3)
	h := p.Kirim(Permintaan{RekeningID: 7, Kanal: "teller", Jenis: "setor", Nominal: "250000", GeserMenit: -26 * 60}, "kelas")
	jalankan(p, time.Now())
	s := p.silver[h.TrxID[0]]
	kemarin := time.Now().Add(-26 * time.Hour).In(WIB).Format("2006-01-02")
	if s == nil || s.Tanggal != kemarin {
		t.Fatalf("silver %+v, harap tanggal %s", s, kemarin)
	}
	ada := false
	for _, f := range s.Flag {
		ada = ada || f == "data terlambat"
	}
	if !ada {
		t.Error("flag data terlambat tidak ada")
	}
}
