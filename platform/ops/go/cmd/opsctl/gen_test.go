package main

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func testGen() *generator {
	return newGenerator(slog.New(slog.NewJSONHandler(io.Discard, nil)), newGateway("http://127.0.0.1:1", ""))
}

func TestWeightedRespectsWeights(t *testing.T) {
	g := testGen()
	n := map[string]int{}
	for i := 0; i < 20000; i++ {
		n[g.weighted(map[string]float64{"a": 0.9, "b": 0.1, "c": 0})]++
	}
	if n["c"] != 0 || n["a"] < 17000 || n["b"] < 1500 {
		t.Fatalf("distribusi tidak sesuai bobot: %v", n)
	}
}

func TestLognormalRoundedAndPositive(t *testing.T) {
	g := testGen()
	for i := 0; i < 1000; i++ {
		v := g.lognormal(400_000, 0.7, 50_000)
		if v <= 0 || v%50_000 != 0 {
			t.Fatalf("nominal %d harus positif dan kelipatan 50.000 (ATM)", v)
		}
	}
}

func TestWaktuDalamHariStaysOnDateAndNotFuture(t *testing.T) {
	g := testGen()
	day := time.Now().In(wib).AddDate(0, 0, -3)
	for i := 0; i < 500; i++ {
		w := g.waktuDalamHari(day)
		if w.Format("2006-01-02") != day.Format("2006-01-02") {
			t.Fatalf("waktu %v keluar dari tanggal %v", w, day)
		}
	}
	today := time.Now().In(wib)
	for i := 0; i < 200; i++ {
		if g.waktuDalamHari(today).After(time.Now().Add(time.Second)) {
			t.Fatal("waktu di masa depan")
		}
	}
}

func TestLateOccurredAtGoesBackwards(t *testing.T) {
	g := testGen()
	g.cfg.Dirt.Terlambat = 1
	occ, late := g.occurred(opts{cabSpike: -1}, true)
	ts, err := time.Parse(time.RFC3339, occ)
	if err != nil || !late || time.Since(ts) < time.Hour {
		t.Fatalf("occurred_at terlambat harus mundur >= 1 jam: %s %v %v", occ, late, err)
	}
}

func TestAsMapAndList(t *testing.T) {
	if asMap(map[string]any{"akad": map[string]any{"id": "x"}}, "akad")["id"] != "x" {
		t.Fatal("asMap bungkus")
	}
	if asMap(map[string]any{"id": "y"}, "nasabah")["id"] != "y" {
		t.Fatal("asMap langsung")
	}
	if len(asList([]any{map[string]any{"id": 1}}, "cabang")) != 1 {
		t.Fatal("asList array")
	}
	if str(float64(4)) != "4" {
		t.Fatal("str angka bulat")
	}
}
