package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func hitung(t *testing.T, pola string) (int, map[string]int) {
	t.Helper()
	files, _ := filepath.Glob(pola)
	n, id := 0, map[string]int{}
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(fh)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var b map[string]any
			if err := json.Unmarshal(sc.Bytes(), &b); err != nil {
				t.Fatal(err)
			}
			n++
			id[b["_event_id"].(string)]++
		}
		fh.Close()
	}
	return n, id
}

func TestHariDeterministikDanKontrol(t *testing.T) {
	g := &Generator{Dir: filepath.Join(t.TempDir(), "bronze")}
	k1, err := g.Hari("2026-11-16", false)
	if err != nil {
		t.Fatal(err)
	}
	k2, _ := g.Hari("2026-11-16", false)
	if k1 != k2 {
		t.Fatalf("tidak deterministik: %+v vs %+v", k1, k2)
	}
	_, ids := hitung(t, filepath.Join(g.partisi("transaksi", "2026-11-16"), "part-*.jsonl"))
	if len(ids) != k1.JumlahTrx {
		t.Fatalf("event unik %d != kontrol %d", len(ids), k1.JumlahTrx)
	}
	if _, err := os.Stat(filepath.Join(g.partisi("transaksi", "2026-11-16"), "_SUCCESS")); err != nil {
		t.Fatal("penanda _SUCCESS tidak ada")
	}
}

func TestSabotaseDanPulihkan(t *testing.T) {
	g := &Generator{Dir: filepath.Join(t.TempDir(), "bronze")}
	if _, err := g.Hari("2026-11-16", false); err != nil {
		t.Fatal(err)
	}
	n, err := g.Sabotase("2026-11-16")
	if err != nil || n != 55 {
		t.Fatalf("sabotase n=%d err=%v", n, err)
	}
	if st := g.Status([]string{"2026-11-16"}); !st[0].Sabotase {
		t.Fatal("status sabotase tidak terbaca")
	}
	if err := g.Pulihkan("2026-11-16"); err != nil {
		t.Fatal(err)
	}
	if st := g.Status([]string{"2026-11-16"}); st[0].Sabotase {
		t.Fatal("berkas sabotase masih ada")
	}
}

func TestTglDariWIB(t *testing.T) {
	s := "2026-11-15T17:00:00Z" // = 16 Nov 00.00 WIB
	if got := TglDari(&s); got != "2026-11-16" {
		t.Fatalf("got %s", got)
	}
}
