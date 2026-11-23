package main

// Jurnal akad dengan hash chain (slide P07, praktikum mini immutable ledger):
// hash_n = SHA-256(hash_{n-1} + JSON(record, kunci terurut)). Mengubah satu baris memutus rantai
// sesudahnya. Penyerang yang punya akses DB bisa menghitung ulang seluruh rantai, sehingga hash terakhir
// perlu "dijangkarkan" di luar database (di sini Redis, mewakili notaris/blockchain/WORM).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type Entri struct {
	ID       int       `json:"id"`
	Akad     string    `json:"akad"`
	Nasabah  string    `json:"nasabah"`
	Nilai    int64     `json:"nilai"`
	Waktu    time.Time `json:"waktu"`
	HashPrev string    `json:"hash_prev"`
	Hash     string    `json:"hash"`
	// hasil verifikasi
	HashHitung string `json:"hash_hitung"`
	IsiCocok   bool   `json:"isi_cocok"`
	RantaiOK   bool   `json:"rantai_ok"`
}

const nol64 = "0000000000000000000000000000000000000000000000000000000000000000"

func HashEntri(prev string, e Entri) string {
	rec := map[string]any{"id": e.ID, "akad": e.Akad, "nasabah": e.Nasabah, "nilai": e.Nilai, "waktu": e.Waktu.UTC().Format(time.RFC3339)}
	b, _ := json.Marshal(rec) // map: kunci terurut
	h := sha256.Sum256(append([]byte(prev), b...))
	return hex.EncodeToString(h[:])
}

// Verifikasi menghitung ulang setiap hash dan memeriksa sambungan ke entri sebelumnya.
func Verifikasi(es []Entri) []Entri {
	prev := nol64
	for i := range es {
		es[i].HashHitung = HashEntri(es[i].HashPrev, es[i])
		es[i].IsiCocok = es[i].HashHitung == es[i].Hash
		es[i].RantaiOK = es[i].HashPrev == prev
		prev = es[i].Hash
	}
	return es
}

var contohAkad = []struct {
	akad, nasabah string
	nilai         int64
}{
	{"MRB-0001 murabahah rumah", "CIF0000012", 450_000_000}, {"IJR-0002 ijarah kendaraan", "CIF0000031", 180_000_000},
	{"MSY-0003 musyarakah usaha", "CIF0000107", 250_000_000}, {"MDH-0004 mudharabah modal kerja", "CIF0000220", 120_000_000},
	{"MRB-0005 murabahah pendidikan", "CIF0000315", 60_000_000}, {"IJR-0006 ijarah alat medis", "CIF0000402", 95_000_000},
}

func (a *App) ledgerBaca(ctx context.Context) ([]Entri, error) {
	rows, err := a.s.PG.Query(ctx, "SELECT id, akad, nasabah, nilai, waktu, hash_prev, hash FROM jurnal_akad ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var es []Entri
	for rows.Next() {
		var e Entri
		if err := rows.Scan(&e.ID, &e.Akad, &e.Nasabah, &e.Nilai, &e.Waktu, &e.HashPrev, &e.Hash); err != nil {
			return nil, err
		}
		es = append(es, e)
	}
	return es, rows.Err()
}

func (a *App) LedgerStatus(ctx context.Context) (map[string]any, error) {
	es, err := a.ledgerBaca(ctx)
	if err != nil {
		return nil, err
	}
	es = Verifikasi(es)
	jangkar, _ := a.s.RD.Get(ctx, "ledger:jangkar").Result()
	utuh := true
	for _, e := range es {
		utuh = utuh && e.IsiCocok && e.RantaiOK
	}
	akhir := ""
	if len(es) > 0 {
		akhir = es[len(es)-1].Hash
	}
	return map[string]any{"entri": es, "rantai_utuh": utuh, "jangkar": jangkar, "hash_terakhir": akhir,
		"cocok_jangkar": jangkar != "" && jangkar == akhir}, nil
}

func (a *App) LedgerTambah(ctx context.Context, akad, nasabah string, nilai int64) error {
	es, err := a.ledgerBaca(ctx)
	if err != nil {
		return err
	}
	prev, id := nol64, 1
	if len(es) > 0 {
		prev, id = es[len(es)-1].Hash, es[len(es)-1].ID+1
	}
	e := Entri{ID: id, Akad: akad, Nasabah: nasabah, Nilai: nilai, Waktu: time.Now().UTC().Truncate(time.Second), HashPrev: prev}
	e.Hash = HashEntri(prev, e)
	if _, err := a.s.PG.Exec(ctx, "INSERT INTO jurnal_akad VALUES ($1,$2,$3,$4,$5,$6,$7)", e.ID, e.Akad, e.Nasabah, e.Nilai, e.Waktu, e.HashPrev, e.Hash); err != nil {
		return err
	}
	// jangkar: hash terakhir disimpan di luar database jurnal setiap kali ada entri sah
	return a.s.RD.Set(ctx, "ledger:jangkar", e.Hash, 0).Err()
}

func (a *App) LedgerReset(ctx context.Context) error {
	if _, err := a.s.PG.Exec(ctx, "TRUNCATE jurnal_akad"); err != nil {
		return err
	}
	_ = a.s.RD.Del(ctx, "ledger:jangkar").Err()
	for _, c := range contohAkad {
		if err := a.LedgerTambah(ctx, c.akad, c.nasabah, c.nilai); err != nil {
			return err
		}
	}
	return nil
}

// LedgerSunting mengubah nilai satu entri langsung di tabel (seperti admin DB yang nakal).
func (a *App) LedgerSunting(ctx context.Context, id int, nilai int64) error {
	tag, err := a.s.PG.Exec(ctx, "UPDATE jurnal_akad SET nilai = $2 WHERE id = $1", id, nilai)
	if err == nil && tag.RowsAffected() == 0 {
		return fmt.Errorf("entri %d tidak ada", id)
	}
	return err
}

// LedgerTutupi menghitung ulang seluruh rantai sesudah sunting: rantai di DB tampak utuh lagi,
// tetapi hash terakhir tidak lagi sama dengan jangkar di luar DB.
func (a *App) LedgerTutupi(ctx context.Context) error {
	es, err := a.ledgerBaca(ctx)
	if err != nil {
		return err
	}
	prev := nol64
	for _, e := range es {
		e.HashPrev = prev
		e.Hash = HashEntri(prev, e)
		if _, err := a.s.PG.Exec(ctx, "UPDATE jurnal_akad SET hash_prev=$2, hash=$3 WHERE id=$1", e.ID, e.HashPrev, e.Hash); err != nil {
			return err
		}
		prev = e.Hash
	}
	return nil
}
