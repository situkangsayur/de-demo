package main

// Klien HTTP ke aplikasi Sister lewat APISIX (satu pintu masuk, header apikey).
// Semua path API aplikasi dikumpulkan di sini agar mudah disesuaikan bila API bank berubah.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type gateway struct {
	base string
	key  string
	http *http.Client
}

func newGateway(base, key string) *gateway {
	return &gateway{base: base, key: key, http: &http.Client{Timeout: 10 * time.Second}}
}

// apiErr adalah jawaban non-2xx dari aplikasi; status disimpan agar generator bisa membedakan
// penolakan bisnis (400/409, mis. saldo kurang) dari gangguan (5xx, 429).
type apiErr struct {
	Status int
	Body   string
}

func (e *apiErr) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body) }

// call mengirim request JSON. idemKey kosong = tanpa Idempotency-Key.
func (g *gateway) call(ctx context.Context, method, path string, body any, idemKey string, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, g.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if g.key != "" {
		req.Header.Set("apikey", g.key)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	req.Header.Set("X-Client", "de-traffic-generator")
	res, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if res.StatusCode/100 != 2 {
		s := string(b)
		if len(s) > 300 {
			s = s[:300]
		}
		return &apiErr{Status: res.StatusCode, Body: s}
	}
	if out != nil && len(b) > 0 {
		return json.Unmarshal(b, out)
	}
	return nil
}

// ---- path API aplikasi (APISIX). Sesuaikan di sini bila penulis S mengubah route.
const (
	pathCabang    = "/api/bank/cabang"
	pathNasabah   = "/api/bank/nasabah"
	pathRekening  = "/api/bank/rekening"
	pathTransaksi = "/api/bank/transaksi"
	pathTransfer  = "/api/bank/transfer"
	pathAkad      = "/api/bank/akad"
	pathProducts  = "/api/catalog/products"
	pathOrders    = "/api/orders"
)
