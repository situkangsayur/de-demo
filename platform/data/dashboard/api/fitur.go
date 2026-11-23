package main

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
)

// Fitur untuk tim AI: gold.fitur_nasabah (grain satu nasabah per tgl_fitur, point-in-time).
// Query sama untuk kedua mesin (SQL standar); {tgl} dan {nid} diganti sebelum dijalankan.
var sqlFitur = map[string]string{
	"ringkas": `SELECT tgl_fitur, count(*) AS nasabah, round(avg(trx_30h), 2) AS rata_trx_30h,
       round(avg(nominal_30h), 2) AS rata_nominal_30h, sum(belanja_ecom_30h) AS belanja_ecom_30h,
       sum(CASE WHEN akad_cair_kumulatif > 0 THEN 1 ELSE 0 END) AS nasabah_berpembiayaan
FROM gold.fitur_nasabah WHERE tgl_fitur BETWEEN {d}'{tgl}'{d2} - 13 AND {d}'{tgl}'{d2}
GROUP BY tgl_fitur ORDER BY tgl_fitur`,
	"sampel": `SELECT nasabah_id, tgl_fitur, segmen_saat_itu, kota_saat_itu, umur_nasabah_hari, trx_7h, nominal_7h, trx_30h,
       nominal_30h, hari_sejak_trx_terakhir, rasio_digital_30h, belanja_ecom_30h, nominal_belanja_ecom_30h,
       akad_cair_kumulatif, pokok_cair_kumulatif
FROM gold.fitur_nasabah WHERE tgl_fitur = {d}'{tgl}'{d2}
ORDER BY nominal_30h DESC, nasabah_id LIMIT 25`,
	"riwayat": `SELECT tgl_fitur, nasabah_sk_saat_itu, segmen_saat_itu, kota_saat_itu, trx_7h, trx_30h, nominal_30h,
       hari_sejak_trx_terakhir, akad_cair_kumulatif
FROM gold.fitur_nasabah WHERE nasabah_id = '{nid}' AND tgl_fitur <= {d}'{tgl}'{d2}
ORDER BY tgl_fitur DESC LIMIT 30`,
}

var reID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,40}$`)

func isiSQL(q, sumber, tgl, nid string) string {
	d, d2 := "toDate(", ")"
	if sumber == "lake" {
		d, d2 = "DATE ", ""
	}
	return strings.NewReplacer("{d}", d, "{d2}", d2, "{tgl}", tgl, "{nid}", nid).Replace(q)
}

func (a *App) handleFitur(w http.ResponseWriter, r *http.Request) {
	tgl, err := tglAcuan(r)
	if err != nil {
		writeErr(w, r, http.StatusBadRequest, err)
		return
	}
	nid := r.URL.Query().Get("nasabah_id")
	if nid == "" {
		nid = "1"
	}
	if !reID.MatchString(nid) {
		writeErr(w, r, http.StatusBadRequest, fmt.Errorf("nasabah_id tidak valid"))
		return
	}
	out := map[string]any{"tgl": tgl, "nasabah_id": nid}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, sumber := range []string{"warehouse", "lake"} {
		for nama, q := range sqlFitur {
			wg.Add(1)
			go func(sumber, nama, q string) {
				defer wg.Done()
				sql := isiSQL(q, sumber, tgl, nid)
				var h *Hasil
				var err error
				if sumber == "lake" {
					h, err = a.lake.Query(r.Context(), sql)
				} else {
					h, err = a.ch.Query(r.Context(), sql)
				}
				if err != nil {
					h = &Hasil{Sumber: sumber, Error: err.Error(), Kolom: []string{}, Baris: [][]any{}}
				}
				mu.Lock()
				out[sumber+"_"+nama] = h
				mu.Unlock()
			}(sumber, nama, q)
		}
	}
	wg.Wait()
	identik := true
	var selisih []string
	for _, nama := range []string{"ringkas", "sampel", "riwayat"} {
		s, b := bandingkan(out["warehouse_"+nama].(*Hasil), out["lake_"+nama].(*Hasil))
		identik = identik && s
		for _, x := range b {
			selisih = append(selisih, nama+": "+x)
		}
	}
	out["identik"] = identik
	out["selisih"] = selisih
	writeJSON(w, http.StatusOK, out)
}
