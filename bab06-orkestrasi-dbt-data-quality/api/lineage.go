package main

// Lineage dari manifest.json dbt: source -> model -> test, diberi status hasil terakhir.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Simpul struct {
	ID        string   `json:"id"`
	Nama      string   `json:"nama"`
	Jenis     string   `json:"jenis"` // source | model | test
	Materi    string   `json:"materialisasi,omitempty"`
	Dimensi   string   `json:"dimensi,omitempty"`
	Status    string   `json:"status,omitempty"`
	Failures  int      `json:"failures,omitempty"`
	Deskripsi string   `json:"deskripsi,omitempty"`
	Hulu      []string `json:"hulu"`
	SQL       string   `json:"sql,omitempty"`
}

type manifest struct {
	Nodes   map[string]manifestNode `json:"nodes"`
	Sources map[string]manifestNode `json:"sources"`
}

type manifestNode struct {
	UniqueID     string `json:"unique_id"`
	Name         string `json:"name"`
	ResourceType string `json:"resource_type"`
	Description  string `json:"description"`
	RawCode      string `json:"raw_code"`
	Config       struct {
		Materialized string         `json:"materialized"`
		Meta         map[string]any `json:"meta"`
	} `json:"config"`
	DependsOn struct {
		Nodes []string `json:"nodes"`
	} `json:"depends_on"`
	SourceName string `json:"source_name"`
}

type runResults struct {
	Results []struct {
		UniqueID string `json:"unique_id"`
		Status   string `json:"status"`
		Failures *int   `json:"failures"`
	} `json:"results"`
}

func bacaJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// statusTerakhir mengambil status per unique_id dari berkas runs/*-<jenis>.json terbaru.
func (a *App) statusTerakhir(jenis string) map[string][2]any {
	files, _ := filepath.Glob(filepath.Join(a.dbtDir, "runs", "*-"+jenis+".json"))
	sort.Slice(files, func(i, j int) bool {
		fi, _ := os.Stat(files[i])
		fj, _ := os.Stat(files[j])
		return fi.ModTime().Before(fj.ModTime())
	})
	out := map[string][2]any{}
	if len(files) == 0 {
		return out
	}
	var rr runResults
	if bacaJSON(files[len(files)-1], &rr) != nil {
		return out
	}
	for _, r := range rr.Results {
		f := 0
		if r.Failures != nil {
			f = *r.Failures
		}
		out[r.UniqueID] = [2]any{r.Status, f}
	}
	return out
}

func (a *App) lineage(w http.ResponseWriter, r *http.Request) {
	var m manifest
	if err := bacaJSON(filepath.Join(a.dbtDir, "target", "manifest.json"), &m); err != nil {
		galat(w, 404, fmt.Errorf("manifest dbt belum ada: jalankan DAG sekali (%v)", err))
		return
	}
	stRun, stTest := a.statusTerakhir("run"), a.statusTerakhir("test")
	var out []Simpul
	for id, s := range m.Sources {
		out = append(out, Simpul{ID: id, Nama: s.SourceName + "." + s.Name, Jenis: "source", Deskripsi: s.Description, Hulu: []string{}})
	}
	for id, n := range m.Nodes {
		if n.ResourceType != "model" && n.ResourceType != "test" {
			continue
		}
		s := Simpul{ID: id, Nama: n.Name, Jenis: n.ResourceType, Materi: n.Config.Materialized, Deskripsi: n.Description,
			Hulu: n.DependsOn.Nodes}
		if s.Hulu == nil {
			s.Hulu = []string{}
		}
		if n.ResourceType == "model" {
			s.SQL = strings.TrimSpace(n.RawCode)
			if st, ok := stRun[id]; ok {
				s.Status = st[0].(string)
			}
		} else {
			if d, ok := n.Config.Meta["dimensi"].(string); ok {
				s.Dimensi = d
			}
			if st, ok := stTest[id]; ok {
				s.Status, s.Failures = st[0].(string), st[1].(int)
			}
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	tulis(w, 200, map[string]any{"simpul": out})
}
