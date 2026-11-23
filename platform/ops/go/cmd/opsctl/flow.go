package main

// API "aliran data": statistik stream/consumer JetStream, aliran live per subject (SSE),
// contoh pesan terakhir per subject (mis. envelope Debezium), dan statistik bronze dari ingestor.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Stream yang diamati dan durable consumer ingestor yang membacanya (lihat ingestor/main.py).
var watched = []struct{ Stream, Consumer, Prefix, Kind string }{
	{"BANK", "de_bronze_bank", "bank.", "app"},
	{"SHOP", "de_bronze_shop", "shop.", "app"},
	{"CDC", "de_bronze_cdc", "cdc.", "cdc"},
}

func streamFor(subject string) (string, string) {
	for _, w := range watched {
		if strings.HasPrefix(subject, w.Prefix) {
			return w.Stream, w.Kind
		}
	}
	return "", ""
}

// liveMsg adalah ringkasan satu pesan yang lewat, dikirim ke browser lewat SSE.
type liveMsg struct {
	At      string `json:"at"`
	Subject string `json:"subject"`
	Stream  string `json:"stream"`
	Kind    string `json:"kind"` // app | cdc
	Bytes   int    `json:"bytes"`
	MsgID   string `json:"msg_id,omitempty"`
	Op      string `json:"op,omitempty"` // cdc: c/u/d/r
	Preview string `json:"preview"`
}

// hub menyebarkan pesan live ke semua klien SSE dan menghitung laju per subject.
type hub struct {
	mu      sync.Mutex
	clients map[chan liveMsg]struct{}
	// counts[detik][subject] untuk laju msg/s 60 detik terakhir
	buckets map[int64]map[string]int
	total   map[string]int64
}

func newHub() *hub {
	return &hub{clients: map[chan liveMsg]struct{}{}, buckets: map[int64]map[string]int{}, total: map[string]int64{}}
}

func (h *hub) publish(m liveMsg) {
	now := time.Now().Unix()
	h.mu.Lock()
	b := h.buckets[now]
	if b == nil {
		b = map[string]int{}
		h.buckets[now] = b
		for k := range h.buckets {
			if k < now-60 {
				delete(h.buckets, k)
			}
		}
	}
	b[m.Subject]++
	h.total[m.Subject]++
	for c := range h.clients {
		select {
		case c <- m:
		default: // klien lambat: lewati (yang penting laju dan total tetap benar)
		}
	}
	h.mu.Unlock()
}

// rates mengembalikan msg/s rata-rata 10 detik terakhir per subject, dan total sejak ops-api start.
func (h *hub) rates() (map[string]float64, map[string]int64, []map[string]any) {
	now := time.Now().Unix()
	h.mu.Lock()
	defer h.mu.Unlock()
	r := map[string]float64{}
	series := []map[string]any{}
	for s := now - 59; s <= now; s++ {
		app, cdc := 0, 0
		for subj, n := range h.buckets[s] {
			if s > now-10 {
				r[subj] += float64(n) / 10
			}
			if strings.HasPrefix(subj, "cdc.") {
				cdc += n
			} else {
				app += n
			}
		}
		series = append(series, map[string]any{"t": s, "app": app, "cdc": cdc})
	}
	tot := make(map[string]int64, len(h.total))
	for k, v := range h.total {
		tot[k] = v
	}
	return r, tot, series
}

func preview(data []byte, n int) string {
	s := string(data)
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// subscribeLive memakai subscription core NATS biasa (bukan consumer JetStream): pesan yang
// diterbitkan ke subject milik stream tetap terlihat oleh subscriber biasa, tanpa mengganggu
// durable consumer ingestor.
func subscribeLive(nc *nats.Conn, h *hub) error {
	for _, w := range watched {
		w := w
		_, err := nc.Subscribe(w.Prefix+">", func(m *nats.Msg) {
			lm := liveMsg{At: time.Now().UTC().Format(time.RFC3339Nano), Subject: m.Subject, Stream: w.Stream,
				Kind: w.Kind, Bytes: len(m.Data), MsgID: m.Header.Get("Nats-Msg-Id"), Preview: preview(m.Data, 280)}
			if w.Kind == "cdc" {
				var env struct {
					Op string `json:"op"`
				}
				_ = json.Unmarshal(m.Data, &env)
				lm.Op = env.Op
			}
			h.publish(lm)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

type flowAPI struct {
	log      *slog.Logger
	js       jetstream.JetStream
	hub      *hub
	ingestor string
	http     *http.Client
}

type consumerView struct {
	Name         string `json:"name"`
	NumPending   uint64 `json:"num_pending"`     // lag: pesan di stream yang belum dikirim ke consumer
	NumAckPend   int    `json:"num_ack_pending"` // sudah dikirim, belum di-ack (menunggu ditulis ke bronze)
	Delivered    uint64 `json:"delivered_seq"`
	AckFloor     uint64 `json:"ack_floor_seq"`
	Redelivered  int    `json:"num_redelivered"`
	LastActiveAt string `json:"last_active,omitempty"`
}

type streamView struct {
	Name      string            `json:"name"`
	Kind      string            `json:"kind"`
	Exists    bool              `json:"exists"`
	Messages  uint64            `json:"messages"`
	Bytes     uint64            `json:"bytes"`
	FirstSeq  uint64            `json:"first_seq"`
	LastSeq   uint64            `json:"last_seq"`
	LastTime  string            `json:"last_time,omitempty"`
	Subjects  map[string]uint64 `json:"subjects"`
	MaxAge    string            `json:"max_age"`
	Consumers []consumerView    `json:"consumers"`
}

func (a *flowAPI) streams(ctx context.Context) []streamView {
	out := []streamView{}
	for _, w := range watched {
		v := streamView{Name: w.Stream, Kind: w.Kind, Subjects: map[string]uint64{}, Consumers: []consumerView{}}
		s, err := a.js.Stream(ctx, w.Stream)
		if err != nil {
			out = append(out, v)
			continue
		}
		info, err := s.Info(ctx, jetstream.WithSubjectFilter(">"))
		if err != nil {
			out = append(out, v)
			continue
		}
		v.Exists = true
		v.Messages, v.Bytes = info.State.Msgs, info.State.Bytes
		v.FirstSeq, v.LastSeq = info.State.FirstSeq, info.State.LastSeq
		if !info.State.LastTime.IsZero() {
			v.LastTime = info.State.LastTime.UTC().Format(time.RFC3339)
		}
		v.MaxAge = info.Config.MaxAge.String()
		for k, n := range info.State.Subjects {
			v.Subjects[k] = n
		}
		lister := s.ListConsumers(ctx)
		for ci := range lister.Info() {
			cv := consumerView{Name: ci.Name, NumPending: ci.NumPending, NumAckPend: ci.NumAckPending,
				Delivered: ci.Delivered.Stream, AckFloor: ci.AckFloor.Stream, Redelivered: ci.NumRedelivered}
			if ci.Delivered.Last != nil {
				cv.LastActiveAt = ci.Delivered.Last.UTC().Format(time.RFC3339)
			}
			v.Consumers = append(v.Consumers, cv)
		}
		sort.Slice(v.Consumers, func(i, j int) bool { return v.Consumers[i].Name < v.Consumers[j].Name })
		out = append(out, v)
	}
	return out
}

func (a *flowAPI) bronze(ctx context.Context) (any, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, a.ingestor+"/stats", nil)
	res, err := a.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var v any
	if err := json.NewDecoder(res.Body).Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func (a *flowAPI) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	rates, totals, series := a.hub.rates()
	resp := map[string]any{"at": time.Now().UTC().Format(time.RFC3339), "streams": a.streams(ctx),
		"rates": rates, "seen_since_start": totals, "series": series}
	if b, err := a.bronze(ctx); err != nil {
		resp["bronze_error"] = err.Error()
	} else {
		resp["bronze"] = b
	}
	writeJSON(w, 200, resp)
}

// handleLive: Server-Sent Events. Browser membuka /api/flow/live dan menerima satu event per
// pesan NATS (dibatasi ~40 pesan/detik per klien agar UI tetap ringan).
func (a *flowAPI) handleLive(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming tidak didukung", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	c := make(chan liveMsg, 256)
	a.hub.mu.Lock()
	a.hub.clients[c] = struct{}{}
	a.hub.mu.Unlock()
	defer func() {
		a.hub.mu.Lock()
		delete(a.hub.clients, c)
		a.hub.mu.Unlock()
	}()
	fmt.Fprint(w, ": terhubung\n\n")
	fl.Flush()
	limit := time.NewTicker(25 * time.Millisecond)
	defer limit.Stop()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		case m := <-c:
			b, _ := json.Marshal(m)
			fmt.Fprintf(w, "data: %s\n\n", b)
			fl.Flush()
			<-limit.C
		}
	}
}

// handleSample mengembalikan pesan terakhir untuk satu subject, mis. cdc.bank.nasabah, agar
// struktur envelope Debezium (before/after/op/source) bisa diperlihatkan di kelas.
func (a *flowAPI) handleSample(w http.ResponseWriter, r *http.Request) {
	subj := r.URL.Query().Get("subject")
	stream, _ := streamFor(subj)
	if stream == "" || strings.ContainsAny(subj, " *>") {
		writeJSON(w, 400, map[string]any{"error": "subject harus konkret dan diawali bank., shop., atau cdc."})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	s, err := a.js.Stream(ctx, stream)
	if err != nil {
		writeJSON(w, 404, map[string]any{"error": "stream " + stream + " belum ada"})
		return
	}
	m, err := s.GetLastMsgForSubject(ctx, subj)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		writeJSON(w, 404, map[string]any{"error": "belum ada pesan untuk " + subj})
		return
	}
	if err != nil {
		writeJSON(w, 502, map[string]any{"error": err.Error()})
		return
	}
	var data any
	if json.Unmarshal(m.Data, &data) != nil {
		data = string(m.Data)
	}
	hdr := map[string]string{}
	for k := range m.Header {
		hdr[k] = m.Header.Get(k)
	}
	writeJSON(w, 200, map[string]any{"stream": stream, "subject": m.Subject, "seq": m.Sequence,
		"time": m.Time.UTC().Format(time.RFC3339Nano), "headers": hdr, "data": data})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, v)
}

// logMW mencatat setiap request sebagai log JSON dengan request_id.
func logMW(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-Id")
		if rid == "" {
			rid = newID()
		}
		w.Header().Set("X-Request-Id", rid)
		start := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path != "/healthz" && r.URL.Path != "/api/flow/stats" && r.URL.Path != "/api/gen/status" {
			log.Info("request", "request_id", rid, "method", r.Method, "path", r.URL.Path, "ms", time.Since(start).Milliseconds())
		}
	})
}

func serve(log *slog.Logger) error {
	nc, js, err := connectNATS(log, "ops-api")
	if err != nil {
		return err
	}
	h := newHub()
	if err := subscribeLive(nc, h); err != nil {
		return err
	}
	api := &flowAPI{log: log, js: js, hub: h, ingestor: env("INGESTOR_URL", "http://ingestor:8080"),
		http: &http.Client{Timeout: 6 * time.Second}}
	gen := newGenerator(log, newGateway(env("GATEWAY_URL", "http://apisix:9080"), env("APISIX_KEY", "")))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": nc.IsConnected()})
	})
	mux.HandleFunc("GET /api/flow/stats", api.handleStats)
	mux.HandleFunc("GET /api/flow/live", api.handleLive)
	mux.HandleFunc("GET /api/flow/sample", api.handleSample)
	gen.routes(mux)
	srv := &http.Server{Addr: ":8080", Handler: logMW(log, mux), ReadHeaderTimeout: 5 * time.Second}
	log.Info("ops-api siap", "addr", srv.Addr)
	return srv.ListenAndServe()
}
