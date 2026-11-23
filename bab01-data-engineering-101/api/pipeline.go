package main

// Pipeline mini satu proses: aplikasi (OLTP) -> event aplikasi + CDC -> NATS -> bronze -> silver -> gold
// -> dashboard. Setiap tahap dijalankan sungguhan di memori dengan jadwalnya sendiri (micro-batch
// ingestor, job silver/gold berkala) sehingga latensi yang terukur nyata, walaupun skalanya kecil.
// Skema baris mengikuti platform/KONTRAK-DATA.md (transaksi bank, 8 kolom metadata bronze).

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

var WIB = time.FixedZone("WIB", 7*3600)

// Transaksi adalah baris tabel OLTP `transaksi` (bertipe benar, seperti di PostgreSQL aplikasi).
type Transaksi struct {
	ID             string    `json:"id"`
	RekeningID     int       `json:"rekening_id"`
	CabangID       int       `json:"cabang_id"`
	Kanal          string    `json:"kanal"`
	Jenis          string    `json:"jenis"`
	Nominal        *float64  `json:"nominal"`
	OccurredAt     time.Time `json:"occurred_at"`
	CreatedAt      time.Time `json:"created_at"`
	IdempotencyKey string    `json:"idempotency_key"`
	SaldoSetelah   float64   `json:"saldo_setelah"`
}

// Pesan adalah satu pesan di NATS JetStream (stream BANK untuk event aplikasi, CDC untuk Debezium).
type Pesan struct {
	Stream    string         `json:"stream"`
	Subject   string         `json:"subject"`
	MsgID     string         `json:"nats_msg_id"`
	Payload   map[string]any `json:"payload"`
	trxID     string
	tiba      time.Time // kapan pesan sampai ke consumer (latensi publish/CDC)
	duplikasi bool      // ack hilang: consumer menerima pesan yang sama dua kali
}

// BarisBronze: kolom metadata W1 + kolom data diratakan (semuanya string, casting di silver).
type BarisBronze struct {
	IngestedAt string            `json:"_ingested_at"`
	Source     string            `json:"_source"`
	BatchID    string            `json:"_batch_id"`
	EventID    string            `json:"_event_id"`
	EventType  string            `json:"_event_type"`
	OccurredAt string            `json:"_occurred_at"`
	TsMs       string            `json:"_ts_ms"`
	Op         *string           `json:"_op"`
	Data       map[string]string `json:"data"`
	Path       string            `json:"path"`
	trxID      string
	diproses   bool
}

// BarisSilver: tipe benar, kode kanal standar, satu baris per transaksi.
type BarisSilver struct {
	TrxID      string   `json:"trx_id"`
	RekeningID int      `json:"rekening_id"`
	CabangID   int      `json:"cabang_id"`
	Kanal      string   `json:"kanal"`
	Jenis      string   `json:"jenis"`
	Nominal    float64  `json:"nominal"`
	OccurredAt string   `json:"occurred_at"`
	Tanggal    string   `json:"tanggal"`
	Sumber     []string `json:"sumber"`
	Flag       []string `json:"flag"`
	tOccurred  time.Time
	idemKey    string
}

// Jejak menyimpan waktu tiap tahap untuk satu transaksi (ditampilkan sebagai garis waktu).
type Jejak struct {
	TrxID      string            `json:"trx_id"`
	Mulai      time.Time         `json:"mulai"`
	Waktu      map[string]string `json:"waktu"` // tahap -> RFC3339Nano
	Catatan    []string          `json:"catatan"`
	Status     string            `json:"status"` // berjalan | selesai | ditolak | karantina
	Asal       string            `json:"asal"`   // kelas | generator | backfill
	versiGold  int
	tahapWaktu map[string]time.Time
}

// Tahap dan perannya; urutan dipakai UI.
var Tahap = []string{"aplikasi", "event_app", "event_cdc", "bronze", "silver", "gold", "dashboard"}

type Penghitung struct {
	Masuk  int            `json:"masuk"`
	Keluar int            `json:"keluar"`
	Tolak  map[string]int `json:"tertangkap"`
}

func baruHitung() *Penghitung { return &Penghitung{Tolak: map[string]int{}} }

type SelGold struct {
	Tanggal  string  `json:"tanggal"`
	CabangID int     `json:"cabang_id"`
	Kanal    string  `json:"kanal"`
	Jumlah   int     `json:"jumlah"`
	Nominal  float64 `json:"nominal"`
}

type Konfig struct {
	FlushDetik  float64 `json:"flush_detik"`
	BatchDetik  float64 `json:"batch_detik"`
	CDCMilidtk  int     `json:"cdc_milidetik"`
	GenAktif    bool    `json:"generator_aktif"`
	GenLaju     float64 `json:"generator_laju"`
	GenKotorPct float64 `json:"generator_persen_kotor"`
}

type Pipeline struct {
	mu        sync.Mutex
	rnd       *rand.Rand
	cfg       Konfig
	oltp      []Transaksi
	idem      map[string]string // idempotency_key -> trx id
	saldo     map[int]float64
	antre     []*Pesan
	msgIDs    map[string]time.Time // jendela duplikat JetStream (2 menit)
	buffer    []*BarisBronze       // belum di-flush ingestor
	bronze    []*BarisBronze
	berkas    map[string]int // path -> jumlah baris
	silver    map[string]*BarisSilver
	silverUrt []string
	eventSeen map[string]bool
	karantina []map[string]string
	karID     map[string]bool
	gold      map[string]*SelGold
	versiGold int
	goldAt    time.Time
	hit       map[string]*Penghitung
	jejak     map[string]*Jejak
	urutJejak []string
	hot       struct {
		jumlah  int
		nominal float64
	}
	nTrx                         int
	nBatch                       int
	lsn                          int64
	terakhirFlush, terakhirBatch time.Time
}

func BaruPipeline(seed int64) *Pipeline {
	p := &Pipeline{rnd: rand.New(rand.NewSource(seed)),
		cfg: Konfig{FlushDetik: 5, BatchDetik: 15, CDCMilidtk: 800, GenLaju: 2, GenKotorPct: 8}}
	p.reset()
	return p
}

func (p *Pipeline) reset() {
	p.oltp, p.antre, p.buffer, p.bronze, p.silverUrt, p.karantina, p.urutJejak = nil, nil, nil, nil, nil, nil, nil
	p.idem, p.msgIDs, p.berkas = map[string]string{}, map[string]time.Time{}, map[string]int{}
	p.silver, p.eventSeen, p.gold, p.jejak = map[string]*BarisSilver{}, map[string]bool{}, map[string]*SelGold{}, map[string]*Jejak{}
	p.karID = map[string]bool{}
	p.saldo = map[int]float64{}
	for _, r := range DataRekening {
		p.saldo[r.ID] = r.SaldoAwal
	}
	p.hit = map[string]*Penghitung{}
	for _, t := range Tahap {
		p.hit[t] = baruHitung()
	}
	p.hot.jumlah, p.hot.nominal, p.nTrx, p.nBatch, p.versiGold, p.lsn = 0, 0, 0, 0, 0, 1_000_000
	p.terakhirFlush, p.terakhirBatch = time.Now(), time.Now()
}

// Permintaan transaksi dari UI atau generator.
type Permintaan struct {
	RekeningID   int     `json:"rekening_id"`
	Kanal        string  `json:"kanal"`
	Jenis        string  `json:"jenis"`
	Nominal      string  `json:"nominal"` // string agar "kosong" bisa dikirim
	GeserMenit   float64 `json:"geser_menit"`
	KirimUlang   string  `json:"kirim_ulang"` // "" | "tanpa_kunci" | "dengan_kunci"
	UlangPublish bool    `json:"ulang_publish"`
	AckHilang    bool    `json:"ack_hilang"`
}

type Hasil struct {
	Status int      `json:"status"`
	Pesan  string   `json:"pesan"`
	TrxID  []string `json:"trx_id"`
}

var ErrMasaDepan = errors.New("occurred_at lebih dari 5 menit di masa depan")

// Kirim menjalankan tahap aplikasi: validasi, tulis OLTP, outbox -> event, WAL -> CDC.
func (p *Pipeline) Kirim(q Permintaan, asal string) Hasil {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.kirim(q, asal, time.Now())
}

func (p *Pipeline) kirim(q Permintaan, asal string, now time.Time) Hasil {
	rek, ok := RekeningByID(q.RekeningID)
	if !ok {
		rek = DataRekening[p.rnd.Intn(len(DataRekening))]
	}
	if q.Jenis == "" {
		q.Jenis = "transfer_keluar"
	}
	occ := now.Add(time.Duration(q.GeserMenit * float64(time.Minute)))
	ulang := 1
	kunci := fmt.Sprintf("idk-%d-%d", now.UnixNano(), p.rnd.Intn(1e6))
	if q.KirimUlang == "tanpa_kunci" || q.KirimUlang == "dengan_kunci" {
		ulang = 2
	}
	if q.KirimUlang == "tanpa_kunci" {
		kunci = ""
	}
	res := Hasil{Status: 201, Pesan: "transaksi tercatat"}
	for k := 0; k < ulang; k++ {
		t := now.Add(time.Duration(k) * 3 * time.Second / 1000) // request ulang beberapa milidetik kemudian
		p.nTrx++
		id := fmt.Sprintf("TRX%s%07d", t.In(WIB).Format("20060102"), p.nTrx)
		j := &Jejak{TrxID: id, Mulai: t, Waktu: map[string]string{}, tahapWaktu: map[string]time.Time{}, Status: "berjalan", Asal: asal}
		p.simpanJejak(j)
		p.hit["aplikasi"].Masuk++
		if occ.Sub(now) > 5*time.Minute {
			p.hit["aplikasi"].Tolak["occurred_at di masa depan (400)"]++
			j.Status, j.Catatan = "ditolak", append(j.Catatan, "aplikasi menolak: "+ErrMasaDepan.Error()+" (HTTP 400)")
			p.tandai(j, "aplikasi", t)
			return Hasil{Status: 400, Pesan: ErrMasaDepan.Error(), TrxID: []string{id}}
		}
		var nom *float64
		if v, err := strconv.ParseFloat(strings.TrimSpace(q.Nominal), 64); err == nil {
			nom = &v
		}
		keluar := strings.HasSuffix(q.Jenis, "keluar") || q.Jenis == "tarik" || q.Jenis == "bayar_belanja"
		if nom != nil && keluar && *nom > p.saldo[rek.ID] {
			p.hit["aplikasi"].Tolak["saldo kurang (409)"]++
			j.Status, j.Catatan = "ditolak", append(j.Catatan, "aplikasi menolak: saldo kurang (HTTP 409)")
			p.tandai(j, "aplikasi", t)
			return Hasil{Status: 409, Pesan: "saldo kurang", TrxID: []string{id}}
		}
		if kunci != "" {
			if lama, ada := p.idem[kunci]; ada {
				p.hit["aplikasi"].Tolak["request ulang dengan Idempotency-Key (dijawab 200, tidak dicatat ganda)"]++
				j.Status = "tidak dicatat ulang"
				j.Catatan = append(j.Catatan, "request ulang membawa Idempotency-Key yang sama: aplikasi mengembalikan "+lama+", tidak ada transaksi baru")
				p.tandai(j, "aplikasi", t)
				res.TrxID = append(res.TrxID, id)
				continue
			}
			p.idem[kunci] = id
		}
		if nom != nil {
			if keluar {
				p.saldo[rek.ID] -= *nom
			} else {
				p.saldo[rek.ID] += *nom
			}
		}
		trx := Transaksi{ID: id, RekeningID: rek.ID, CabangID: rek.CabangID, Kanal: q.Kanal, Jenis: q.Jenis, Nominal: nom,
			OccurredAt: occ, CreatedAt: t, IdempotencyKey: kunci, SaldoSetelah: p.saldo[rek.ID]}
		p.oltp = append(p.oltp, trx)
		p.hit["aplikasi"].Keluar++
		if q.KirimUlang == "tanpa_kunci" && k == 1 {
			j.Catatan = append(j.Catatan, "request ulang tanpa Idempotency-Key: aplikasi mencatat transaksi kedua (duplikat bisnis)")
		}
		if q.GeserMenit < -1 {
			j.Catatan = append(j.Catatan, fmt.Sprintf("occurred_at mundur %.0f menit (perangkat offline): data terlambat", -q.GeserMenit))
		}
		p.tandai(j, "aplikasi", t.Add(time.Duration(200+p.rnd.Intn(800))*time.Microsecond))
		// event aplikasi (outbox -> BANK) dan CDC (WAL -> Debezium -> CDC)
		ev := Pesan{Stream: "BANK", Subject: "bank.transaksi.dibuat", MsgID: uuidAcak(p.rnd), trxID: id,
			Payload: map[string]any{"event_type": "bank.transaksi.dibuat", "occurred_at": occ.In(WIB).Format("2006-01-02T15:04:05.000-07:00"), "data": kolomString(trx)},
			tiba:    t.Add(time.Duration(2+p.rnd.Intn(15)) * time.Millisecond), duplikasi: q.AckHilang}
		ev.Payload["event_id"] = ev.MsgID
		p.antre = append(p.antre, &ev)
		if q.UlangPublish {
			ev2 := ev
			ev2.tiba = ev.tiba.Add(40 * time.Millisecond)
			ev2.duplikasi = false
			p.antre = append(p.antre, &ev2)
		}
		p.lsn += int64(100 + p.rnd.Intn(400))
		cdc := Pesan{Stream: "CDC", Subject: "cdc.bank.transaksi", MsgID: fmt.Sprintf("transaksi:%d:%s", p.lsn, id), trxID: id,
			Payload: map[string]any{"op": "c", "before": nil, "after": kolomDebezium(trx),
				"source": map[string]any{"db": "bank", "table": "transaksi", "lsn": p.lsn, "ts_ms": t.UnixMilli()}},
			tiba: t.Add(time.Duration(p.cfg.CDCMilidtk/2+p.rnd.Intn(p.cfg.CDCMilidtk+1)) * time.Millisecond)}
		p.antre = append(p.antre, &cdc)
		res.TrxID = append(res.TrxID, id)
	}
	if len(res.TrxID) == 2 && q.KirimUlang == "dengan_kunci" {
		res.Pesan = "request kedua dikenali lewat Idempotency-Key; hanya satu transaksi tercatat"
	}
	return res
}

func (p *Pipeline) simpanJejak(j *Jejak) {
	p.jejak[j.TrxID] = j
	p.urutJejak = append(p.urutJejak, j.TrxID)
	if len(p.urutJejak) > 5000 { // batasi memori
		delete(p.jejak, p.urutJejak[0])
		p.urutJejak = p.urutJejak[1:]
	}
}

func (p *Pipeline) tandai(j *Jejak, tahap string, t time.Time) {
	if j == nil {
		return
	}
	if _, ada := j.tahapWaktu[tahap]; ada {
		return
	}
	j.tahapWaktu[tahap] = t
	j.Waktu[tahap] = t.UTC().Format(time.RFC3339Nano)
}

func kolomString(t Transaksi) map[string]any {
	nom := any(nil)
	if t.Nominal != nil {
		nom = strconv.FormatFloat(*t.Nominal, 'f', 2, 64)
	}
	idk := any(nil)
	if t.IdempotencyKey != "" {
		idk = t.IdempotencyKey
	}
	return map[string]any{"id": t.ID, "rekening_id": strconv.Itoa(t.RekeningID), "cabang_id": strconv.Itoa(t.CabangID),
		"kanal": t.Kanal, "jenis": t.Jenis, "nominal": nom, "occurred_at": t.OccurredAt.In(WIB).Format("2006-01-02T15:04:05.000-07:00"),
		"created_at": t.CreatedAt.In(WIB).Format("2006-01-02T15:04:05.000-07:00"), "idempotency_key": idk,
		"saldo_setelah": strconv.FormatFloat(t.SaldoSetelah, 'f', 2, 64)}
}

// kolomDebezium: gaya Debezium (timestamp epoch mikrodetik, numeric sebagai string).
func kolomDebezium(t Transaksi) map[string]any {
	m := kolomString(t)
	m["rekening_id"], m["cabang_id"] = t.RekeningID, t.CabangID
	m["occurred_at"], m["created_at"] = t.OccurredAt.UnixMicro(), t.CreatedAt.UnixMicro()
	return m
}

// Detak dipanggil berkala: antar pesan NATS, flush ingestor, jalankan batch silver+gold, generator.
func (p *Pipeline) Detak(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.antarPesan(now)
	if now.Sub(p.terakhirFlush).Seconds() >= p.cfg.FlushDetik {
		p.flush(now)
	}
	if now.Sub(p.terakhirBatch).Seconds() >= p.cfg.BatchDetik {
		p.batch(now)
	}
	for k := range p.msgIDs {
		if now.Sub(p.msgIDs[k]) > 2*time.Minute {
			delete(p.msgIDs, k)
		}
	}
}

func (p *Pipeline) antarPesan(now time.Time) {
	sisa := p.antre[:0]
	for _, m := range p.antre {
		if m.tiba.After(now) {
			sisa = append(sisa, m)
			continue
		}
		tahap := "event_app"
		if m.Stream == "CDC" {
			tahap = "event_cdc"
		}
		j := p.jejak[m.trxID]
		p.hit[tahap].Masuk++
		if _, dup := p.msgIDs[m.MsgID]; dup && m.Stream == "BANK" {
			p.hit[tahap].Tolak["publish ulang, Nats-Msg-Id sama (dibuang JetStream)"]++
			if j != nil {
				j.Catatan = append(j.Catatan, "publish ulang dengan Nats-Msg-Id yang sama dibuang jendela duplikat JetStream (2 menit)")
			}
			continue
		}
		p.msgIDs[m.MsgID] = now
		p.hit[tahap].Keluar++
		p.tandai(j, tahap, m.tiba)
		if m.Stream == "BANK" { // hot path: konsumen real-time langsung menjumlah event
			p.hot.jumlah++
			if d, ok := m.Payload["data"].(map[string]any); ok {
				if s, ok := d["nominal"].(string); ok {
					v, _ := strconv.ParseFloat(s, 64)
					p.hot.nominal += v
				}
			}
		}
		n := 1
		if m.duplikasi {
			n = 2
			if j != nil {
				j.Catatan = append(j.Catatan, "ack ingestor hilang: NATS mengirim ulang pesan, bronze menerima baris yang sama dua kali (at-least-once)")
			}
		}
		for i := 0; i < n; i++ {
			p.buffer = append(p.buffer, keBronze(m, now))
		}
	}
	p.antre = sisa
}

func keBronze(m *Pesan, now time.Time) *BarisBronze {
	b := &BarisBronze{IngestedAt: now.UTC().Format(time.RFC3339Nano), EventID: m.MsgID, trxID: m.trxID, Data: map[string]string{}}
	var data map[string]any
	if m.Stream == "BANK" {
		b.Source, b.EventType = "app", m.Subject
		data, _ = m.Payload["data"].(map[string]any)
		b.OccurredAt, _ = m.Payload["occurred_at"].(string)
	} else {
		op := "c"
		b.Source, b.EventType, b.Op = "cdc", m.Subject, &op
		data, _ = m.Payload["after"].(map[string]any)
		if us, ok := data["occurred_at"].(int64); ok {
			b.OccurredAt = time.UnixMicro(us).In(WIB).Format("2006-01-02T15:04:05.000-07:00")
		}
		src, _ := m.Payload["source"].(map[string]any)
		b.TsMs = fmt.Sprint(src["ts_ms"])
	}
	if b.TsMs == "" {
		b.TsMs = strconv.FormatInt(now.UnixMilli(), 10)
	}
	for k, v := range data {
		if v != nil {
			b.Data[k] = fmt.Sprint(v)
		}
	}
	return b
}

// flush: ingestor menulis buffer sebagai satu berkas per (sumber, dt) -> latensi micro-batch.
func (p *Pipeline) flush(now time.Time) {
	p.terakhirFlush = now
	if len(p.buffer) == 0 {
		return
	}
	p.nBatch++
	for _, b := range p.buffer {
		dt := tanggalBisnis(b.OccurredAt, now)
		b.BatchID = fmt.Sprintf("%s-%s-%04d", now.UTC().Format("20060102T150405"), b.Source, p.nBatch)
		b.Path = fmt.Sprintf("bronze/%s/transaksi/dt=%s/part-%s.parquet", b.Source, dt, b.BatchID)
		p.berkas[b.Path]++
		p.bronze = append(p.bronze, b)
		p.hit["bronze"].Masuk++
		p.hit["bronze"].Keluar++
		p.tandai(p.jejak[b.trxID], "bronze", now)
	}
	p.buffer = nil
}

func tanggalBisnis(occ string, now time.Time) string {
	if t, err := time.Parse("2006-01-02T15:04:05.000-07:00", occ); err == nil {
		return t.UTC().Format("2006-01-02")
	}
	return now.UTC().Format("2006-01-02")
}

var petaKanal = map[string]string{"mobile": "mobile", "mobile banking": "mobile", "m-banking": "mobile", "atm": "atm",
	"teller": "teller", "web": "web", "internet banking": "web", "ecommerce": "ecommerce", "e-commerce": "ecommerce"}

// StandarKanal menormalkan kode kanal sumber; kosong bila tidak dikenal.
func StandarKanal(s string) string { return petaKanal[strings.ToLower(strings.TrimSpace(s))] }

// batch: job silver lalu gold (di platform: DAG Airflow harian; di sini tiap BatchDetik detik).
func (p *Pipeline) batch(now time.Time) {
	p.terakhirBatch = now
	baru := false
	for _, b := range p.bronze {
		if b.diproses {
			continue
		}
		b.diproses = true
		baru = true
		p.hit["silver"].Masuk++
		j := p.jejak[b.trxID]
		karantina := func(alasan string) {
			p.karID[b.Data["id"]] = true
			p.hit["silver"].Tolak[alasan]++
			p.karantina = append(p.karantina, map[string]string{"trx_id": b.Data["id"], "alasan": alasan, "sumber": b.Source,
				"kanal": b.Data["kanal"], "nominal": b.Data["nominal"], "path": b.Path})
			if j != nil && j.Status == "berjalan" {
				j.Status = "karantina"
				j.Catatan = append(j.Catatan, "silver mengarantina: "+alasan)
				p.tandai(j, "silver", now)
			}
		}
		if p.eventSeen[b.EventID] {
			p.hit["silver"].Tolak["duplikat event_id (redelivery)"]++
			continue
		}
		p.eventSeen[b.EventID] = true
		if p.karID[b.Data["id"]] {
			p.hit["silver"].Tolak["duplikat lintas sumber app+cdc (digabung)"]++
			continue
		}
		if s, ada := p.silver[b.Data["id"]]; ada {
			if !mengandung(s.Sumber, b.Source) {
				s.Sumber = append(s.Sumber, b.Source)
			}
			p.hit["silver"].Tolak["duplikat lintas sumber app+cdc (digabung)"]++
			continue
		}
		nom, err := strconv.ParseFloat(b.Data["nominal"], 64)
		if err != nil {
			karantina("nominal kosong")
			continue
		}
		if nom <= 0 {
			karantina("nominal tidak positif")
			continue
		}
		kanal := StandarKanal(b.Data["kanal"])
		if kanal == "" {
			karantina(fmt.Sprintf("kanal tidak dikenal (%q)", b.Data["kanal"]))
			continue
		}
		occ, err1 := parseWaktu(b.Data["occurred_at"])
		cre, err2 := parseWaktu(b.Data["created_at"])
		if err1 != nil || err2 != nil {
			karantina("waktu tidak terbaca")
			continue
		}
		rek, _ := strconv.Atoi(b.Data["rekening_id"])
		cab, _ := strconv.Atoi(b.Data["cabang_id"])
		s := &BarisSilver{TrxID: b.Data["id"], RekeningID: rek, CabangID: cab, Kanal: kanal, Jenis: b.Data["jenis"], Nominal: nom,
			OccurredAt: occ.In(WIB).Format(time.RFC3339), Tanggal: occ.In(WIB).Format("2006-01-02"), Sumber: []string{b.Source},
			tOccurred: occ, idemKey: b.Data["idempotency_key"]}
		if kanal != b.Data["kanal"] {
			s.Flag = append(s.Flag, fmt.Sprintf("kanal distandarkan %q -> %q", b.Data["kanal"], kanal))
		}
		if cre.Sub(occ) > time.Minute {
			s.Flag = append(s.Flag, "data terlambat")
			p.hit["silver"].Tolak["data terlambat (masuk partisi tanggal kejadian)"]++
		}
		if s.idemKey == "" {
			for _, id := range lastN(p.silverUrt, 200) {
				o := p.silver[id]
				if o.RekeningID == rek && o.Jenis == s.Jenis && o.Nominal == nom && absDur(o.tOccurred.Sub(occ)) < time.Minute {
					s.Flag = append(s.Flag, "dugaan duplikat bisnis dengan "+id)
					p.hit["silver"].Tolak["dugaan duplikat bisnis (ditandai, perlu review)"]++
					break
				}
			}
		}
		p.silver[s.TrxID] = s
		p.silverUrt = append(p.silverUrt, s.TrxID)
		p.hit["silver"].Keluar++
		if j != nil {
			p.tandai(j, "silver", now)
			if len(s.Flag) > 0 {
				j.Catatan = append(j.Catatan, "silver: "+strings.Join(s.Flag, "; "))
			}
		}
	}
	if !baru {
		return
	}
	// gold: agregat fakta per tanggal x cabang x kanal dihitung ulang dari silver (idempoten)
	tGold := now.Add(time.Duration(30+p.rnd.Intn(50)) * time.Millisecond)
	gold := map[string]*SelGold{}
	for _, id := range p.silverUrt {
		s := p.silver[id]
		k := fmt.Sprintf("%s|%d|%s", s.Tanggal, s.CabangID, s.Kanal)
		g := gold[k]
		if g == nil {
			g = &SelGold{Tanggal: s.Tanggal, CabangID: s.CabangID, Kanal: s.Kanal}
			gold[k] = g
		}
		g.Jumlah++
		g.Nominal += s.Nominal
	}
	p.gold = gold
	p.versiGold++
	p.goldAt = tGold
	p.hit["gold"].Masuk = len(p.silver)
	p.hit["gold"].Keluar = len(gold)
	for _, id := range p.silverUrt {
		if j := p.jejak[id]; j != nil {
			if _, ada := j.tahapWaktu["gold"]; !ada {
				p.tandai(j, "gold", tGold)
				j.versiGold = p.versiGold
			}
		}
	}
}

// Dilayani mencatat kapan dashboard pertama kali menampilkan gold yang memuat transaksi.
func (p *Pipeline) dilayani(now time.Time) {
	for _, id := range lastN(p.urutJejak, 400) {
		j := p.jejak[id]
		if j != nil && j.versiGold > 0 && j.versiGold <= p.versiGold {
			if _, ada := j.tahapWaktu["dashboard"]; !ada {
				p.tandai(j, "dashboard", now)
				p.hit["dashboard"].Masuk++
				p.hit["dashboard"].Keluar++
				if j.Status == "berjalan" {
					j.Status = "selesai"
				}
			}
		}
	}
}

func parseWaktu(s string) (time.Time, error) {
	if t, err := time.Parse("2006-01-02T15:04:05.000-07:00", s); err == nil {
		return t, nil
	}
	if us, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMicro(us), nil
	}
	return time.Time{}, fmt.Errorf("format waktu tidak dikenal: %q", s)
}

func mengandung(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func lastN(xs []string, n int) []string {
	if len(xs) > n {
		return xs[len(xs)-n:]
	}
	return xs
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func uuidAcak(r *rand.Rand) string {
	b := make([]byte, 16)
	r.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// ---- tampilan untuk API ----

type Dashboard struct {
	VersiGold     int                `json:"versi_gold"`
	GoldAt        string             `json:"gold_at"`
	KesegaranDtk  float64            `json:"kesegaran_detik"`
	PerKanal      map[string]float64 `json:"per_kanal"`
	PerCabang     map[string]float64 `json:"per_cabang"`
	PerTanggal    map[string]float64 `json:"per_tanggal"`
	HariIni       map[string]float64 `json:"hari_ini"` // hot, bronze_mentah, gold
	JumlahHariIni map[string]int     `json:"jumlah_hari_ini"`
	Rekonsiliasi  map[string]any     `json:"rekonsiliasi"`
}

func (p *Pipeline) Dashboard(now time.Time) Dashboard {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dilayani(now)
	d := Dashboard{VersiGold: p.versiGold, PerKanal: map[string]float64{}, PerCabang: map[string]float64{}, PerTanggal: map[string]float64{},
		HariIni: map[string]float64{}, JumlahHariIni: map[string]int{}}
	if !p.goldAt.IsZero() {
		d.GoldAt = p.goldAt.UTC().Format(time.RFC3339Nano)
		d.KesegaranDtk = now.Sub(p.goldAt).Seconds()
	}
	hari := now.In(WIB).Format("2006-01-02")
	var totGold float64
	for _, g := range p.gold {
		d.PerKanal[g.Kanal] += g.Nominal
		d.PerCabang[NamaCabang(g.CabangID)] += g.Nominal
		d.PerTanggal[g.Tanggal] += g.Nominal
		totGold += g.Nominal
		if g.Tanggal == hari {
			d.HariIni["gold"] += g.Nominal
			d.JumlahHariIni["gold"] += g.Jumlah
		}
	}
	for _, b := range p.bronze {
		if tanggalWIB(b.OccurredAt) == hari {
			v, _ := strconv.ParseFloat(b.Data["nominal"], 64)
			d.HariIni["bronze_mentah"] += v
			d.JumlahHariIni["bronze_mentah"]++
		}
	}
	d.HariIni["hot"] = p.hot.nominal
	d.JumlahHariIni["hot"] = p.hot.jumlah
	var totSilver float64
	for _, s := range p.silver {
		totSilver += s.Nominal
	}
	d.Rekonsiliasi = map[string]any{"silver": totSilver, "gold": totGold, "cocok": fmt.Sprintf("%.2f", totSilver) == fmt.Sprintf("%.2f", totGold)}
	return d
}

func tanggalWIB(occ string) string {
	if t, err := time.Parse("2006-01-02T15:04:05.000-07:00", occ); err == nil {
		return t.In(WIB).Format("2006-01-02")
	}
	return ""
}

type Status struct {
	Konfig       Konfig                 `json:"konfig"`
	Hitung       map[string]*Penghitung `json:"hitung"`
	Antre        int                    `json:"antre_nats"`
	Buffer       int                    `json:"buffer_ingestor"`
	BerkasBronze int                    `json:"berkas_bronze"`
	BarisOLTP    int                    `json:"baris_oltp"`
	BarisBronze  int                    `json:"baris_bronze"`
	BarisSilver  int                    `json:"baris_silver"`
	Karantina    int                    `json:"baris_karantina"`
	SelGold      int                    `json:"sel_gold"`
	FlushBerikut float64                `json:"flush_berikut_detik"`
	BatchBerikut float64                `json:"batch_berikut_detik"`
	Jejak        []map[string]any       `json:"jejak_terbaru"`
	Contoh       map[string]any         `json:"contoh"`
}

func (p *Pipeline) Status(now time.Time) Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := Status{Konfig: p.cfg, Hitung: p.hit, Antre: len(p.antre), Buffer: len(p.buffer), BerkasBronze: len(p.berkas),
		BarisOLTP: len(p.oltp), BarisBronze: len(p.bronze), BarisSilver: len(p.silver), Karantina: len(p.karantina), SelGold: len(p.gold),
		FlushBerikut: maxF(0, p.cfg.FlushDetik-now.Sub(p.terakhirFlush).Seconds()),
		BatchBerikut: maxF(0, p.cfg.BatchDetik-now.Sub(p.terakhirBatch).Seconds())}
	// jejak dari kelas didahulukan, lalu beberapa jejak generator terbaru
	var kelas, gen []map[string]any
	for _, id := range lastN(p.urutJejak, 2000) {
		j := p.jejak[id]
		if j == nil || j.Asal == "backfill" {
			continue
		}
		v := map[string]any{"trx_id": j.TrxID, "status": j.Status, "asal": j.Asal, "mulai": j.Mulai.UTC().Format(time.RFC3339Nano)}
		if j.Asal == "kelas" {
			kelas = append([]map[string]any{v}, kelas...)
		} else {
			gen = append([]map[string]any{v}, gen...)
		}
	}
	if len(kelas) > 10 {
		kelas = kelas[:10]
	}
	if len(gen) > 5 {
		gen = gen[:5]
	}
	s.Jejak = append(kelas, gen...)
	kar := p.karantina
	if len(kar) > 8 {
		kar = kar[len(kar)-8:]
	}
	s.Contoh = map[string]any{"karantina": kar}
	return s
}

func maxF(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// DetailJejak mengumpulkan wujud satu transaksi di setiap tahap.
func (p *Pipeline) DetailJejak(id string) (map[string]any, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	j, ok := p.jejak[id]
	if !ok {
		return nil, false
	}
	out := map[string]any{"jejak": j}
	lat := map[string]float64{}
	urut := []string{"aplikasi", "event_app", "event_cdc", "bronze", "silver", "gold", "dashboard"}
	acuan := map[string]string{"event_app": "aplikasi", "event_cdc": "aplikasi", "bronze": "event_app", "silver": "bronze", "gold": "silver", "dashboard": "gold"}
	for _, t := range urut {
		w, ada := j.tahapWaktu[t]
		if !ada {
			continue
		}
		if t == "aplikasi" {
			lat[t] = w.Sub(j.Mulai).Seconds() * 1000
			continue
		}
		a := acuan[t]
		if t == "bronze" {
			if _, ok := j.tahapWaktu["event_app"]; !ok {
				a = "event_cdc"
			}
		}
		if w0, ok := j.tahapWaktu[a]; ok {
			lat[t] = w.Sub(w0).Seconds() * 1000
		}
	}
	out["latensi_ms"] = lat
	if akhir, ok := j.tahapWaktu["dashboard"]; ok {
		out["total_ms"] = akhir.Sub(j.Mulai).Seconds() * 1000
	}
	for _, t := range p.oltp {
		if t.ID == id {
			out["oltp"] = t
		}
	}
	var evs []map[string]any
	for _, m := range p.antre {
		if m.trxID == id {
			evs = append(evs, map[string]any{"stream": m.Stream, "subject": m.Subject, "nats_msg_id": m.MsgID, "payload": m.Payload, "status": "dalam antrean"})
		}
	}
	var br []*BarisBronze
	for _, b := range append(append([]*BarisBronze{}, p.bronze...), p.buffer...) {
		if b.trxID == id {
			br = append(br, b)
		}
	}
	for _, b := range br {
		if b.Source == "app" {
			evs = append(evs, map[string]any{"stream": "BANK", "subject": b.EventType, "nats_msg_id": b.EventID,
				"payload": map[string]any{"event_id": b.EventID, "event_type": b.EventType, "occurred_at": b.OccurredAt, "data": b.Data}})
		} else {
			evs = append(evs, map[string]any{"stream": "CDC", "subject": b.EventType, "nats_msg_id": b.EventID,
				"payload": map[string]any{"op": b.Op, "after": b.Data, "source": map[string]any{"table": "transaksi", "ts_ms": b.TsMs}}})
		}
	}
	out["event"] = evs
	out["bronze"] = br
	if s, ok := p.silver[id]; ok {
		out["silver"] = s
		k := fmt.Sprintf("%s|%d|%s", s.Tanggal, s.CabangID, s.Kanal)
		if g, ok := p.gold[k]; ok {
			out["gold"] = map[string]any{"sel": g, "cabang": NamaCabang(g.CabangID)}
		}
	}
	for _, k := range p.karantina {
		if k["trx_id"] == id {
			out["karantina"] = k
		}
	}
	return out, true
}

func (p *Pipeline) SetKonfig(k Konfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if k.FlushDetik >= 1 && k.FlushDetik <= 120 {
		p.cfg.FlushDetik = k.FlushDetik
	}
	if k.BatchDetik >= 2 && k.BatchDetik <= 600 {
		p.cfg.BatchDetik = k.BatchDetik
	}
	if k.GenLaju > 0 && k.GenLaju <= 50 {
		p.cfg.GenLaju = k.GenLaju
	}
	if k.GenKotorPct >= 0 && k.GenKotorPct <= 100 {
		p.cfg.GenKotorPct = k.GenKotorPct
	}
	p.cfg.GenAktif = k.GenAktif
}

func (p *Pipeline) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reset()
	p.backfill(time.Now())
}

// PermintaanAcak membangkitkan satu permintaan generator; sebagian "kotor" sesuai persen_kotor.
func (p *Pipeline) PermintaanAcak() Permintaan {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.permintaanAcak()
}

func (p *Pipeline) permintaanAcak() Permintaan {
	r := p.rnd
	kanal := pilihBobot(r, []string{"mobile", "atm", "teller", "web", "ecommerce"}, []int{45, 18, 9, 13, 15})
	jenis := map[string][]string{"mobile": {"transfer_keluar", "transfer_masuk", "angsuran"}, "atm": {"tarik", "setor"},
		"teller": {"setor", "tarik", "angsuran"}, "web": {"transfer_keluar", "transfer_masuk"}, "ecommerce": {"bayar_belanja"}}[kanal]
	q := Permintaan{RekeningID: DataRekening[r.Intn(len(DataRekening))].ID, Kanal: kanal, Jenis: jenis[r.Intn(len(jenis))],
		Nominal: strconv.Itoa([]int{50, 100, 200, 250, 500, 750, 1000, 2500}[r.Intn(8)] * 1000)}
	if r.Float64()*100 < p.cfg.GenKotorPct {
		switch r.Intn(6) {
		case 0:
			q.Kanal = []string{"Mobile Banking", "m-banking", "ATM", "teller ", "WEB"}[r.Intn(5)]
		case 1:
			q.Nominal = ""
		case 2:
			q.GeserMenit = -float64(120 + r.Intn(1000))
		case 3:
			q.KirimUlang = "tanpa_kunci"
		case 4:
			q.AckHilang = true
		case 5:
			q.Kanal = "qris"
		}
	}
	return q
}

func pilihBobot(r *rand.Rand, xs []string, w []int) string {
	tot := 0
	for _, v := range w {
		tot += v
	}
	n := r.Intn(tot)
	for i, v := range w {
		if n < v {
			return xs[i]
		}
		n -= v
	}
	return xs[0]
}

// backfill: riwayat 7 hari diproses sekali agar dashboard tidak kosong saat kelas dimulai.
func (p *Pipeline) backfill(now time.Time) {
	for d := 7; d >= 1; d-- {
		for i := 0; i < 120; i++ {
			q := p.permintaanAcak()
			q.GeserMenit, q.KirimUlang, q.AckHilang, q.Jenis = 0, "", false, "setor"
			q.Kanal = StandarKanal(q.Kanal)
			if q.Kanal == "" || q.Nominal == "" {
				q.Kanal, q.Nominal = "mobile", "100000"
			}
			t := now.Add(-time.Duration(d*24)*time.Hour + time.Duration(p.rnd.Intn(12*3600))*time.Second)
			p.kirim(q, "backfill", t)
		}
	}
	p.antarPesan(now)
	p.flush(now)
	p.batch(now)
	p.terakhirFlush, p.terakhirBatch = now, now
	for k := range p.hit {
		p.hit[k] = baruHitung()
	}
	p.hot.jumlah, p.hot.nominal = 0, 0
}
