// opsctl adalah satu binary untuk bagian ops platform Data Engineering:
//
//	opsctl init-streams   membuat stream CDC di NATS JetStream (sekali jalan, lalu keluar)
//	opsctl serve          API generator lalu lintas + aliran data (port 8080 di container)
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "pakai: opsctl init-streams | serve")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "init-streams":
		log := newLogger("nats-init")
		if err := initStreams(context.Background(), log); err != nil {
			log.Error("init stream gagal", "err", err)
			os.Exit(1)
		}
	case "serve":
		log := newLogger("ops-api")
		if err := serve(log); err != nil {
			log.Error("ops-api berhenti", "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "subcommand tidak dikenal:", os.Args[1])
		os.Exit(2)
	}
}

// newLogger membuat logger JSON satu baris (time, level, msg, service) sesuai konvensi repo.
func newLogger(service string) *slog.Logger {
	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("service", service)
	slog.SetDefault(l)
	return l
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func connectNATS(log *slog.Logger, name string) (*nats.Conn, jetstream.JetStream, error) {
	url := env("NATS_URL", "nats://nats:4222")
	var nc *nats.Conn
	var err error
	for i := 0; i < 90; i++ {
		nc, err = nats.Connect(url, nats.Name(name), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
		if err == nil {
			break
		}
		log.Warn("menunggu nats", "err", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, nil, err
	}
	js, err := jetstream.New(nc)
	return nc, js, err
}

// cdcStreamConfig: stream CDC menampung envelope Debezium dari database bank dan orders.
// Retensi 7 hari / 2 GiB: cukup untuk demo dan uji backfill, tetap hemat disk server lab.
func cdcStreamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        "CDC",
		Description: "Debezium CDC: cdc.<database>.<tabel>",
		Subjects:    []string{"cdc.>"},
		Storage:     jetstream.FileStorage,
		MaxAge:      7 * 24 * time.Hour,
		MaxBytes:    2 << 30,
		Discard:     jetstream.DiscardOld,
		Duplicates:  2 * time.Minute,
	}
}

func initStreams(ctx context.Context, log *slog.Logger) error {
	nc, js, err := connectNATS(log, "nats-init")
	if err != nil {
		return err
	}
	defer nc.Close()
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	s, err := js.CreateOrUpdateStream(cctx, cdcStreamConfig())
	if err != nil {
		return err
	}
	log.Info("stream siap", "stream", s.CachedInfo().Config.Name, "subjects", s.CachedInfo().Config.Subjects)
	return nil
}
