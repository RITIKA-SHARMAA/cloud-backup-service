// Command worker claims and runs backup, restore and gc jobs.
package main

import (
	"net/http"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/catalog"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/config"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/metrics"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/worker"
)

func main() {
	log := config.Logger("worker")
	ctx, stop := config.SignalContext()
	defer stop()

	cfg, err := config.Load(":8082")
	if err != nil {
		config.Fatal(log, "config", err)
	}
	db, err := catalog.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		config.Fatal(log, "catalog database", err)
	}
	defer db.Close()
	eng, err := cfg.Engine(ctx, true)
	if err != nil {
		config.Fatal(log, "vault", err)
	}
	log.Info("vault ready", "vault", eng.Store.String())

	m := metrics.New()
	w := &worker.Worker{DB: db, ID: cfg.WorkerID, Lease: cfg.Lease, Handler: worker.NewHandler(db, eng, m), Log: log, Metrics: m}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.PingContext(r.Context()); err != nil {
			http.Error(w, "catalog database unreachable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /metrics", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "text/plain; version=0.0.4")
		m.Write(rw, nil)
	})
	go config.Serve(ctx, cfg.HTTPAddr, mux, log)

	if err := w.Run(ctx); err != nil {
		config.Fatal(log, "worker", err)
	}
}
