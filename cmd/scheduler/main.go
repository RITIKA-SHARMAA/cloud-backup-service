// Command scheduler enqueues backup jobs for policies whose schedule is due.
// Several replicas may run; they coordinate through the database (see scheduler.Tick).
package main

import (
	"net/http"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/catalog"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/config"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/scheduler"
)

func main() {
	log := config.Logger("scheduler")
	ctx, stop := config.SignalContext()
	defer stop()

	cfg, err := config.Load(":8081")
	if err != nil {
		config.Fatal(log, "config", err)
	}
	db, err := catalog.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		config.Fatal(log, "catalog database", err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.PingContext(r.Context()); err != nil {
			http.Error(w, "catalog database unreachable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok\n"))
	})
	go config.Serve(ctx, cfg.HTTPAddr, mux, log)

	log.Info("scheduler started")
	scheduler.Run(ctx, db, 5*time.Second, log)
	log.Info("scheduler stopped")
}
