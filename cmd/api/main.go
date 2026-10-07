// Command api serves the REST API.
package main

import (
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/api"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/catalog"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/config"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/metrics"
)

func main() {
	log := config.Logger("api")
	ctx, stop := config.SignalContext()
	defer stop()

	cfg, err := config.Load(":8080")
	if err != nil {
		config.Fatal(log, "config", err)
	}
	db, err := catalog.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		config.Fatal(log, "catalog database", err)
	}
	defer db.Close()
	eng, err := cfg.Engine(ctx, false)
	if err != nil {
		config.Fatal(log, "vault", err)
	}
	m := metrics.New()
	m.Describe("cbs_http_requests_total", "HTTP requests by status code.")
	srv := &api.Server{DB: db, Engine: eng, Log: log, Metrics: m}
	if err := config.Serve(ctx, cfg.HTTPAddr, srv.Handler(), log); err != nil {
		config.Fatal(log, "http server", err)
	}
	log.Info("api stopped")
}
