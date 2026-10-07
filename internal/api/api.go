// Package api is the REST API: manage policies, trigger backups and restores, inspect jobs and
// snapshots. It only writes to the catalog; the actual work is done by workers.
package api

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/catalog"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/engine"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/metrics"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/queue"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/worker"
)

// Server holds the API's dependencies.
type Server struct {
	DB      *sql.DB
	Engine  *engine.Engine // used read-only, to show a snapshot's manifest
	Log     *slog.Logger
	Metrics *metrics.Registry
}

// Handler returns the HTTP routes (Go 1.22+ pattern syntax: method and {wildcards}).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("POST /v1/policies", s.createPolicy)
	mux.HandleFunc("GET /v1/policies", s.listPolicies)
	mux.HandleFunc("GET /v1/policies/{id}", s.getPolicy)
	mux.HandleFunc("POST /v1/policies/{id}/backups", s.triggerBackup)
	mux.HandleFunc("GET /v1/snapshots", s.listSnapshots)
	mux.HandleFunc("GET /v1/snapshots/{id}", s.getSnapshot)
	mux.HandleFunc("POST /v1/restores", s.createRestore)
	mux.HandleFunc("POST /v1/gc", s.createGC)
	mux.HandleFunc("GET /v1/jobs", s.listJobs)
	mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
	return s.logRequests(mux)
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		if r.URL.Path != "/healthz" && r.URL.Path != "/metrics" {
			s.Log.Info("http request", "method", r.Method, "path", r.URL.Path, "status", rec.status,
				"duration_ms", time.Since(start).Milliseconds())
		}
		s.Metrics.Add("cbs_http_requests_total", 1, "code", strconv.Itoa(rec.status))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// fail maps errors to status codes; unexpected errors are logged and hidden from the client.
func (s *Server) fail(w http.ResponseWriter, err error) {
	if errors.Is(err, catalog.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	s.Log.Error("request failed", "error", err)
	writeErr(w, http.StatusInternalServerError, "internal error")
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "id must be a number")
	}
	return id, err == nil
}

// idempotencyKey uses the client's Idempotency-Key header if given, so a retried request
// returns the original job; otherwise every request is new.
func idempotencyKey(r *http.Request, prefix string) string {
	if k := r.Header.Get("Idempotency-Key"); k != "" {
		return prefix + ":client:" + k
	}
	b := make([]byte, 12)
	rand.Read(b)
	return prefix + ":" + hex.EncodeToString(b)
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request, n queue.NewJob) {
	job, created, err := queue.Enqueue(r.Context(), s.DB, n)
	if err != nil {
		s.fail(w, err)
		return
	}
	status := http.StatusAccepted
	if !created {
		status = http.StatusOK // same Idempotency-Key as an earlier request
	}
	w.Header().Set("Location", fmt.Sprintf("/v1/jobs/%d", job.ID))
	writeJSON(w, status, job)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.DB.PingContext(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "catalog database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	gauges := map[string]float64{}
	if counts, err := queue.CountByState(r.Context(), s.DB); err == nil {
		for state, n := range counts {
			gauges[fmt.Sprintf(`cbs_jobs{state=%q}`, state)] = float64(n)
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	s.Metrics.Write(w, gauges)
}

func (s *Server) createPolicy(w http.ResponseWriter, r *http.Request) {
	var p catalog.Policy
	if !decode(w, r, &p) {
		return
	}
	if err := catalog.CreatePolicy(r.Context(), s.DB, &p); err != nil {
		if p.ID == 0 { // validation or duplicate name
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, p)
}

func (s *Server) listPolicies(w http.ResponseWriter, r *http.Request) {
	ps, err := catalog.ListPolicies(r.Context(), s.DB)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ps)
}

func (s *Server) getPolicy(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	p, err := catalog.GetPolicy(r.Context(), s.DB, id)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) triggerBackup(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if _, err := catalog.GetPolicy(r.Context(), s.DB, id); err != nil {
		s.fail(w, err)
		return
	}
	s.enqueue(w, r, queue.NewJob{Kind: queue.KindBackup, PolicyID: &id,
		Payload: worker.BackupPayload{PolicyID: id}, IdempotencyKey: idempotencyKey(r, fmt.Sprintf("backup-%d", id))})
}

func (s *Server) listSnapshots(w http.ResponseWriter, r *http.Request) {
	snaps, err := catalog.ListSnapshots(r.Context(), s.DB)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snaps)
}

// getSnapshot reads the manifest from the vault (file list, sizes, checksums; not chunk lists).
func (s *Server) getSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := s.Engine.Load(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	type file struct {
		Path   string `json:"path"`
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256"`
		Chunks int    `json:"chunks"`
	}
	files := make([]file, 0, len(snap.Files))
	for _, f := range snap.Files {
		files = append(files, file{f.Path, f.Size, f.SHA256, len(f.Chunks)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": snap.ID, "source_type": snap.SourceType, "source": snap.Source,
		"created_at": snap.CreatedAt, "stats": snap.Stats, "files": files})
}

func (s *Server) createRestore(w http.ResponseWriter, r *http.Request) {
	var p worker.RestorePayload
	if !decode(w, r, &p) {
		return
	}
	if p.SnapshotID == "" || (p.TargetDir == "") == (p.TargetDSN == "") {
		writeErr(w, http.StatusBadRequest, "need snapshot_id and exactly one of target_dir or target_dsn")
		return
	}
	s.enqueue(w, r, queue.NewJob{Kind: queue.KindRestore, Payload: p, IdempotencyKey: idempotencyKey(r, "restore")})
}

func (s *Server) createGC(w http.ResponseWriter, r *http.Request) {
	var p worker.GCPayload
	if !decode(w, r, &p) {
		return
	}
	if p.KeepDays < 1 {
		writeErr(w, http.StatusBadRequest, "keep_days must be at least 1")
		return
	}
	s.enqueue(w, r, queue.NewJob{Kind: queue.KindGC, Payload: p, IdempotencyKey: idempotencyKey(r, "gc")})
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := queue.List(r.Context(), s.DB, 100)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	j, err := queue.Get(r.Context(), s.DB, id)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}
