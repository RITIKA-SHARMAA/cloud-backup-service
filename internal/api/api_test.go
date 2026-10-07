package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/api"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/crypto"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/engine"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/metrics"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/testutil"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/vault"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	db := testutil.Catalog(t)
	codec, _ := crypto.NewCodec(bytes.Repeat([]byte{1}, 32))
	s := &api.Server{DB: db, Engine: engine.New(&vault.Local{Root: t.TempDir()}, codec),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: metrics.New()}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func call(t *testing.T, method, url, body string, headers ...string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	b, _ := io.ReadAll(resp.Body)
	json.Unmarshal(b, &out)
	return resp.StatusCode, out
}

func TestPolicyBackupAndJobFlow(t *testing.T) {
	ts := newServer(t)
	code, pol := call(t, "POST", ts.URL+"/v1/policies", `{"name":"files","source_type":"dir","source":"/data","schedule":"0 2 * * *"}`)
	if code != 201 || pol["id"] == nil || pol["retention_days"].(float64) != 30 {
		t.Fatalf("create policy: %d %v", code, pol)
	}

	code, job := call(t, "POST", ts.URL+"/v1/policies/1/backups", "", "Idempotency-Key", "abc")
	if code != 202 || job["state"] != "queued" || job["kind"] != "backup" {
		t.Fatalf("trigger: %d %v", code, job)
	}
	// Same Idempotency-Key: the same job comes back, with 200 instead of 202.
	code, again := call(t, "POST", ts.URL+"/v1/policies/1/backups", "", "Idempotency-Key", "abc")
	if code != 200 || again["id"] != job["id"] {
		t.Fatalf("retry: %d %v", code, again)
	}
	code, got := call(t, "GET", ts.URL+"/v1/jobs/1", "")
	if code != 200 || got["id"].(float64) != 1 {
		t.Fatalf("get job: %d %v", code, got)
	}
}

func TestValidationErrors(t *testing.T) {
	ts := newServer(t)
	cases := []struct{ method, path, body string }{
		{"POST", "/v1/policies", `{"name":"x","source_type":"dir","source":"/d","schedule":"often"}`},
		{"POST", "/v1/policies", `{"name":"x","unknown_field":1}`},
		{"POST", "/v1/policies", `not json`},
		{"POST", "/v1/restores", `{"snapshot_id":"s"}`},
		{"POST", "/v1/restores", `{"snapshot_id":"s","target_dir":"/a","target_dsn":"postgres://"}`},
		{"POST", "/v1/gc", `{"keep_days":0}`},
		{"GET", "/v1/jobs/abc", ``},
	}
	for _, c := range cases {
		if code, _ := call(t, c.method, ts.URL+c.path, c.body); code != 400 {
			t.Errorf("%s %s %s: got %d, want 400", c.method, c.path, c.body, code)
		}
	}
	for _, p := range []string{"/v1/jobs/999", "/v1/policies/999", "/v1/snapshots/nope"} {
		if code, _ := call(t, "GET", ts.URL+p, ""); code != 404 {
			t.Errorf("GET %s: got %d, want 404", p, code)
		}
	}
	if code, _ := call(t, "POST", ts.URL+"/v1/policies/999/backups", ""); code != 404 {
		t.Errorf("backup of a missing policy: got %d", code)
	}
}

func TestHealthAndMetrics(t *testing.T) {
	ts := newServer(t)
	if code, body := call(t, "GET", ts.URL+"/healthz", ""); code != 200 || body["status"] != "ok" {
		t.Fatalf("healthz: %d %v", code, body)
	}
	call(t, "POST", ts.URL+"/v1/gc", `{"keep_days":7}`)
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{`cbs_jobs{state="queued"} 1`, `# TYPE cbs_http_requests_total counter`, `cbs_http_requests_total{code="202"} 1`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("metrics output missing %q:\n%s", want, b)
		}
	}
}
