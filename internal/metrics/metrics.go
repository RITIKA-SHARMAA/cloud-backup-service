// Package metrics keeps a few counters and writes them in the Prometheus text format.
// A hand-written exporter keeps the dependency list short; the format is just
// "name{labels} value" lines, which Prometheus scrapes from /metrics.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// Registry is a set of named counters. Safe for concurrent use.
type Registry struct {
	mu       sync.Mutex
	counters map[string]float64 // key: name{labels}
	help     map[string]string  // metric name -> help text
}

func New() *Registry {
	return &Registry{counters: map[string]float64{}, help: map[string]string{}}
}

// Describe sets the HELP line for a metric name.
func (r *Registry) Describe(name, help string) {
	r.mu.Lock()
	r.help[name] = help
	r.mu.Unlock()
}

// Add increases a counter, e.g. Add("cbs_jobs_total", 1, "kind", "backup", "result", "succeeded").
func (r *Registry) Add(name string, v float64, labels ...string) {
	r.mu.Lock()
	r.counters[key(name, labels)] += v
	r.mu.Unlock()
}

// Get returns the current value of a counter (used by tests).
func (r *Registry) Get(name string, labels ...string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters[key(name, labels)]
}

func key(name string, labels []string) string {
	if len(labels) == 0 {
		return name
	}
	var parts []string
	for i := 0; i+1 < len(labels); i += 2 {
		parts = append(parts, fmt.Sprintf("%s=%q", labels[i], labels[i+1]))
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

// Write outputs all counters, sorted, plus any extra gauge lines the caller supplies.
func (r *Registry) Write(w io.Writer, gauges map[string]float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lines := map[string][]string{}
	add := func(k string, v float64, typ string) {
		name := k
		if i := strings.IndexByte(k, '{'); i >= 0 {
			name = k[:i]
		}
		if lines[name] == nil {
			if h := r.help[name]; h != "" {
				lines[name] = append(lines[name], fmt.Sprintf("# HELP %s %s", name, h))
			}
			lines[name] = append(lines[name], fmt.Sprintf("# TYPE %s %s", name, typ))
		}
		lines[name] = append(lines[name], fmt.Sprintf("%s %g", k, v))
	}
	keys := make([]string, 0, len(r.counters))
	for k := range r.counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		add(k, r.counters[k], "counter")
	}
	gk := make([]string, 0, len(gauges))
	for k := range gauges {
		gk = append(gk, k)
	}
	sort.Strings(gk)
	for _, k := range gk {
		add(k, gauges[k], "gauge")
	}
	names := make([]string, 0, len(lines))
	for n := range lines {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintln(w, strings.Join(lines[n], "\n"))
	}
}
