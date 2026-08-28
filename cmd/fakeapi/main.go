/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Command fakeapi is a stand-in vendor API for the demo and the e2e suite.
//
// It behaves the way the APIs this operator was written against behave: cursor
// pagination, an inclusive "since" filter, new records appearing over time,
// occasional 429s, and a payload whose shape changes one day without notice.
// Having those behaviors in a binary anyone can run is what makes the demo
// honest, rather than a screenshot of a system nobody else can stand up.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func main() {
	var (
		addr       = flag.String("addr", ":8080", "address to listen on")
		interval   = flag.Duration("interval", time.Second, "spacing between generated records")
		seed       = flag.Int("seed", 120, "records that already exist when the server starts")
		pageSize   = flag.Int("page-size", 50, "records per page when the client does not ask")
		driftAfter = flag.Int("drift-after", 0, "change the record shape after this many requests (0 never)")
		rateLimit  = flag.Int("rate-limit-every", 0, "return 429 on every Nth request (0 never)")
		token      = flag.String("token", "", "require this bearer token")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	server := &fakeAPI{
		start:      time.Now().UTC().Truncate(time.Second),
		interval:   *interval,
		seed:       *seed,
		pageSize:   *pageSize,
		driftAfter: *driftAfter,
		rateLimit:  *rateLimit,
		token:      *token,
		log:        log,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/events", server.handleEvents)

	log.Info("fake vendor API listening",
		"addr", *addr, "seed", *seed, "interval", interval.String(), "driftAfter", *driftAfter)

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

type fakeAPI struct {
	start      time.Time
	interval   time.Duration
	seed       int
	pageSize   int
	driftAfter int
	rateLimit  int
	token      string
	log        *slog.Logger

	requests atomic.Int64
}

// handleEvents serves one page of records at or after the "since" parameter.
//
// The since filter is inclusive, which is not an oversight. Vendor APIs
// commonly are, and a client that resumes from its last watermark without
// filtering will re-ingest the boundary record every run. The worker's
// watermark tracker exists because of this.
func (f *fakeAPI) handleEvents(w http.ResponseWriter, r *http.Request) {
	count := f.requests.Add(1)

	if f.token != "" && r.Header.Get("Authorization") != "Bearer "+f.token {
		writeError(w, http.StatusUnauthorized, "missing or wrong bearer token")
		return
	}
	if f.rateLimit > 0 && count%int64(f.rateLimit) == 0 {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "slow down")
		return
	}

	since, err := parseSince(r.URL.Query().Get("since"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	until, err := parseSince(r.URL.Query().Get("until"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	limit := f.pageSize
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	offset := 0
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	drifted := f.driftAfter > 0 && count > int64(f.driftAfter)
	all := f.records(since, until, drifted)

	end := min(offset+limit, len(all))
	if offset > len(all) {
		offset = len(all)
	}
	page := all[offset:end]

	body := map[string]any{
		"data": map[string]any{"items": page},
		"meta": map[string]any{},
	}
	if end < len(all) {
		body["meta"] = map[string]any{"next_cursor": strconv.Itoa(end)}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(body); err != nil {
		f.log.Error("encoding response", "error", err)
	}
}

// records returns every record whose timestamp falls in the requested range.
// Records accrue in real time, so a schedule that runs every minute finds new
// data on every run.
func (f *fakeAPI) records(since, until time.Time, drifted bool) []map[string]any {
	elapsed := int(time.Since(f.start) / f.interval)
	total := f.seed + elapsed

	out := make([]map[string]any, 0, total)
	for i := range total {
		// The seed records sit before the process start so the very first run
		// has something to ingest.
		updated := f.start.Add(time.Duration(i-f.seed) * f.interval)
		if !since.IsZero() && updated.Before(since) {
			continue
		}
		if !until.IsZero() && updated.After(until) {
			continue
		}
		out = append(out, f.record(i, updated, drifted))
	}
	return out
}

func (f *fakeAPI) record(i int, updated time.Time, drifted bool) map[string]any {
	record := map[string]any{
		"id":         i,
		"name":       fmt.Sprintf("event-%04d", i),
		"amount":     float64(i%97) + 0.5,
		"region":     []string{"us-east1", "us-central1", "europe-west1"}[i%3],
		"updated_at": updated.Format(time.RFC3339),
	}
	if drifted {
		// The shape change this simulates is the one that hurts: a field that
		// vanishes, and a numeric field that starts arriving as a string. Both
		// load without error into a permissive table and quietly corrupt it.
		record["amount"] = fmt.Sprintf("%.2f", float64(i%97)+0.5)
		record["tenant"] = fmt.Sprintf("tenant-%d", i%4)
		return record
	}
	record["email"] = fmt.Sprintf("event-%04d@example.invalid", i)
	return record
}

func parseSince(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse %q as RFC3339: %w", raw, err)
	}
	return parsed, nil
}

func writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
