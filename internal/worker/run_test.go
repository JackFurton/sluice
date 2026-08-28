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

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JackFurton/sluice/internal/runspec"
)

// eventsServer serves records with an inclusive "since" filter, the way the
// vendor APIs this operator was built against behave.
func eventsServer(t *testing.T, records []map[string]any) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		since := r.URL.Query().Get("since")
		page := make([]map[string]any, 0, len(records))
		for _, rec := range records {
			updated, _ := rec["updated_at"].(string)
			if since != "" && updated < since {
				continue
			}
			page = append(page, rec)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": page})
	}))
	t.Cleanup(server.Close)
	return server
}

func at(day int) string {
	return time.Date(2026, 1, day, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
}

func testConfig(url, drift string) runspec.RunConfig {
	return runspec.RunConfig{
		Source: runspec.SourceConfig{
			URL:         url,
			Method:      http.MethodGet,
			RecordsPath: "items",
			Pagination:  runspec.PaginationConfig{Type: "None"},
		},
		Watermark: &runspec.WatermarkConfig{
			RecordField: "updated_at",
			Param:       "since",
			Format:      "RFC3339",
		},
		Destination: runspec.DestinationConfig{Type: "Stdout"},
		SchemaDrift: drift,
	}
}

func runWorker(t *testing.T, opts Options) (runspec.RunResult, string, error) {
	t.Helper()
	var out bytes.Buffer
	opts.Stdout = &out
	opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	result, err := Run(context.Background(), opts)
	return result, out.String(), err
}

func lines(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	return len(strings.Split(s, "\n"))
}

func TestRunIngestsAndAdvancesWatermark(t *testing.T) {
	server := eventsServer(t, []map[string]any{
		{"id": 1, "updated_at": at(1)},
		{"id": 2, "updated_at": at(2)},
		{"id": 3, "updated_at": at(3)},
	})
	store := &MemoryStore{}

	result, out, err := runWorker(t, Options{
		Config: testConfig(server.URL, "Warn"),
		State:  store,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got, want := result.RowsIngested, int64(3); got != want {
		t.Errorf("RowsIngested = %d, want %d", got, want)
	}
	if got, want := lines(out), 3; got != want {
		t.Errorf("wrote %d NDJSON lines, want %d", got, want)
	}
	if got, want := result.Watermark, at(3); got != want {
		t.Errorf("Watermark = %q, want %q", got, want)
	}
	if store.Saved == nil {
		t.Fatal("the first run must record the shape it accepted")
	}
	if _, ok := store.Saved.Fields["updated_at"]; !ok {
		t.Error("the accepted shape should include updated_at")
	}
}

// The second run is where duplicates come from: the server's since filter is
// inclusive, so it re-sends the record the watermark points at.
func TestRunResumingDoesNotDuplicateTheBoundaryRecord(t *testing.T) {
	server := eventsServer(t, []map[string]any{
		{"id": 1, "updated_at": at(1)},
		{"id": 2, "updated_at": at(2)},
		{"id": 3, "updated_at": at(3)},
	})
	store := &MemoryStore{WatermarkValue: at(2)}

	result, out, err := runWorker(t, Options{Config: testConfig(server.URL, "Warn"), State: store})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got, want := result.RowsIngested, int64(1); got != want {
		t.Errorf("RowsIngested = %d, want %d; the record at the watermark was already loaded", got, want)
	}
	if !strings.Contains(out, `"id":3`) {
		t.Errorf("expected only the record after the watermark, got %q", out)
	}
	if got, want := result.Watermark, at(3); got != want {
		t.Errorf("Watermark = %q, want %q", got, want)
	}
}

func TestRunFailsClosedOnBreakingDrift(t *testing.T) {
	server := eventsServer(t, []map[string]any{
		{"id": 1, "amount": "12.50", "updated_at": at(1)},
	})
	// The last accepted shape had a numeric amount and an email field.
	store := &MemoryStore{SchemaValue: runspec.NewSchema(map[string]string{
		"id":         TypeNumber,
		"amount":     TypeNumber,
		"email":      TypeString,
		"updated_at": TypeString,
	})}

	result, out, err := runWorker(t, Options{Config: testConfig(server.URL, "EvolveAdditive"), State: store})
	if err == nil {
		t.Fatal("a retyped column and a dropped field must fail the run")
	}
	if got, want := result.RowsIngested, int64(0); got != want {
		t.Errorf("RowsIngested = %d, want %d; nothing may be written once drift is detected", got, want)
	}
	if out != "" {
		t.Errorf("no records should reach the sink, got %q", out)
	}
	if result.SchemaChange == nil || !result.SchemaChange.Breaking() {
		t.Error("the result should describe the breaking change")
	}
}

func TestRunAcceptsAdditiveDrift(t *testing.T) {
	server := eventsServer(t, []map[string]any{
		{"id": 1, "tenant": "acme", "updated_at": at(1)},
	})
	store := &MemoryStore{SchemaValue: runspec.NewSchema(map[string]string{
		"id":         TypeNumber,
		"updated_at": TypeString,
	})}

	result, _, err := runWorker(t, Options{Config: testConfig(server.URL, "EvolveAdditive"), State: store})
	if err != nil {
		t.Fatalf("a new field is additive and must not fail the run: %v", err)
	}
	if got, want := result.RowsIngested, int64(1); got != want {
		t.Errorf("RowsIngested = %d, want %d", got, want)
	}
	if store.Saved == nil {
		t.Fatal("the widened shape must be recorded so the next run compares against it")
	}
	if _, ok := store.Saved.Fields["tenant"]; !ok {
		t.Error("the recorded shape should include the new field")
	}
}

func TestRunFailPolicyRejectsAnyChange(t *testing.T) {
	server := eventsServer(t, []map[string]any{{"id": 1, "tenant": "acme", "updated_at": at(1)}})
	store := &MemoryStore{SchemaValue: runspec.NewSchema(map[string]string{
		"id":         TypeNumber,
		"updated_at": TypeString,
	})}

	if _, _, err := runWorker(t, Options{Config: testConfig(server.URL, "Fail"), State: store}); err == nil {
		t.Fatal("the Fail policy must reject even an additive change")
	}
}

// Nothing has been accepted yet on a first run, so whatever arrives defines
// the shape rather than conflicting with one.
func TestRunFirstRunNeverReportsDrift(t *testing.T) {
	server := eventsServer(t, []map[string]any{{"id": 1, "updated_at": at(1)}})

	result, _, err := runWorker(t, Options{
		Config: testConfig(server.URL, "Fail"),
		State:  &MemoryStore{},
	})
	if err != nil {
		t.Fatalf("the first run must not fail on drift: %v", err)
	}
	if result.SchemaFingerprint == "" {
		t.Error("the first run should record a fingerprint")
	}
}

// A backfill pulls a closed historical range. Letting it move the watermark
// would skip everything between that range and the present.
func TestRunBackfillDoesNotMoveTheWatermark(t *testing.T) {
	server := eventsServer(t, []map[string]any{
		{"id": 1, "updated_at": at(1)},
		{"id": 2, "updated_at": at(2)},
		{"id": 3, "updated_at": at(9)},
	})
	store := &MemoryStore{WatermarkValue: at(8)}

	result, _, err := runWorker(t, Options{
		Config:   testConfig(server.URL, "Warn"),
		State:    store,
		Backfill: &BackfillRange{ID: "jan", From: at(1), To: at(3)},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got, want := result.RowsIngested, int64(1); got != want {
		t.Errorf("RowsIngested = %d, want %d; only records inside the range belong to a backfill", got, want)
	}
	if result.Watermark != "" {
		t.Errorf("a backfill must not report a watermark, got %q", result.Watermark)
	}
	if got, want := result.BackfillID, "jan"; got != want {
		t.Errorf("BackfillID = %q, want %q", got, want)
	}
}

func TestRunCountsNonObjectRecordsAsRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{"not-a-record", map[string]any{"id": 1}}})
	}))
	defer server.Close()

	config := testConfig(server.URL, "Warn")
	config.Watermark = nil

	result, _, err := runWorker(t, Options{Config: config, State: &MemoryStore{}})
	if err != nil {
		t.Fatalf("one bad item should not fail the whole run: %v", err)
	}
	if got, want := result.RowsIngested, int64(1); got != want {
		t.Errorf("RowsIngested = %d, want %d", got, want)
	}
	if got, want := result.RowsRejected, int64(1); got != want {
		t.Errorf("RowsRejected = %d, want %d", got, want)
	}
}

func TestRunReportsCountsOnFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer server.Close()

	result, _, err := runWorker(t, Options{Config: testConfig(server.URL, "Warn"), State: &MemoryStore{}})
	if err == nil {
		t.Fatal("expected the run to fail")
	}
	if result.Error == "" {
		t.Error("a failed run must carry its error in the result the controller reads")
	}
	if result.RequestCount == 0 {
		t.Error("a failed run should still report how many upstream calls it made")
	}
}
