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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/JackFurton/sluice/internal/runspec"
)

// items builds a page of records with sequential ids.
func items(from, count int) []map[string]any {
	out := make([]map[string]any, 0, count)
	for i := range count {
		out = append(out, map[string]any{"id": from + i})
	}
	return out
}

func collect(t *testing.T, f *Fetcher, bounds WatermarkBounds) []any {
	t.Helper()
	var all []any
	err := f.Fetch(context.Background(), bounds, func(records []any) error {
		all = append(all, records...)
		return nil
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	return all
}

func TestFetchCursorPagination(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		offset, _ := strconv.Atoi(r.URL.Query().Get("cursor"))
		body := map[string]any{"data": map[string]any{"items": items(offset, 2)}, "meta": map[string]any{}}
		if offset < 4 {
			body["meta"] = map[string]any{"next_cursor": strconv.Itoa(offset + 2)}
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer server.Close()

	f := NewFetcher(runspec.SourceConfig{
		URL:         server.URL,
		RecordsPath: "data.items",
		Pagination: runspec.PaginationConfig{
			Type:           "Cursor",
			NextCursorPath: "meta.next_cursor",
			CursorParam:    "cursor",
			MaxPages:       10,
		},
	}, Credentials{})

	if got, want := len(collect(t, f, WatermarkBounds{})), 6; got != want {
		t.Errorf("collected %d records, want %d", got, want)
	}
	if got, want := requests.Load(), int32(3); got != want {
		t.Errorf("made %d requests, want %d", got, want)
	}
}

// A cursor that repeats itself would otherwise loop until maxPages, turning a
// vendor bug into a runaway job.
func TestFetchStopsOnRepeatedCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"items": items(0, 1)},
			"meta": map[string]any{"next_cursor": "same"},
		})
	}))
	defer server.Close()

	f := NewFetcher(runspec.SourceConfig{
		URL:         server.URL,
		RecordsPath: "data.items",
		Pagination: runspec.PaginationConfig{
			Type: "Cursor", NextCursorPath: "meta.next_cursor", CursorParam: "cursor", MaxPages: 100,
		},
	}, Credentials{})

	// The first repeat ends the walk: page one asks with no cursor, page two
	// asks with "same", and page three would ask with "same" again.
	if got, want := len(collect(t, f, WatermarkBounds{})), 2; got != want {
		t.Errorf("collected %d records, want %d", got, want)
	}
}

func TestFetchPageNumberStopsOnShortPage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		count := 3
		if page >= 3 {
			count = 1
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items(page*3, count)})
	}))
	defer server.Close()

	f := NewFetcher(runspec.SourceConfig{
		URL:         server.URL,
		RecordsPath: "items",
		Pagination: runspec.PaginationConfig{
			Type: "PageNumber", PageParam: "page", SizeParam: "limit", PageSize: 3, MaxPages: 10,
		},
	}, Credentials{})

	// Pages are 1-based by default, so pages 1 and 2 are full and page 3 is short.
	if got, want := len(collect(t, f, WatermarkBounds{})), 7; got != want {
		t.Errorf("collected %d records, want %d", got, want)
	}
}

// Running out of pages is a failure, not a clean partial load. Reporting
// success here is how half a table gets loaded and nobody notices.
func TestFetchFailsWhenMaxPagesIsReached(t *testing.T) {
	var page atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Every response advertises a fresh cursor, so only the page cap can
		// end the walk.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": items(0, 5),
			"meta":  map[string]any{"next_cursor": strconv.Itoa(int(page.Add(1)))},
		})
	}))
	defer server.Close()

	f := NewFetcher(runspec.SourceConfig{
		URL:         server.URL,
		RecordsPath: "items",
		Pagination: runspec.PaginationConfig{
			Type: "Cursor", NextCursorPath: "meta.next_cursor", CursorParam: "c", MaxPages: 2,
		},
	}, Credentials{})

	err := f.Fetch(context.Background(), WatermarkBounds{}, func([]any) error { return nil })
	if err == nil {
		t.Fatal("expected an error when the result set never ends")
	}
	if !strings.Contains(err.Error(), "maxPages") {
		t.Errorf("error should name the limit that stopped the run, got %v", err)
	}
}

func TestFetchRetriesRateLimitAndHonorsRetryAfter(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items(0, 2)})
	}))
	defer server.Close()

	f := NewFetcher(runspec.SourceConfig{URL: server.URL, RecordsPath: "items"}, Credentials{})

	if got, want := len(collect(t, f, WatermarkBounds{})), 2; got != want {
		t.Errorf("collected %d records, want %d", got, want)
	}
	// Retries count as upstream calls, because the vendor's rate limit saw them.
	if got, want := f.Requests(), int32(2); got != want {
		t.Errorf("Requests() = %d, want %d", got, want)
	}
}

func TestFetchDoesNotRetryClientErrors(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, `{"error":"bad token"}`, http.StatusUnauthorized)
	}))
	defer server.Close()

	f := NewFetcher(runspec.SourceConfig{URL: server.URL, RecordsPath: "items"}, Credentials{})

	err := f.Fetch(context.Background(), WatermarkBounds{}, func([]any) error { return nil })
	if err == nil {
		t.Fatal("expected a 401 to fail the run")
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("a 401 was retried %d times; it will never succeed", got-1)
	}
}

func TestFetchSendsAuthAndWatermark(t *testing.T) {
	var (
		gotAuth  string
		gotSince string
		gotKey   string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotKey = r.Header.Get("X-Api-Key")
		gotSince = r.URL.Query().Get("since")
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items(0, 1)})
	}))
	defer server.Close()

	f := NewFetcher(runspec.SourceConfig{
		URL:         server.URL,
		RecordsPath: "items",
		AuthType:    "Bearer",
		Headers: []runspec.HeaderConfig{
			{Name: "X-Api-Key", ValueEnv: "IGNORED", FromSecret: true},
		},
	}, Credentials{
		Token:   "s3cret",
		Headers: map[string]string{"X-Api-Key": "from-secret"},
	})

	collect(t, f, WatermarkBounds{Param: "since", From: "2026-01-01T00:00:00Z"})

	if want := "Bearer s3cret"; gotAuth != want {
		t.Errorf("Authorization = %q, want %q", gotAuth, want)
	}
	if want := "from-secret"; gotKey != want {
		t.Errorf("X-Api-Key = %q, want %q", gotKey, want)
	}
	if want := "2026-01-01T00:00:00Z"; gotSince != want {
		t.Errorf("since = %q, want %q", gotSince, want)
	}
}

func TestFetchFollowsLinkHeader(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("p")
		if page == "" {
			w.Header().Set("Link", fmt.Sprintf(`<%s/?p=2>; rel="next", <%s/?p=9>; rel="last"`, server.URL, server.URL))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items(0, 1)})
	}))
	defer server.Close()

	f := NewFetcher(runspec.SourceConfig{
		URL:         server.URL,
		RecordsPath: "items",
		Pagination:  runspec.PaginationConfig{Type: "LinkHeader", MaxPages: 5},
	}, Credentials{})

	if got, want := len(collect(t, f, WatermarkBounds{})), 2; got != want {
		t.Errorf("collected %d records, want %d", got, want)
	}
}

func TestNextLink(t *testing.T) {
	tests := []struct {
		header string
		want   string
	}{
		{`<https://api.invalid/2>; rel="next"`, "https://api.invalid/2"},
		{`<https://api.invalid/9>; rel="last", <https://api.invalid/2>; rel="next"`, "https://api.invalid/2"},
		{`<https://api.invalid/9>; rel="last"`, ""},
		{"", ""},
		{"garbage", ""},
	}
	for _, tc := range tests {
		if got := nextLink(tc.header); got != tc.want {
			t.Errorf("nextLink(%q) = %q, want %q", tc.header, got, tc.want)
		}
	}
}
