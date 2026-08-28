//go:build integration

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

// These tests run the BigQuery sink against the BigQuery emulator rather than
// a fake. The sink is the part of this operator that touches a real warehouse,
// and "it compiles and looks right" is not the same as "it writes rows".
//
// Run them with: make test-bigquery
package worker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/JackFurton/sluice/internal/runspec"
)

const (
	testProject = "test-project"
	testDataset = "vendor"
)

func endpoint(t *testing.T) []option.ClientOption {
	t.Helper()

	host := os.Getenv("BIGQUERY_EMULATOR_HOST")
	if host == "" {
		t.Skip("BIGQUERY_EMULATOR_HOST is not set; run make test-bigquery")
	}
	return []option.ClientOption{
		option.WithEndpoint(host),
		option.WithoutAuthentication(),
	}
}

func testTable(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("events_%d", time.Now().UnixNano())
}

func bigQueryConfig(table string) runspec.BigQueryConfig {
	return runspec.BigQueryConfig{
		ProjectID:        testProject,
		Dataset:          testDataset,
		Table:            table,
		WriteDisposition: runspec.WriteDispositionAppend,
		CreateIfMissing:  true,
	}
}

// rowCount reads the table back, because the only way to know a write landed
// is to read it.
func rowCount(t *testing.T, ctx context.Context, table string) int64 {
	t.Helper()

	client, err := bigquery.NewClient(ctx, testProject, endpoint(t)...)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer func() { _ = client.Close() }()

	query := client.Query(fmt.Sprintf("SELECT COUNT(*) AS n FROM `%s.%s.%s`", testProject, testDataset, table))
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var row struct {
		N int64 `bigquery:"n"`
	}
	if err := it.Next(&row); err != nil && err != iterator.Done {
		t.Fatalf("read: %v", err)
	}
	return row.N
}

func TestBigQuerySinkCreatesTheTableAndWritesRows(t *testing.T) {
	ctx := context.Background()
	table := testTable(t)

	records := []map[string]any{
		{"id": 1.0, "name": "first", "active": true},
		{"id": 2.0, "name": "second", "active": false},
	}
	// The table is created from the shape the run actually observed, which is
	// the path that runs on a brand new source.
	fields := ObserveSchema([]any{records[0], records[1]})

	sink, err := NewBigQuerySink(ctx, bigQueryConfig(table), fields, endpoint(t)...)
	if err != nil {
		t.Fatalf("NewBigQuerySink: %v", err)
	}
	defer func() { _ = sink.Close(ctx) }()

	written, rejected, err := sink.Write(ctx, records)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if written != 2 || rejected != 0 {
		t.Fatalf("Write() = %d written, %d rejected; want 2, 0", written, rejected)
	}
	if got := rowCount(t, ctx, table); got != 2 {
		t.Errorf("table holds %d rows, want 2", got)
	}
}

// A nested object has no column of its own, so it is stored as JSON text. This
// is what keeps a vendor adding a nested field from breaking the load.
func TestBigQuerySinkStoresNestedValuesAsJSON(t *testing.T) {
	ctx := context.Background()
	table := testTable(t)

	records := []map[string]any{{
		"id":   1.0,
		"meta": map[string]any{"source": "api", "retries": 2.0},
		"tags": []any{"a", "b"},
	}}
	fields := ObserveSchema([]any{records[0]})

	sink, err := NewBigQuerySink(ctx, bigQueryConfig(table), fields, endpoint(t)...)
	if err != nil {
		t.Fatalf("NewBigQuerySink: %v", err)
	}
	defer func() { _ = sink.Close(ctx) }()

	written, _, err := sink.Write(ctx, records)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if written != 1 {
		t.Fatalf("wrote %d rows, want 1", written)
	}
	if got := rowCount(t, ctx, table); got != 1 {
		t.Errorf("table holds %d rows, want 1", got)
	}
}

// createIfMissing false is how a source is pinned to a table someone designed
// on purpose, with real types and partitioning.
func TestBigQuerySinkRefusesToCreateWhenTold(t *testing.T) {
	ctx := context.Background()

	cfg := bigQueryConfig(testTable(t))
	cfg.CreateIfMissing = false

	_, err := NewBigQuerySink(ctx, cfg, map[string]string{"id": TypeNumber}, endpoint(t)...)
	if err == nil {
		t.Fatal("expected an error when the table is missing and createIfMissing is false")
	}
}

// A truncating load declares that this run defines the table, so the previous
// contents go.
func TestBigQuerySinkTruncates(t *testing.T) {
	ctx := context.Background()
	table := testTable(t)
	fields := map[string]string{"id": TypeNumber}

	first, err := NewBigQuerySink(ctx, bigQueryConfig(table), fields, endpoint(t)...)
	if err != nil {
		t.Fatalf("NewBigQuerySink: %v", err)
	}
	if _, _, err := first.Write(ctx, []map[string]any{{"id": 1.0}, {"id": 2.0}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = first.Close(ctx)

	cfg := bigQueryConfig(table)
	cfg.WriteDisposition = runspec.WriteDispositionTruncate

	second, err := NewBigQuerySink(ctx, cfg, fields, endpoint(t)...)
	if err != nil {
		t.Fatalf("NewBigQuerySink truncating: %v", err)
	}
	defer func() { _ = second.Close(ctx) }()

	if _, _, err := second.Write(ctx, []map[string]any{{"id": 3.0}}); err != nil {
		t.Fatalf("Write after truncate: %v", err)
	}
	if got := rowCount(t, ctx, table); got != 1 {
		t.Errorf("table holds %d rows after a truncating load, want 1", got)
	}
}
