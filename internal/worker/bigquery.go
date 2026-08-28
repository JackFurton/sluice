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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/JackFurton/sluice/internal/runspec"
)

// deadLetterSchema is the fixed shape of the dead letter table. Keeping the
// payload as a string rather than a typed column is deliberate: the reason a
// record ended up here is usually that its shape was wrong.
var deadLetterSchema = bigquery.Schema{
	{Name: "rejected_at", Type: bigquery.TimestampFieldType, Required: true},
	{Name: "reason", Type: bigquery.StringFieldType},
	{Name: "payload", Type: bigquery.StringFieldType},
}

// BigQuerySink streams records into a BigQuery table.
type BigQuerySink struct {
	cfg        runspec.BigQueryConfig
	client     *bigquery.Client
	inserter   *bigquery.Inserter
	deadLetter *bigquery.Inserter
}

// NewBigQuerySink connects to BigQuery and prepares the destination table.
// Credentials come from Application Default Credentials, which on GKE means
// the Workload Identity binding on the run pod's service account. The operator
// never handles a service account key.
func NewBigQuerySink(ctx context.Context, cfg runspec.BigQueryConfig, fields map[string]string, opts ...option.ClientOption) (*BigQuerySink, error) {
	client, err := bigquery.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("bigquery client: %w", err)
	}
	if cfg.Location != "" {
		client.Location = cfg.Location
	}

	sink := &BigQuerySink{cfg: cfg, client: client}
	table := client.Dataset(cfg.Dataset).Table(cfg.Table)

	if err := sink.prepare(ctx, table, fields); err != nil {
		_ = client.Close()
		return nil, err
	}
	sink.inserter = table.Inserter()

	if cfg.DeadLetterTable != "" {
		dl := client.Dataset(cfg.Dataset).Table(cfg.DeadLetterTable)
		if err := ensureTable(ctx, dl, deadLetterSchema); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("prepare dead letter table: %w", err)
		}
		sink.deadLetter = dl.Inserter()
	}
	return sink, nil
}

// prepare creates or truncates the destination table as the spec requires.
func (s *BigQuerySink) prepare(ctx context.Context, table *bigquery.Table, fields map[string]string) error {
	_, err := table.Metadata(ctx)
	exists := err == nil
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("read table metadata: %w", err)
	}

	if exists && s.cfg.WriteDisposition == runspec.WriteDispositionTruncate {
		// Streaming inserts have no truncate mode, so the table is replaced.
		// Recreating it also discards the previous schema, which is the point:
		// a truncating load is declaring that this run defines the table.
		if err := table.Delete(ctx); err != nil {
			return fmt.Errorf("truncate table: %w", err)
		}
		exists = false
	}
	if exists {
		return nil
	}
	if !s.cfg.CreateIfMissing {
		return fmt.Errorf("table %s.%s does not exist and createIfMissing is false",
			s.cfg.Dataset, s.cfg.Table)
	}
	schema, err := inferSchema(fields)
	if err != nil {
		return err
	}
	return ensureTable(ctx, table, schema)
}

// Write streams a batch. Per-record rejections are counted and, when a dead
// letter table is configured, preserved. Everything else fails the run.
func (s *BigQuerySink) Write(ctx context.Context, records []map[string]any) (int64, int64, error) {
	if len(records) == 0 {
		return 0, 0, nil
	}
	savers := make([]*rowSaver, 0, len(records))
	for _, rec := range records {
		saver, err := newRowSaver(rec)
		if err != nil {
			return 0, 0, err
		}
		savers = append(savers, saver)
	}

	err := s.inserter.Put(ctx, savers)
	if err == nil {
		return int64(len(records)), 0, nil
	}

	var multi bigquery.PutMultiError
	if !errors.As(err, &multi) {
		return 0, 0, fmt.Errorf("bigquery insert: %w", err)
	}

	rejected := make(map[int]string, len(multi))
	for _, rowErr := range multi {
		rejected[rowErr.RowIndex] = rowErr.Error()
	}
	if err := s.writeDeadLetters(ctx, records, rejected); err != nil {
		return 0, 0, err
	}
	return int64(len(records) - len(rejected)), int64(len(rejected)), nil
}

func (s *BigQuerySink) writeDeadLetters(ctx context.Context, records []map[string]any, rejected map[int]string) error {
	if s.deadLetter == nil || len(rejected) == 0 {
		return nil
	}
	now := time.Now().UTC()
	rows := make([]*rowSaver, 0, len(rejected))
	for index, reason := range rejected {
		if index < 0 || index >= len(records) {
			continue
		}
		payload, err := json.Marshal(records[index])
		if err != nil {
			payload = fmt.Appendf(nil, "<unmarshalable record: %v>", err)
		}
		rows = append(rows, &rowSaver{
			row: map[string]bigquery.Value{
				"rejected_at": now,
				"reason":      truncate(reason, 4096),
				"payload":     truncate(string(payload), 1<<20),
			},
			insertID: digest(payload),
		})
	}
	if err := s.deadLetter.Put(ctx, rows); err != nil {
		return fmt.Errorf("write dead letter rows: %w", err)
	}
	return nil
}

// Close releases the client.
func (s *BigQuerySink) Close(context.Context) error { return s.client.Close() }

// rowSaver adapts a decoded JSON record to the BigQuery insert API.
type rowSaver struct {
	row      map[string]bigquery.Value
	insertID string
}

// Save implements bigquery.ValueSaver.
func (r *rowSaver) Save() (map[string]bigquery.Value, string, error) {
	return r.row, r.insertID, nil
}

// newRowSaver converts a record to BigQuery values. Nested objects and arrays
// become JSON strings, which keeps a vendor adding a nested field from
// breaking the load.
//
// The insert ID is a digest of the record, so a run that is retried after a
// partial insert does not duplicate rows. BigQuery deduplicates on this for a
// best-effort window, which is the strongest guarantee streaming inserts give.
func newRowSaver(record map[string]any) (*rowSaver, error) {
	row := make(map[string]bigquery.Value, len(record))
	for key, value := range record {
		switch v := value.(type) {
		case map[string]any, []any:
			encoded, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("encode field %q: %w", key, err)
			}
			row[key] = string(encoded)
		default:
			row[key] = v
		}
	}
	canonical, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("encode record: %w", err)
	}
	return &rowSaver{row: row, insertID: digest(canonical)}, nil
}

// inferSchema builds a table schema from an observed record shape. Only
// top-level fields become columns; anything nested is stored as JSON text.
//
// Numbers become FLOAT64 because the observed shape cannot tell an integer
// from a float. A table that needs INT64 or a partitioning column should be
// created ahead of time with createIfMissing left on; this path exists to make
// the first run work, not to design a warehouse.
func inferSchema(fields map[string]string) (bigquery.Schema, error) {
	var schema bigquery.Schema
	for _, path := range Paths(fields) {
		if strings.Contains(path, ".") {
			continue
		}
		schema = append(schema, &bigquery.FieldSchema{
			Name: path,
			Type: columnType(fields[path]),
		})
	}
	if len(schema) == 0 {
		return nil, errors.New("cannot create table: no fields observed in the first batch")
	}
	return schema, nil
}

func columnType(observed string) bigquery.FieldType {
	switch {
	case observed == TypeString:
		return bigquery.StringFieldType
	case observed == TypeNumber:
		return bigquery.FloatFieldType
	case observed == TypeBool:
		return bigquery.BooleanFieldType
	case observed == TypeObject, strings.HasPrefix(observed, "array<"), observed == TypeMixed:
		return bigquery.StringFieldType
	default:
		return bigquery.StringFieldType
	}
}

func ensureTable(ctx context.Context, table *bigquery.Table, schema bigquery.Schema) error {
	err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema})
	if err != nil && !isConflict(err) {
		return fmt.Errorf("create table %s: %w", table.TableID, err)
	}
	return nil
}

func isNotFound(err error) bool { return hasStatus(err, 404) }
func isConflict(err error) bool { return hasStatus(err, 409) }

func hasStatus(err error, code int) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == code
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
