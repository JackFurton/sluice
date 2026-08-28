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
	"io"
	"sync"

	"github.com/JackFurton/sluice/internal/runspec"
)

// Sink accepts records and reports how many landed.
type Sink interface {
	// Write sends a batch. It returns the number of records written and the
	// number rejected. A rejected record is one the sink refused, which is a
	// data problem; an error is a sink problem and fails the run.
	Write(ctx context.Context, records []map[string]any) (written, rejected int64, err error)
	// Close flushes anything buffered.
	Close(ctx context.Context) error
}

// NewSink builds the sink named by a RunConfig.
func NewSink(ctx context.Context, cfg runspec.DestinationConfig, stdout io.Writer, fields map[string]string) (Sink, error) {
	switch cfg.Type {
	case "", runspec.DestinationStdout:
		return NewStdoutSink(stdout), nil
	case runspec.DestinationBigQuery:
		if cfg.BigQuery == nil {
			return nil, fmt.Errorf("destination type BigQuery requires bigQuery configuration")
		}
		return NewBigQuerySink(ctx, *cfg.BigQuery, fields)
	default:
		return nil, fmt.Errorf("unsupported destination type %q", cfg.Type)
	}
}

// StdoutSink writes newline-delimited JSON. It is what makes the operator
// demonstrable on a laptop: the same controller, CronJob, watermark and schema
// logic runs, with the warehouse replaced by a stream anyone can read.
type StdoutSink struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewStdoutSink builds a StdoutSink over w.
func NewStdoutSink(w io.Writer) *StdoutSink {
	return &StdoutSink{enc: json.NewEncoder(w)}
}

// Write emits one JSON object per record.
func (s *StdoutSink) Write(_ context.Context, records []map[string]any) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var written int64
	for _, rec := range records {
		if err := s.enc.Encode(rec); err != nil {
			return written, 0, fmt.Errorf("encode record: %w", err)
		}
		written++
	}
	return written, 0, nil
}

// Close is a no-op; the encoder writes straight through.
func (s *StdoutSink) Close(context.Context) error { return nil }
