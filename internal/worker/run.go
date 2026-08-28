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
	"fmt"
	"io"
	"log/slog"

	"github.com/JackFurton/sluice/internal/runspec"
)

// batchSize is how many records are handed to the sink at once. It bounds
// memory for a source that returns very large pages and matches the order of
// magnitude BigQuery streaming inserts are happy with.
const batchSize = 500

// StateStore is the run's view of the cluster: the watermark to resume from
// and the record shape the last run accepted. It is an interface so the run
// logic can be tested without an API server.
type StateStore interface {
	// Watermark returns the value the next run should resume after.
	Watermark(ctx context.Context) (string, error)
	// Schema returns the last accepted record shape.
	Schema(ctx context.Context) (runspec.Schema, error)
	// SaveSchema records a newly accepted record shape.
	SaveSchema(ctx context.Context, schema runspec.Schema) error
}

// Options configure a single run.
type Options struct {
	Config runspec.RunConfig
	Creds  Credentials
	State  StateStore
	// Backfill, when set, replaces the stored watermark with a fixed range and
	// suppresses advancing it.
	Backfill *BackfillRange
	// Stdout receives records when the destination is Stdout.
	Stdout io.Writer
	Logger *slog.Logger
}

// BackfillRange is a closed watermark range pulled outside the normal schedule.
type BackfillRange struct {
	ID   string
	From string
	To   string
}

// Run performs one ingestion run and returns what it did. A returned error
// always comes with a RunResult, because the counts up to the failure are the
// useful part of a failed run.
func Run(ctx context.Context, opts Options) (runspec.RunResult, error) {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	result := runspec.RunResult{}

	bounds, tracker, err := resolveWatermark(ctx, opts)
	if err != nil {
		return result, err
	}
	if opts.Backfill != nil {
		result.BackfillID = opts.Backfill.ID
	}

	previous, err := opts.State.Schema(ctx)
	if err != nil {
		return result, fmt.Errorf("read accepted schema: %w", err)
	}

	fetcher := NewFetcher(opts.Config.Source, opts.Creds)

	var (
		sink     Sink
		observed = map[string]string{}
		pending  = make([]map[string]any, 0, batchSize)
		change   runspec.SchemaChange
	)
	defer func() {
		if sink != nil {
			if cerr := sink.Close(ctx); cerr != nil {
				log.Error("closing sink", "error", cerr)
			}
		}
	}()

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if sink == nil {
			// The sink is opened lazily so a BigQuery table can be created
			// from a shape that was actually observed rather than declared.
			sink, err = NewSink(ctx, opts.Config.Destination, opts.Stdout, observed)
			if err != nil {
				return err
			}
		}
		written, rejected, err := sink.Write(ctx, pending)
		result.RowsIngested += written
		result.RowsRejected += rejected
		pending = pending[:0]
		return err
	}

	page := func(records []any) error {
		for _, record := range records {
			object, ok := record.(map[string]any)
			if !ok {
				// A non-object record cannot become a row. Counting it is
				// more useful than failing the whole run over one item.
				result.RowsRejected++
				continue
			}
			if !tracker.Accept(object) {
				continue
			}
			observe(observed, "", object, 0)
			pending = append(pending, object)
			if len(pending) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}

		// Drift is checked before the batch is written, so a Fail policy stops
		// bad data at the door rather than reporting it afterwards.
		change = runspec.Diff(previous.Fields, observed)
		return checkDrift(opts.Config.SchemaDrift, previous, change, log)
	}

	fetchErr := fetcher.Fetch(ctx, bounds, page)

	// A failed fetch does not get a final flush. Drift detection is what makes
	// this matter: the policy rejects the batch before it is written, and
	// flushing anyway would write exactly the records the policy refused.
	var flushErr error
	if fetchErr == nil {
		flushErr = flush()
	}

	result.RequestCount = fetcher.Requests()
	result.SchemaFingerprint = runspec.Fingerprint(observed)
	if !change.Empty() {
		result.SchemaChange = &change
	}
	if opts.Backfill == nil {
		result.Watermark = tracker.Value()
	}

	if err := firstError(fetchErr, flushErr); err != nil {
		result.Error = err.Error()
		return result, err
	}

	if len(observed) > 0 && !change.Empty() {
		if err := opts.State.SaveSchema(ctx, runspec.NewSchema(observed)); err != nil {
			return result, fmt.Errorf("save accepted schema: %w", err)
		}
	}

	log.Info("run complete",
		"rowsIngested", result.RowsIngested,
		"rowsRejected", result.RowsRejected,
		"rowsSkipped", tracker.Skipped(),
		"requests", result.RequestCount,
		"watermark", result.Watermark,
		"schemaFingerprint", result.SchemaFingerprint,
	)
	return result, nil
}

// resolveWatermark works out the bounds for this run: a backfill's fixed range,
// or everything since the last successful run.
func resolveWatermark(ctx context.Context, opts Options) (WatermarkBounds, *Tracker, error) {
	wm := opts.Config.Watermark
	if wm == nil {
		return WatermarkBounds{}, NewTracker("", "", "", ""), nil
	}

	if bf := opts.Backfill; bf != nil {
		bounds := WatermarkBounds{Param: wm.Param, ToParam: wm.ToParam, From: bf.From, To: bf.To}
		return bounds, NewTracker(wm.RecordField, wm.Format, bf.From, bf.To), nil
	}

	from, err := opts.State.Watermark(ctx)
	if err != nil {
		return WatermarkBounds{}, nil, fmt.Errorf("read watermark: %w", err)
	}
	if from == "" {
		from = wm.InitialValue
	}
	bounds := WatermarkBounds{Param: wm.Param, ToParam: wm.ToParam, From: from}
	return bounds, NewTracker(wm.RecordField, wm.Format, from, ""), nil
}

// checkDrift applies the configured policy to an observed shape change.
func checkDrift(policy string, previous runspec.Schema, change runspec.SchemaChange, log *slog.Logger) error {
	if change.Empty() || len(previous.Fields) == 0 {
		// Nothing to compare against on the first run. Whatever shows up is,
		// by definition, the shape being adopted.
		return nil
	}
	switch policy {
	case "", runspec.DriftIgnore:
		return nil
	case runspec.DriftWarn:
		log.Warn("record shape changed", "change", change.String())
		return nil
	case runspec.DriftEvolveAdditive:
		if change.Breaking() {
			return fmt.Errorf("record shape changed in a non-additive way: %s", change.String())
		}
		log.Info("record shape gained fields", "change", change.String())
		return nil
	case runspec.DriftFail:
		return fmt.Errorf("record shape changed: %s", change.String())
	default:
		return fmt.Errorf("unsupported schema drift policy %q", policy)
	}
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
