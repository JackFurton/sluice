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
	"fmt"
	"strconv"
	"time"

	"github.com/JackFurton/sluice/internal/runspec"
)

// WatermarkBounds are the request-side bounds for one run. From is the value
// the last successful run reached; To is set only for a backfill.
type WatermarkBounds struct {
	Param   string
	ToParam string
	From    string
	To      string
}

// Tracker filters records against the watermark and remembers the highest
// value seen, which becomes the resume point for the next run.
//
// The filtering matters because most vendor APIs treat a "since" parameter as
// inclusive. Resuming from the last value therefore re-delivers the boundary
// records, and appending them again is how a table quietly grows duplicates.
type Tracker struct {
	field    string
	format   string
	from     string
	to       string
	ordered  bool
	highest  string
	hasValue bool
	// skipped counts records filtered out as already seen or out of range.
	skipped int64
}

// NewTracker builds a Tracker. A nil watermark config yields a Tracker that
// accepts everything and reports no value, which is the full-pull case.
func NewTracker(field, format, from, to string) *Tracker {
	return &Tracker{
		field:   field,
		format:  format,
		from:    from,
		to:      to,
		ordered: field != "" && format != runspec.WatermarkOpaque,
	}
}

// Enabled reports whether a watermark field was configured.
func (t *Tracker) Enabled() bool { return t != nil && t.field != "" }

// Skipped reports how many records were filtered out.
func (t *Tracker) Skipped() int64 {
	if t == nil {
		return 0
	}
	return t.skipped
}

// Value reports the highest watermark observed, or the empty string if the
// run saw none.
func (t *Tracker) Value() string {
	if t == nil || !t.hasValue {
		return ""
	}
	return t.highest
}

// Accept decides whether a record belongs in this run and folds its watermark
// into the running maximum.
//
// A record missing the watermark field is accepted rather than dropped. The
// alternative silently discards data whenever a vendor omits a field, and
// losing rows is worse than carrying a duplicate.
func (t *Tracker) Accept(record any) bool {
	if !t.Enabled() {
		return true
	}
	value, ok := ExtractString(record, t.field)
	if !ok {
		return true
	}

	if t.ordered {
		if t.from != "" {
			if cmp, err := compare(t.format, value, t.from); err == nil && cmp <= 0 {
				t.skipped++
				return false
			}
		}
		if t.to != "" {
			if cmp, err := compare(t.format, value, t.to); err == nil && cmp > 0 {
				t.skipped++
				return false
			}
		}
	}

	t.observe(value)
	return true
}

func (t *Tracker) observe(value string) {
	if !t.hasValue {
		t.highest, t.hasValue = value, true
		return
	}
	if !t.ordered {
		// Opaque watermarks have no order, so the last value wins. That is
		// correct only if the source returns records in ascending order, which
		// is the contract an opaque cursor implies.
		t.highest = value
		return
	}
	if cmp, err := compare(t.format, value, t.highest); err == nil && cmp > 0 {
		t.highest = value
	}
}

// compare orders two watermark values under the configured format. It returns
// a negative number, zero, or a positive number, like strings.Compare.
func compare(format, a, b string) (int, error) {
	switch format {
	case runspec.WatermarkUnixSeconds:
		ai, err := strconv.ParseInt(a, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse watermark %q as unix seconds: %w", a, err)
		}
		bi, err := strconv.ParseInt(b, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse watermark %q as unix seconds: %w", b, err)
		}
		switch {
		case ai < bi:
			return -1, nil
		case ai > bi:
			return 1, nil
		default:
			return 0, nil
		}
	case "", runspec.WatermarkRFC3339:
		at, err := time.Parse(time.RFC3339, a)
		if err != nil {
			return 0, fmt.Errorf("parse watermark %q as RFC3339: %w", a, err)
		}
		bt, err := time.Parse(time.RFC3339, b)
		if err != nil {
			return 0, fmt.Errorf("parse watermark %q as RFC3339: %w", b, err)
		}
		return at.Compare(bt), nil
	default:
		return 0, fmt.Errorf("watermark format %q has no ordering", format)
	}
}
