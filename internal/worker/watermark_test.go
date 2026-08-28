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

import "testing"

func record(updated string) map[string]any {
	return map[string]any{"id": 1, "updated_at": updated}
}

// The inclusive-since problem is the reason this type exists: resuming from
// the last watermark re-delivers the boundary record, and appending it again
// is how a table grows duplicates one run at a time.
func TestTrackerDropsTheBoundaryRecord(t *testing.T) {
	tracker := NewTracker("updated_at", "RFC3339", "2026-01-02T00:00:00Z", "")

	if tracker.Accept(record("2026-01-02T00:00:00Z")) {
		t.Error("a record exactly at the watermark was already ingested and must be dropped")
	}
	if !tracker.Accept(record("2026-01-02T00:00:01Z")) {
		t.Error("a record after the watermark must be accepted")
	}
	if got, want := tracker.Skipped(), int64(1); got != want {
		t.Errorf("Skipped() = %d, want %d", got, want)
	}
	if got, want := tracker.Value(), "2026-01-02T00:00:01Z"; got != want {
		t.Errorf("Value() = %q, want %q", got, want)
	}
}

func TestTrackerKeepsTheHighestValueNotTheLast(t *testing.T) {
	tracker := NewTracker("updated_at", "RFC3339", "", "")

	for _, ts := range []string{"2026-01-03T00:00:00Z", "2026-01-05T00:00:00Z", "2026-01-04T00:00:00Z"} {
		if !tracker.Accept(record(ts)) {
			t.Fatalf("record %s should have been accepted", ts)
		}
	}
	if got, want := tracker.Value(), "2026-01-05T00:00:00Z"; got != want {
		t.Errorf("Value() = %q, want %q; out-of-order pages must not move the watermark backwards", got, want)
	}
}

// Dropping a record because the vendor omitted the watermark field loses data,
// which is worse than the duplicate that keeping it might cause.
func TestTrackerAcceptsRecordsMissingTheField(t *testing.T) {
	tracker := NewTracker("updated_at", "RFC3339", "2026-01-02T00:00:00Z", "")

	if !tracker.Accept(map[string]any{"id": 9}) {
		t.Error("a record without the watermark field must be accepted, not silently dropped")
	}
	if got, want := tracker.Value(), "2026-01-02T00:00:00Z"; got == want {
		t.Error("a record without the field must not set the watermark")
	}
}

func TestTrackerHonorsBackfillUpperBound(t *testing.T) {
	tracker := NewTracker("updated_at", "RFC3339", "2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z")

	if !tracker.Accept(record("2026-01-15T00:00:00Z")) {
		t.Error("a record inside the range must be accepted")
	}
	if tracker.Accept(record("2026-02-01T00:00:00Z")) {
		t.Error("a record past the upper bound must be dropped")
	}
}

func TestTrackerUnixSeconds(t *testing.T) {
	tracker := NewTracker("updated_at", "UnixSeconds", "1000", "")

	if tracker.Accept(record("999")) {
		t.Error("an older record must be dropped")
	}
	if !tracker.Accept(record("1001")) {
		t.Error("a newer record must be accepted")
	}
	if got, want := tracker.Value(), "1001"; got != want {
		t.Errorf("Value() = %q, want %q", got, want)
	}
}

// An opaque watermark has no ordering, so the contract is that the source
// returns records in ascending order and the last value seen wins.
func TestTrackerOpaqueTakesTheLastValue(t *testing.T) {
	tracker := NewTracker("updated_at", "Opaque", "zzz", "")

	if !tracker.Accept(record("aaa")) {
		t.Error("an opaque watermark cannot filter, so everything is accepted")
	}
	if !tracker.Accept(record("bbb")) {
		t.Fatal("expected acceptance")
	}
	if got, want := tracker.Value(), "bbb"; got != want {
		t.Errorf("Value() = %q, want %q", got, want)
	}
}

func TestTrackerDisabledWithoutAField(t *testing.T) {
	tracker := NewTracker("", "", "", "")

	if !tracker.Accept(record("2026-01-01T00:00:00Z")) {
		t.Error("a full pull accepts everything")
	}
	if tracker.Value() != "" {
		t.Error("a full pull reports no watermark")
	}
}

// A value the format cannot parse must not silently drop the record. Failing
// open keeps a malformed timestamp from erasing a night of data.
func TestTrackerKeepsUnparseableValues(t *testing.T) {
	tracker := NewTracker("updated_at", "RFC3339", "2026-01-02T00:00:00Z", "")

	if !tracker.Accept(record("not-a-timestamp")) {
		t.Error("an unparseable watermark must not cause the record to be dropped")
	}
}
