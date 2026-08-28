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
	"encoding/json"
	"reflect"
	"testing"

	"github.com/JackFurton/sluice/internal/runspec"
)

func decode(t *testing.T, raw string) []any {
	t.Helper()
	var records []any
	if err := json.Unmarshal([]byte(raw), &records); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	return records
}

func TestObserveSchema(t *testing.T) {
	tests := []struct {
		name    string
		records string
		want    map[string]string
	}{
		{
			name:    "scalars",
			records: `[{"id":1,"name":"a","ok":true}]`,
			want:    map[string]string{"id": TypeNumber, "name": TypeString, "ok": TypeBool},
		},
		{
			name: "a field missing from one record still counts",
			// A field only some records carry is exactly what a hand-written
			// schema misses, so every record has to contribute.
			records: `[{"id":1},{"id":2,"email":"a@b.invalid"}]`,
			want:    map[string]string{"id": TypeNumber, "email": TypeString},
		},
		{
			name:    "null carries no shape and loses to a concrete type",
			records: `[{"id":null},{"id":7}]`,
			want:    map[string]string{"id": TypeNumber},
		},
		{
			name:    "two concrete types for one field are mixed",
			records: `[{"amount":1.5},{"amount":"1.50"}]`,
			want:    map[string]string{"amount": TypeMixed},
		},
		{
			name:    "nested objects are walked with dotted paths",
			records: `[{"meta":{"source":"api","retries":2}}]`,
			want: map[string]string{
				"meta":         TypeObject,
				"meta.source":  TypeString,
				"meta.retries": TypeNumber,
			},
		},
		{
			name:    "arrays record their element type",
			records: `[{"tags":["a","b"],"scores":[1,2]}]`,
			want:    map[string]string{"tags": "array<string>", "scores": "array<number>"},
		},
		{
			name:    "an empty array has no element type",
			records: `[{"tags":[]}]`,
			want:    map[string]string{"tags": "array<>"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ObserveSchema(decode(t, tc.records))
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ObserveSchema() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestObserveSchemaStopsAtMaxDepth(t *testing.T) {
	// Build a document nested deeper than the limit and check the walk stops
	// rather than following it all the way down.
	deep := `{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":1}}}}}}}}}}`
	fields := ObserveSchema(decode(t, "["+deep+"]"))

	for _, path := range Paths(fields) {
		if len(path) > 2*maxSchemaDepth {
			t.Fatalf("walked past the depth limit: %s", path)
		}
	}
	if len(fields) == 0 {
		t.Fatal("expected the shallow fields to still be observed")
	}
}

func TestFingerprintIgnoresFieldOrder(t *testing.T) {
	a := map[string]string{"id": TypeNumber, "name": TypeString}
	b := map[string]string{"name": TypeString, "id": TypeNumber}

	if runspec.Fingerprint(a) != runspec.Fingerprint(b) {
		t.Error("fingerprint changed with field order; it must not")
	}
	if runspec.Fingerprint(a) == "" {
		t.Error("fingerprint of a non-empty schema must not be empty")
	}
	if runspec.Fingerprint(nil) != "" {
		t.Error("fingerprint of an empty schema must be empty")
	}
}

func TestDiffClassifiesChanges(t *testing.T) {
	previous := map[string]string{"id": TypeNumber, "email": TypeString, "amount": TypeNumber}
	observed := map[string]string{"id": TypeNumber, "amount": TypeString, "tenant": TypeString}

	change := runspec.Diff(previous, observed)

	if got, want := change.Added, []string{"tenant"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Added = %v, want %v", got, want)
	}
	if got, want := change.Removed, []string{"email"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Removed = %v, want %v", got, want)
	}
	if got, want := change.Changed, []string{"amount(number->string)"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Changed = %v, want %v", got, want)
	}
	if !change.Breaking() {
		t.Error("a removed field and a retyped field are breaking")
	}
}

func TestAddedFieldsAreNotBreaking(t *testing.T) {
	change := runspec.Diff(
		map[string]string{"id": TypeNumber},
		map[string]string{"id": TypeNumber, "tenant": TypeString},
	)
	if change.Empty() {
		t.Fatal("expected a change")
	}
	if change.Breaking() {
		t.Error("a new field is additive, not breaking")
	}
}

func TestExtract(t *testing.T) {
	var doc any
	if err := json.Unmarshal([]byte(`{"data":{"items":[{"id":1}]},"meta":{"next_cursor":"50","count":7}}`), &doc); err != nil {
		t.Fatalf("decode: %v", err)
	}

	records, err := ExtractRecords(doc, "data.items")
	if err != nil {
		t.Fatalf("ExtractRecords: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("got %d records, want 1", len(records))
	}

	if cursor, ok := ExtractString(doc, "meta.next_cursor"); !ok || cursor != "50" {
		t.Errorf("ExtractString(meta.next_cursor) = %q, %v; want \"50\", true", cursor, ok)
	}
	if count, ok := ExtractString(doc, "meta.count"); !ok || count != "7" {
		t.Errorf("a numeric cursor should render as a string, got %q, %v", count, ok)
	}
	if _, ok := ExtractString(doc, "meta.absent"); ok {
		t.Error("a missing path must report not found")
	}
	if _, err := ExtractRecords(doc, "meta"); err == nil {
		t.Error("pointing recordsPath at an object should be an error, not an empty run")
	}
}
