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
	"fmt"
	"slices"
	"strings"
)

// Type names used in an observed schema. They are JSON types, not BigQuery
// types, because the point is to notice that the upstream changed, not to
// model the warehouse.
const (
	TypeString  = "string"
	TypeNumber  = "number"
	TypeBool    = "bool"
	TypeObject  = "object"
	TypeNull    = "null"
	TypeMixed   = "mixed"
	TypeUnknown = "unknown"
)

// maxSchemaDepth bounds recursion into nested records. Deeply nested vendor
// payloads exist, and an unbounded walk over a hostile or cyclic-looking
// document is a way to hang a run.
const maxSchemaDepth = 8

// ObserveSchema derives the record shape of a batch. Every record contributes,
// because a field that only appears in some records is exactly the kind of
// thing that is missing from a hand-written schema.
//
// Types are merged across records: a field seen as null in one record and as a
// string in another is a string, since null carries no shape information. A
// field seen with two different concrete types is "mixed", which is itself
// worth surfacing.
func ObserveSchema(records []any) map[string]string {
	fields := map[string]string{}
	for _, rec := range records {
		observe(fields, "", rec, 0)
	}
	return fields
}

func observe(fields map[string]string, prefix string, value any, depth int) {
	switch v := value.(type) {
	case map[string]any:
		if depth >= maxSchemaDepth {
			merge(fields, prefix, TypeObject)
			return
		}
		if prefix != "" {
			merge(fields, prefix, TypeObject)
		}
		for key, child := range v {
			observe(fields, join(prefix, key), child, depth+1)
		}
	case []any:
		merge(fields, prefix, arrayType(v))
	default:
		merge(fields, prefix, scalarType(value))
	}
}

func arrayType(items []any) string {
	elem := ""
	for _, item := range items {
		var t string
		switch item.(type) {
		case map[string]any:
			t = TypeObject
		case []any:
			t = "array"
		default:
			t = scalarType(item)
		}
		switch {
		case elem == "" || elem == TypeNull:
			elem = t
		case t == TypeNull || t == elem:
			// no new information
		default:
			elem = TypeMixed
		}
	}
	if elem == "" {
		return "array<>"
	}
	return "array<" + elem + ">"
}

func scalarType(value any) string {
	switch value.(type) {
	case nil:
		return TypeNull
	case bool:
		return TypeBool
	case string:
		return TypeString
	case float64, int, int64, json.Number:
		return TypeNumber
	default:
		return TypeUnknown
	}
}

// merge folds a newly observed type for a path into what is already known.
func merge(fields map[string]string, path, typ string) {
	if path == "" {
		return
	}
	existing, ok := fields[path]
	switch {
	case !ok, existing == TypeNull:
		fields[path] = typ
	case typ == TypeNull, typ == existing:
		// no new information
	default:
		fields[path] = TypeMixed
	}
}

func join(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

// Paths returns the field paths of a schema in sorted order.
func Paths(fields map[string]string) []string {
	paths := make([]string, 0, len(fields))
	for p := range fields {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	return paths
}

// Extract reads a dotted path out of a decoded JSON document. It walks objects
// only; the paths this operator cares about (a records array, a next cursor)
// never sit behind an array index in practice, and pretending to support a
// full JSONPath dialect would promise more than it delivers.
func Extract(doc any, path string) (any, bool) {
	if path == "" {
		return doc, true
	}
	current := doc
	for part := range strings.SplitSeq(path, ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = obj[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// ExtractString reads a dotted path and renders it as a string. Numeric
// cursors and IDs are common, so a number is formatted rather than rejected.
func ExtractString(doc any, path string) (string, bool) {
	value, ok := Extract(doc, path)
	if !ok || value == nil {
		return "", false
	}
	switch v := value.(type) {
	case string:
		return v, v != ""
	case float64:
		return strings.TrimSuffix(strings.TrimRight(fmt.Sprintf("%f", v), "0"), "."), true
	case bool:
		return fmt.Sprintf("%t", v), true
	default:
		return "", false
	}
}

// ExtractRecords reads the array of records out of a response body.
func ExtractRecords(doc any, path string) ([]any, error) {
	value, ok := Extract(doc, path)
	if !ok {
		return nil, fmt.Errorf("recordsPath %q not found in response", path)
	}
	records, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("recordsPath %q is %s, want an array", path, scalarTypeName(value))
	}
	return records, nil
}

func scalarTypeName(value any) string {
	switch value.(type) {
	case map[string]any:
		return "an object"
	case []any:
		return "an array"
	case nil:
		return "null"
	default:
		return scalarType(value)
	}
}
