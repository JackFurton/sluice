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

// Package runspec is the contract between the controller and the worker.
//
// The controller renders an IngestionSource spec into a RunConfig and stores it
// in a ConfigMap the run pod mounts. The worker reads it, performs the pull,
// and writes a RunResult as JSON to its termination message file. The
// controller reads that message back off the pod and folds it into status.
//
// Keeping both halves in one package means a field cannot drift between the
// producer and the consumer without the compiler noticing, which is the whole
// point: the failure this operator exists to prevent is a schema that changed
// on one side of a boundary and not the other.
package runspec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// Env var names the controller sets on the run pod. The worker reads only
// these; nothing about the run is passed positionally.
const (
	// EnvConfigPath points at the mounted RunConfig JSON.
	EnvConfigPath = "SLUICE_CONFIG"
	// EnvSourceName and EnvSourceNamespace identify the IngestionSource whose
	// status carries the watermark to resume from.
	EnvSourceName      = "SLUICE_SOURCE_NAME"
	EnvSourceNamespace = "SLUICE_SOURCE_NAMESPACE"
	// EnvSchemaConfigMap names the ConfigMap holding the last accepted record
	// shape. The controller pre-creates it so the worker needs no create verb.
	EnvSchemaConfigMap = "SLUICE_SCHEMA_CONFIGMAP"
	// EnvResultPath is where the worker writes its RunResult. It is the pod's
	// termination message file in a cluster and a temp file in tests.
	EnvResultPath = "SLUICE_RESULT_PATH"
	// EnvBackfillID, EnvBackfillFrom and EnvBackfillTo are set only on backfill
	// runs. A backfill ignores the stored watermark and never advances it.
	EnvBackfillID   = "SLUICE_BACKFILL_ID"
	EnvBackfillFrom = "SLUICE_BACKFILL_FROM"
	EnvBackfillTo   = "SLUICE_BACKFILL_TO"

	// EnvAuthToken, EnvAuthUsername and EnvAuthPassword carry credentials
	// projected from the Secret the IngestionSource points at. Credentials
	// reach the worker through the pod's env, never through the RunConfig,
	// so the ConfigMap holding the RunConfig stays safe to read and to dump
	// into a bug report.
	EnvAuthToken    = "SLUICE_AUTH_TOKEN"
	EnvAuthUsername = "SLUICE_AUTH_USERNAME"
	EnvAuthPassword = "SLUICE_AUTH_PASSWORD"
)

// The vocabulary a RunConfig is written in. These are the same words the CRD
// uses, but the worker must not import the API package: it would drag the whole
// controller-runtime scheme into a process whose job is to make HTTP requests.
const (
	PaginationNone       = "None"
	PaginationCursor     = "Cursor"
	PaginationPageNumber = "PageNumber"
	PaginationOffset     = "Offset"
	PaginationLinkHeader = "LinkHeader"

	DestinationStdout   = "Stdout"
	DestinationBigQuery = "BigQuery"

	AuthNone   = "None"
	AuthBearer = "Bearer"
	AuthHeader = "Header"
	AuthBasic  = "Basic"

	DriftIgnore         = "Ignore"
	DriftWarn           = "Warn"
	DriftEvolveAdditive = "EvolveAdditive"
	DriftFail           = "Fail"

	WatermarkRFC3339     = "RFC3339"
	WatermarkUnixSeconds = "UnixSeconds"
	WatermarkOpaque      = "Opaque"

	WriteDispositionAppend   = "Append"
	WriteDispositionTruncate = "Truncate"
)

// ConfigKey is the ConfigMap key holding the marshalled RunConfig.
const ConfigKey = "runconfig.json"

// SchemaKey is the ConfigMap key holding the marshalled Schema.
const SchemaKey = "schema.json"

// DefaultConfigMountPath is where the controller mounts the RunConfig ConfigMap.
const DefaultConfigMountPath = "/etc/sluice"

// RunConfig is everything a single run needs that does not change between
// runs. Anything that does change (the watermark, the accepted schema) is read
// from the cluster at run start so a long-lived CronJob template never goes
// stale.
type RunConfig struct {
	Source      SourceConfig      `json:"source"`
	Watermark   *WatermarkConfig  `json:"watermark,omitempty"`
	Destination DestinationConfig `json:"destination"`
	SchemaDrift string            `json:"schemaDrift"`
}

// SourceConfig describes the upstream pull.
type SourceConfig struct {
	URL                  string            `json:"url"`
	Method               string            `json:"method"`
	Headers              []HeaderConfig    `json:"headers,omitempty"`
	Query                map[string]string `json:"query,omitempty"`
	Body                 string            `json:"body,omitempty"`
	AuthType             string            `json:"authType"`
	AuthHeaderName       string            `json:"authHeaderName,omitempty"`
	Pagination           PaginationConfig  `json:"pagination"`
	RecordsPath          string            `json:"recordsPath,omitempty"`
	TimeoutSeconds       int               `json:"timeoutSeconds"`
	MaxRequestsPerSecond int32             `json:"maxRequestsPerSecond,omitempty"`
}

// HeaderConfig is a static request header. Values sourced from a Secret arrive
// as an env var name rather than a literal.
type HeaderConfig struct {
	Name       string `json:"name"`
	Value      string `json:"value,omitempty"`
	ValueEnv   string `json:"valueEnv,omitempty"`
	FromSecret bool   `json:"fromSecret,omitempty"`
}

// PaginationConfig describes how to walk a result set.
type PaginationConfig struct {
	Type           string `json:"type"`
	NextCursorPath string `json:"nextCursorPath,omitempty"`
	CursorParam    string `json:"cursorParam,omitempty"`
	PageParam      string `json:"pageParam,omitempty"`
	SizeParam      string `json:"sizeParam,omitempty"`
	PageSize       int32  `json:"pageSize,omitempty"`
	StartPage      int32  `json:"startPage,omitempty"`
	MaxPages       int32  `json:"maxPages,omitempty"`
}

// WatermarkConfig describes the incremental pull.
type WatermarkConfig struct {
	RecordField  string `json:"recordField"`
	Param        string `json:"param"`
	ToParam      string `json:"toParam,omitempty"`
	Format       string `json:"format"`
	InitialValue string `json:"initialValue,omitempty"`
}

// DestinationConfig describes the sink.
type DestinationConfig struct {
	Type     string          `json:"type"`
	BigQuery *BigQueryConfig `json:"bigQuery,omitempty"`
}

// BigQueryConfig describes the BigQuery sink.
type BigQueryConfig struct {
	ProjectID        string `json:"projectID"`
	Dataset          string `json:"dataset"`
	Table            string `json:"table"`
	Location         string `json:"location,omitempty"`
	WriteDisposition string `json:"writeDisposition"`
	CreateIfMissing  bool   `json:"createIfMissing"`
	DeadLetterTable  string `json:"deadLetterTable,omitempty"`
}

// Hash returns a stable digest of the config. The controller stamps it on the
// CronJob pod template so a spec edit rolls the template, and so an unchanged
// spec does not.
func (c *RunConfig) Hash() (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:16], nil
}

// Schema is the record shape a run observed: a field path mapped to the JSON
// type seen at that path. It is deliberately coarse. The goal is catching a
// field that changed type or vanished, not describing the data.
type Schema struct {
	Fields      map[string]string `json:"fields"`
	Fingerprint string            `json:"fingerprint"`
}

// NewSchema builds a Schema from an observed field map.
func NewSchema(fields map[string]string) Schema {
	return Schema{Fields: fields, Fingerprint: Fingerprint(fields)}
}

// Fingerprint hashes a field map into a short stable identifier. Field order
// from the upstream JSON is irrelevant, so the paths are sorted first.
func Fingerprint(fields map[string]string) string {
	if len(fields) == 0 {
		return ""
	}
	paths := make([]string, 0, len(fields))
	for p := range fields {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	var b strings.Builder
	for _, p := range paths {
		fmt.Fprintf(&b, "%s:%s\n", p, fields[p])
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])[:16]
}

// SchemaChange summarizes the difference between two record shapes.
type SchemaChange struct {
	Added   []string `json:"added,omitempty"`
	Removed []string `json:"removed,omitempty"`
	Changed []string `json:"changed,omitempty"`
}

// Empty reports whether the shapes matched.
func (c SchemaChange) Empty() bool {
	return len(c.Added) == 0 && len(c.Removed) == 0 && len(c.Changed) == 0
}

// Breaking reports whether the change is the kind that silently corrupts a
// table rather than erroring: a field that disappeared, or one whose type
// changed underneath a column that is already typed. New fields are additive
// and safe.
func (c SchemaChange) Breaking() bool {
	return len(c.Removed) > 0 || len(c.Changed) > 0
}

// String renders the change for an event message.
func (c SchemaChange) String() string {
	parts := make([]string, 0, 3)
	if len(c.Added) > 0 {
		parts = append(parts, "added "+strings.Join(c.Added, ","))
	}
	if len(c.Removed) > 0 {
		parts = append(parts, "removed "+strings.Join(c.Removed, ","))
	}
	if len(c.Changed) > 0 {
		parts = append(parts, "changed "+strings.Join(c.Changed, ","))
	}
	return strings.Join(parts, "; ")
}

// Diff compares an observed shape against the last accepted one.
func Diff(previous, observed map[string]string) SchemaChange {
	var change SchemaChange
	for path, typ := range observed {
		prev, ok := previous[path]
		switch {
		case !ok:
			change.Added = append(change.Added, path)
		case prev != typ:
			change.Changed = append(change.Changed, fmt.Sprintf("%s(%s->%s)", path, prev, typ))
		}
	}
	for path := range previous {
		if _, ok := observed[path]; !ok {
			change.Removed = append(change.Removed, path)
		}
	}
	slices.Sort(change.Added)
	slices.Sort(change.Removed)
	slices.Sort(change.Changed)
	return change
}

// RunResult is what the worker reports back. It is written as JSON to the
// pod's termination message file, so the numbers in status come from the
// process that did the work rather than from parsing logs.
type RunResult struct {
	RowsIngested      int64         `json:"rowsIngested"`
	RowsRejected      int64         `json:"rowsRejected"`
	RequestCount      int32         `json:"requestCount"`
	Watermark         string        `json:"watermark,omitempty"`
	SchemaFingerprint string        `json:"schemaFingerprint,omitempty"`
	SchemaChange      *SchemaChange `json:"schemaChange,omitempty"`
	BackfillID        string        `json:"backfillID,omitempty"`
	Error             string        `json:"error,omitempty"`
}
