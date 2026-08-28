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

package v1alpha1

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// SourceType names the kind of upstream system an IngestionSource pulls from.
// +kubebuilder:validation:Enum=HTTP
type SourceType string

const (
	// SourceTypeHTTP pulls JSON over HTTP, optionally paginated.
	SourceTypeHTTP SourceType = "HTTP"
)

// AuthType names the credential scheme used against the upstream API.
// +kubebuilder:validation:Enum=None;Bearer;Header;Basic
type AuthType string

const (
	AuthTypeNone   AuthType = "None"
	AuthTypeBearer AuthType = "Bearer"
	AuthTypeHeader AuthType = "Header"
	AuthTypeBasic  AuthType = "Basic"
)

// PaginationType names how the upstream API splits a result set across responses.
// +kubebuilder:validation:Enum=None;Cursor;PageNumber;Offset;LinkHeader
type PaginationType string

const (
	// PaginationNone reads a single response and stops.
	PaginationNone PaginationType = "None"
	// PaginationCursor reads an opaque cursor out of each response body and
	// sends it back as a query parameter on the next request.
	PaginationCursor PaginationType = "Cursor"
	// PaginationPageNumber increments a 1-based page parameter until a short or
	// empty page is returned.
	PaginationPageNumber PaginationType = "PageNumber"
	// PaginationOffset advances an offset parameter by the page size.
	PaginationOffset PaginationType = "Offset"
	// PaginationLinkHeader follows RFC 8288 rel="next" links.
	PaginationLinkHeader PaginationType = "LinkHeader"
)

// DestinationType names where extracted records are written.
// +kubebuilder:validation:Enum=BigQuery;Stdout
type DestinationType string

const (
	// DestinationBigQuery streams records into a BigQuery table.
	DestinationBigQuery DestinationType = "BigQuery"
	// DestinationStdout writes newline-delimited JSON to the worker's stdout.
	// It exists so the operator can be exercised end to end with no cloud
	// account, which is how the kind demo and the e2e suite run.
	DestinationStdout DestinationType = "Stdout"
)

// SchemaDriftPolicy decides what happens when a run observes a record shape
// that differs from the shape recorded in status.
// +kubebuilder:validation:Enum=Ignore;Warn;EvolveAdditive;Fail
type SchemaDriftPolicy string

const (
	// SchemaDriftIgnore writes records without comparing shapes.
	SchemaDriftIgnore SchemaDriftPolicy = "Ignore"
	// SchemaDriftWarn writes the records, records the new fingerprint, and
	// raises a Degraded condition plus an event.
	SchemaDriftWarn SchemaDriftPolicy = "Warn"
	// SchemaDriftEvolveAdditive accepts new fields but fails the run if a
	// previously seen field disappears or changes type, which is the shape of
	// drift that silently corrupts a table rather than erroring.
	SchemaDriftEvolveAdditive SchemaDriftPolicy = "EvolveAdditive"
	// SchemaDriftFail fails the run on any change to the record shape.
	SchemaDriftFail SchemaDriftPolicy = "Fail"
)

// WatermarkFormat describes how to compare and serialize watermark values.
// +kubebuilder:validation:Enum=RFC3339;UnixSeconds;Opaque
type WatermarkFormat string

const (
	WatermarkRFC3339     WatermarkFormat = "RFC3339"
	WatermarkUnixSeconds WatermarkFormat = "UnixSeconds"
	// WatermarkOpaque stores the value verbatim and does not order it. Useful
	// for monotonic IDs the operator has no business parsing.
	WatermarkOpaque WatermarkFormat = "Opaque"
)

// Phase is a coarse, human-facing rollup of status. Conditions remain the
// machine-readable source of truth.
// +kubebuilder:validation:Enum=Pending;Scheduled;Running;Suspended;Degraded
type Phase string

const (
	PhasePending   Phase = "Pending"
	PhaseScheduled Phase = "Scheduled"
	PhaseRunning   Phase = "Running"
	PhaseSuspended Phase = "Suspended"
	PhaseDegraded  Phase = "Degraded"
)

// Condition types set by the controller.
const (
	// ConditionReady is true when a CronJob matching the spec exists and the
	// source is not failing repeatedly.
	ConditionReady = "Ready"
	// ConditionScheduled is true when the owned CronJob is present and unsuspended.
	ConditionScheduled = "Scheduled"
	// ConditionDegraded is true when runs are failing or schema drift was seen.
	ConditionDegraded = "Degraded"
	// ConditionBackfilling is true while a one-off backfill Job is running.
	ConditionBackfilling = "Backfilling"
)

// HeaderValue is a request header whose value may come from a Secret.
type HeaderValue struct {
	// name is the HTTP header name.
	// +kubebuilder:validation:MinLength=1
	// +required
	Name string `json:"name"`

	// value is a literal header value. Do not put credentials here; use
	// valueFrom so the value stays in a Secret.
	// +optional
	Value string `json:"value,omitempty"`

	// valueFrom reads the header value from a Secret key.
	// +optional
	ValueFrom *corev1.SecretKeySelector `json:"valueFrom,omitempty"`
}

// SourceAuth describes how the worker authenticates to the upstream API. Every
// scheme reads its credential from a Secret; nothing is ever inlined in the
// IngestionSource, which is what makes the object safe to commit to git.
type SourceAuth struct {
	// type selects the credential scheme.
	// +kubebuilder:default=None
	// +optional
	Type AuthType `json:"type,omitempty"`

	// secretRef holds the credential. For Bearer and Header it is the token.
	// For Basic the referenced Secret must carry "username" and "password"
	// keys and the key field is ignored.
	// +optional
	SecretRef *corev1.SecretKeySelector `json:"secretRef,omitempty"`

	// headerName is the header the credential is sent in when type is Header.
	// +optional
	HeaderName string `json:"headerName,omitempty"`
}

// Pagination describes how to walk a multi-response result set.
type Pagination struct {
	// type selects the pagination strategy.
	// +kubebuilder:default=None
	// +optional
	Type PaginationType `json:"type,omitempty"`

	// nextCursorPath is a dotted path into the response body holding the next
	// cursor, for example "meta.next_cursor". Required when type is Cursor.
	// +optional
	NextCursorPath string `json:"nextCursorPath,omitempty"`

	// cursorParam is the query parameter the cursor is sent back in.
	// +optional
	CursorParam string `json:"cursorParam,omitempty"`

	// pageParam is the query parameter carrying the page number or offset.
	// +optional
	PageParam string `json:"pageParam,omitempty"`

	// sizeParam is the query parameter carrying the page size.
	// +optional
	SizeParam string `json:"sizeParam,omitempty"`

	// pageSize is how many records to request per page.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=100
	// +optional
	PageSize int32 `json:"pageSize,omitempty"`

	// startPage is the first page number or offset to request.
	// +kubebuilder:validation:Minimum=0
	// +optional
	StartPage int32 `json:"startPage,omitempty"`

	// maxPages caps how many responses a single run will read. It is a
	// circuit breaker against an upstream that never signals the end of the
	// result set, which is a real failure mode and not a theoretical one.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1000
	// +optional
	MaxPages int32 `json:"maxPages,omitempty"`
}

// HTTPSource describes a JSON-over-HTTP upstream.
type HTTPSource struct {
	// url is the endpoint to pull from.
	// +kubebuilder:validation:Pattern=`^https?://`
	// +required
	URL string `json:"url"`

	// method is the HTTP method used for each request.
	// +kubebuilder:validation:Enum=GET;POST
	// +kubebuilder:default=GET
	// +optional
	Method string `json:"method,omitempty"`

	// headers are sent on every request.
	// +listType=map
	// +listMapKey=name
	// +optional
	Headers []HeaderValue `json:"headers,omitempty"`

	// query holds static query parameters sent on every request.
	// +optional
	Query map[string]string `json:"query,omitempty"`

	// body is a literal request body used when method is POST.
	// +optional
	Body string `json:"body,omitempty"`

	// auth selects the credential scheme.
	// +optional
	Auth *SourceAuth `json:"auth,omitempty"`

	// pagination describes how to walk multi-response result sets.
	// +optional
	Pagination *Pagination `json:"pagination,omitempty"`

	// recordsPath is a dotted path to the array of records in each response,
	// for example "data.items". Empty means the response body is itself the
	// array.
	// +optional
	RecordsPath string `json:"recordsPath,omitempty"`

	// timeout bounds a single HTTP request.
	// +kubebuilder:default="30s"
	// +optional
	Timeout metav1.Duration `json:"timeout,omitempty"`

	// maxRequestsPerSecond throttles the worker so a nightly pull does not
	// trip an upstream rate limit. Zero means unthrottled.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxRequestsPerSecond int32 `json:"maxRequestsPerSecond,omitempty"`
}

// Source describes the upstream system.
// +kubebuilder:validation:XValidation:rule="self.type != 'HTTP' || has(self.http)",message="http must be set when type is HTTP"
type Source struct {
	// type names the kind of upstream.
	// +kubebuilder:default=HTTP
	// +optional
	Type SourceType `json:"type,omitempty"`

	// http configures an HTTP source.
	// +optional
	HTTP *HTTPSource `json:"http,omitempty"`
}

// Watermark makes runs incremental: each run asks the upstream only for
// records newer than the value the last successful run recorded.
type Watermark struct {
	// recordField is the field on each record holding the watermark value,
	// for example "updated_at".
	// +kubebuilder:validation:MinLength=1
	// +required
	RecordField string `json:"recordField"`

	// param is the query parameter used to request records at or after the
	// stored watermark.
	// +kubebuilder:validation:MinLength=1
	// +required
	Param string `json:"param"`

	// toParam is an optional query parameter carrying an upper bound. It is
	// used only by backfills, which pull a closed range rather than everything
	// since the last run.
	// +optional
	ToParam string `json:"toParam,omitempty"`

	// format describes how to parse and order watermark values.
	// +kubebuilder:default=RFC3339
	// +optional
	Format WatermarkFormat `json:"format,omitempty"`

	// initialValue is used on the first run, before any watermark is stored.
	// +optional
	InitialValue string `json:"initialValue,omitempty"`
}

// BigQueryDestination describes a BigQuery table to load into.
type BigQueryDestination struct {
	// projectID owning the dataset.
	// +kubebuilder:validation:MinLength=1
	// +required
	ProjectID string `json:"projectID"`

	// dataset to write into.
	// +kubebuilder:validation:MinLength=1
	// +required
	Dataset string `json:"dataset"`

	// table to write into.
	// +kubebuilder:validation:MinLength=1
	// +required
	Table string `json:"table"`

	// location is the BigQuery dataset location, for example "US".
	// +optional
	Location string `json:"location,omitempty"`

	// writeDisposition controls whether a run appends to or replaces the table.
	// +kubebuilder:validation:Enum=Append;Truncate
	// +kubebuilder:default=Append
	// +optional
	WriteDisposition string `json:"writeDisposition,omitempty"`

	// createIfMissing creates the table from the observed record shape when it
	// does not exist.
	// +kubebuilder:default=true
	// +optional
	CreateIfMissing *bool `json:"createIfMissing,omitempty"`

	// deadLetterTable receives records that could not be written, along with
	// the reason. Without one, rejected records are counted and dropped.
	// +optional
	DeadLetterTable string `json:"deadLetterTable,omitempty"`
}

// Destination describes where records land.
// +kubebuilder:validation:XValidation:rule="self.type != 'BigQuery' || has(self.bigQuery)",message="bigQuery must be set when type is BigQuery"
type Destination struct {
	// type names the sink.
	// +kubebuilder:default=Stdout
	// +optional
	Type DestinationType `json:"type,omitempty"`

	// bigQuery configures the BigQuery sink.
	// +optional
	BigQuery *BigQueryDestination `json:"bigQuery,omitempty"`
}

// SchemaPolicy controls how record shape changes are handled.
type SchemaPolicy struct {
	// drift selects the reaction to a change in record shape.
	// +kubebuilder:default=Warn
	// +optional
	Drift SchemaDriftPolicy `json:"drift,omitempty"`
}

// FailurePolicy controls retry and give-up behavior.
type FailurePolicy struct {
	// backoffLimit is the number of pod retries within a single run.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	// +optional
	BackoffLimit int32 `json:"backoffLimit,omitempty"`

	// suspendAfterConsecutiveFailures stops scheduling once this many runs
	// have failed in a row, so a broken source does not page someone every
	// night for a week. Zero disables the behavior.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=5
	// +optional
	SuspendAfterConsecutiveFailures int32 `json:"suspendAfterConsecutiveFailures,omitempty"`
}

// Backfill requests a one-off historical pull alongside the normal schedule.
type Backfill struct {
	// requestID identifies this backfill. Changing it starts a new one; the
	// controller records the last completed ID so an unchanged spec never
	// re-runs a backfill.
	// +kubebuilder:validation:MinLength=1
	// +required
	RequestID string `json:"requestID"`

	// from is the watermark value to start at.
	// +optional
	From string `json:"from,omitempty"`

	// to is the watermark value to stop at.
	// +optional
	To string `json:"to,omitempty"`
}

// Runner describes the pod that performs a run.
type Runner struct {
	// image is the worker image. Defaults to the operator's own image, which
	// ships the worker as a subcommand.
	// +optional
	Image string `json:"image,omitempty"`

	// imagePullPolicy for the worker container.
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// serviceAccountName the run executes as. On GKE this is the account bound
	// to a Google service account through Workload Identity.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// resources for the worker container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// activeDeadlineSeconds bounds the wall time of a single run.
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// env holds extra environment variables for the worker container.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// nodeSelector for the run pod.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// tolerations for the run pod.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// IngestionSourceSpec defines the desired state of IngestionSource.
type IngestionSourceSpec struct {
	// schedule is a cron expression in the cluster's timezone, matching
	// CronJob semantics.
	// +kubebuilder:validation:MinLength=1
	// +required
	Schedule string `json:"schedule"`

	// suspend stops new runs from being scheduled without deleting the
	// recorded watermark, so ingestion resumes where it left off.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// concurrencyPolicy controls what happens when a run is still going at the
	// next scheduled time. Forbid is the default because two concurrent runs
	// share one watermark and will duplicate rows.
	// +kubebuilder:validation:Enum=Allow;Forbid;Replace
	// +kubebuilder:default=Forbid
	// +optional
	ConcurrencyPolicy batchv1.ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// startingDeadlineSeconds is how late a missed run may still start.
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// successfulRunHistoryLimit is how many completed run Jobs to keep.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	// +optional
	SuccessfulRunHistoryLimit *int32 `json:"successfulRunHistoryLimit,omitempty"`

	// failedRunHistoryLimit is how many failed run Jobs to keep for debugging.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	// +optional
	FailedRunHistoryLimit *int32 `json:"failedRunHistoryLimit,omitempty"`

	// source describes the upstream system.
	// +required
	Source Source `json:"source"`

	// watermark makes runs incremental. Without it every run is a full pull.
	// +optional
	Watermark *Watermark `json:"watermark,omitempty"`

	// destination describes where records land.
	// +required
	Destination Destination `json:"destination"`

	// schema controls the reaction to record shape changes.
	// +optional
	Schema SchemaPolicy `json:"schema,omitzero"`

	// failurePolicy controls retry and give-up behavior.
	// +optional
	FailurePolicy FailurePolicy `json:"failurePolicy,omitzero"`

	// backfill requests a one-off historical pull.
	// +optional
	Backfill *Backfill `json:"backfill,omitempty"`

	// runner describes the pod that performs a run.
	// +optional
	Runner Runner `json:"runner,omitzero"`
}

// RunResult is the outcome of a single run.
// +kubebuilder:validation:Enum=Running;Succeeded;Failed
type RunResult string

const (
	RunRunning   RunResult = "Running"
	RunSucceeded RunResult = "Succeeded"
	RunFailed    RunResult = "Failed"
)

// RunStatus records what one run did. The controller reads these numbers out
// of the run pod's termination message, which the worker writes as JSON, so
// the counts come from the process that did the work rather than from a log
// scrape.
type RunStatus struct {
	// jobName is the Job that performed the run.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// startTime is when the run began.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// completionTime is when the run finished.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// result is the outcome of the run.
	// +optional
	Result RunResult `json:"result,omitempty"`

	// rowsIngested is how many records the run wrote.
	// +optional
	RowsIngested int64 `json:"rowsIngested,omitempty"`

	// rowsRejected is how many records the run could not write.
	// +optional
	RowsRejected int64 `json:"rowsRejected,omitempty"`

	// requestCount is how many upstream requests the run made.
	// +optional
	RequestCount int32 `json:"requestCount,omitempty"`

	// watermark is the highest watermark value the run observed.
	// +optional
	Watermark string `json:"watermark,omitempty"`

	// schemaFingerprint is a stable hash of the record shape the run saw.
	// +optional
	SchemaFingerprint string `json:"schemaFingerprint,omitempty"`

	// backfillID is set when the run was a backfill rather than a scheduled pull.
	// +optional
	BackfillID string `json:"backfillID,omitempty"`

	// message explains a failure.
	// +optional
	Message string `json:"message,omitempty"`
}

// IngestionSourceStatus defines the observed state of IngestionSource.
type IngestionSourceStatus struct {
	// observedGeneration is the spec generation the controller last acted on.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the IngestionSource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// phase is a coarse rollup for humans reading kubectl output.
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// cronJobName is the owned CronJob driving the schedule.
	// +optional
	CronJobName string `json:"cronJobName,omitempty"`

	// lastScheduleTime is when a run was last started by the schedule.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// lastRun is the most recent run, successful or not.
	// +optional
	LastRun *RunStatus `json:"lastRun,omitempty"`

	// lastSuccessfulRun is the most recent run that completed.
	// +optional
	LastSuccessfulRun *RunStatus `json:"lastSuccessfulRun,omitempty"`

	// consecutiveFailures counts runs that have failed in a row.
	// +optional
	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`

	// watermark is the value the next run will resume from.
	// +optional
	Watermark string `json:"watermark,omitempty"`

	// totalRowsIngested is the cumulative count across all runs.
	// +optional
	TotalRowsIngested int64 `json:"totalRowsIngested,omitempty"`

	// schemaFingerprint is the record shape accepted by the last run.
	// +optional
	SchemaFingerprint string `json:"schemaFingerprint,omitempty"`

	// lastCompletedBackfillID is the backfill request the controller has
	// already satisfied.
	// +optional
	LastCompletedBackfillID string `json:"lastCompletedBackfillID,omitempty"`

	// lastTriggerID is the value of the trigger annotation the controller has
	// already started a run for, so the same trigger never runs twice.
	// +optional
	LastTriggerID string `json:"lastTriggerID,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=isrc;isrcs,categories=sluice
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Rows",type=integer,JSONPath=`.status.totalRowsIngested`
// +kubebuilder:printcolumn:name="Last Run",type=string,JSONPath=`.status.lastRun.result`
// +kubebuilder:printcolumn:name="Watermark",type=string,JSONPath=`.status.watermark`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// IngestionSource declares a vendor API to pull on a schedule and a table to
// land the records in. The controller owns a CronJob for the schedule, a Job
// for any requested backfill, and the watermark that makes runs incremental.
type IngestionSource struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of IngestionSource
	// +required
	Spec IngestionSourceSpec `json:"spec"`

	// status defines the observed state of IngestionSource
	// +optional
	Status IngestionSourceStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// IngestionSourceList contains a list of IngestionSource
type IngestionSourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []IngestionSource `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &IngestionSource{}, &IngestionSourceList{})
		return nil
	})
}
