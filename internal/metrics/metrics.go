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

// Package metrics holds the collectors this operator exports alongside the
// controller-runtime defaults.
//
// Everything here is labelled by namespace and IngestionSource name, which are
// bounded by what an operator created. Nothing is labelled by anything a
// vendor API returns.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// The label names every collector here shares. An IngestionSource is
// identified by exactly these two things everywhere else, so they are defined
// once rather than repeated per collector.
const (
	labelNamespace = "namespace"
	labelSource    = "source"
)

var sourceLabels = []string{labelNamespace, labelSource}

var (
	// RowsIngested counts records written, which is the number an operator
	// actually watches. It only moves when a run finishes, because that is
	// when the count becomes trustworthy.
	RowsIngested = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_rows_ingested_total",
		Help: "Records written to the destination, by source.",
	}, sourceLabels)

	// RowsRejected counts records the destination refused. A nonzero value
	// here with a healthy run count is the shape of a schema problem that has
	// not yet become an outage.
	RowsRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_rows_rejected_total",
		Help: "Records the destination refused, by source.",
	}, sourceLabels)

	// Runs counts finished runs by result and kind, so a backfill failing does
	// not look like the schedule failing.
	Runs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_runs_total",
		Help: "Finished runs by result and kind.",
	}, []string{labelNamespace, labelSource, "result", "kind"})

	// RunDuration measures wall time per run. The buckets run long because
	// these are nightly pulls, not request handlers.
	RunDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sluice_run_duration_seconds",
		Help:    "Wall time of a finished run.",
		Buckets: []float64{1, 5, 15, 30, 60, 300, 900, 1800, 3600, 7200},
	}, sourceLabels)

	// UpstreamRequests counts calls made to vendor APIs, which is the number
	// that matters when a vendor asks why their rate limit is being hit.
	UpstreamRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_upstream_requests_total",
		Help: "Requests made to the upstream API, by source.",
	}, sourceLabels)

	// SchemaChanges counts observed record shape changes by whether they were
	// additive. A breaking change is the one worth alerting on.
	SchemaChanges = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_schema_changes_total",
		Help: "Record shape changes observed, by kind.",
	}, []string{labelNamespace, labelSource, "kind"})

	// WatermarkLagSeconds is how far behind the present the resume point is.
	//
	// This is the alert that catches the failure nothing else does: runs that
	// succeed, report rows, and are quietly reading a window that stopped
	// advancing. Only sources with an RFC3339 watermark can report it.
	WatermarkLagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sluice_watermark_lag_seconds",
		Help: "Age of the watermark the next run will resume from.",
	}, sourceLabels)

	// ConsecutiveFailures mirrors status, so an alert can fire before the
	// source suspends itself rather than after.
	ConsecutiveFailures = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sluice_consecutive_failures",
		Help: "Runs that have failed in a row, by source.",
	}, sourceLabels)

	// Suspended reports whether the schedule is stopped, and distinguishes an
	// operator pausing a source from the controller giving up on one.
	Suspended = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sluice_suspended",
		Help: "1 when the schedule is suspended, by reason.",
	}, []string{labelNamespace, labelSource, "reason"})
)

func init() {
	metrics.Registry.MustRegister(
		RowsIngested,
		RowsRejected,
		Runs,
		RunDuration,
		UpstreamRequests,
		SchemaChanges,
		WatermarkLagSeconds,
		ConsecutiveFailures,
		Suspended,
	)
}

// Forget drops every series for a source. Without this, a deleted
// IngestionSource keeps reporting the numbers it had when it was removed, and
// its watermark lag climbs forever.
func Forget(namespace, name string) {
	labels := prometheus.Labels{labelNamespace: namespace, labelSource: name}
	RowsIngested.Delete(labels)
	RowsRejected.Delete(labels)
	RunDuration.Delete(labels)
	UpstreamRequests.Delete(labels)
	WatermarkLagSeconds.Delete(labels)
	ConsecutiveFailures.Delete(labels)
	Runs.DeletePartialMatch(labels)
	SchemaChanges.DeletePartialMatch(labels)
	Suspended.DeletePartialMatch(labels)
}
