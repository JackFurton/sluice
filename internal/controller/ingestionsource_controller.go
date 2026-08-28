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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ingestv1alpha1 "github.com/JackFurton/sluice/api/v1alpha1"
	"github.com/JackFurton/sluice/internal/runspec"
)

// DefaultWorkerImage is used when neither the spec nor the manager flag names
// an image. It is overwritten at deploy time by the kustomize overlay.
const DefaultWorkerImage = "ghcr.io/jackfurton/sluice:latest"

// IngestionSourceReconciler drives an IngestionSource toward its declared
// state: a CronJob on the schedule, a ConfigMap holding the rendered run
// config, a ConfigMap holding the accepted record shape, and a Job for any
// requested backfill.
//
// The controller owns every write to status. The worker only reads, which is
// what keeps two concurrent runs from racing over the watermark.
type IngestionSourceReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// WorkerImage is the image used when an IngestionSource does not name one.
	WorkerImage string
}

// +kubebuilder:rbac:groups=ingest.sluice.dev,resources=ingestionsources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ingest.sluice.dev,resources=ingestionsources/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ingest.sluice.dev,resources=ingestionsources/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=cronjobs;jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile brings the owned resources in line with the spec and folds the
// results of finished runs into status.
func (r *IngestionSourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var src ingestv1alpha1.IngestionSource
	if err := r.Get(ctx, req.NamespacedName, &src); err != nil {
		// Owned objects carry a controller reference, so deletion is garbage
		// collection's job. There is nothing outside the cluster to clean up,
		// which is why this controller has no finalizer: adding one would only
		// create a way for deletes to hang.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status := src.Status.DeepCopy()
	status.ObservedGeneration = src.Generation

	config, err := buildRunConfig(&src)
	if err != nil {
		// An invalid spec cannot be fixed by retrying. Report it and wait for
		// the next spec change rather than hot-looping.
		r.event(&src, corev1.EventTypeWarning, "InvalidSpec", err.Error())
		setCondition(status, ingestv1alpha1.ConditionReady, metav1.ConditionFalse, "InvalidSpec", err.Error())
		status.Phase = ingestv1alpha1.PhaseDegraded
		return ctrl.Result{}, r.writeStatus(ctx, &src, status)
	}

	configHash, err := config.Hash()
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("hash run config: %w", err)
	}

	if err := r.ensureSchemaConfigMap(ctx, &src); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureRunConfigMap(ctx, &src, config); err != nil {
		return ctrl.Result{}, err
	}

	// Runs are folded in before scheduling decisions, so a failure streak that
	// crossed the threshold suspends the CronJob in the same pass.
	if err := r.collectRuns(ctx, &src, status); err != nil {
		return ctrl.Result{}, err
	}

	suspend := ptr.Deref(src.Spec.Suspend, false)
	autoSuspended := false
	if limit := src.Spec.FailurePolicy.SuspendAfterConsecutiveFailures; limit > 0 && status.ConsecutiveFailures >= limit {
		suspend, autoSuspended = true, true
	}

	cronJob, err := r.ensureCronJob(ctx, &src, configHash, suspend)
	if err != nil {
		return ctrl.Result{}, err
	}
	status.CronJobName = cronJob.Name
	status.LastScheduleTime = cronJob.Status.LastScheduleTime

	backfilling, err := r.ensureBackfill(ctx, &src, status, configHash)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.ensureTrigger(ctx, &src, status, configHash); err != nil {
		return ctrl.Result{}, err
	}

	r.setConditions(&src, status, suspend, autoSuspended, backfilling)

	if err := r.writeStatus(ctx, &src, status); err != nil {
		return ctrl.Result{}, err
	}
	log.V(1).Info("reconciled", "phase", status.Phase, "rows", status.TotalRowsIngested)
	return ctrl.Result{}, nil
}

// ensureRunConfigMap writes the rendered RunConfig the run pods mount.
func (r *IngestionSourceReconciler) ensureRunConfigMap(ctx context.Context, src *ingestv1alpha1.IngestionSource, config runspec.RunConfig) error {
	encoded, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("encode run config: %w", err)
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      runConfigMapName(src),
		Namespace: src.Namespace,
	}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = ownedLabels(src, "")
		cm.Data = map[string]string{runspec.ConfigKey: string(encoded)}
		return controllerutil.SetControllerReference(src, cm, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("reconcile run config ConfigMap: %w", err)
	}
	return nil
}

// ensureSchemaConfigMap creates the ConfigMap the worker records the accepted
// record shape in. The controller creates it and never writes its data, so the
// worker's role can be scoped to get and update on this one name.
func (r *IngestionSourceReconciler) ensureSchemaConfigMap(ctx context.Context, src *ingestv1alpha1.IngestionSource) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      schemaConfigMapName(src),
		Namespace: src.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = ownedLabels(src, "")
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		if _, ok := cm.Data[runspec.SchemaKey]; !ok {
			cm.Data[runspec.SchemaKey] = "{}"
		}
		return controllerutil.SetControllerReference(src, cm, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("reconcile schema ConfigMap: %w", err)
	}
	return nil
}

// ensureCronJob reconciles the schedule.
func (r *IngestionSourceReconciler) ensureCronJob(ctx context.Context, src *ingestv1alpha1.IngestionSource, configHash string, suspend bool) (*batchv1.CronJob, error) {
	desired := buildCronJob(src, r.imageFor(src), configHash, suspend)

	existing := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{
		Name:      desired.Name,
		Namespace: desired.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, existing, func() error {
		existing.Labels = desired.Labels
		// Only the fields this controller owns are overwritten. Anything a
		// cluster admin or a mutating webhook added to the pod template stays,
		// which matters on clusters that inject sidecars.
		existing.Spec.Schedule = desired.Spec.Schedule
		existing.Spec.Suspend = desired.Spec.Suspend
		existing.Spec.ConcurrencyPolicy = desired.Spec.ConcurrencyPolicy
		existing.Spec.StartingDeadlineSeconds = desired.Spec.StartingDeadlineSeconds
		existing.Spec.SuccessfulJobsHistoryLimit = desired.Spec.SuccessfulJobsHistoryLimit
		existing.Spec.FailedJobsHistoryLimit = desired.Spec.FailedJobsHistoryLimit
		existing.Spec.JobTemplate = desired.Spec.JobTemplate
		return controllerutil.SetControllerReference(src, existing, r.Scheme)
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile CronJob: %w", err)
	}
	return existing, nil
}

// ensureBackfill creates the Job for a requested historical pull and reports
// whether one is still running. A backfill runs once per request ID; the
// completed ID is recorded in status so an unchanged spec never repeats it.
func (r *IngestionSourceReconciler) ensureBackfill(ctx context.Context, src *ingestv1alpha1.IngestionSource, status *ingestv1alpha1.IngestionSourceStatus, configHash string) (bool, error) {
	backfill := src.Spec.Backfill
	if backfill == nil || backfill.RequestID == status.LastCompletedBackfillID {
		return false, nil
	}

	name := backfillJobName(src, backfill.RequestID)
	var job batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: src.Namespace}, &job)
	switch {
	case apierrors.IsNotFound(err):
		desired := buildBackfillJob(src, r.imageFor(src), configHash)
		if err := controllerutil.SetControllerReference(src, desired, r.Scheme); err != nil {
			return false, fmt.Errorf("set owner on backfill Job: %w", err)
		}
		if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
			return false, fmt.Errorf("create backfill Job: %w", err)
		}
		r.event(src, corev1.EventTypeNormal, "BackfillStarted",
			fmt.Sprintf("started backfill %q over [%s, %s]", backfill.RequestID, backfill.From, backfill.To))
		return true, nil
	case err != nil:
		return false, fmt.Errorf("get backfill Job: %w", err)
	}
	return !jobFinished(&job), nil
}

// ensureTrigger starts one immediate run when the trigger annotation carries a
// value the controller has not acted on yet.
//
// This exists because the obvious alternative, creating a Job from the CronJob
// with kubectl, produces a Job this controller does not own. It runs, and its
// rows never reach status.
func (r *IngestionSourceReconciler) ensureTrigger(ctx context.Context, src *ingestv1alpha1.IngestionSource, status *ingestv1alpha1.IngestionSourceStatus, configHash string) error {
	trigger := src.Annotations[AnnotationTrigger]
	if trigger == "" || trigger == status.LastTriggerID {
		return nil
	}

	desired := buildTriggerJob(src, r.imageFor(src), configHash, trigger)
	if err := controllerutil.SetControllerReference(src, desired, r.Scheme); err != nil {
		return fmt.Errorf("set owner on triggered Job: %w", err)
	}
	if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create triggered Job: %w", err)
	}

	// Recording the trigger before the run finishes is deliberate: the point
	// is one run per trigger value, not one successful run per trigger value.
	status.LastTriggerID = trigger
	r.event(src, corev1.EventTypeNormal, "RunTriggered", fmt.Sprintf("started run %s", desired.Name))
	return nil
}

// collectRuns folds every finished run that has not been counted yet into
// status, oldest first.
func (r *IngestionSourceReconciler) collectRuns(ctx context.Context, src *ingestv1alpha1.IngestionSource, status *ingestv1alpha1.IngestionSourceStatus) error {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs,
		client.InNamespace(src.Namespace),
		client.MatchingLabels{labelSource: src.Name},
	); err != nil {
		return fmt.Errorf("list run Jobs: %w", err)
	}

	finished := make([]*batchv1.Job, 0, len(jobs.Items))
	var running *batchv1.Job
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if !ownedBySource(job, src) {
			continue
		}
		if jobFinished(job) {
			if job.Annotations[annotationAccounted] != "true" {
				finished = append(finished, job)
			}
			continue
		}
		if running == nil || startedAfter(job, running) {
			running = job
		}
	}

	slices.SortFunc(finished, func(a, b *batchv1.Job) int {
		return runTime(a).Compare(runTime(b).Time)
	})

	for _, job := range finished {
		run, err := r.runStatus(ctx, job)
		if err != nil {
			return err
		}
		r.applyRun(src, status, run)
		if err := r.markAccounted(ctx, job); err != nil {
			return err
		}
	}

	if running != nil {
		status.LastRun = &ingestv1alpha1.RunStatus{
			JobName:    running.Name,
			StartTime:  running.Status.StartTime,
			Result:     ingestv1alpha1.RunRunning,
			BackfillID: running.Annotations[annotationBackfillID],
		}
	}
	return nil
}

// applyRun folds one finished run into status.
func (r *IngestionSourceReconciler) applyRun(src *ingestv1alpha1.IngestionSource, status *ingestv1alpha1.IngestionSourceStatus, run *ingestv1alpha1.RunStatus) {
	status.LastRun = run
	status.TotalRowsIngested += run.RowsIngested

	if run.Result != ingestv1alpha1.RunSucceeded {
		status.ConsecutiveFailures++
		r.event(src, corev1.EventTypeWarning, "RunFailed",
			fmt.Sprintf("run %s failed: %s", run.JobName, run.Message))
		return
	}

	status.ConsecutiveFailures = 0
	status.LastSuccessfulRun = run
	if run.SchemaFingerprint != "" {
		if status.SchemaFingerprint != "" && status.SchemaFingerprint != run.SchemaFingerprint {
			r.event(src, corev1.EventTypeWarning, "SchemaDrift",
				fmt.Sprintf("record shape changed in run %s: %s", run.JobName, run.Message))
		}
		status.SchemaFingerprint = run.SchemaFingerprint
	}
	if run.BackfillID != "" {
		// A backfill pulls a closed historical range, so letting it move the
		// watermark would skip everything between the range and now.
		status.LastCompletedBackfillID = run.BackfillID
		r.event(src, corev1.EventTypeNormal, "BackfillCompleted",
			fmt.Sprintf("backfill %q ingested %d rows", run.BackfillID, run.RowsIngested))
		return
	}
	if run.Watermark != "" {
		status.Watermark = run.Watermark
	}
}

// runStatus reads what a finished Job actually did out of its pod's
// termination message, falling back to the Job's own conditions when the
// worker died before it could report.
func (r *IngestionSourceReconciler) runStatus(ctx context.Context, job *batchv1.Job) (*ingestv1alpha1.RunStatus, error) {
	run := &ingestv1alpha1.RunStatus{
		JobName:    job.Name,
		StartTime:  job.Status.StartTime,
		BackfillID: job.Annotations[annotationBackfillID],
		Result:     ingestv1alpha1.RunFailed,
	}
	if job.Status.CompletionTime != nil {
		run.CompletionTime = job.Status.CompletionTime
	}
	if jobSucceeded(job) {
		run.Result = ingestv1alpha1.RunSucceeded
	} else {
		run.Message = jobFailureMessage(job)
	}

	result, ok, err := r.readRunResult(ctx, job)
	if err != nil {
		return nil, err
	}
	if !ok {
		return run, nil
	}

	run.RowsIngested = result.RowsIngested
	run.RowsRejected = result.RowsRejected
	run.RequestCount = result.RequestCount
	run.Watermark = result.Watermark
	run.SchemaFingerprint = result.SchemaFingerprint
	if result.BackfillID != "" {
		run.BackfillID = result.BackfillID
	}
	switch {
	case result.Error != "":
		run.Message = result.Error
	case result.SchemaChange != nil && !result.SchemaChange.Empty():
		run.Message = "record shape changed: " + result.SchemaChange.String()
	}
	return run, nil
}

// readRunResult finds the run pod and parses the JSON the worker wrote to its
// termination message.
func (r *IngestionSourceReconciler) readRunResult(ctx context.Context, job *batchv1.Job) (runspec.RunResult, bool, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{labelJobName: job.Name},
	); err != nil {
		return runspec.RunResult{}, false, fmt.Errorf("list run pods: %w", err)
	}

	var latest *corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if latest == nil || podStartedAfter(pod, latest) {
			latest = pod
		}
	}
	if latest == nil {
		// The pod was garbage collected before the controller looked. The
		// Job's own conditions still say whether the run passed.
		return runspec.RunResult{}, false, nil
	}

	for _, cs := range latest.Status.ContainerStatuses {
		if cs.Name != containerNameWorker || cs.State.Terminated == nil {
			continue
		}
		message := cs.State.Terminated.Message
		if message == "" {
			return runspec.RunResult{}, false, nil
		}
		var result runspec.RunResult
		if err := json.Unmarshal([]byte(message), &result); err != nil {
			// With FallbackToLogsOnError the message may be raw log output
			// rather than a result document. That is still diagnostic, so it
			// is not treated as an error here.
			return runspec.RunResult{}, false, nil
		}
		return result, true, nil
	}
	return runspec.RunResult{}, false, nil
}

// markAccounted records that a Job's counts are already in status totals.
func (r *IngestionSourceReconciler) markAccounted(ctx context.Context, job *batchv1.Job) error {
	patch := client.MergeFrom(job.DeepCopy())
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[annotationAccounted] = "true"
	if err := r.Patch(ctx, job, patch); err != nil {
		return fmt.Errorf("mark Job %s accounted: %w", job.Name, err)
	}
	return nil
}

func (r *IngestionSourceReconciler) setConditions(src *ingestv1alpha1.IngestionSource, status *ingestv1alpha1.IngestionSourceStatus, suspend, autoSuspended, backfilling bool) {
	switch {
	case autoSuspended:
		message := fmt.Sprintf("scheduling stopped after %d consecutive failures", status.ConsecutiveFailures)
		setCondition(status, ingestv1alpha1.ConditionScheduled, metav1.ConditionFalse, "AutoSuspended", message)
		setCondition(status, ingestv1alpha1.ConditionDegraded, metav1.ConditionTrue, "ConsecutiveFailures", message)
		setCondition(status, ingestv1alpha1.ConditionReady, metav1.ConditionFalse, "AutoSuspended", message)
		status.Phase = ingestv1alpha1.PhaseDegraded
		r.event(src, corev1.EventTypeWarning, "AutoSuspended", message)
	case suspend:
		setCondition(status, ingestv1alpha1.ConditionScheduled, metav1.ConditionFalse, "Suspended", "spec.suspend is true")
		setCondition(status, ingestv1alpha1.ConditionDegraded, metav1.ConditionFalse, "Suspended", "not scheduling runs")
		setCondition(status, ingestv1alpha1.ConditionReady, metav1.ConditionTrue, "Suspended", "suspended as requested")
		status.Phase = ingestv1alpha1.PhaseSuspended
	default:
		setCondition(status, ingestv1alpha1.ConditionScheduled, metav1.ConditionTrue, "CronJobReady",
			fmt.Sprintf("scheduled as %q", src.Spec.Schedule))
		degraded := status.ConsecutiveFailures > 0
		if degraded {
			setCondition(status, ingestv1alpha1.ConditionDegraded, metav1.ConditionTrue, "RunFailed",
				fmt.Sprintf("%d consecutive failed runs", status.ConsecutiveFailures))
			setCondition(status, ingestv1alpha1.ConditionReady, metav1.ConditionFalse, "RunFailed", "the last run failed")
			status.Phase = ingestv1alpha1.PhaseDegraded
		} else {
			setCondition(status, ingestv1alpha1.ConditionDegraded, metav1.ConditionFalse, "NoFailures", "runs are succeeding")
			setCondition(status, ingestv1alpha1.ConditionReady, metav1.ConditionTrue, "Scheduled", "ready to ingest")
			status.Phase = ingestv1alpha1.PhaseScheduled
			if status.LastRun != nil && status.LastRun.Result == ingestv1alpha1.RunRunning {
				status.Phase = ingestv1alpha1.PhaseRunning
			}
		}
	}

	if backfilling {
		setCondition(status, ingestv1alpha1.ConditionBackfilling, metav1.ConditionTrue, "BackfillRunning",
			fmt.Sprintf("backfill %q in progress", src.Spec.Backfill.RequestID))
	} else {
		setCondition(status, ingestv1alpha1.ConditionBackfilling, metav1.ConditionFalse, "NoBackfill", "no backfill in progress")
	}
}

func (r *IngestionSourceReconciler) writeStatus(ctx context.Context, src *ingestv1alpha1.IngestionSource, status *ingestv1alpha1.IngestionSourceStatus) error {
	if equalStatus(&src.Status, status) {
		return nil
	}
	patch := client.MergeFrom(src.DeepCopy())
	src.Status = *status
	if err := r.Status().Patch(ctx, src, patch); err != nil {
		return fmt.Errorf("patch status: %w", err)
	}
	return nil
}

func (r *IngestionSourceReconciler) imageFor(src *ingestv1alpha1.IngestionSource) string {
	if src.Spec.Runner.Image != "" {
		return src.Spec.Runner.Image
	}
	if r.WorkerImage != "" {
		return r.WorkerImage
	}
	return DefaultWorkerImage
}

// eventAction is the "action" field the events API requires. Everything this
// controller reports happened while reconciling one IngestionSource.
const eventAction = "Reconcile"

func (r *IngestionSourceReconciler) event(src *ingestv1alpha1.IngestionSource, eventType, reason, message string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(src, nil, eventType, reason, eventAction, "%s", message)
}

// SetupWithManager wires the controller to its own resource and to the objects
// it owns, so a finished run triggers a reconcile instead of a poll.
func (r *IngestionSourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("ingestionsource-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&ingestv1alpha1.IngestionSource{}).
		Owns(&batchv1.CronJob{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.ConfigMap{}).
		Named("ingestionsource").
		Complete(r)
}

func setCondition(status *ingestv1alpha1.IngestionSourceStatus, conditionType string, value metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             value,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: status.ObservedGeneration,
	})
}

// ownedBySource reports whether a Job belongs to this IngestionSource.
//
// Runs arrive by two paths with two different owners. Backfills and triggered
// runs are created here and point at the IngestionSource. Scheduled runs are
// created by the CronJob controller and point at the CronJob, which is itself
// owned by the IngestionSource. Checking only the direct owner silently drops
// every scheduled run, which is to say all of them.
func ownedBySource(job *batchv1.Job, src *ingestv1alpha1.IngestionSource) bool {
	if metav1.IsControlledBy(job, src) {
		return true
	}
	owner := metav1.GetControllerOf(job)
	return owner != nil &&
		owner.Kind == "CronJob" &&
		owner.Name == cronJobName(src) &&
		job.Labels[labelSource] == src.Name
}

func jobFinished(job *batchv1.Job) bool {
	return jobSucceeded(job) || jobHasCondition(job, batchv1.JobFailed)
}

func jobSucceeded(job *batchv1.Job) bool {
	return jobHasCondition(job, batchv1.JobComplete)
}

func jobHasCondition(job *batchv1.Job, conditionType batchv1.JobConditionType) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == conditionType && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailureMessage(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			if c.Message != "" {
				return c.Message
			}
			return c.Reason
		}
	}
	return "run did not complete"
}

func runTime(job *batchv1.Job) metav1.Time {
	if job.Status.CompletionTime != nil {
		return *job.Status.CompletionTime
	}
	if job.Status.StartTime != nil {
		return *job.Status.StartTime
	}
	return job.CreationTimestamp
}

func startedAfter(a, b *batchv1.Job) bool {
	return runTime(a).After(runTime(b).Time)
}

func podStartedAfter(a, b *corev1.Pod) bool {
	at, bt := a.CreationTimestamp, b.CreationTimestamp
	if a.Status.StartTime != nil {
		at = *a.Status.StartTime
	}
	if b.Status.StartTime != nil {
		bt = *b.Status.StartTime
	}
	return at.After(bt.Time)
}

// equalStatus compares status ignoring condition timestamps, so an unchanged
// reconcile does not generate a write and wake every watcher in the cluster.
func equalStatus(a, b *ingestv1alpha1.IngestionSourceStatus) bool {
	left, err := json.Marshal(normalizeConditions(a))
	if err != nil {
		return false
	}
	right, err := json.Marshal(normalizeConditions(b))
	if err != nil {
		return false
	}
	return string(left) == string(right)
}

func normalizeConditions(status *ingestv1alpha1.IngestionSourceStatus) ingestv1alpha1.IngestionSourceStatus {
	out := *status.DeepCopy()
	for i := range out.Conditions {
		out.Conditions[i].LastTransitionTime = metav1.Time{}
	}
	return out
}
