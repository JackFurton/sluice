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
	"encoding/json"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ingestv1alpha1 "github.com/JackFurton/sluice/api/v1alpha1"
	"github.com/JackFurton/sluice/internal/runspec"
)

var _ = Describe("IngestionSource Controller", func() {
	const namespace = "default"

	var (
		name       string
		key        types.NamespacedName
		reconciler *IngestionSourceReconciler
	)

	// newSource returns a minimal valid spec: an HTTP source, an incremental
	// watermark, and the stdout sink so no cloud account is involved.
	newSource := func() *ingestv1alpha1.IngestionSource {
		return &ingestv1alpha1.IngestionSource{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: ingestv1alpha1.IngestionSourceSpec{
				Schedule: "*/5 * * * *",
				Source: ingestv1alpha1.Source{
					Type: ingestv1alpha1.SourceTypeHTTP,
					HTTP: &ingestv1alpha1.HTTPSource{
						URL:         "http://vendor.invalid/events",
						RecordsPath: "data.items",
					},
				},
				Watermark: &ingestv1alpha1.Watermark{
					RecordField: "updated_at",
					Param:       "since",
					Format:      ingestv1alpha1.WatermarkRFC3339,
				},
				Destination: ingestv1alpha1.Destination{Type: ingestv1alpha1.DestinationStdout},
				FailurePolicy: ingestv1alpha1.FailurePolicy{
					SuspendAfterConsecutiveFailures: 2,
				},
			},
		}
	}

	reconcile := func() {
		GinkgoHelper()
		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	}

	get := func() *ingestv1alpha1.IngestionSource {
		GinkgoHelper()
		var src ingestv1alpha1.IngestionSource
		Expect(k8sClient.Get(ctx, key, &src)).To(Succeed())
		return &src
	}

	// completeRun fabricates a finished run: a Job owned by the source with a
	// terminal condition, and its pod carrying the JSON result the worker
	// would have written to its termination message. envtest runs no kubelet,
	// so the statuses a real cluster would set are set here.
	completeRun := func(suffix string, succeeded bool, result runspec.RunResult, backfillID string) {
		GinkgoHelper()
		src := get()
		jobName := fmt.Sprintf("%s-run-%s", name, suffix)
		labels := ownedLabels(src, runTypeScheduled)

		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: namespace, Labels: labels},
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: "worker", Image: "busybox"}},
					},
				},
			},
		}
		if backfillID != "" {
			job.Annotations = map[string]string{annotationBackfillID: backfillID}
		}
		Expect(controllerutil.SetControllerReference(src, job, scheme.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, job)).To(Succeed())

		// The API server requires the interim condition before the terminal
		// one, so both are set the way the Job controller would set them.
		interim, terminal := batchv1.JobSuccessCriteriaMet, batchv1.JobComplete
		if !succeeded {
			interim, terminal = batchv1.JobFailureTarget, batchv1.JobFailed
		}
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{{
			Type:    interim,
			Status:  corev1.ConditionTrue,
			Reason:  "Test",
			Message: "fabricated by the test",
		}}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

		if succeeded {
			// A failed Job never carries a completion time; the API server
			// rejects one without a Complete=True condition.
			job.Status.CompletionTime = &now
		}
		job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
			Type:    terminal,
			Status:  corev1.ConditionTrue,
			Reason:  "Test",
			Message: "fabricated by the test",
		})
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

		encoded, err := json.Marshal(result)
		Expect(err).NotTo(HaveOccurred())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName + "-pod",
				Namespace: namespace,
				Labels:    map[string]string{labelJobName: jobName},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: "worker", Image: "busybox"}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "worker",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 0,
				Message:  string(encoded),
			}},
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	BeforeEach(func() {
		name = fmt.Sprintf("vendor-%d", GinkgoRandomSeed()+int64(GinkgoParallelProcess())+int64(CurrentSpecReport().LineNumber()))
		key = types.NamespacedName{Name: name, Namespace: namespace}
		reconciler = &IngestionSourceReconciler{
			Client:      k8sClient,
			Scheme:      k8sClient.Scheme(),
			Recorder:    events.NewFakeRecorder(100),
			WorkerImage: "ghcr.io/jackfurton/sluice:test",
		}
		Expect(k8sClient.Create(ctx, newSource())).To(Succeed())
	})

	AfterEach(func() {
		var src ingestv1alpha1.IngestionSource
		if err := k8sClient.Get(ctx, key, &src); err == nil {
			Expect(k8sClient.Delete(ctx, &src)).To(Succeed())
		} else {
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}
	})

	It("creates the CronJob and both ConfigMaps, all owned by the source", func() {
		reconcile()

		var cronJob batchv1.CronJob
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cronJob)).To(Succeed())
		Expect(cronJob.Spec.Schedule).To(Equal("*/5 * * * *"))
		// Two concurrent runs share one watermark and would duplicate rows.
		Expect(cronJob.Spec.ConcurrencyPolicy).To(Equal(batchv1.ForbidConcurrent))
		Expect(cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image).
			To(Equal("ghcr.io/jackfurton/sluice:test"))
		Expect(cronJob.OwnerReferences).To(HaveLen(1))
		Expect(cronJob.OwnerReferences[0].Controller).To(Equal(ptr.To(true)))

		var runConfig corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-runconfig", Namespace: namespace}, &runConfig)).To(Succeed())

		var config runspec.RunConfig
		Expect(json.Unmarshal([]byte(runConfig.Data[runspec.ConfigKey]), &config)).To(Succeed())
		Expect(config.Source.URL).To(Equal("http://vendor.invalid/events"))
		Expect(config.Watermark.Param).To(Equal("since"))

		var schema corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-schema", Namespace: namespace}, &schema)).To(Succeed())

		Expect(get().Status.Phase).To(Equal(ingestv1alpha1.PhaseScheduled))
	})

	It("rolls the pod template only when the rendered config changes", func() {
		reconcile()

		var cronJob batchv1.CronJob
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cronJob)).To(Succeed())
		first := cronJob.Spec.JobTemplate.Spec.Template.Annotations[annotationConfigHash]
		Expect(first).NotTo(BeEmpty())

		reconcile()
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cronJob)).To(Succeed())
		Expect(cronJob.Spec.JobTemplate.Spec.Template.Annotations[annotationConfigHash]).
			To(Equal(first), "an unchanged spec must not roll the template")

		src := get()
		src.Spec.Source.HTTP.URL = "http://vendor.invalid/other"
		Expect(k8sClient.Update(ctx, src)).To(Succeed())
		reconcile()

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cronJob)).To(Succeed())
		Expect(cronJob.Spec.JobTemplate.Spec.Template.Annotations[annotationConfigHash]).NotTo(Equal(first))
	})

	It("folds a finished run into status and advances the watermark", func() {
		reconcile()
		completeRun("a", true, runspec.RunResult{
			RowsIngested:      42,
			RequestCount:      3,
			Watermark:         "2026-01-02T00:00:00Z",
			SchemaFingerprint: "abc123",
		}, "")
		reconcile()

		status := get().Status
		Expect(status.TotalRowsIngested).To(Equal(int64(42)))
		Expect(status.Watermark).To(Equal("2026-01-02T00:00:00Z"))
		Expect(status.SchemaFingerprint).To(Equal("abc123"))
		Expect(status.LastSuccessfulRun).NotTo(BeNil())
		Expect(status.LastSuccessfulRun.RequestCount).To(Equal(int32(3)))
		Expect(status.ConsecutiveFailures).To(BeZero())
	})

	// Scheduled runs are created by the CronJob controller and are owned by
	// the CronJob, not by the IngestionSource. A controller that only accepts
	// its direct children counts none of them.
	It("counts runs created by the CronJob", func() {
		reconcile()

		var cronJob batchv1.CronJob
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cronJob)).To(Succeed())

		jobName := name + "-29798722"
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName,
				Namespace: namespace,
				Labels:    ownedLabels(get(), runTypeScheduled),
			},
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: "worker", Image: "busybox"}},
					},
				},
			},
		}
		Expect(controllerutil.SetControllerReference(&cronJob, job, scheme.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, job)).To(Succeed())

		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{{
			Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, Reason: "Test",
		}}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
		job.Status.CompletionTime = &now
		job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue, Reason: "Test",
		})
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

		result, err := json.Marshal(runspec.RunResult{RowsIngested: 64, Watermark: "2026-04-01T00:00:00Z"})
		Expect(err).NotTo(HaveOccurred())
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName + "-pod",
				Namespace: namespace,
				Labels:    map[string]string{labelJobName: jobName},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: "worker", Image: "busybox"}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  "worker",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: string(result)}},
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		reconcile()

		status := get().Status
		Expect(status.TotalRowsIngested).To(Equal(int64(64)))
		Expect(status.Watermark).To(Equal("2026-04-01T00:00:00Z"))
	})

	It("counts each run exactly once across repeated reconciles", func() {
		reconcile()
		completeRun("a", true, runspec.RunResult{RowsIngested: 10, Watermark: "2026-01-02T00:00:00Z"}, "")
		reconcile()
		reconcile()
		reconcile()

		Expect(get().Status.TotalRowsIngested).To(Equal(int64(10)),
			"reconciling again must not double count a run that was already folded in")
	})

	It("suspends the schedule after the configured failure streak", func() {
		reconcile()
		completeRun("a", false, runspec.RunResult{Error: "upstream returned 500"}, "")
		reconcile()

		Expect(get().Status.ConsecutiveFailures).To(Equal(int32(1)))

		var cronJob batchv1.CronJob
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cronJob)).To(Succeed())
		Expect(cronJob.Spec.Suspend).To(Equal(ptr.To(false)), "one failure is not a streak")

		completeRun("b", false, runspec.RunResult{Error: "upstream returned 500"}, "")
		reconcile()

		status := get().Status
		Expect(status.ConsecutiveFailures).To(Equal(int32(2)))
		Expect(status.Phase).To(Equal(ingestv1alpha1.PhaseDegraded))

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cronJob)).To(Succeed())
		Expect(cronJob.Spec.Suspend).To(Equal(ptr.To(true)),
			"a source failing every run should stop paging someone every night")

		Expect(conditionStatus(status.Conditions, ingestv1alpha1.ConditionDegraded)).To(Equal(metav1.ConditionTrue))
		Expect(conditionStatus(status.Conditions, ingestv1alpha1.ConditionReady)).To(Equal(metav1.ConditionFalse))
	})

	It("clears the failure streak after a run succeeds", func() {
		reconcile()
		completeRun("a", false, runspec.RunResult{Error: "boom"}, "")
		reconcile()
		Expect(get().Status.ConsecutiveFailures).To(Equal(int32(1)))

		completeRun("b", true, runspec.RunResult{RowsIngested: 5, Watermark: "2026-02-01T00:00:00Z"}, "")
		reconcile()

		status := get().Status
		Expect(status.ConsecutiveFailures).To(BeZero())
		Expect(status.Phase).To(Equal(ingestv1alpha1.PhaseScheduled))
	})

	It("suspends without losing the watermark", func() {
		reconcile()
		completeRun("a", true, runspec.RunResult{RowsIngested: 7, Watermark: "2026-03-01T00:00:00Z"}, "")
		reconcile()

		src := get()
		src.Spec.Suspend = ptr.To(true)
		Expect(k8sClient.Update(ctx, src)).To(Succeed())
		reconcile()

		status := get().Status
		Expect(status.Phase).To(Equal(ingestv1alpha1.PhaseSuspended))
		Expect(status.Watermark).To(Equal("2026-03-01T00:00:00Z"),
			"suspending pauses scheduling; it does not discard progress")

		var cronJob batchv1.CronJob
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cronJob)).To(Succeed())
		Expect(cronJob.Spec.Suspend).To(Equal(ptr.To(true)))
	})

	It("runs a backfill once per request ID and never lets it move the watermark", func() {
		reconcile()
		completeRun("a", true, runspec.RunResult{RowsIngested: 3, Watermark: "2026-06-01T00:00:00Z"}, "")
		reconcile()

		src := get()
		src.Spec.Backfill = &ingestv1alpha1.Backfill{
			RequestID: "january",
			From:      "2026-01-01T00:00:00Z",
			To:        "2026-02-01T00:00:00Z",
		}
		Expect(k8sClient.Update(ctx, src)).To(Succeed())
		reconcile()

		jobName := backfillJobName(src, "january")
		var job batchv1.Job
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jobName, Namespace: namespace}, &job)).To(Succeed())

		env := job.Spec.Template.Spec.Containers[0].Env
		Expect(envValue(env, runspec.EnvBackfillID)).To(Equal("january"))
		Expect(envValue(env, runspec.EnvBackfillFrom)).To(Equal("2026-01-01T00:00:00Z"))
		Expect(conditionStatus(get().Status.Conditions, ingestv1alpha1.ConditionBackfilling)).To(Equal(metav1.ConditionTrue))

		// A backfill reports no watermark, so the scheduled cursor holds.
		completeRun("bf", true, runspec.RunResult{RowsIngested: 100, BackfillID: "january"}, "january")
		reconcile()

		status := get().Status
		Expect(status.LastCompletedBackfillID).To(Equal("january"))
		Expect(status.TotalRowsIngested).To(Equal(int64(103)))
		Expect(status.Watermark).To(Equal("2026-06-01T00:00:00Z"),
			"a backfill covers a historical range and must not rewind the schedule")

		// Deleting the finished Job must not cause the backfill to run again.
		// The default orphan policy leaves a finalizer that nothing in envtest
		// removes, so the delete is explicitly a background one.
		Expect(k8sClient.Delete(ctx, &job, client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		reconcile()
		err := k8sClient.Get(ctx, types.NamespacedName{Name: jobName, Namespace: namespace}, &job)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "a completed backfill request must not be recreated")
	})

	// The controller has a code path for a spec it cannot render, but the CEL
	// rules on the CRD mean the API server rejects that spec first. Catching
	// it at admission is the better outcome, so the test asserts on that.
	It("rejects a source whose type and configuration disagree", func() {
		src := get()
		src.Spec.Source.HTTP = nil
		err := k8sClient.Update(ctx, src)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("http must be set when type is HTTP"))
	})

	It("rejects a BigQuery destination with no BigQuery configuration", func() {
		src := get()
		src.Spec.Destination.Type = ingestv1alpha1.DestinationBigQuery
		err := k8sClient.Update(ctx, src)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("bigQuery must be set when type is BigQuery"))
	})

	It("starts one owned run per trigger annotation value", func() {
		reconcile()

		src := get()
		src.Annotations = map[string]string{AnnotationTrigger: "1787923174"}
		Expect(k8sClient.Update(ctx, src)).To(Succeed())
		reconcile()

		jobName := triggerJobName(src, "1787923174")
		var job batchv1.Job
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jobName, Namespace: namespace}, &job)).To(Succeed())
		Expect(job.OwnerReferences).To(HaveLen(1), "a triggered run must be owned, or its rows never reach status")
		Expect(job.Labels[labelRunType]).To(Equal(runTypeTrigger))
		Expect(envValue(job.Spec.Template.Spec.Containers[0].Env, runspec.EnvBackfillID)).To(BeEmpty(),
			"a triggered run is an ordinary run, not a backfill")
		Expect(get().Status.LastTriggerID).To(Equal("1787923174"))

		// Reconciling again with the same value must not start a second run.
		Expect(k8sClient.Delete(ctx, &job, client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		reconcile()
		err := k8sClient.Get(ctx, types.NamespacedName{Name: jobName, Namespace: namespace}, &job)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("ignores a source that no longer exists", func() {
		var src ingestv1alpha1.IngestionSource
		Expect(k8sClient.Get(ctx, key, &src)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &src)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	})
})

func conditionStatus(conditions []metav1.Condition, conditionType string) metav1.ConditionStatus {
	for _, c := range conditions {
		if c.Type == conditionType {
			return c.Status
		}
	}
	return metav1.ConditionUnknown
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
