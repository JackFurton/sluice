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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	ingestv1alpha1 "github.com/JackFurton/sluice/api/v1alpha1"
	"github.com/JackFurton/sluice/internal/runspec"
)

// Labels and annotations the controller stamps on everything it owns.
const (
	labelName    = "app.kubernetes.io/name"
	labelManaged = "app.kubernetes.io/managed-by"
	labelSource  = "ingest.sluice.dev/source"
	labelRunType = "ingest.sluice.dev/run-type"

	// annotationConfigHash rolls the CronJob pod template when, and only when,
	// the rendered RunConfig actually changes.
	annotationConfigHash = "ingest.sluice.dev/config-hash"
	// annotationAccounted marks a finished Job whose counts are already folded
	// into status totals. Recording it on the Job rather than in status keeps
	// the bookkeeping bounded by the Job history limits.
	annotationAccounted = "ingest.sluice.dev/accounted"
	// annotationBackfillID ties a backfill Job to the request that asked for it.
	annotationBackfillID = "ingest.sluice.dev/backfill-id"
	// AnnotationTrigger asks for one run right now. Set it to any new value,
	// usually a timestamp, and the controller creates a single owned Job.
	// Creating a Job with "kubectl create job --from=cronjob/..." also runs,
	// but nothing owns it, so its rows never reach status.
	AnnotationTrigger = "ingest.sluice.dev/trigger"
	// annotationTriggerID records which trigger a Job was created for.
	annotationTriggerID = "ingest.sluice.dev/trigger-id"

	// containerNameWorker is the container the controller reads the run
	// result from, so it must match on both the write and the read side.
	containerNameWorker = "worker"
	// labelJobName is the label the Job controller stamps on run pods.
	labelJobName = "batch.kubernetes.io/job-name"

	runTypeScheduled = "scheduled"
	runTypeBackfill  = "backfill"
	runTypeTrigger   = "trigger"

	appName = "sluice"
)

// runConfigMapName is the ConfigMap holding the rendered RunConfig.
func runConfigMapName(src *ingestv1alpha1.IngestionSource) string {
	return src.Name + "-runconfig"
}

// schemaConfigMapName is the ConfigMap holding the last accepted record shape.
func schemaConfigMapName(src *ingestv1alpha1.IngestionSource) string {
	return src.Name + "-schema"
}

// cronJobName is the CronJob driving the schedule.
func cronJobName(src *ingestv1alpha1.IngestionSource) string {
	return src.Name
}

// backfillJobName is derived from the request ID so the same request maps to
// the same Job name, which makes creating it idempotent.
func backfillJobName(src *ingestv1alpha1.IngestionSource, requestID string) string {
	return oneOffJobName(src, runTypeBackfill, requestID)
}

// triggerJobName is derived from the trigger value, for the same reason.
func triggerJobName(src *ingestv1alpha1.IngestionSource, trigger string) string {
	return oneOffJobName(src, runTypeTrigger, trigger)
}

func oneOffJobName(src *ingestv1alpha1.IngestionSource, runType, id string) string {
	sum := sha256.Sum256([]byte(id))
	return fmt.Sprintf("%s-%s-%s", src.Name, runType, hex.EncodeToString(sum[:])[:8])
}

func ownedLabels(src *ingestv1alpha1.IngestionSource, runType string) map[string]string {
	labels := map[string]string{
		labelName:    appName,
		labelManaged: "sluice-controller",
		labelSource:  src.Name,
	}
	if runType != "" {
		labels[labelRunType] = runType
	}
	return labels
}

// buildRunConfig renders the parts of a spec that a run needs and that do not
// change between runs. Credentials are deliberately absent: they reach the
// worker as environment variables projected from the Secret, so this document
// stays safe to read.
func buildRunConfig(src *ingestv1alpha1.IngestionSource) (runspec.RunConfig, error) {
	http := src.Spec.Source.HTTP
	if http == nil {
		return runspec.RunConfig{}, fmt.Errorf("spec.source.http is required when spec.source.type is HTTP")
	}

	source := runspec.SourceConfig{
		URL:                  http.URL,
		Method:               defaultString(http.Method, "GET"),
		Query:                http.Query,
		Body:                 http.Body,
		AuthType:             string(ingestv1alpha1.AuthTypeNone),
		RecordsPath:          http.RecordsPath,
		TimeoutSeconds:       int(http.Timeout.Seconds()),
		MaxRequestsPerSecond: http.MaxRequestsPerSecond,
	}
	for i, header := range http.Headers {
		hc := runspec.HeaderConfig{Name: header.Name, Value: header.Value}
		if header.ValueFrom != nil {
			hc.Value = ""
			hc.FromSecret = true
			hc.ValueEnv = headerEnvName(i)
		}
		source.Headers = append(source.Headers, hc)
	}
	if auth := http.Auth; auth != nil {
		source.AuthType = defaultString(string(auth.Type), string(ingestv1alpha1.AuthTypeNone))
		source.AuthHeaderName = auth.HeaderName
	}
	if page := http.Pagination; page != nil {
		source.Pagination = runspec.PaginationConfig{
			Type:           defaultString(string(page.Type), string(ingestv1alpha1.PaginationNone)),
			NextCursorPath: page.NextCursorPath,
			CursorParam:    page.CursorParam,
			PageParam:      page.PageParam,
			SizeParam:      page.SizeParam,
			PageSize:       page.PageSize,
			StartPage:      page.StartPage,
			MaxPages:       page.MaxPages,
		}
	} else {
		source.Pagination = runspec.PaginationConfig{Type: string(ingestv1alpha1.PaginationNone)}
	}

	config := runspec.RunConfig{
		Source:      source,
		Destination: buildDestination(src.Spec.Destination),
		SchemaDrift: defaultString(string(src.Spec.Schema.Drift), string(ingestv1alpha1.SchemaDriftWarn)),
	}
	if wm := src.Spec.Watermark; wm != nil {
		config.Watermark = &runspec.WatermarkConfig{
			RecordField:  wm.RecordField,
			Param:        wm.Param,
			ToParam:      wm.ToParam,
			Format:       defaultString(string(wm.Format), string(ingestv1alpha1.WatermarkRFC3339)),
			InitialValue: wm.InitialValue,
		}
	}
	return config, nil
}

func buildDestination(dst ingestv1alpha1.Destination) runspec.DestinationConfig {
	out := runspec.DestinationConfig{Type: defaultString(string(dst.Type), string(ingestv1alpha1.DestinationStdout))}
	if bq := dst.BigQuery; bq != nil {
		out.BigQuery = &runspec.BigQueryConfig{
			ProjectID:        bq.ProjectID,
			Dataset:          bq.Dataset,
			Table:            bq.Table,
			Location:         bq.Location,
			WriteDisposition: defaultString(bq.WriteDisposition, "Append"),
			CreateIfMissing:  ptr.Deref(bq.CreateIfMissing, true),
			DeadLetterTable:  bq.DeadLetterTable,
		}
	}
	return out
}

// headerEnvName is the environment variable a Secret-sourced header arrives in.
func headerEnvName(index int) string {
	return "SLUICE_HEADER_" + strconv.Itoa(index)
}

// buildPodSpec renders the pod that performs a run. Scheduled runs and
// backfills share it, so a backfill exercises exactly the same code path as
// the thing it is backfilling.
func buildPodSpec(src *ingestv1alpha1.IngestionSource, image string, backfill *ingestv1alpha1.Backfill) corev1.PodSpec {
	env := []corev1.EnvVar{
		{Name: runspec.EnvConfigPath, Value: filepath.Join(runspec.DefaultConfigMountPath, runspec.ConfigKey)},
		{Name: runspec.EnvSourceName, Value: src.Name},
		{Name: runspec.EnvSourceNamespace, Value: src.Namespace},
		{Name: runspec.EnvSchemaConfigMap, Value: schemaConfigMapName(src)},
	}
	env = append(env, authEnv(src)...)
	if backfill != nil {
		env = append(env,
			corev1.EnvVar{Name: runspec.EnvBackfillID, Value: backfill.RequestID},
			corev1.EnvVar{Name: runspec.EnvBackfillFrom, Value: backfill.From},
			corev1.EnvVar{Name: runspec.EnvBackfillTo, Value: backfill.To},
		)
	}
	env = append(env, src.Spec.Runner.Env...)

	return corev1.PodSpec{
		RestartPolicy:         corev1.RestartPolicyNever,
		ServiceAccountName:    src.Spec.Runner.ServiceAccountName,
		NodeSelector:          src.Spec.Runner.NodeSelector,
		Tolerations:           src.Spec.Runner.Tolerations,
		ActiveDeadlineSeconds: src.Spec.Runner.ActiveDeadlineSeconds,
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot:   ptr.To(true),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		Containers: []corev1.Container{{
			Name:            containerNameWorker,
			Image:           image,
			ImagePullPolicy: src.Spec.Runner.ImagePullPolicy,
			Command:         []string{"/worker"},
			Env:             env,
			Resources:       src.Spec.Runner.Resources,
			VolumeMounts: []corev1.VolumeMount{{
				Name:      "runconfig",
				MountPath: runspec.DefaultConfigMountPath,
				ReadOnly:  true,
			}},
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptr.To(false),
				ReadOnlyRootFilesystem:   ptr.To(true),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
			// The worker reports its counts as JSON here. Falling back to logs
			// on error means a crash that never reached the write still leaves
			// something for the controller to surface.
			TerminationMessagePath:   corev1.TerminationMessagePathDefault,
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		}},
		Volumes: []corev1.Volume{{
			Name: "runconfig",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: runConfigMapName(src)},
				},
			},
		}},
	}
}

// authEnv projects credentials from the referenced Secret into the run pod.
func authEnv(src *ingestv1alpha1.IngestionSource) []corev1.EnvVar {
	http := src.Spec.Source.HTTP
	if http == nil {
		return nil
	}

	var env []corev1.EnvVar
	for i, header := range http.Headers {
		if header.ValueFrom == nil {
			continue
		}
		env = append(env, corev1.EnvVar{
			Name:      headerEnvName(i),
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: header.ValueFrom},
		})
	}

	auth := http.Auth
	if auth == nil || auth.SecretRef == nil {
		return env
	}
	switch auth.Type {
	case ingestv1alpha1.AuthTypeBearer, ingestv1alpha1.AuthTypeHeader:
		env = append(env, corev1.EnvVar{
			Name:      runspec.EnvAuthToken,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: auth.SecretRef},
		})
	case ingestv1alpha1.AuthTypeBasic:
		// Basic auth needs two values, so the key on the selector is ignored
		// and the Secret is required to carry "username" and "password".
		env = append(env,
			corev1.EnvVar{Name: runspec.EnvAuthUsername, ValueFrom: secretKey(auth.SecretRef.Name, "username")},
			corev1.EnvVar{Name: runspec.EnvAuthPassword, ValueFrom: secretKey(auth.SecretRef.Name, "password")},
		)
	}
	return env
}

func secretKey(name, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: name},
		Key:                  key,
	}}
}

// buildCronJob renders the CronJob that drives the schedule.
func buildCronJob(src *ingestv1alpha1.IngestionSource, image, configHash string, suspend bool) *batchv1.CronJob {
	labels := ownedLabels(src, runTypeScheduled)
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cronJobName(src),
			Namespace: src.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   src.Spec.Schedule,
			Suspend:                    ptr.To(suspend),
			ConcurrencyPolicy:          concurrencyPolicy(src),
			StartingDeadlineSeconds:    src.Spec.StartingDeadlineSeconds,
			SuccessfulJobsHistoryLimit: src.Spec.SuccessfulRunHistoryLimit,
			FailedJobsHistoryLimit:     src.Spec.FailedRunHistoryLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: batchv1.JobSpec{
					BackoffLimit: ptr.To(src.Spec.FailurePolicy.BackoffLimit),
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      labels,
							Annotations: map[string]string{annotationConfigHash: configHash},
						},
						Spec: buildPodSpec(src, image, nil),
					},
				},
			},
		},
	}
}

// buildBackfillJob renders the one-off Job for a requested historical pull.
func buildBackfillJob(src *ingestv1alpha1.IngestionSource, image, configHash string) *batchv1.Job {
	backfill := src.Spec.Backfill
	job := buildOneOffJob(src, image, configHash, runTypeBackfill, backfillJobName(src, backfill.RequestID), backfill)
	job.Annotations[annotationBackfillID] = backfill.RequestID
	return job
}

// buildTriggerJob renders a single immediate run. It is an ordinary scheduled
// run that happens now, so it advances the watermark like any other.
func buildTriggerJob(src *ingestv1alpha1.IngestionSource, image, configHash, trigger string) *batchv1.Job {
	job := buildOneOffJob(src, image, configHash, runTypeTrigger, triggerJobName(src, trigger), nil)
	job.Annotations[annotationTriggerID] = trigger
	return job
}

func buildOneOffJob(src *ingestv1alpha1.IngestionSource, image, configHash, runType, name string, backfill *ingestv1alpha1.Backfill) *batchv1.Job {
	labels := ownedLabels(src, runType)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   src.Namespace,
			Labels:      labels,
			Annotations: map[string]string{},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To(src.Spec.FailurePolicy.BackoffLimit),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: map[string]string{annotationConfigHash: configHash},
				},
				Spec: buildPodSpec(src, image, backfill),
			},
		},
	}
}

func concurrencyPolicy(src *ingestv1alpha1.IngestionSource) batchv1.ConcurrencyPolicy {
	if src.Spec.ConcurrencyPolicy == "" {
		return batchv1.ForbidConcurrent
	}
	return src.Spec.ConcurrencyPolicy
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
