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
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ingestv1alpha1 "github.com/JackFurton/sluice/api/v1alpha1"
	"github.com/JackFurton/sluice/internal/runspec"
)

// KubeStore reads run state from the cluster.
//
// The watermark is read from the IngestionSource's status at run start rather
// than baked into the CronJob template, so a template that was rendered weeks
// ago still resumes from the right place. The worker only reads the custom
// resource; the controller owns every write to status. That split is what
// keeps two runs from fighting over the same field.
type KubeStore struct {
	Client    client.Client
	Name      string
	Namespace string
	// SchemaConfigMap holds the last accepted record shape. The controller
	// pre-creates it, so the worker's role needs no create verb and can be
	// scoped to this one object by name.
	SchemaConfigMap string
}

// Watermark reads the resume point from the IngestionSource status.
func (s *KubeStore) Watermark(ctx context.Context) (string, error) {
	var source ingestv1alpha1.IngestionSource
	key := types.NamespacedName{Name: s.Name, Namespace: s.Namespace}
	if err := s.Client.Get(ctx, key, &source); err != nil {
		return "", fmt.Errorf("get IngestionSource %s: %w", key, err)
	}
	return source.Status.Watermark, nil
}

// Schema reads the last accepted record shape. A missing or empty ConfigMap
// means no shape has been accepted yet, which is not an error.
func (s *KubeStore) Schema(ctx context.Context) (runspec.Schema, error) {
	if s.SchemaConfigMap == "" {
		return runspec.Schema{}, nil
	}
	var cm corev1.ConfigMap
	key := types.NamespacedName{Name: s.SchemaConfigMap, Namespace: s.Namespace}
	if err := s.Client.Get(ctx, key, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return runspec.Schema{}, nil
		}
		return runspec.Schema{}, fmt.Errorf("get ConfigMap %s: %w", key, err)
	}
	raw, ok := cm.Data[runspec.SchemaKey]
	if !ok || raw == "" {
		return runspec.Schema{}, nil
	}
	var schema runspec.Schema
	if err := json.Unmarshal([]byte(raw), &schema); err != nil {
		// A corrupt schema record should not wedge ingestion forever. Treat it
		// as "nothing accepted yet" and let this run establish the shape.
		return runspec.Schema{}, nil
	}
	return schema, nil
}

// SaveSchema records a newly accepted record shape.
func (s *KubeStore) SaveSchema(ctx context.Context, schema runspec.Schema) error {
	if s.SchemaConfigMap == "" {
		return nil
	}
	encoded, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("encode schema: %w", err)
	}
	var cm corev1.ConfigMap
	key := types.NamespacedName{Name: s.SchemaConfigMap, Namespace: s.Namespace}
	if err := s.Client.Get(ctx, key, &cm); err != nil {
		return fmt.Errorf("get ConfigMap %s: %w", key, err)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[runspec.SchemaKey] = string(encoded)
	if err := s.Client.Update(ctx, &cm); err != nil {
		return fmt.Errorf("update ConfigMap %s: %w", key, err)
	}
	return nil
}

// MemoryStore keeps run state in memory. It backs `sluice worker --local`,
// which runs the exact ingestion path against any HTTP endpoint with no
// cluster involved, and it is what the run tests use.
type MemoryStore struct {
	WatermarkValue string
	SchemaValue    runspec.Schema
	// Saved records the shape the run accepted, for assertions.
	Saved *runspec.Schema
}

// Watermark returns the in-memory resume point.
func (s *MemoryStore) Watermark(context.Context) (string, error) { return s.WatermarkValue, nil }

// Schema returns the in-memory accepted shape.
func (s *MemoryStore) Schema(context.Context) (runspec.Schema, error) { return s.SchemaValue, nil }

// SaveSchema stores the accepted shape in memory.
func (s *MemoryStore) SaveSchema(_ context.Context, schema runspec.Schema) error {
	s.SchemaValue = schema
	s.Saved = &schema
	return nil
}
