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

// Command worker performs a single ingestion run.
//
// It is the process a CronJob or backfill Job starts. Everything it needs
// arrives through the environment and a mounted RunConfig; nothing is passed
// positionally, so a run is reproducible from the pod spec alone.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ingestv1alpha1 "github.com/JackFurton/sluice/api/v1alpha1"
	"github.com/JackFurton/sluice/internal/runspec"
	"github.com/JackFurton/sluice/internal/worker"
)

// terminationMessageLimit is the kubelet's cap on a termination message. A
// result that does not fit is trimmed rather than dropped, because the counts
// matter more than the schema detail.
const terminationMessageLimit = 4096

func main() {
	var (
		configPath = flag.String("config", "", "path to the RunConfig JSON (defaults to $"+runspec.EnvConfigPath+")")
		resultPath = flag.String("result", "",
			"where to write the RunResult JSON (defaults to $"+runspec.EnvResultPath+", then /dev/termination-log)")
		local    = flag.Bool("local", false, "run without a cluster, keeping watermark and schema state in memory")
		logLevel = flag.String("log-level", "info", "one of debug, info, warn, error")
	)
	flag.Parse()

	log := newLogger(*logLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	result, err := run(ctx, log, *configPath, *local)
	writeResult(log, *resultPath, result)
	if err != nil {
		log.Error("run failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger, configPath string, local bool) (runspec.RunResult, error) {
	if configPath == "" {
		configPath = os.Getenv(runspec.EnvConfigPath)
	}
	if configPath == "" {
		return runspec.RunResult{}, fmt.Errorf("no RunConfig path: pass --config or set %s", runspec.EnvConfigPath)
	}

	raw, err := os.ReadFile(configPath)
	if err != nil {
		return runspec.RunResult{}, fmt.Errorf("read RunConfig: %w", err)
	}
	var config runspec.RunConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return runspec.RunResult{}, fmt.Errorf("parse RunConfig %s: %w", configPath, err)
	}

	state, err := newStore(local, config)
	if err != nil {
		return runspec.RunResult{}, err
	}

	opts := worker.Options{
		Config:   config,
		Creds:    credentials(config),
		State:    state,
		Backfill: backfill(),
		Stdout:   os.Stdout,
		Logger:   log,
	}
	return worker.Run(ctx, opts)
}

func newStore(local bool, config runspec.RunConfig) (worker.StateStore, error) {
	if local {
		return &worker.MemoryStore{WatermarkValue: initialWatermark(config)}, nil
	}

	name := os.Getenv(runspec.EnvSourceName)
	namespace := os.Getenv(runspec.EnvSourceNamespace)
	if name == "" || namespace == "" {
		return nil, fmt.Errorf("%s and %s must be set outside --local mode",
			runspec.EnvSourceName, runspec.EnvSourceNamespace)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("register core scheme: %w", err)
	}
	if err := ingestv1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("register ingest scheme: %w", err)
	}
	restConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	c, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("build client: %w", err)
	}
	return &worker.KubeStore{
		Client:          c,
		Name:            name,
		Namespace:       namespace,
		SchemaConfigMap: os.Getenv(runspec.EnvSchemaConfigMap),
	}, nil
}

// initialWatermark seeds a local run so --local behaves like a first scheduled run.
func initialWatermark(config runspec.RunConfig) string {
	if config.Watermark == nil {
		return ""
	}
	return config.Watermark.InitialValue
}

// credentials reads what the controller projected from the referenced Secret.
func credentials(config runspec.RunConfig) worker.Credentials {
	creds := worker.Credentials{
		Token:    os.Getenv(runspec.EnvAuthToken),
		Username: os.Getenv(runspec.EnvAuthUsername),
		Password: os.Getenv(runspec.EnvAuthPassword),
		Headers:  map[string]string{},
	}
	for _, header := range config.Source.Headers {
		if header.FromSecret && header.ValueEnv != "" {
			creds.Headers[header.Name] = os.Getenv(header.ValueEnv)
		}
	}
	return creds
}

// backfill reports the requested historical range, if this run is one.
func backfill() *worker.BackfillRange {
	id := os.Getenv(runspec.EnvBackfillID)
	if id == "" {
		return nil
	}
	return &worker.BackfillRange{
		ID:   id,
		From: os.Getenv(runspec.EnvBackfillFrom),
		To:   os.Getenv(runspec.EnvBackfillTo),
	}
}

// writeResult hands the run's counts back to the controller through the pod's
// termination message. A failure to write is logged and otherwise ignored: the
// run already happened, and losing the report is not worth changing its outcome.
func writeResult(log *slog.Logger, path string, result runspec.RunResult) {
	if path == "" {
		path = os.Getenv(runspec.EnvResultPath)
	}
	if path == "" {
		path = "/dev/termination-log"
	}

	encoded, err := json.Marshal(result)
	if err != nil {
		log.Error("encoding run result", "error", err)
		return
	}
	if len(encoded) > terminationMessageLimit {
		trimmed := result
		trimmed.SchemaChange = nil
		trimmed.Error = truncate(trimmed.Error, 1024)
		if retry, err := json.Marshal(trimmed); err == nil {
			encoded = retry
		}
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil && !errors.Is(err, os.ErrPermission) {
		log.Error("writing run result", "path", path, "error", err)
	}
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(strings.ToLower(level))); err != nil {
		l = slog.LevelInfo
	}
	// Records go to stdout when the destination is Stdout, so logs go to
	// stderr to keep the two streams separable.
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
