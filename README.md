# sluice

A Kubernetes operator for pulling vendor APIs into BigQuery on a schedule.

You declare an `IngestionSource`: where to pull from, how often, and where the
records land. The controller owns the CronJob, the watermark that makes each
run incremental, and the record shape it has agreed to accept. When the vendor
changes that shape, the run fails instead of quietly corrupting the table.

```yaml
apiVersion: ingest.sluice.dev/v1alpha1
kind: IngestionSource
metadata:
  name: vendor-events
spec:
  schedule: "0 2 * * *"
  source:
    type: HTTP
    http:
      url: https://api.vendor.example/v1/events
      recordsPath: data.items
      auth:
        type: Bearer
        secretRef: {name: vendor-api, key: token}
      pagination:
        type: Cursor
        nextCursorPath: meta.next_cursor
        cursorParam: cursor
        sizeParam: limit
        pageSize: 500
  watermark:
    recordField: updated_at
    param: since
    format: RFC3339
  destination:
    type: BigQuery
    bigQuery:
      projectID: my-project
      dataset: vendor
      table: events
      deadLetterTable: events_dead_letter
  schema:
    drift: EvolveAdditive
```

```
$ kubectl get ingestionsources
NAME            SCHEDULE      PHASE       ROWS   LAST RUN    WATERMARK
vendor-events   */1 * * * *   Scheduled   1191   Succeeded   2026-08-28T13:25:12Z
```

## Try it in about three minutes

No cloud account, no credentials, no registry. The demo runs a fake vendor API
inside the cluster that pages with a cursor, filters inclusively, rate limits
you occasionally, and can be told to change its record shape mid-demo.

```bash
make demo-up      # kind cluster, operator, fake vendor API, IngestionSource
make demo-run     # start a run now instead of waiting for the schedule
make demo-watch   # status, events, and the record shape the operator accepted
make demo-drift   # make the vendor API change its payload
make demo-run     # watch the run fail before anything is written
make demo-down
```

Requires `docker`, `kind`, and `kubectl`.

## What it actually handles

Most of this exists because the naive version of it broke something.

**The watermark is inclusive, so runs would duplicate rows.** Nearly every
vendor `since` parameter returns records at the boundary as well as after it.
Resuming from the last watermark therefore re-delivers the last record of the
previous run, and appending it is how a table grows a duplicate every night.
The worker filters records against the watermark itself rather than trusting
the upstream filter. In the demo this shows up as `rowsSkipped: 1` on every
incremental run.

**A schema that drifts is worse than one that breaks.** A vendor dropping a
field or changing a number to a string loads perfectly well into a permissive
table, and nobody finds out until a dashboard is wrong. Each run derives the
record shape it saw and compares it to the last accepted one:

| `schema.drift` | Behavior |
| --- | --- |
| `Ignore` | Write, compare nothing |
| `Warn` | Write, record the new shape, raise a `SchemaDrift` event |
| `EvolveAdditive` | New fields are fine; a removed or retyped field fails the run |
| `Fail` | Any change fails the run |

The check runs before the batch is written, so a failing policy stops the data
at the door. The accepted shape lives in a ConfigMap the controller creates and
the run pod updates.

**A source that fails keeps failing.** After
`failurePolicy.suspendAfterConsecutiveFailures` runs fail in a row, the
controller suspends the CronJob and marks the resource `Degraded`. A broken
vendor pages someone once, not every night for a week. A successful run clears
the streak; `spec.suspend` stays untouched, because that field belongs to you.

**Backfills must not rewind the schedule.** `spec.backfill` runs a closed
historical range as a one-off Job, and its result never advances the watermark.
The completed request ID is recorded, so re-applying the same manifest does not
re-run it.

**Two concurrent runs share one watermark.** `concurrencyPolicy` defaults to
`Forbid` for that reason.

**Pagination that never ends.** `maxPages` caps a run, and reaching the cap is
a failure rather than a clean partial load. A repeated cursor also ends the
walk, so a vendor bug does not become a runaway Job.

**Rate limits and transient failures.** `429` and `5xx` are retried with
backoff, honoring `Retry-After`. `4xx` is not retried, because it will not
start working. `maxRequestsPerSecond` throttles the run.

## How it fits together

```
IngestionSource  ──owns──>  CronJob  ──creates──>  Job  ──>  run pod (/worker)
       │                                                          │
       ├──owns──> ConfigMap  <name>-runconfig   mounted ─────────>│
       ├──owns──> ConfigMap  <name>-schema      read + updated <──┤
       └──owns──> Job (backfill / triggered run)                  │
                                                                  │
             status  <──  termination message (JSON) ─────────────┘
```

A run reports what it did by writing JSON to its pod's termination message. The
controller reads it off the pod and folds it into status, so the row counts
come from the process that did the work rather than from scraping logs. Each
finished Job is annotated once it has been counted, which keeps the totals
correct across repeated reconciles without an unbounded list in status.

The controller owns every write to status; the worker only reads. That split is
what keeps concurrent runs from fighting over the watermark.

The watermark is read from the cluster at the start of each run rather than
baked into the CronJob template, so a template rendered weeks ago still resumes
from the right place.

### Running a job right now

```bash
kubectl annotate ingestionsource vendor-events \
  ingest.sluice.dev/trigger="$(date +%s)" --overwrite
```

The controller creates a single owned Job. `kubectl create job --from=cronjob/...`
also runs, but the resulting Job is owned by the CronJob template rather than
by the operator, so its rows never reach status.

## Permissions

Run pods use their own ServiceAccount, not the controller's. It can read its
`IngestionSource` and read and update one ConfigMap. It cannot write status,
cannot create or delete anything, and cannot read Secrets: credentials arrive
as environment variables the kubelet projects from the referenced Secret, so
the process never needs access to the Secret itself.

Nothing sensitive is written into the rendered run config, which means the
`<name>-runconfig` ConfigMap is safe to read and safe to paste into a bug
report.

For BigQuery, bind the runner ServiceAccount to a Google service account with
Workload Identity. The operator has no code path that reads a service account
key.

## Install

```bash
make install                        # CRDs
make deploy IMG=<your-image>        # controller
```

The image ships three binaries: `/manager`, `/worker`, and `/fakeapi`. Run pods
default to the controller's own image with the entrypoint overridden, so a
`spec.runner.image` is only needed if you want a different one.

## Development

```bash
make test        # unit tests plus an envtest suite against a real API server
make test-e2e    # kind cluster, deployed operator
make lint
```

The envtest suite covers the parts that are easy to get wrong: counting each
run exactly once, folding a run created by the CronJob controller (which owns
it, rather than the IngestionSource) into status, suspending on a failure
streak, and refusing to re-run a completed backfill.

## Status

`v1alpha1`, and the API may change. The HTTP source and the BigQuery and stdout
destinations are implemented; nothing else is.

## License

Apache 2.0.
