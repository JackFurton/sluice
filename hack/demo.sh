#!/usr/bin/env bash
#
# Stand the whole thing up on a kind cluster: the operator, a fake vendor API,
# and an IngestionSource pulling from it every minute. No cloud account, no
# registry, no credentials.
#
#   hack/demo.sh up      build the image, create the cluster, deploy everything
#   hack/demo.sh run     start a run immediately instead of waiting for the schedule
#   hack/demo.sh watch   follow status and the records the run writes
#   hack/demo.sh drift   make the vendor API change its record shape
#   hack/demo.sh down    delete the cluster

set -euo pipefail

CLUSTER="${CLUSTER:-sluice-demo}"
IMAGE="${IMAGE:-sluice:demo}"
NAMESPACE="sluice-demo"
SOURCE="vendor-events"

repo_root() { cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd; }
cd "$(repo_root)"

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }
}

# sha256 prints the first 8 hex characters of a value's digest, matching how
# the controller names one-off Jobs.
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$1" | sha256sum | cut -c1-8
  else
    printf '%s' "$1" | shasum -a 256 | cut -c1-8
  fi
}

up() {
  require docker; require kind; require kubectl

  if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
    echo "==> creating kind cluster ${CLUSTER}"
    kind create cluster --name "${CLUSTER}"
  fi
  kubectl config use-context "kind-${CLUSTER}" >/dev/null

  echo "==> building ${IMAGE}"
  docker build -t "${IMAGE}" .

  echo "==> loading ${IMAGE} into the cluster"
  kind load docker-image "${IMAGE}" --name "${CLUSTER}"

  echo "==> installing CRDs and the controller"
  make install
  make deploy IMG="${IMAGE}"
  # The image tag does not change between builds, so the Deployment spec is
  # identical and nothing would roll on its own. Restarting picks up the image
  # that was just loaded.
  kubectl -n sluice-system rollout restart deployment/sluice-controller-manager
  kubectl -n sluice-system rollout status deployment/sluice-controller-manager --timeout=180s

  echo "==> deploying the fake vendor API and the IngestionSource"
  kubectl apply -f demo/00-namespace.yaml
  kubectl apply -f demo/10-fakeapi.yaml
  kubectl apply -f demo/20-runner-rbac.yaml
  kubectl -n "${NAMESPACE}" rollout status deployment/fakeapi --timeout=120s
  kubectl apply -f demo/30-ingestionsource.yaml

  cat <<'MSG'

Up. The schedule fires every minute; to start a run right now:

    hack/demo.sh run

Then watch what happens:

    hack/demo.sh watch

MSG
}

run() {
  local trigger
  trigger="$(date +%s)"

  # The annotation asks the controller for one run. Creating a Job straight
  # from the CronJob would also run, but nothing would own it, so its rows
  # would never reach status.
  kubectl -n "${NAMESPACE}" annotate ingestionsource "${SOURCE}" \
    "ingest.sluice.dev/trigger=${trigger}" --overwrite

  # The Job name is derived from the trigger value, so it can be computed here
  # rather than guessed by listing and hoping the newest sorts last.
  local job="${SOURCE}-trigger-$(sha256 "${trigger}")"

  echo "==> waiting for ${job}"
  local found=""
  for _ in $(seq 1 30); do
    if kubectl -n "${NAMESPACE}" get "job/${job}" >/dev/null 2>&1; then
      found=yes
      break
    fi
    sleep 1
  done
  if [ -z "${found}" ]; then
    echo "no run started; check the controller logs" >&2
    return 1
  fi

  kubectl -n "${NAMESPACE}" wait --for=condition=complete --timeout=180s "job/${job}" >/dev/null 2>&1 || \
    kubectl -n "${NAMESPACE}" wait --for=condition=failed --timeout=30s "job/${job}" >/dev/null 2>&1 || true
  kubectl -n "${NAMESPACE}" logs "job/${job}" --tail=6 || true
}

watch_demo() {
  echo "==> IngestionSource"
  kubectl -n "${NAMESPACE}" get ingestionsource "${SOURCE}" -o wide
  echo
  echo "==> status"
  kubectl -n "${NAMESPACE}" get ingestionsource "${SOURCE}" \
    -o jsonpath='{.status}' | python3 -m json.tool 2>/dev/null || \
    kubectl -n "${NAMESPACE}" get ingestionsource "${SOURCE}" -o yaml
  echo
  echo "==> events"
  kubectl -n "${NAMESPACE}" get events --field-selector "involvedObject.name=${SOURCE}" \
    --sort-by=.lastTimestamp | tail -10
  echo
  echo "==> accepted record shape"
  kubectl -n "${NAMESPACE}" get configmap "${SOURCE}-schema" \
    -o go-template='{{index .data "schema.json"}}' | python3 -m json.tool 2>/dev/null || true
}

drift() {
  echo "==> restarting the vendor API so it changes its record shape after 1 request"
  kubectl -n "${NAMESPACE}" patch deployment fakeapi --type=json -p \
    '[{"op":"replace","path":"/spec/template/spec/containers/0/args","value":["--addr=:8080","--seed=120","--interval=1s","--page-size=50","--drift-after=1"]}]'
  kubectl -n "${NAMESPACE}" rollout status deployment/fakeapi --timeout=120s
  cat <<'MSG'

The API now drops the email field and returns amount as a string. Start a run
and the drift policy will fail it before anything is written:

    hack/demo.sh run
    hack/demo.sh watch

MSG
}

down() {
  require kind
  kind delete cluster --name "${CLUSTER}"
}

case "${1:-up}" in
  up) up ;;
  run) run ;;
  watch) watch_demo ;;
  drift) drift ;;
  down) down ;;
  *) echo "usage: $0 {up|run|watch|drift|down}" >&2; exit 1 ;;
esac
