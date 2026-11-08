#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Local Kubernetes runs of the driver regression cases on a kind cluster. Called by the mise tasks
# in mise.toml; see docs/guides/kubernetes-local.md. Stacks and cases come from
# e2e-tests/compose/stacks.txt and cases.txt, and the driver configurations and workloads from
# e2e-tests/compose/drivers and workloads are used unchanged: every stack gets its own namespace
# omb-<stack> whose broker Services have the Compose service names.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
LOCAL_DIR=$ROOT/deployment/kubernetes/local
CHART_DIR=$ROOT/deployment/kubernetes/helm/open-messaging-benchmark-chart
SUITE_DIR=$ROOT/e2e-tests/compose
OUT_DIR=$ROOT/e2e-tests/target/k8s

CLUSTER=${CLUSTER:-omb}
IMAGE=${IMAGE:-openmessaging-benchmark:local}
PUBLISHED_REPOSITORY=docker.io/mihkels/open-messaging-benchmark
RELEASE=omb
# Seconds: broker startup, and one benchmark case (the e2e workloads run for one minute)
STARTUP_TIMEOUT=${OMB_K8S_STARTUP_TIMEOUT:-900}
CASE_TIMEOUT=${OMB_K8S_CASE_TIMEOUT:-600}
# 1: use the published :latest images instead of building locally; 1 for OMB_K8S_REBUILD rebuilds
PUBLISHED=${OMB_K8S_PUBLISHED:-0}
REBUILD=${OMB_K8S_REBUILD:-0}
KAFKA_JMS_JAR=${OMB_K8S_KAFKA_JMS_JAR:-}
# 1: keep the stack namespace after k8s:run instead of deleting it
KEEP=${OMB_K8S_KEEP:-0}
# Seconds a pod may stay unschedulable before a wait fails
UNSCHEDULABLE_GRACE=${OMB_K8S_UNSCHEDULABLE_GRACE:-90}

usage() {
    cat <<'EOF'
Usage: omb-k8s.sh <command> [arguments]

  list                      broker stacks and benchmark cases
  image [drivers]           build the image (all, or a comma-separated driver list) and load it into kind
  broker <stack>            deploy the stack's broker into namespace omb-<stack> and wait until ready
  deploy <stack>            broker + chart with an interactive coordinator (contract case configuration)
  run <stack> [case...]     broker + chart, run the cases (default: every case of the stack) as Jobs
  shell <stack>             shell in the interactive coordinator
  logs <stack>              pods and recent logs of the stack namespace
  down [stack]              delete the stack namespace, or the whole cluster without a stack
  lint                      lint and render the chart and broker manifests, validate with kubeconform

Environment: CLUSTER (omb), IMAGE (openmessaging-benchmark:local), DRIVERS, OMB_K8S_PUBLISHED=1,
OMB_K8S_REBUILD=1, OMB_K8S_KEEP=1, OMB_K8S_KAFKA_JMS_JAR, OMB_K8S_STARTUP_TIMEOUT,
OMB_K8S_CASE_TIMEOUT, OMB_K8S_UNSCHEDULABLE_GRACE.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

log() {
    echo "==> $*"
}

# Rows of a suite table without comments, with fields trimmed and separated by "|". Readers must
# consume all of it: under pipefail, one that stops early (awk exit, head, grep -q) can make the
# writer fail with SIGPIPE (exit 141)
table() {
    grep -v -E '^[[:space:]]*(#|$)' "$1" | sed -E 's/[[:space:]]*\|[[:space:]]*/|/g; s/^[[:space:]]+//; s/[[:space:]]+$//'
}

stack_row() {
    table "$SUITE_DIR/stacks.txt" | awk -F'|' -v s="$1" '$1 == s && !n++'
}

require_stack() {
    [[ -n "${1:-}" ]] || die "missing stack; see: mise run k8s:list"
    [[ -n "$(stack_row "$1")" ]] || die "unknown stack '$1'; see: mise run k8s:list"
}

case_row() {
    table "$SUITE_DIR/cases.txt" | awk -F'|' -v c="$1" '$1 == c && !n++'
}

stack_cases() {
    table "$SUITE_DIR/cases.txt" | awk -F'|' -v s="$1" '$3 == s { print $1 }'
}

# The driver plugin, which is also the image variant, of a stack
stack_plugin() {
    table "$SUITE_DIR/cases.txt" | awk -F'|' -v s="$1" '$3 == s && !n++ { print $2 }'
}

# jms-* stacks reuse the broker of their provider
broker_dir() {
    case "$1" in
        jms-artemis) echo artemis ;;
        jms-pulsar) echo pulsar ;;
        jms-kafka) echo kafka ;;
        *) echo "$1" ;;
    esac
}

namespace() {
    echo "omb-$1"
}

require_cluster() {
    local context
    context=$(kubectl config current-context 2>/dev/null || true)
    [[ "$context" == "kind-$CLUSTER" ]] || die "kubectl context is '${context:-none}', expected kind-$CLUSTER; run: mise run k8s:cluster"
}

cluster_exists() {
    kind get clusters 2>/dev/null | grep -x "$CLUSTER" >/dev/null
}

# Build an image with the given drivers through docker-bake.hcl and load it into kind
build_image() {
    local drivers=$1 tag=$2
    log "Building $tag (drivers: $drivers)"
    (cd "$ROOT" && DRIVERS="$drivers" IMAGE="$tag" docker buildx bake local)
}

load_image() {
    if cluster_exists; then
        log "Loading $1 into kind cluster $CLUSTER"
        kind load docker-image "$1" --name "$CLUSTER"
    fi
}

cmd_image() {
    local drivers=${1:-${DRIVERS:-all}} tag=$IMAGE
    [[ "$drivers" == "all" ]] || tag="$IMAGE-${drivers//,/-}"
    build_image "$drivers" "$tag"
    load_image "$tag"
}

# Adds the user-supplied Confluent Kafka JMS client to an image with the JMS driver
kafka_jms_image() {
    local base=$1 tag=$2 jar=$KAFKA_JMS_JAR context
    if [[ -z "$jar" ]]; then
        jar=$(ls "$SUITE_DIR"/jms-providers/kafka/*.jar 2>/dev/null | head -n 1 || true)
    fi
    [[ -n "$jar" && -f "$jar" ]] || die "Kafka JMS provider jar not supplied: put the Confluent kafka-jms-client fat jar in e2e-tests/compose/jms-providers/kafka/ or set OMB_K8S_KAFKA_JMS_JAR"
    context=$(mktemp -d)
    cp "$jar" "$context/provider.jar"
    log "Building $tag with $(basename "$jar")"
    docker build -q -t "$tag" -f - "$context" <<EOF >/dev/null
FROM $base
COPY --chown=root:root --chmod=444 provider.jar /benchmark/drivers/driver-jms/ext/kafka-jms-provider.jar
EOF
    rm -rf "$context"
}

# Sets STACK_REPOSITORY and STACK_TAG to the image a stack runs, building and loading it when needed
STACK_REPOSITORY=""
STACK_TAG=""
PREPARED_STACK=""
prepare_image() {
    local stack=$1 plugin
    [[ "$PREPARED_STACK" != "$stack" ]] || return 0
    plugin=$(stack_plugin "$stack")
    [[ -n "$plugin" ]] || die "stack $stack has no cases in cases.txt"
    if [[ "$PUBLISHED" == "1" ]]; then
        STACK_REPOSITORY=$PUBLISHED_REPOSITORY
        STACK_TAG="latest-$plugin"
        if [[ "$stack" == "jms-kafka" ]]; then
            docker pull -q "$STACK_REPOSITORY:$STACK_TAG"
            kafka_jms_image "$STACK_REPOSITORY:$STACK_TAG" "$IMAGE-jms-kafka"
            STACK_REPOSITORY=${IMAGE%:*}
            STACK_TAG="${IMAGE##*:}-jms-kafka"
            load_image "$STACK_REPOSITORY:$STACK_TAG"
        fi
    else
        STACK_REPOSITORY=${IMAGE%:*}
        STACK_TAG="${IMAGE##*:}-$plugin"
        if [[ "$REBUILD" == "1" ]] || ! docker image inspect "$STACK_REPOSITORY:$STACK_TAG" >/dev/null 2>&1; then
            build_image "$plugin" "$STACK_REPOSITORY:$STACK_TAG"
        fi
        if [[ "$stack" == "jms-kafka" ]]; then
            kafka_jms_image "$STACK_REPOSITORY:$STACK_TAG" "$STACK_REPOSITORY:$STACK_TAG-kafka"
            STACK_TAG="$STACK_TAG-kafka"
        fi
        load_image "$STACK_REPOSITORY:$STACK_TAG"
    fi
    PREPARED_STACK=$stack
}

# Pods of a namespace that the scheduler cannot place, usually for lack of memory
unschedulable_pods() {
    kubectl get pods -n "$1" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.conditions[?(@.type=="PodScheduled")].reason}{"\n"}{end}' 2>/dev/null |
        awk '$2 == "Unschedulable" { print $1 }'
}

# Fails the current wait when pods stay unschedulable longer than UNSCHEDULABLE_GRACE seconds.
# Uses UNSCHEDULABLE_SINCE, which callers reset to 0 before their wait loop.
UNSCHEDULABLE_SINCE=0
check_schedulable() {
    local ns=$1 pods
    pods=$(unschedulable_pods "$ns")
    if [[ -z "$pods" ]]; then
        UNSCHEDULABLE_SINCE=0
        return 0
    fi
    ((UNSCHEDULABLE_SINCE > 0)) || UNSCHEDULABLE_SINCE=$SECONDS
    ((SECONDS - UNSCHEDULABLE_SINCE < UNSCHEDULABLE_GRACE)) && return 0
    echo "error: pods cannot be scheduled: $(echo $pods)" >&2
    kubectl get events -n "$ns" --field-selector reason=FailedScheduling 2>/dev/null | tail -n 3 >&2 || true
    other=$(kubectl get namespaces -o name 2>/dev/null | sed 's|namespace/||' | grep '^omb-' | grep -vx "$ns" || true)
    if [[ -n "$other" ]]; then
        echo "Other stacks still use cluster memory: $(echo $other). Remove them with: mise run k8s:down <stack>" >&2
    else
        echo "Give Docker more memory (about 8 GB for the larger stacks)." >&2
    fi
    return 1
}

# kind nodes each report the whole Docker VM memory, so the scheduler cannot see other containers.
# When Docker runs out of memory its OOM killer stops brokers and benchmark pods mid-run.
warn_other_containers() {
    local others
    others=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -v "^$CLUSTER-" || true)
    if [[ -n "$others" ]]; then
        echo "warning: other Docker containers share the Docker VM memory with the kind cluster: $(echo $others)" >&2
        echo "warning: stop them if pods get OOMKilled (docker stats shows their usage)" >&2
    fi
}

# Waits until at least one pod matches and all matching pods are Ready
wait_pods_ready() {
    local ns=$1 selector=$2 deadline=$((SECONDS + STARTUP_TIMEOUT)) states
    UNSCHEDULABLE_SINCE=0
    while ((SECONDS < deadline)); do
        states=$(kubectl get pods -n "$ns" -l "$selector" \
            -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null || true)
        if [[ -n "$states" ]] && ! grep -qv '^True$' <<< "$states"; then
            return 0
        fi
        check_schedulable "$ns" || return 1
        sleep 3
    done
    echo "error: pods ($selector) not ready after ${STARTUP_TIMEOUT}s" >&2
    return 1
}

dump_namespace() {
    local ns=$1
    kubectl get pods,jobs -n "$ns" -o wide >&2 || true
    kubectl get events -n "$ns" --sort-by=.lastTimestamp 2>/dev/null | tail -n 20 >&2 || true
}

cmd_broker() {
    local stack=${1:-} row arch dir ns overlay
    require_stack "$stack"
    require_cluster
    row=$(stack_row "$stack")
    arch=$(cut -d'|' -f3 <<< "$row")
    if [[ "$arch" == "emulated-amd64" ]] && ! kubectl get nodes -l kubernetes.io/arch=amd64 -o name | grep . >/dev/null; then
        die "stack $stack needs linux/amd64 broker images, but this cluster has no amd64 node (kind nodes use the host architecture)"
    fi
    dir=$(broker_dir "$stack")
    [[ -d "$LOCAL_DIR/brokers/$dir" ]] || die "no broker manifests in deployment/kubernetes/local/brokers/$dir"
    ns=$(namespace "$stack")
    prepare_image "$stack"

    # The overlay points the init jobs that run benchmark tools (image omb-image) at the stack image
    overlay="$OUT_DIR/overlays/$stack"
    mkdir -p "$overlay"
    cat > "$overlay/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - $(python3 -c 'import os,sys; print(os.path.relpath(sys.argv[1], sys.argv[2]))' "$LOCAL_DIR/brokers/$dir" "$overlay")
images:
  - name: omb-image
    newName: $STACK_REPOSITORY
    newTag: $STACK_TAG
EOF
    log "Deploying broker $dir into namespace $ns"
    kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    kubectl apply -n "$ns" -k "$overlay"
    log "Waiting for the broker (up to ${STARTUP_TIMEOUT}s)"
    if ! wait_pods_ready "$ns" app.kubernetes.io/part-of=omb-broker; then
        dump_namespace "$ns"
        die "broker of $stack did not become ready"
    fi
    if [[ -n "$(kubectl get jobs -n "$ns" -l app.kubernetes.io/part-of=omb-broker-init -o name)" ]]; then
        if ! kubectl wait -n "$ns" --for=condition=complete job -l app.kubernetes.io/part-of=omb-broker-init --timeout="${STARTUP_TIMEOUT}s"; then
            dump_namespace "$ns"
            die "broker initialisation of $stack did not complete"
        fi
    fi
    log "Broker of $stack is ready"
}

# helm upgrade --install of the chart for a stack and case in the given coordinator mode
install_chart() {
    local stack=$1 case_id=$2 mode=$3 row driver workload ns values=()
    row=$(case_row "$case_id")
    [[ -n "$row" ]] || die "unknown case '$case_id'"
    [[ "$(cut -d'|' -f3 <<< "$row")" == "$stack" ]] || die "case $case_id does not belong to stack $stack"
    driver=$(cut -d'|' -f5 <<< "$row")
    workload=$(cut -d'|' -f6 <<< "$row")
    ns=$(namespace "$stack")
    prepare_image "$stack"
    if [[ -f "$LOCAL_DIR/values/$stack.yaml" ]]; then
        values+=(-f "$LOCAL_DIR/values/$stack.yaml")
    fi
    # ${values[@]+...}: bash 3.2 (macOS) treats an empty array as unset under set -u
    helm upgrade --install "$RELEASE" "$CHART_DIR" -n "$ns" ${values[@]+"${values[@]}"} \
        --set fullnameOverride="$RELEASE" \
        --set image.repository="$STACK_REPOSITORY" \
        --set image.tag="$STACK_TAG" \
        --set image.variant= \
        --set image.pullPolicy=IfNotPresent \
        --set coordinator.mode="$mode" \
        --set workers.auth.enabled=true \
        --set coordinator.job.activeDeadlineSeconds="$CASE_TIMEOUT" \
        --set workers.replicas=2 \
        --set workers.heapOpts="-Xms512m -Xmx512m" \
        --set workers.supervisorHeapOpts="-Xmx256m" \
        --set workers.resources.requests.cpu=250m \
        --set workers.resources.requests.memory=1536Mi \
        --set workers.resources.limits.memory=1536Mi \
        --set-file driverConfig="$SUITE_DIR/drivers/$driver" \
        --set-file workloadConfig="$SUITE_DIR/workloads/$workload" >/dev/null
    # Not helm --wait: it would block silently on a worker that cannot be scheduled
    if ! wait_pods_ready "$ns" "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=worker"; then
        dump_namespace "$ns"
        die "workers of $stack did not become ready"
    fi
}

cmd_deploy() {
    local stack=${1:-} contract
    require_stack "$stack"
    cmd_broker "$stack"
    contract=$(table "$SUITE_DIR/cases.txt" | awk -F'|' -v s="$1" '$3 == s && $4 == "contract" && !n++ { print $1 }')
    contract=${contract:-$(stack_cases "$stack" | awk 'NR == 1')}
    log "Installing the chart with an interactive coordinator ($contract configuration)"
    install_chart "$stack" "$contract" interactive
    wait_pods_ready "$(namespace "$stack")" "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=coordinator" ||
        die "coordinator of $stack did not become ready"
    helm get notes "$RELEASE" -n "$(namespace "$stack")"
}

# Runs one case as a Job and stores its logs and result in e2e-tests/target/k8s/<case>
run_case() {
    local stack=$1 case_id=$2 ns job status deadline out
    ns=$(namespace "$stack")
    job="$RELEASE-coordinator"
    out="$OUT_DIR/$case_id"
    rm -rf "$out"
    mkdir -p "$out"
    log "Case $case_id"
    # A Job's pod template is immutable: remove the previous run, keep the workers
    kubectl delete job "$job" -n "$ns" --ignore-not-found --wait=true >/dev/null
    install_chart "$stack" "$case_id" job

    deadline=$((SECONDS + CASE_TIMEOUT + 120))
    status=""
    UNSCHEDULABLE_SINCE=0
    while ((SECONDS < deadline)); do
        status=$(kubectl get job "$job" -n "$ns" -o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null || true)
        [[ "$status" == 1/* || "$status" == */1 ]] && break
        if ! check_schedulable "$ns"; then
            status="unschedulable"
            break
        fi
        sleep 5
    done

    kubectl logs -n "$ns" "job/$job" -c coordinator > "$out/coordinator.log" 2>&1 || true
    for pod in $(kubectl get pods -n "$ns" -l app.kubernetes.io/component=worker -o name); do
        kubectl logs -n "$ns" "$pod" > "$out/${pod#pod/}.log" 2>&1 || true
    done
    awk '/^----- OMB RESULT END -----$/ { found = 0 } found { print } /^----- OMB RESULT BEGIN -----$/ { found = 1 }' \
        "$out/coordinator.log" > "$out/result.json"

    if [[ "$status" == 1/* ]] && python3 -c 'import json, sys; json.load(open(sys.argv[1]))' "$out/result.json" 2>/dev/null; then
        log "PASSED $case_id (result: ${out#"$ROOT"/}/result.json)"
        return 0
    fi
    [[ -n "$status" && "$status" != "/" ]] || status="timed out"
    tail -n 30 "$out/coordinator.log" >&2 || true
    dump_namespace "$ns"
    log "FAILED $case_id ($status; logs: ${out#"$ROOT"/})"
    return 1
}

cmd_run() {
    local stack=${1:-} cases=() failed=()
    require_stack "$stack"
    shift
    if [[ $# -gt 0 ]]; then
        cases=("$@")
    else
        while read -r case_id; do cases+=("$case_id"); done < <(stack_cases "$stack")
    fi
    [[ ${#cases[@]} -gt 0 ]] || die "stack $stack has no cases"
    warn_other_containers
    cmd_broker "$stack"
    for case_id in "${cases[@]}"; do
        run_case "$stack" "$case_id" || failed+=("$case_id")
    done
    # Free the memory for the next stack; logs and results are already in e2e-tests/target/k8s
    if [[ "$KEEP" != "1" ]]; then
        log "Deleting namespace $(namespace "$stack") (OMB_K8S_KEEP=1 keeps it)"
        kubectl delete namespace "$(namespace "$stack")" --wait=true >/dev/null
    fi
    echo
    echo "Passed: $((${#cases[@]} - ${#failed[@]})) of ${#cases[@]} case(s) on stack $stack"
    if [[ ${#failed[@]} -gt 0 ]]; then
        echo "Failed: ${failed[*]}"
        exit 1
    fi
}

cmd_shell() {
    local stack=${1:-}
    require_stack "$stack"
    require_cluster
    kubectl exec -n "$(namespace "$stack")" -it "deploy/$RELEASE-coordinator" -c coordinator -- bash
}

cmd_logs() {
    local stack=${1:-} ns
    require_stack "$stack"
    require_cluster
    ns=$(namespace "$stack")
    kubectl get pods,jobs -n "$ns" -o wide
    for pod in $(kubectl get pods -n "$ns" -o name); do
        echo
        echo "----- $pod"
        kubectl logs -n "$ns" "$pod" --all-containers --tail=20 2>&1 || true
    done
}

cmd_down() {
    if [[ $# -eq 0 ]]; then
        log "Deleting kind cluster $CLUSTER"
        kind delete cluster --name "$CLUSTER" --kubeconfig "${KUBECONFIG:-$HOME/.kube/config}"
        return
    fi
    require_stack "$1"
    require_cluster
    log "Deleting namespace $(namespace "$1")"
    kubectl delete namespace "$(namespace "$1")" --ignore-not-found --wait=true
}

cmd_list() {
    echo "Stacks (mise run k8s:run <stack>):"
    table "$SUITE_DIR/stacks.txt" | awk -F'|' '{ printf "  %-15s %-15s %s\n", $1, $3, $5 }'
    echo
    echo "Cases (mise run k8s:run <stack> <case>):"
    table "$SUITE_DIR/cases.txt" | awk -F'|' '{ printf "  %-28s %-15s %s\n", $1, $3, $7 }'
}

cmd_lint() {
    local rendered k8s_version=1.31.0 dir
    rendered="$OUT_DIR/lint"
    rm -rf "$rendered"
    mkdir -p "$rendered"
    log "helm lint"
    helm lint --strict "$CHART_DIR"
    helm lint --strict "$CHART_DIR" --set coordinator.mode=job \
        --set-file driverConfig="$SUITE_DIR/drivers/kafka.yaml" \
        --set-file workloadConfig="$SUITE_DIR/workloads/contract.yaml"
    helm lint --strict "$CHART_DIR" --set workers.auth.enabled=true --set workers.auth.protectMetrics=true
    helm lint --strict "$CHART_DIR" --set workers.auth.enabled=true --set workers.auth.existingSecret=omb-token
    for values in "$LOCAL_DIR"/values/*.yaml; do
        helm lint --strict "$CHART_DIR" -f "$values"
    done

    log "Rendering the chart"
    helm template "$RELEASE" "$CHART_DIR" > "$rendered/default.yaml"
    helm template "$RELEASE" "$CHART_DIR" --set coordinator.mode=job --set networkPolicy.enabled=false \
        --set image.digest=sha256:0000000000000000000000000000000000000000000000000000000000000000 \
        --set-file driverConfig="$SUITE_DIR/drivers/kafka.yaml" \
        --set-file workloadConfig="$SUITE_DIR/workloads/contract.yaml" > "$rendered/job.yaml"
    helm template "$RELEASE" "$CHART_DIR" --set workers.auth.enabled=true \
        --set workers.auth.protectMetrics=true > "$rendered/auth.yaml"
    helm template "$RELEASE" "$CHART_DIR" --set workers.auth.enabled=true \
        --set workers.auth.existingSecret=omb-token --set workers.auth.secretKey=key > "$rendered/auth-existing.yaml"
    grep -q 'name: OMB_WORKER_TOKEN' "$rendered/auth.yaml" || die "auth.yaml has no OMB_WORKER_TOKEN"
    grep -q -- '--protect-metrics' "$rendered/auth.yaml" || die "auth.yaml has no --protect-metrics"
    grep -q 'kind: Secret' "$rendered/auth-existing.yaml" && die "existingSecret must not create a Secret"
    grep -q 'OMB_WORKER_TOKEN' "$rendered/default.yaml" && die "auth must be off by default"
    for values in "$LOCAL_DIR"/values/*.yaml; do
        helm template "$RELEASE" "$CHART_DIR" -f "$values" > "$rendered/values-$(basename "$values")"
    done
    if [[ -d "$LOCAL_DIR/brokers" ]]; then
        for dir in "$LOCAL_DIR"/brokers/*/; do
            kubectl kustomize "$dir" > "$rendered/broker-$(basename "$dir").yaml"
        done
    fi

    log "kubeconform (Kubernetes $k8s_version)"
    kubeconform -strict -summary -kubernetes-version "$k8s_version" "$rendered"/*.yaml
}

command=${1:-}
[[ $# -gt 0 ]] && shift
case "$command" in
    list) cmd_list ;;
    image) cmd_image "$@" ;;
    broker) cmd_broker "$@" ;;
    deploy) cmd_deploy "$@" ;;
    run) cmd_run "$@" ;;
    shell) cmd_shell "$@" ;;
    logs) cmd_logs "$@" ;;
    down) cmd_down "$@" ;;
    lint) cmd_lint ;;
    -h|--help|help|"") usage ;;
    *) usage >&2; exit 2 ;;
esac
