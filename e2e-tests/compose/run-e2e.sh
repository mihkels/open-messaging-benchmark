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

# Docker Compose regression suite for every benchmark driver plugin.
#
# For each messaging service stack (stacks.txt) this script starts the brokers and two benchmark
# workers in Docker Compose, runs every selected case (cases.txt) with the benchmark coordinator in
# a third container, collects results and logs, and tears the stack down before moving on.
# Failures never stop the run. A report is always generated and the exit code is non-zero when
# any selected case failed, was blocked or did not run. Compatible with bash 3.2 (macOS).

set -u

SUITE_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_DIR=$(cd "$SUITE_DIR/../.." && pwd)
TARGET_DIR="$REPO_DIR/e2e-tests/target"
CACHE_DIR="$TARGET_DIR/e2e-cache"

STACK_FILTER=""
CASE_FILTER=""
BUILD_MODE="auto"
CASE_TIMEOUT=${OMB_E2E_CASE_TIMEOUT:-420}
STARTUP_TIMEOUT=${OMB_E2E_STARTUP_TIMEOUT:-600}
IMAGE=${OMB_E2E_IMAGE:-omb-e2e-runner:local}
PYTHON_IMAGE=${OMB_E2E_PYTHON_IMAGE:-python:3.13-slim}
MAVEN_IMAGE=${OMB_E2E_MAVEN_IMAGE:-maven:3.9.11-eclipse-temurin-25}
KAFKA_JMS_JAR=${OMB_E2E_KAFKA_JMS_JAR:-}
LIST_ONLY=0

usage() {
    cat <<'EOF'
Usage: e2e-tests/compose/run-e2e.sh [options]

Runs the Docker Compose regression suite for all driver plugins and writes a report to
e2e-tests/target/e2e-reports/<run-id>/ (index.html, summary.md, junit.xml, results.json).

Options:
  --stacks a,b        Only run these stacks (see stacks.txt)
  --cases a,b         Only run these cases (see cases.txt); combines with --stacks
  --build MODE        auto (default): build the distribution in Docker only when it is missing
                      docker: always rebuild with Maven in Docker
                      host:   always rebuild with the host Maven and JDK 25
                      skip:   never build; fail when the distribution is missing
  --kafka-jms-jar F   Confluent Kafka JMS client fat jar (default: jms-providers/kafka/*.jar)
  --case-timeout S    Timeout for one case in seconds (default 420)
  --list              Print the stacks and cases and exit
  -h, --help          Show this help

Environment: OMB_E2E_CASE_TIMEOUT, OMB_E2E_STARTUP_TIMEOUT, OMB_E2E_KAFKA_JMS_JAR, OMB_E2E_IMAGE,
OMB_E2E_PYTHON_IMAGE, OMB_E2E_MAVEN_IMAGE, OMB_E2E_M2 (Maven repository mounted for --build docker).
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --stacks|--cases|--build|--kafka-jms-jar|--case-timeout)
            if [ $# -lt 2 ] || [ -z "$2" ] || [[ "$2" == --* ]]; then
                echo "Missing value for $1" >&2
                exit 2
            fi
            option=$1 value=$2
            shift 2
            ;;
        --stacks=*|--cases=*|--build=*|--kafka-jms-jar=*|--case-timeout=*)
            option=${1%%=*} value=${1#*=}
            if [ -z "$value" ]; then
                echo "Missing value for $option" >&2
                exit 2
            fi
            shift
            ;;
        --list) LIST_ONLY=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
    case "${option:-}" in
        --stacks) STACK_FILTER=$value ;;
        --cases) CASE_FILTER=$value ;;
        --build) BUILD_MODE=$value ;;
        --kafka-jms-jar) KAFKA_JMS_JAR=$value ;;
        --case-timeout) CASE_TIMEOUT=$value ;;
    esac
    option=""
done

case "$BUILD_MODE" in
    auto|docker|host|skip) ;;
    *) echo "Invalid --build mode: $BUILD_MODE" >&2; exit 2 ;;
esac

positive_integer() {
    case "$1" in ""|*[!0-9]*) return 1 ;; esac
    [ "$1" -gt 0 ] 2>/dev/null
}
positive_integer "$CASE_TIMEOUT" || { echo "Invalid case timeout: $CASE_TIMEOUT" >&2; exit 2; }
positive_integer "$STARTUP_TIMEOUT" || { echo "Invalid startup timeout: $STARTUP_TIMEOUT" >&2; exit 2; }

# --- matrix helpers -------------------------------------------------------------------------------

# Prints the non-comment lines of a matrix file with fields trimmed and separated by '|'.
matrix_rows() {
    sed -e 's/#.*//' "$1" | awk -F'|' 'NF > 1 {
        out = ""
        for (i = 1; i <= NF; i++) { gsub(/^[ \t]+|[ \t]+$/, "", $i); out = out (i > 1 ? "|" : "") $i }
        print out
    }'
}

field() { echo "$1" | cut -d'|' -f"$2"; }

in_list() {
    # in_list <comma separated list> <value>; an empty list matches everything
    [ -z "$1" ] && return 0
    case ",$1," in *",$2,"*) return 0 ;; esac
    return 1
}

selected_cases_for_stack() {
    matrix_rows "$SUITE_DIR/cases.txt" | while IFS= read -r row; do
        local id stack
        id=$(field "$row" 1)
        stack=$(field "$row" 3)
        [ "$stack" = "$1" ] || continue
        in_list "$CASE_FILTER" "$id" || continue
        echo "$row"
    done
}

validate_filter() (
    local values=$1 matrix=$2 kind=$3 item
    [ -z "$values" ] && return 0
    case "$values" in
        ,*|*,|*,,*) echo "Invalid $kind filter: $values" >&2; return 1 ;;
    esac
    set -f
    local IFS=,
    for item in $values; do
        if ! matrix_rows "$matrix" | awk -F'|' -v id="$item" '$1 == id { found = 1 } END { exit !found }'; then
            echo "Unknown $kind: $item" >&2
            return 1
        fi
    done
)

validate_case_stacks() (
    [ -z "$CASE_FILTER" ] && return 0
    set -f
    local IFS=, id row stack
    for id in $CASE_FILTER; do
        row=$(matrix_rows "$SUITE_DIR/cases.txt" | awk -F'|' -v id="$id" '$1 == id { print; exit }')
        stack=$(field "$row" 3)
        if ! in_list "$STACK_FILTER" "$stack"; then
            echo "Case $id belongs to stack $stack, which is not selected" >&2
            return 1
        fi
        if ! matrix_rows "$SUITE_DIR/stacks.txt" | awk -F'|' -v id="$stack" '$1 == id { found = 1 } END { exit !found }'; then
            echo "Case $id references unknown stack $stack" >&2
            return 1
        fi
    done
)

validate_filter "$STACK_FILTER" "$SUITE_DIR/stacks.txt" stack || exit 2
validate_filter "$CASE_FILTER" "$SUITE_DIR/cases.txt" case || exit 2
validate_case_stacks || exit 2

if [ "$LIST_ONLY" = 1 ]; then
    matrix_rows "$SUITE_DIR/stacks.txt" | while IFS= read -r srow; do
        stack=$(field "$srow" 1)
        in_list "$STACK_FILTER" "$stack" || continue
        echo "$stack ($(field "$srow" 3)): $(field "$srow" 5)"
        selected_cases_for_stack "$stack" | while IFS= read -r crow; do
            echo "    $(field "$crow" 1) [$(field "$crow" 4)] $(field "$crow" 7)"
        done
    done
    exit 0
fi

# --- run directory and metadata -------------------------------------------------------------------

RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
REPORT_DIR="$TARGET_DIR/e2e-reports/$RUN_ID"
mkdir -p "$REPORT_DIR/cases" "$REPORT_DIR/stacks" "$CACHE_DIR"
ln -sfn "$RUN_ID" "$TARGET_DIR/e2e-reports/latest"
RUN_LOG="$REPORT_DIR/run.log"
RUN_PROPS="$REPORT_DIR/run.properties"

log() {
    local line
    line="$(date -u +%H:%M:%S) $*"
    echo "$line"
    echo "$line" >> "$RUN_LOG"
}

# prop <file> <key> <value>: appends a single-line key=value entry.
prop() {
    printf '%s=%s\n' "$2" "$(printf '%s' "$3" | tr '\n\r' '  ')" >> "$1"
}

ORIGINAL_ARGS="$*"
prop "$RUN_PROPS" run_id "$RUN_ID"
prop "$RUN_PROPS" started_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
prop "$RUN_PROPS" stack_filter "$STACK_FILTER"
prop "$RUN_PROPS" case_filter "$CASE_FILTER"
prop "$RUN_PROPS" git_revision "$(git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
prop "$RUN_PROPS" git_branch "$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
prop "$RUN_PROPS" git_dirty_files "$(git -C "$REPO_DIR" status --porcelain 2>/dev/null | wc -l | tr -d ' ')"
prop "$RUN_PROPS" host_os "$(uname -s)"
prop "$RUN_PROPS" host_arch "$(uname -m)"
prop "$RUN_PROPS" case_timeout_seconds "$CASE_TIMEOUT"

CURRENT_PROJECT=""
CURRENT_CHILD=""
INTERRUPTED=0
COMPOSE_FILES=()

compose() {
    if [ "${#COMPOSE_FILES[@]}" -eq 0 ]; then
        echo "No Compose stack files are configured for $CURRENT_PROJECT" >&2
        return 1
    fi
    docker compose -p "$CURRENT_PROJECT" --project-directory "$SUITE_DIR" \
        -f "$SUITE_DIR/common.yaml" "${COMPOSE_FILES[@]}" "$@"
}

teardown_current() {
    [ -n "$CURRENT_PROJECT" ] || return 0
    local dir="$1"
    log "Tearing down $CURRENT_PROJECT"
    compose --profile runner logs --no-color --timestamps > "$dir/compose.log" 2>&1
    compose --profile runner ps -a > "$dir/compose-ps.txt" 2>&1
    if compose --profile runner down -v --remove-orphans --timeout 20 > "$dir/teardown.log" 2>&1; then
        prop "$dir/stack.properties" cleanup ok
    else
        prop "$dir/stack.properties" cleanup failed
        log "WARNING: teardown of $CURRENT_PROJECT failed, see $dir/teardown.log"
    fi
    CURRENT_PROJECT=""
}

CURRENT_STACK_DIR=""
on_interrupt() {
    INTERRUPTED=1
    log "Interrupted, cleaning up"
    prop "$RUN_PROPS" interrupted true
    if [ -n "$CURRENT_CHILD" ]; then
        kill -TERM "$CURRENT_CHILD" 2>/dev/null
    fi
}
trap on_interrupt INT TERM

# run_with_timeout <seconds> <logfile> <command...>: returns 124 on timeout, 130 on interruption.
run_with_timeout() {
    local secs=$1 logfile=$2 waited=0 pid rc
    shift 2
    "$@" >> "$logfile" 2>&1 &
    pid=$!
    CURRENT_CHILD=$pid
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$INTERRUPTED" = 1 ]; then
            kill -TERM "$pid" 2>/dev/null
            wait "$pid" 2>/dev/null
            CURRENT_CHILD=""
            return 130
        fi
        if [ "$waited" -ge "$secs" ]; then
            echo "*** run-e2e.sh: timed out after ${secs}s ***" >> "$logfile"
            kill -TERM "$pid" 2>/dev/null
            sleep 5
            kill -KILL "$pid" 2>/dev/null
            wait "$pid" 2>/dev/null
            CURRENT_CHILD=""
            return 124
        fi
        sleep 1
        waited=$((waited + 1))
    done
    wait "$pid"
    rc=$?
    CURRENT_CHILD=""
    return $rc
}

reports_present() {
    local file
    for file in index.html summary.md junit.xml results.json; do
        [ -s "$REPORT_DIR/$file" ] || return 1
    done
}

generate_report() {
    log "Generating report"
    prop "$RUN_PROPS" finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    local rc
    if docker run --rm -v "$REPORT_DIR":/report -v "$SUITE_DIR":/suite:ro \
        "$PYTHON_IMAGE" python3 /suite/report/report.py --suite /suite --report /report \
        >> "$RUN_LOG" 2>&1; then
        rc=0
    else
        rc=$?
    fi
    if { [ "$rc" -ne 0 ] && [ "$rc" -ne 1 ]; } || ! reports_present; then
        if command -v python3 > /dev/null 2>&1; then
            log "Report container failed or produced incomplete output (exit $rc), falling back to host python3"
            python3 "$SUITE_DIR/report/report.py" --suite "$SUITE_DIR" --report "$REPORT_DIR" >> "$RUN_LOG" 2>&1
            rc=$?
        else
            log "ERROR: Report container failed or produced incomplete output, and host python3 is unavailable"
            return 2
        fi
    fi
    if ! reports_present; then
        log "ERROR: Report generation did not produce all four report files; see $RUN_LOG"
        return 2
    fi
    if [ -f "$REPORT_DIR/summary.md" ]; then
        sed -n '1,/^## Cases/p' "$REPORT_DIR/summary.md" | sed '$d'
    fi
    log "Report: $REPORT_DIR/index.html"
    return $rc
}

finish() {
    local rc
    generate_report
    rc=$?
    if [ "$INTERRUPTED" = 1 ]; then
        exit 130
    fi
    # Exit code 1 from the report means failed cases; anything else is an infrastructure problem.
    exit $rc
}

# Marks every case of a stack with a status that prevents it from running.
mark_stack_cases() {
    local stack=$1 status=$2 phase=$3 reason=$4
    selected_cases_for_stack "$stack" | while IFS= read -r row; do
        local cdir="$REPORT_DIR/cases/$(field "$row" 1)"
        mkdir -p "$cdir"
        prop "$cdir/case.properties" status "$status"
        prop "$cdir/case.properties" phase "$phase"
        prop "$cdir/case.properties" reason "$reason"
    done
}

register_cases() {
    matrix_rows "$SUITE_DIR/stacks.txt" | while IFS= read -r srow; do
        local stack
        stack=$(field "$srow" 1)
        in_list "$STACK_FILTER" "$stack" || continue
        selected_cases_for_stack "$stack" | while IFS= read -r row; do
            local cdir="$REPORT_DIR/cases/$(field "$row" 1)"
            mkdir -p "$cdir"
            prop "$cdir/case.properties" case "$(field "$row" 1)"
            prop "$cdir/case.properties" plugin "$(field "$row" 2)"
            prop "$cdir/case.properties" stack "$stack"
            prop "$cdir/case.properties" kind "$(field "$row" 4)"
            prop "$cdir/case.properties" driver_config "$(field "$row" 5)"
            prop "$cdir/case.properties" workload "$(field "$row" 6)"
            prop "$cdir/case.properties" description "$(field "$row" 7)"
            prop "$cdir/case.properties" architecture "$(field "$srow" 3)"
        done
    done
}

mark_all_cases() {
    matrix_rows "$SUITE_DIR/stacks.txt" | while IFS= read -r srow; do
        local stack
        stack=$(field "$srow" 1)
        in_list "$STACK_FILTER" "$stack" || continue
        mark_stack_cases "$stack" "$1" "$2" "$3"
    done
}

# --- preflight ------------------------------------------------------------------------------------

register_cases
if [ -z "$(ls "$REPORT_DIR/cases")" ]; then
    echo "No cases match --stacks '$STACK_FILTER' --cases '$CASE_FILTER'" >&2
    exit 2
fi

log "Run $RUN_ID, report directory $REPORT_DIR"

if ! docker info > /dev/null 2>&1; then
    log "ERROR: Docker is not available"
    mark_all_cases error preflight "Docker daemon is not reachable"
    prop "$RUN_PROPS" preflight_error "Docker daemon is not reachable"
    if command -v python3 > /dev/null 2>&1; then
        prop "$RUN_PROPS" finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        python3 "$SUITE_DIR/report/report.py" --suite "$SUITE_DIR" --report "$REPORT_DIR"
    fi
    exit 1
fi
prop "$RUN_PROPS" docker_version "$(docker version --format '{{.Server.Version}} ({{.Server.Os}}/{{.Server.Arch}})' 2>/dev/null)"
prop "$RUN_PROPS" compose_version "$(docker compose version --short 2>/dev/null)"

preflight_fail() {
    log "ERROR: $1"
    prop "$RUN_PROPS" preflight_error "$1"
    mark_all_cases error preflight "$1"
    finish
}

# --- distribution and runner image ----------------------------------------------------------------

find_tarball() {
    ls "$REPO_DIR"/package/target/openmessaging-benchmark-*-bin.tar.gz 2>/dev/null | head -n 1
}

build_distribution() {
    local mvn_args="-B -q install -DskipTests -Dspotbugs.skip -Dcheckstyle.skip -Dlicense.skip -Dspotless.check.skip -Djacoco.skip"
    case "$1" in
        host)
            log "Building the distribution with the host Maven"
            (cd "$REPO_DIR" && mvn $mvn_args) >> "$REPORT_DIR/build.log" 2>&1
            ;;
        docker)
            local m2=${OMB_E2E_M2:-$HOME/.m2}
            mkdir -p "$m2"
            log "Building the distribution with Maven in Docker ($MAVEN_IMAGE)"
            docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -e MAVEN_CONFIG=/tmp/.m2 \
                -v "$REPO_DIR":/src -v "$m2":/tmp/.m2 -w /src "$MAVEN_IMAGE" \
                mvn -Dmaven.repo.local=/tmp/.m2/repository $mvn_args >> "$REPORT_DIR/build.log" 2>&1
            ;;
    esac
}

TARBALL=$(find_tarball)
if [ "$BUILD_MODE" = docker ] || [ "$BUILD_MODE" = host ] || { [ "$BUILD_MODE" = auto ] && [ -z "$TARBALL" ]; }; then
    mode=$BUILD_MODE
    [ "$mode" = auto ] && mode=docker
    build_distribution "$mode" || preflight_fail "Building the distribution failed, see build.log"
    TARBALL=$(find_tarball)
fi
[ -n "$TARBALL" ] || preflight_fail "Distribution tarball package/target/openmessaging-benchmark-*-bin.tar.gz is missing"
prop "$RUN_PROPS" distribution "$(basename "$TARBALL")"
prop "$RUN_PROPS" distribution_built_at "$(date -u -r "$TARBALL" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || stat -c %y "$TARBALL" 2>/dev/null)"

log "Building runner image $IMAGE from $(basename "$TARBALL")"
IMAGE_CONTEXT="$TARGET_DIR/e2e-image"
rm -rf "$IMAGE_CONTEXT"
mkdir -p "$IMAGE_CONTEXT"
cp "$SUITE_DIR/runner/Dockerfile" "$IMAGE_CONTEXT/Dockerfile"
cp "$TARBALL" "$IMAGE_CONTEXT/openmessaging-benchmark-bin.tar.gz"
docker build -q -t "$IMAGE" "$IMAGE_CONTEXT" > "$REPORT_DIR/image-build.log" 2>&1 \
    || preflight_fail "Building the runner image failed, see image-build.log"
rm -rf "$IMAGE_CONTEXT"
prop "$RUN_PROPS" runner_image "$IMAGE $(docker image inspect --format '{{.Id}}' "$IMAGE" 2>/dev/null)"
prop "$RUN_PROPS" runner_java "$(docker run --rm "$IMAGE" java -version 2>&1 | head -n 1)"
export OMB_E2E_IMAGE="$IMAGE"

# Lists the plugin bundles shipped in the image so the report can verify coverage.
docker run --rm "$IMAGE" sh -c 'for f in drivers/*/plugin.yaml; do printf "%s|" "$(dirname "$f")"; sed -n "s/^id: *//p" "$f"; done' \
    > "$REPORT_DIR/shipped-plugins.txt" 2>> "$RUN_LOG"

# --- third-party artifacts ------------------------------------------------------------------------

sha256_of() {
    if command -v sha256sum > /dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# fetch_artifact <name>: downloads a pinned artifact once and prints its directory.
fetch_artifact() {
    local row url sha dir file
    row=$(matrix_rows "$SUITE_DIR/artifacts.txt" | awk -F'|' -v n="$1" '$1 == n')
    [ -n "$row" ] || return 1
    url=$(field "$row" 2)
    sha=$(field "$row" 3)
    dir="$CACHE_DIR/$1"
    file="$dir/$(basename "$url")"
    mkdir -p "$dir"
    if [ ! -f "$file" ] || [ "$(sha256_of "$file")" != "$sha" ]; then
        log "Downloading $url" >&2
        curl -fsSL --retry 3 -o "$file.tmp" "$url" >> "$RUN_LOG" 2>&1 || { rm -f "$file.tmp"; return 1; }
        if [ "$(sha256_of "$file.tmp")" != "$sha" ]; then
            log "Checksum mismatch for $url"
            rm -f "$file.tmp"
            return 1
        fi
        mv "$file.tmp" "$file"
    fi
    # The directory is mounted as a whole, so drop artifacts left by older pinned versions.
    find "$dir" -mindepth 1 -maxdepth 1 ! -name "$(basename "$file")" -exec rm -rf {} +
    echo "$dir"
}

# prepare_prerequisite <prerequisite> <stack dir>: exports the variables needed by the stack.
# Prints a failure reason and returns non-zero when the prerequisite is unavailable.
prepare_prerequisite() {
    local dir jar
    EMPTY_EXT="$CACHE_DIR/empty-ext"
    mkdir -p "$EMPTY_EXT"
    export OMB_E2E_JMS_EXT="$EMPTY_EXT"
    export OMB_E2E_KOP_DIR="$EMPTY_EXT"
    case "$1" in
        -) return 0 ;;
        kop-nar)
            dir=$(fetch_artifact kop-nar) || { echo "Could not download the pinned KoP protocol handler NAR"; return 1; }
            export OMB_E2E_KOP_DIR="$dir"
            ;;
        jms:artemis|jms:pulsar)
            dir=$(fetch_artifact "jms-${1#jms:}") || { echo "Could not download the pinned ${1#jms:} JMS provider jar"; return 1; }
            export OMB_E2E_JMS_EXT="$dir"
            ;;
        jms:kafka)
            jar=$KAFKA_JMS_JAR
            if [ -z "$jar" ]; then
                jar=$(ls "$SUITE_DIR"/jms-providers/kafka/*.jar 2>/dev/null | head -n 1)
            fi
            if [ -z "$jar" ] || [ ! -f "$jar" ]; then
                echo "Kafka JMS provider jar not supplied: put the Confluent kafka-jms-client fat jar in e2e-tests/compose/jms-providers/kafka/ or pass --kafka-jms-jar"
                return 1
            fi
            dir="$CACHE_DIR/jms-kafka-user"
            rm -rf "$dir"
            mkdir -p "$dir"
            cp "$jar" "$dir/"
            export OMB_E2E_JMS_EXT="$dir"
            ;;
        *) echo "Unknown prerequisite $1"; return 1 ;;
    esac
    for jar in "$OMB_E2E_JMS_EXT"/*.jar "$OMB_E2E_KOP_DIR"/*.nar; do
        [ -f "$jar" ] && prop "$2/stack.properties" artifact "$(basename "$jar") sha256=$(sha256_of "$jar")"
    done
    return 0
}

# --- stack and case execution ---------------------------------------------------------------------

collect_worker_files() {
    local cdir=$1 w
    for w in worker-1 worker-2; do
        compose exec -T "$w" curl -fsS http://localhost:8080/metrics < /dev/null > "$cdir/$w-metrics.txt" 2>/dev/null
        compose logs --no-color "$w" < /dev/null > "$cdir/$w.log" 2>&1
        compose cp "$w:/benchmark/benchmark-driver.log" "$cdir/$w-driver.log" < /dev/null > /dev/null 2>&1
    done
}

run_case() {
    local row=$1 id plugin kind driver workload cdir start rc
    id=$(field "$row" 1)
    driver=$(field "$row" 5)
    workload=$(field "$row" 6)
    cdir="$REPORT_DIR/cases/$id"
    mkdir -p "$cdir"
    chmod 777 "$cdir"
    export OMB_E2E_CASE_DIR="$cdir"
    cp "$SUITE_DIR/drivers/$driver" "$cdir/driver.yaml"
    cp "$SUITE_DIR/workloads/$workload" "$cdir/workload.yaml"

    log "  case $id ($driver + $workload)"
    start=$(date +%s)
    prop "$cdir/case.properties" started_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    # Fresh workers for every case so one failing case cannot poison the next one.
    run_with_timeout 180 "$cdir/workers-start.log" \
        compose up -d --wait --wait-timeout 150 --force-recreate --no-deps worker-1 worker-2
    rc=$?
    if [ "$rc" != 0 ]; then
        prop "$cdir/case.properties" status error
        prop "$cdir/case.properties" phase worker-startup
        prop "$cdir/case.properties" reason "Benchmark workers did not become healthy (exit $rc)"
        prop "$cdir/case.properties" duration_seconds "$(($(date +%s) - start))"
        collect_worker_files "$cdir"
        return
    fi

    run_with_timeout "$CASE_TIMEOUT" "$cdir/coordinator.log" \
        compose --profile runner run --rm --no-deps -T --name "$CURRENT_PROJECT-runner" runner \
        bin/benchmark --drivers "/e2e/drivers/$driver" \
        --workers http://worker-1:8080,http://worker-2:8080 \
        --output /out/result.json "/e2e/workloads/$workload"
    rc=$?
    if [ "$rc" = 124 ] || [ "$rc" = 130 ]; then
        docker rm -f "$CURRENT_PROJECT-runner" > /dev/null 2>&1
    fi
    prop "$cdir/case.properties" coordinator_exit_code "$rc"
    [ "$rc" = 124 ] && prop "$cdir/case.properties" timed_out true
    [ "$rc" = 130 ] && prop "$cdir/case.properties" status aborted
    prop "$cdir/case.properties" duration_seconds "$(($(date +%s) - start))"
    collect_worker_files "$cdir"
}

run_stack() {
    local srow=$1 stack files arch prereq sdir reason start rc
    stack=$(field "$srow" 1)
    files=$(field "$srow" 2)
    arch=$(field "$srow" 3)
    prereq=$(field "$srow" 4)
    sdir="$REPORT_DIR/stacks/$stack"
    mkdir -p "$sdir"

    log "=== Stack $stack: $(field "$srow" 5) [$arch]"
    prop "$sdir/stack.properties" stack "$stack"
    prop "$sdir/stack.properties" description "$(field "$srow" 5)"
    prop "$sdir/stack.properties" architecture "$arch"
    prop "$sdir/stack.properties" compose_files "$files"
    prop "$sdir/stack.properties" prerequisite "$prereq"

    if ! prepare_prerequisite "$prereq" "$sdir" > "$sdir/prerequisite.txt" 2>&1; then
        reason=$(tail -n 1 "$sdir/prerequisite.txt")
        log "  BLOCKED: $reason"
        prop "$sdir/stack.properties" status blocked
        prop "$sdir/stack.properties" reason "$reason"
        mark_stack_cases "$stack" blocked prerequisite "$reason"
        return
    fi

    COMPOSE_FILES=()
    for f in $files; do
        COMPOSE_FILES+=(-f "$SUITE_DIR/stacks/$f")
    done
    CURRENT_PROJECT=$(echo "omb-e2e-$RUN_ID-$stack" | tr 'A-Z' 'a-z')
    CURRENT_STACK_DIR="$sdir"
    export OMB_E2E_SUITE_DIR="$SUITE_DIR"
    export OMB_E2E_CASE_DIR="$sdir"

    compose config > "$sdir/compose-effective.yaml" 2>&1
    start=$(date +%s)
    log "  starting services (project $CURRENT_PROJECT)"
    run_with_timeout "$STARTUP_TIMEOUT" "$sdir/startup.log" \
        compose up -d --wait --wait-timeout "$STARTUP_TIMEOUT" worker-1 worker-2
    rc=$?
    prop "$sdir/stack.properties" startup_seconds "$(($(date +%s) - start))"
    compose images > "$sdir/images.txt" 2>&1
    for img in $(compose config --images 2>/dev/null | sort -u); do
        prop "$sdir/stack.properties" image "$img $(docker image inspect --format '{{index .RepoDigests 0}} {{.Os}}/{{.Architecture}}' "$img" 2>/dev/null)"
    done

    if [ "$rc" != 0 ]; then
        if [ "$INTERRUPTED" = 1 ]; then
            mark_stack_cases "$stack" aborted stack-startup "Run interrupted during stack startup"
        else
            log "  ERROR: stack startup failed (exit $rc), see stacks/$stack/startup.log"
            prop "$sdir/stack.properties" status error
            prop "$sdir/stack.properties" reason "Stack startup failed (exit $rc)"
            mark_stack_cases "$stack" error stack-startup "Messaging service stack did not become ready (exit $rc), see stacks/$stack/startup.log and compose.log"
        fi
        teardown_current "$sdir"
        return
    fi
    prop "$sdir/stack.properties" status started

    selected_cases_for_stack "$stack" > "$sdir/cases.txt"
    while IFS= read -r row <&3; do
        if [ "$INTERRUPTED" = 1 ]; then
            local cdir="$REPORT_DIR/cases/$(field "$row" 1)"
            grep -q '^status=' "$cdir/case.properties" 2>/dev/null \
                || prop "$cdir/case.properties" status not-run
            continue
        fi
        run_case "$row" < /dev/null
    done 3< "$sdir/cases.txt"

    teardown_current "$sdir"
}

STACK_ROWS=$(matrix_rows "$SUITE_DIR/stacks.txt")
OLD_IFS=$IFS
IFS='
'
for srow in $STACK_ROWS; do
    IFS=$OLD_IFS
    stack=$(field "$srow" 1)
    in_list "$STACK_FILTER" "$stack" || continue
    [ -n "$(selected_cases_for_stack "$stack")" ] || continue
    if [ "$INTERRUPTED" = 1 ]; then
        mark_stack_cases "$stack" not-run interrupted "Run interrupted before this stack started"
        continue
    fi
    run_stack "$srow"
done
IFS=$OLD_IFS

finish
