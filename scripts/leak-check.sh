#!/usr/bin/env bash
#
# Runs the simulation unbounded for a while and samples live-object counts, to show whether
# per-fetcher and per-split bookkeeping is released as fetchers and splits come and go.
#
# Two sources of evidence are collected:
#   * jcmd GC.class_histogram, sampled on an interval. Each sample forces a full GC first, so the
#     counts are live objects only -- anything that keeps climbing is retained, not garbage.
#   * a JFR recording, for the wider picture (allocation, GC, threads) and as an archive.
#
# Usage:
#   scripts/leak-check.sh [-d seconds] [-i seconds] [-x mx] [-v version] [-c dir] [-n name]
#                         [-- sim args...]
#
#   -d  total run time, default 600 (10 minutes)
#   -i  histogram sampling interval, default 60
#   -x  JVM max heap, default 4g
#   -v  Flink version to resolve, default whatever the pom says
#   -c  directory to put first on the classpath, shadowing the resolved jars. Use it to drop in a
#       locally built module, e.g. a patched flink-connector-base, without installing it. Repeat
#       to shadow several modules; they are prepended in the order given.
#   -n  label for the output directory, so runs can be compared side by side
#
# Exits non-zero if the simulation did not survive the full duration, so a caller running two of
# these back to back can tell a real result from a job that died.
#
# Example:
#   scripts/leak-check.sh -d 600 -i 60 -- recordsPerEvent=20 queueCapacity=1
#   scripts/leak-check.sh -n patched -v 2.2.1-SNAPSHOT \
#     -c ../flink/flink-connectors/flink-connector-base/target/classes
#
set -uo pipefail

DURATION=600
INTERVAL=60
MAX_HEAP=4g
FLINK_VERSION=""
CP_PREFIXES=()
RUN_NAME="default"

while getopts "d:i:x:v:c:n:" opt; do
  case "$opt" in
    d) DURATION="$OPTARG" ;;
    i) INTERVAL="$OPTARG" ;;
    x) MAX_HEAP="$OPTARG" ;;
    v) FLINK_VERSION="$OPTARG" ;;
    c) CP_PREFIXES+=("$OPTARG") ;;
    n) RUN_NAME="$OPTARG" ;;
    *) echo "usage: $0 [-d s] [-i s] [-x mx] [-v version] [-c dir] [-n name] [-- sim args...]" >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))
[ "${1:-}" = "--" ] && shift
SIM_ARGS=("$@")

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$PROJECT_DIR/target/leak-check/$RUN_NAME"
CLASSPATH_FILE="$PROJECT_DIR/target/classpath-${FLINK_VERSION:-pom}.txt"
MVN_VERSION_ARG=()
[ -n "$FLINK_VERSION" ] && MVN_VERSION_ARG=("-Dflink.version=$FLINK_VERSION")

# Classes whose live count answers the question. Counts that track the number of splits currently
# in flight are healthy; counts that track the number of splits processed since startup are not.
# WATCH_NAMES runs in lockstep with WATCH: it labels the summary columns and keys result.env, so
# nothing downstream has to know which column a class landed in.
WATCH=(
  'FutureCompletingBlockingQueue\$ConditionAndFlag'
  'GenericMetricGroup'
  'GenericValueMetricGroup'
  'QueryScopeInfo\$OperatorQueryScopeInfo'
  'com\.example\.splitfetcherleaks\.EventSplit$'
  'fetcher\.SplitFetcher$'
)
WATCH_NAMES=(
  ConditionAndFlag
  GenericMetricGroup
  GenericValueMetricGroup
  OperatorQueryScopeInfo
  EventSplit
  SplitFetcher
)

mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/*.txt "$OUT_DIR"/*.jfr "$OUT_DIR"/*.env

echo "==> Building (flink ${FLINK_VERSION:-from pom})"
# -nsu: never re-resolve a -SNAPSHOT mid-comparison. Without it Maven's daily update check can
# swap a locally installed snapshot for a newer one from a remote between two runs, and the two
# arms end up measuring different code.
# ${a[@]+"${a[@]}"} rather than "${a[@]}": bash 3.2, which is what macOS ships, treats an empty
# array as unset under `set -u` and aborts.
( cd "$PROJECT_DIR" && mvn -q -nsu compile ${MVN_VERSION_ARG[@]+"${MVN_VERSION_ARG[@]}"} ) \
  || { echo "build failed" >&2; exit 1; }
if [ ! -s "$CLASSPATH_FILE" ]; then
  ( cd "$PROJECT_DIR" && mvn -q -nsu dependency:build-classpath ${MVN_VERSION_ARG[@]+"${MVN_VERSION_ARG[@]}"} \
      "-Dmdep.outputFile=$CLASSPATH_FILE" ) \
    || { echo "classpath resolution failed" >&2; exit 1; }
fi
# Built left to right so the prefixes keep the order they were given on the command line.
PREFIX_CP=""
for d in ${CP_PREFIXES[@]+"${CP_PREFIXES[@]}"}; do
  [ -d "$d" ] || { echo "classpath prefix not a directory: $d" >&2; exit 1; }
  PREFIX_CP="${PREFIX_CP}$(cd "$d" && pwd):"
  echo "    shadowing with: $d"
done
CP="${PREFIX_CP}$PROJECT_DIR/target/classes:$(cat "$CLASSPATH_FILE")"

JFR_FILE="$OUT_DIR/sim.jfr"
LOG_FILE="$OUT_DIR/sim.log"
HIST_FILE="$OUT_DIR/histograms.txt"

# What this run actually ran against. Two summary.txt files a week apart are unattributable
# without it, and "which classes were really on the classpath" is the first thing to doubt.
{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "runName=$RUN_NAME"
  echo "flinkVersion=${FLINK_VERSION:-<from pom>}"
  echo "duration=$DURATION"
  echo "interval=$INTERVAL"
  echo "maxHeap=$MAX_HEAP"
  echo "simArgs=${SIM_ARGS[*]:-<defaults, unbounded>}"
  for d in ${CP_PREFIXES[@]+"${CP_PREFIXES[@]}"}; do echo "classpathPrefix=$d"; done
  echo "classpath=$CP"
} > "$OUT_DIR/run.env"

echo "==> Starting simulation for ${DURATION}s (heap ${MAX_HEAP}, sampling every ${INTERVAL}s)"
echo "    sim args: ${SIM_ARGS[*]:-<defaults, unbounded>}"

java \
  "-Xmx${MAX_HEAP}" \
  "-XX:StartFlightRecording:name=sim,settings=profile,filename=${JFR_FILE},dumponexit=true,maxsize=500m,jdk.ObjectCount#enabled=true" \
  -cp "$CP" \
  com.example.splitfetcherleaks.SimulationJob maxEvents=-1 ${SIM_ARGS[@]+"${SIM_ARGS[@]}"} \
  > "$LOG_FILE" 2>&1 &
PID=$!

cleanup() {
  if kill -0 "$PID" 2>/dev/null; then
    echo "==> Dumping JFR and stopping pid $PID"
    jcmd "$PID" JFR.dump name=sim filename="$JFR_FILE" > /dev/null 2>&1
    kill "$PID" 2>/dev/null
    wait "$PID" 2>/dev/null
  fi
}
# A bare `trap cleanup INT` would run the handler and then *resume* the loop, which would see a
# dead pid, break, and exit 0 -- so a Ctrl-C would look like a completed run to a caller.
trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

# Give the mini cluster time to come up before the first sample.
sleep 15
if ! kill -0 "$PID" 2>/dev/null; then
  echo "!! Simulation died during startup; last lines of $LOG_FILE:" >&2
  tail -20 "$LOG_FILE" >&2
  exit 1
fi

START=$(date +%s)
END=$((START + DURATION))
SAMPLE=0
RUN_OK=1

printf '%-6s %-9s %-9s' "t(s)" "splits" "fetchers" > "$OUT_DIR/summary.txt"
for n in "${WATCH_NAMES[@]}"; do printf ' %-22s' "$n"; done >> "$OUT_DIR/summary.txt"
printf '\n' >> "$OUT_DIR/summary.txt"

while [ "$(date +%s)" -lt "$END" ]; do
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "!! Simulation exited early after $(( $(date +%s) - START ))s" >&2
    tail -25 "$LOG_FILE" >&2
    RUN_OK=0
    break
  fi

  ELAPSED=$(( $(date +%s) - START ))
  SAMPLE=$((SAMPLE + 1))

  # GC.class_histogram runs a full GC first, so these are live objects.
  HIST=$(jcmd "$PID" GC.class_histogram 2>/dev/null)
  {
    echo "===== sample $SAMPLE at t=${ELAPSED}s ====="
    # awk rather than head: head closes the pipe early and the writer dies on SIGPIPE.
    echo "$HIST" | awk 'NR <= 30'
  } >> "$HIST_FILE"

  # Splits completed, and fetcher ids burned. Count splits from the fetcher's own "finished
  # reading" line rather than from reaping, which never happens in the non-reaping mode. The id
  # count is the real diagnostic: it is what the handover queue keys its producer state by.
  SPLITS=$(grep -c 'Finished reading from splits' "$LOG_FILE" 2>/dev/null || echo 0)
  FETCHER_IDS=$(grep -oE 'Starting split fetcher [0-9]+' "$LOG_FILE" 2>/dev/null | sort -u | wc -l | tr -d ' ')

  printf '%-6s %-9s %-9s' "$ELAPSED" "$SPLITS" "$FETCHER_IDS" >> "$OUT_DIR/summary.txt"
  LINE="t=${ELAPSED}s splits=${SPLITS} fetcherIds=${FETCHER_IDS}"
  # Rewritten each sample rather than once at the end, so a run that dies early still leaves the
  # last good numbers behind in a form a caller can read without parsing the table.
  {
    echo "elapsed=$ELAPSED"
    echo "splits=$SPLITS"
    echo "fetcherIds=$FETCHER_IDS"
  } > "$OUT_DIR/result.env"
  IDX=0
  for w in "${WATCH[@]}"; do
    COUNT=$(echo "$HIST" | grep -E "$w" | awk '{s+=$2} END {print s+0}')
    printf ' %-22s' "$COUNT" >> "$OUT_DIR/summary.txt"
    LINE="$LINE | ${WATCH_NAMES[$IDX]}=$COUNT"
    echo "${WATCH_NAMES[$IDX]}=$COUNT" >> "$OUT_DIR/result.env"
    IDX=$((IDX + 1))
  done
  printf '\n' >> "$OUT_DIR/summary.txt"
  echo "$LINE"

  sleep "$INTERVAL"
done

echo "==> Run complete"
cleanup
trap - EXIT

echo
echo "==> Live object counts over time (each sample preceded by a full GC)"
cat "$OUT_DIR/summary.txt"
if [ -s "$JFR_FILE" ]; then
  echo
  echo "==> Same classes as seen by JFR (jdk.ObjectCount, sampled at GC points)"
  jfr print --events jdk.ObjectCount "$JFR_FILE" 2>/dev/null | awk '
    /startTime *=/   { t=$3 }
    /objectClass *=/ { c=$3 }
    /^ *count *=/    { print t"\t"c"\t"$3 }
  ' | grep -E "$(IFS='|'; echo "${WATCH[*]}" | sed 's/\\//g')" \
    | sort -k2,2 -k1,1 \
    | awk -F'\t' '{ n=split($2,p,"."); printf "%-14s %-46s %12s\n", $1, p[n-1]"."p[n], $3 }' \
    | tee "$OUT_DIR/jfr-objectcount.txt"

  echo
  echo "==> Live heap after GC over the run (jdk.GCHeapSummary)"
  jfr print --events jdk.GCHeapSummary "$JFR_FILE" 2>/dev/null \
    | awk '/startTime *=/{t=$3} /when *=/{w=$3} /heapUsed *=/{print t"\t"$3" "$4"\t"w}' \
    | grep 'After' | awk 'NR%500==1 {printf "%-14s %s\n", $1, $2" "$3}'
fi

echo
echo "    full histograms: $HIST_FILE"
echo "    JFR recording:   $JFR_FILE"
echo "    job log:         $LOG_FILE"
echo "    run provenance:  $OUT_DIR/run.env"

# Non-zero if the job did not last the full duration, so a caller comparing two runs does not
# quietly treat a crashed arm as a result.
[ "$RUN_OK" -eq 1 ] || { echo "!! Run did not complete" >&2; exit 1; }
