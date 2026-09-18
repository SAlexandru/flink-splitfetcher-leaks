#!/usr/bin/env bash
#
# Builds Flink with a pull request applied and runs this project against it.
#
# By default it measures twice -- once at the PR commit, once at its parent -- so the only
# difference between the two arms is the PR itself. Both arms are built from source on purpose:
# ~/.m2 may already hold a snapshot built from the PR branch, in which case an arm that merely
# resolves the published jar is silently a second patched run, and the comparison shows the fix
# doing nothing. That is not hypothetical; it was true on this machine for 2.4-SNAPSHOT.
#
# Defaults target https://github.com/apache/flink/pull/29218,
# "[FLINK-40657][connector-base] Release per-fetcher state in FutureCompletingBlockingQueue".
#
# Usage:
#   scripts/run-pr.sh [-p pr] [-b base] [-f dir] [-w dir] [-r ref] [-o ref] [-v version]
#                     [-M module] [-m mode] [-d seconds] [-i seconds] [-x mx] [-t] [-K]
#                     [-- sim args...]
#
#   -p  PR number, default 29218
#   -b  base to apply the PR to: master (default) or 2.2.1
#   -f  Flink checkout to work from, default ../flink
#   -w  worktree path, default <flink-dir>/../flink-pr<N>
#   -r  build this git ref as-is instead of fetching the PR
#   -o  with -b 2.2.1, the ref to cherry-pick onto, default the release-2.2.1 tag
#   -v  Flink version to resolve the rest of the classpath at, default read from the built tree
#   -M  module to build and shadow, repeatable, default flink-connectors/flink-connector-base
#   -m  compare (default) | patched | smoke
#   -d  duration per measured run, default 180 (45 in smoke mode)
#   -i  sampling interval, default 60 (30 in smoke mode)
#   -x  JVM max heap, default 4g
#   -t  run the PR's own connector-base unit tests before measuring
#   -K  remove the worktree when done; the default keeps it, so re-runs skip the checkout
#
# Examples:
#   scripts/run-pr.sh -m smoke                      # fastest end-to-end check
#   scripts/run-pr.sh                               # stock vs patched, 3 minutes per arm
#   scripts/run-pr.sh -t -d 600 -x 6g               # tests first, then the README's workload
#   scripts/run-pr.sh -b 2.2.1 -- recordsPerEvent=200 queueCapacity=1
#
set -uo pipefail

PR=29218
BASE=master
FLINK_DIR=""
WORKTREE=""
REF=""
ONTO="release-2.2.1"
FLINK_VERSION=""
MODE=compare
DURATION=180
INTERVAL=60
MAX_HEAP=4g
RUN_TESTS=0
DROP_WORKTREE=0
DURATION_SET=0
INTERVAL_SET=0
MODULES=()

# The PR only touches flink-connector-base. Deriving this from the diff is possible but has to
# walk up to the nearest pom.xml and cope with dependents that would need rebuilding too, so it
# stays a constant with -M as the override.
DEFAULT_MODULES=(flink-connectors/flink-connector-base)

# FLINK-37663 rewrote the same file this PR rewrites and is not in release-2.2.1, so the PR will
# not cherry-pick onto the tag without it. Test commit first, then the fix.
PREREQ_2_2_1=(06a929fbd6f 22c99868375)

# The PR's own tests, for -t. QueueProbe is a helper, not a test class, so it is not listed.
PR_TESTS="FutureCompletingBlockingQueueTest,SplitFetcherTest,SplitFetcherManagerTest"

usage() { sed -n '2,/^set -uo/p' "${BASH_SOURCE[0]}" | sed 's/^#//;s/^ //;$d'; }

while getopts "p:b:f:w:r:o:v:M:m:d:i:x:tKh" opt; do
  case "$opt" in
    p) PR="$OPTARG" ;;
    b) BASE="$OPTARG" ;;
    f) FLINK_DIR="$OPTARG" ;;
    w) WORKTREE="$OPTARG" ;;
    r) REF="$OPTARG" ;;
    o) ONTO="$OPTARG" ;;
    v) FLINK_VERSION="$OPTARG" ;;
    M) MODULES+=("$OPTARG") ;;
    m) MODE="$OPTARG" ;;
    d) DURATION="$OPTARG"; DURATION_SET=1 ;;
    i) INTERVAL="$OPTARG"; INTERVAL_SET=1 ;;
    x) MAX_HEAP="$OPTARG" ;;
    t) RUN_TESTS=1 ;;
    K) DROP_WORKTREE=1 ;;
    h) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))
[ "${1:-}" = "--" ] && shift
SIM_ARGS=("$@")
# The workload the README's before/after tables use: long splits, a queue small enough that
# producers actually block.
[ ${#SIM_ARGS[@]} -eq 0 ] && SIM_ARGS=(recordsPerEvent=200 queueCapacity=1)

[ ${#MODULES[@]} -eq 0 ] && MODULES=("${DEFAULT_MODULES[@]}")

case "$MODE" in
  compare|patched) ;;
  smoke)
    # Just short presets over the patched arm; nothing else about the path differs.
    [ "$DURATION_SET" -eq 1 ] || DURATION=45
    [ "$INTERVAL_SET" -eq 1 ] || INTERVAL=30
    ;;
  *) echo "unknown mode: $MODE (want compare, patched or smoke)" >&2; exit 2 ;;
esac
case "$BASE" in master|2.2.1) ;; *) echo "unknown base: $BASE (want master or 2.2.1)" >&2; exit 2 ;; esac

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ -n "$FLINK_DIR" ] || FLINK_DIR="$PROJECT_DIR/../flink"
FLINK_DIR="$(cd "$FLINK_DIR" 2>/dev/null && pwd)" \
  || { echo "no such directory: ${FLINK_DIR}. Pass -f <flink checkout>" >&2; exit 1; }
git -C "$FLINK_DIR" rev-parse --git-dir > /dev/null 2>&1 \
  || { echo "not a git repository: $FLINK_DIR" >&2; exit 1; }
[ -n "$WORKTREE" ] || WORKTREE="$(cd "$FLINK_DIR/.." && pwd)/flink-pr$PR"
WORK_DIR="$PROJECT_DIR/target/pr-$PR"

# What a worktree was built from, kept in that worktree's own git admin directory: one stamp per
# worktree, so alternating between bases reuses both; outside target/, so `mvn clean` here does
# not cost a 1 GB re-checkout; and removed with the worktree, so it cannot outlive it.
stamp_of() { echo "$(git -C "$1" rev-parse --git-dir)/run-pr.stamp"; }

trap 'exit 130' INT TERM

# ---------------------------------------------------------------- resolve the commit to apply

if [ -n "$REF" ]; then
  SHA=$(git -C "$FLINK_DIR" rev-parse --verify --quiet "${REF}^{commit}") \
    || { echo "no such ref in $FLINK_DIR: $REF" >&2; exit 1; }
  echo "==> Using $REF ($(echo "$SHA" | cut -c1-11)) instead of fetching PR $PR"
else
  # By URL rather than by remote name: PR refs live on apache/flink, and a checkout whose
  # "origin" is a fork would send us looking for refs/pull/N/head where it does not exist.
  echo "==> Fetching refs/pull/$PR/head from apache/flink"
  git -C "$FLINK_DIR" fetch --no-tags https://github.com/apache/flink.git "refs/pull/$PR/head" \
    || { echo "fetch failed. Offline? Pass -r <ref> to build a ref you already have." >&2; exit 1; }
  # Read it once: FETCH_HEAD is rewritten by any other fetch against this repo.
  SHA=$(git -C "$FLINK_DIR" rev-parse FETCH_HEAD)
fi

# RECIPE is what a worktree gets stamped with, so it names only what actually shaped the tree:
# -o on the master base changes nothing and should not force a re-checkout.
case "$BASE" in
  master)
    START_POINT="$SHA"
    RECIPE="pr=$PR base=master sha=$SHA"
    ;;
  2.2.1)
    START_POINT=$(git -C "$FLINK_DIR" rev-parse --verify --quiet "${ONTO}^{commit}") \
      || { echo "no such ref in $FLINK_DIR: $ONTO" >&2; exit 1; }
    RECIPE="pr=$PR base=2.2.1 sha=$SHA onto=$ONTO prereq=${PREREQ_2_2_1[*]}"
    ;;
esac

# ------------------------------------------------------------------------ worktree management

worktree_registered() {
  [ -d "$1" ] || return 1
  git -C "$FLINK_DIR" worktree list --porcelain \
    | grep -Fxq "worktree $(cd "$1" && pwd)"
}

git -C "$FLINK_DIR" worktree prune

REUSE=0
if worktree_registered "$WORKTREE"; then
  STAMP=$(stamp_of "$WORKTREE")
  [ -f "$STAMP" ] && [ "$(head -1 "$STAMP")" = "$RECIPE" ] && REUSE=1
fi

if [ "$REUSE" -eq 1 ]; then
  echo "==> Reusing worktree $WORKTREE"
  # From the stamp, not from HEAD: a compare run leaves the worktree wherever it was last
  # checked out, so an interrupted one would otherwise have us measure stock against its parent.
  PATCHED_SHA=$(sed -n 's/^patched=//p' "$STAMP")
  git -C "$WORKTREE" rev-parse --verify --quiet "${PATCHED_SHA:-missing}^{commit}" > /dev/null \
    || { echo "!! $STAMP does not name a commit that still exists." >&2
         echo "   Remove the worktree and re-run: git -C $FLINK_DIR worktree remove --force $WORKTREE" >&2
         exit 1; }
else
  if worktree_registered "$WORKTREE"; then
    echo "==> Recreating worktree $WORKTREE (built from a different recipe)"
    git -C "$FLINK_DIR" worktree remove --force "$WORKTREE" \
      || { echo "could not remove worktree $WORKTREE" >&2; exit 1; }
  elif [ -e "$WORKTREE" ]; then
    echo "!! $WORKTREE exists and is not a worktree of $FLINK_DIR. Move it or pass -w." >&2
    exit 1
  fi
  echo "==> Adding worktree $WORKTREE at $(echo "$START_POINT" | cut -c1-11)"
  git -C "$FLINK_DIR" worktree add --detach "$WORKTREE" "$START_POINT" \
    || { echo "worktree add failed" >&2; exit 1; }

  if [ "$BASE" = "2.2.1" ]; then
    # A worktree inherits repo config, but a cherry-pick still needs some identity.
    CPICK_CFG=()
    git -C "$WORKTREE" config user.email > /dev/null 2>&1 \
      || CPICK_CFG=(-c user.email=run-pr@localhost -c user.name="run-pr.sh")

    cherry_pick() {
      local sha="$1" conflicted
      git -C "$WORKTREE" "${CPICK_CFG[@]+${CPICK_CFG[@]}}" cherry-pick -x "$sha" > /dev/null 2>&1 \
        && return 0
      conflicted=$(git -C "$WORKTREE" diff --name-only --diff-filter=U)
      if [ -z "$conflicted" ] \
         && git -C "$WORKTREE" rev-parse -q --verify CHERRY_PICK_HEAD > /dev/null 2>&1; then
        echo "    $(echo "$sha" | cut -c1-11) is already in the base, skipping"
        git -C "$WORKTREE" cherry-pick --skip > /dev/null 2>&1 && return 0
      fi
      # ArchUnit keeps a store of known violations that most commits rewrite. It is never
      # compiled or run on this path, so a conflict confined to it is noise. Anything else is
      # a real conflict and the caller should see it.
      if [ -n "$conflicted" ] && ! echo "$conflicted" | grep -qv '^flink-architecture-tests/'; then
        echo "    $(echo "$sha" | cut -c1-11) conflicted only in archunit stores, keeping the base's"
        git -C "$WORKTREE" checkout --ours -- flink-architecture-tests \
          && git -C "$WORKTREE" add -A flink-architecture-tests \
          && git -C "$WORKTREE" "${CPICK_CFG[@]+${CPICK_CFG[@]}}" -c core.editor=true \
               cherry-pick --continue > /dev/null 2>&1 \
          && return 0
      fi
      echo "!! cherry-pick of $sha conflicted:" >&2
      echo "${conflicted:-<no conflicted paths reported>}" | sed 's/^/     /' >&2
      git -C "$WORKTREE" cherry-pick --abort > /dev/null 2>&1
      return 1
    }

    echo "==> Cherry-picking FLINK-37663 (prerequisite) then PR $PR onto $ONTO"
    for c in "${PREREQ_2_2_1[@]}" "$SHA"; do
      cherry_pick "$c" || {
        echo "   Resolve it once by hand and pass the result with -r, or point -o at a ref" >&2
        echo "   that already carries the prerequisite." >&2
        git -C "$FLINK_DIR" worktree remove --force "$WORKTREE" > /dev/null 2>&1
        exit 1
      }
    done
  fi

  # Recorded here, where HEAD is the applied commit by construction.
  PATCHED_SHA=$(git -C "$WORKTREE" rev-parse HEAD)
  printf '%s\npatched=%s\n' "$RECIPE" "$PATCHED_SHA" > "$(stamp_of "$WORKTREE")"
fi

STOCK_SHA=$(git -C "$WORKTREE" rev-parse "${PATCHED_SHA}^")

echo "    patched: $(git -C "$WORKTREE" log -1 --format='%h %s' "$PATCHED_SHA")"
echo "    stock:   $(git -C "$WORKTREE" log -1 --format='%h %s' "$STOCK_SHA")"

# --------------------------------------------------------------------- version and preflight

if [ -z "$FLINK_VERSION" ]; then
  # From the worktree's own pom, which is the tree being built -- not the main checkout's, which
  # may carry a local version bump.
  FLINK_VERSION=$(xmllint --xpath "/*[local-name()='project']/*[local-name()='version']/text()" \
    "$WORKTREE/pom.xml" 2>/dev/null)
  [ -n "$FLINK_VERSION" ] || FLINK_VERSION=$(awk '
    /<artifactId>flink-parent<\/artifactId>/ { f = 1; next }
    f && /<version>/ { gsub(/.*<version>|<\/version>.*/, ""); print; exit }' "$WORKTREE/pom.xml")
  [ -n "$FLINK_VERSION" ] || { echo "could not read the Flink version from $WORKTREE/pom.xml" >&2; exit 1; }
fi
echo "    rest of the classpath resolves at flink $FLINK_VERSION"

# A -SNAPSHOT has to have been installed locally; a release comes from Central. Check the jars
# directly rather than asking Maven, which is slower than the build this protects.
case "$FLINK_VERSION" in
  *-SNAPSHOT)
    M2_REPO=$(awk -F'[<>]' '/<localRepository>/ {print $3; exit}' "$HOME/.m2/settings.xml" 2>/dev/null)
    [ -n "${M2_REPO:-}" ] || M2_REPO="$HOME/.m2/repository"
    MISSING=""
    for a in flink-streaming-java flink-connector-base flink-clients flink-runtime; do
      [ -f "$M2_REPO/org/apache/flink/$a/$FLINK_VERSION/$a-$FLINK_VERSION.jar" ] || MISSING="$MISSING $a"
    done
    if [ -n "$MISSING" ]; then
      echo "!! flink $FLINK_VERSION is not installed in $M2_REPO:$MISSING" >&2
      echo "   Install it from the Flink tree first, e.g." >&2
      echo "     mvn clean install -DskipTests -Dfast -Pskip-webui-build,java21-target -T1C \\" >&2
      echo "       -pl flink-connectors/flink-connector-base,flink-clients,flink-runtime,flink-streaming-java -am" >&2
      echo "   or pass -v <version> to resolve against one you do have." >&2
      exit 1
    fi
    ;;
esac

# --------------------------------------------------------------------------------- build arms

PL=$(IFS=,; echo "${MODULES[*]}")

build_arm() { # $1 = commit, $2 = label
  echo "==> Building $PL at $(echo "$1" | cut -c1-11) ($2)"
  git -C "$WORKTREE" checkout --detach --quiet "$1" \
    || { echo "checkout of $1 failed" >&2; return 1; }
  # clean every time: Maven's incremental compiler otherwise keeps classes from whatever was
  # built here before, and the jar links but fails at runtime. See the README.
  ( cd "$WORKTREE" && mvn -q -nsu clean compile -pl "$PL" \
      -DskipTests -Dfast -Pskip-webui-build,java21-target ) \
    || { echo "build failed" >&2; return 1; }
  for m in "${MODULES[@]}"; do
    local dest="$WORK_DIR/$2/$(basename "$m")"
    rm -rf "$dest" && mkdir -p "$dest" || return 1
    # Snapshot it out: the next arm's `clean` deletes this target/classes.
    cp -R "$WORKTREE/$m/target/classes/." "$dest/" || return 1
  done
}

if [ "$MODE" = "compare" ]; then
  build_arm "$STOCK_SHA" stock || exit 1
fi
build_arm "$PATCHED_SHA" patched || exit 1

if [ "$RUN_TESTS" -eq 1 ]; then
  echo "==> Running the PR's own tests in $PL"
  # -Dfast skips rat/checkstyle/spotless/enforcer, not tests. failIfNoSpecifiedTests matters
  # once -M names more than one module, since the filter will not match in all of them.
  ( cd "$WORKTREE" && mvn -nsu test -pl "$PL" -Dfast -Pskip-webui-build,java21-target \
      -Dtest="$PR_TESTS" -Dsurefire.failIfNoSpecifiedTests=false ) \
    || { echo "!! the PR's tests failed; not measuring" >&2; exit 1; }
fi

# ------------------------------------------------------------------------------------ measure

if [ "$DURATION" -gt 400 ] && [ "$MAX_HEAP" = "4g" ]; then
  echo "!! ${DURATION}s at 4g: the stock arm may start GC-thrashing and you end up measuring the"
  echo "   allocator rather than the leak. Consider -x 6g."
fi

# Resolved once per comparison so both arms see the same jars, and not left to go stale.
rm -f "$PROJECT_DIR/target/classpath-$FLINK_VERSION.txt"

run_arm() { # $1 = label
  local args=(-n "pr$PR-$1" -v "$FLINK_VERSION" -d "$DURATION" -i "$INTERVAL" -x "$MAX_HEAP")
  for m in "${MODULES[@]}"; do args+=(-c "$WORK_DIR/$1/$(basename "$m")"); done
  echo
  echo "=================== $1 ($DURATION s) ==================="
  "$PROJECT_DIR/scripts/leak-check.sh" "${args[@]}" -- "${SIM_ARGS[@]}"
}

STOCK_OK=0
if [ "$MODE" = "compare" ]; then
  run_arm stock && STOCK_OK=1 || echo "!! the stock arm did not complete" >&2
  # Let the machine settle before the second arm so they start from comparable state.
  sleep 10
fi
run_arm patched || { echo "!! the patched arm did not complete" >&2; exit 1; }

# ------------------------------------------------------------------------------------ verdict

result() { # $1 = label, $2 = key
  grep -m1 "^$2=" "$PROJECT_DIR/target/leak-check/pr$PR-$1/result.env" 2>/dev/null | cut -d= -f2
}

echo
echo "==> Verdict: flink $FLINK_VERSION, ${DURATION}s per arm, ${SIM_ARGS[*]}"
printf '    %-10s %-10s %-13s %-18s %s\n' "" splits "fetcher ids" "ConditionAndFlag" "per split"
for arm in stock patched; do
  [ "$arm" = "stock" ] && [ "$MODE" != "compare" ] && continue
  [ "$arm" = "stock" ] && [ "$STOCK_OK" -eq 0 ] && continue
  S=$(result "$arm" splits); C=$(result "$arm" ConditionAndFlag); F=$(result "$arm" fetcherIds)
  # Per split, not the raw count: the patched arm gets through roughly twice the splits in the
  # same wall time, so the two arms are not the same workload.
  RATIO=$(awk -v c="${C:-0}" -v s="${S:-0}" 'BEGIN { printf (s > 0 ? "%.2f" : "n/a"), c / (s ? s : 1) }')
  printf '    %-10s %-10s %-13s %-18s %s\n' "$arm" "${S:-?}" "${F:-?}" "${C:-?}" "$RATIO"
done
echo
echo "    A healthy count tracks splits in flight; a leak tracks splits processed since startup,"
echo "    so it is the per-split column that answers the question."
echo "    full tables: $PROJECT_DIR/target/leak-check/pr$PR-*/summary.txt"

if [ "$DROP_WORKTREE" -eq 1 ]; then
  echo "==> Removing worktree $WORKTREE"
  git -C "$FLINK_DIR" worktree remove --force "$WORKTREE"
else
  echo "    worktree kept at $WORKTREE (-K to remove it)"
fi
