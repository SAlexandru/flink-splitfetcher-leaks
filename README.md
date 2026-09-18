# flink-splitfetcher-leaks

A self-contained Flink job that reproduces the *architecture* of a split-based object-store
source connector without needing the object store, a notification broker, or any other
infrastructure.

A random generator produces events, each event becomes one indivisible split, splits are handed
to readers one at a time, each split gets its own fetcher, and the reader synthesizes records
instead of decoding a remote stream. Everything runs on a local `StreamExecutionEnvironment`; the
only dependencies are `org.apache.flink` artifacts.

The parts worth exercising are not the I/O, they are the `flink-connector-base` machinery around
it: splits assigned one at a time, a new `SplitFetcher` per split so fetcher ids churn
continuously, a handover queue small enough that producers actually block, and splits that
genuinely finish. Fetcher ids come from a monotonic counter and are never reused, so any
per-fetcher bookkeeping held downstream has to be *released* when the fetcher dies rather than
merely abandoned. That is the thing this project measures.

## Running

### Against a pull request

`scripts/run-pr.sh` does the whole sequence: fetch the PR, put it in a git worktree so your own
Flink checkout keeps whatever branch it is on, build the module it changes, and run this project
against it — twice by default, once at the PR commit and once at its parent, so the only
difference between the two arms is the PR.

```bash
scripts/run-pr.sh -m smoke         # fetch, build, 45-second run — does it link?
scripts/run-pr.sh                  # stock vs patched, 3 minutes per arm
scripts/run-pr.sh -t -d 600 -x 6g  # the PR's own tests first, then 10 minutes per arm
scripts/run-pr.sh -b 2.2.1         # cherry-pick onto release-2.2.1 instead of master
```

It defaults to [apache/flink#29218](https://github.com/apache/flink/pull/29218), the fix for the
accumulation measured below. `-p` takes another PR number, `-M` another module, `-r` a ref you
prepared by hand; `-h` lists everything.

Both arms are built from source rather than measured against whatever `~/.m2` holds. That is not
caution for its own sake: the `2.4-SNAPSHOT` installed on the machine this was written on had
itself been built from the PR branch, so a "stock" arm resolving the published jar would have
been a second *patched* run, and the comparison would have shown the fix doing nothing.

Because the PR targets master, the rest of the classpath resolves at master's version, which has
to be installed locally — the script checks up front and prints the install command if it is not.
`-b 2.2.1` instead cherry-picks onto the `release-2.2.1` tag and runs against 2.2.1 from Maven
Central, needing no local Flink install. Every build passes `clean`: Maven's incremental compiler
otherwise keeps classes from whatever branch was built in that tree before, and the result links
but dies at the first checkpoint.

### The job on its own

```bash
mvn package
mvn exec:exec
mvn exec:exec -Dsim.args="maxEvents=5000 recordsPerEvent=40 queueCapacity=1"
```

`exec:exec` forks a JVM on purpose. Under `exec:java` the mini cluster deserializes the stream
graph with a classloader that cannot see Flink's operator classes and submission fails with
`ClassNotFoundException`.

To run against a Flink you built yourself, either install it and pass the version
(`-Dflink.version=2.2.1-mypatch-SNAPSHOT`), or leave the version alone and put one module's
`target/classes` first on the classpath with `leak-check.sh -c` below. `run-pr.sh` automates the
second.

| Option | Default | Meaning |
| --- | --- | --- |
| `maxEvents` | `2000` | Events to generate; negative runs forever (soak mode) |
| `recordsPerEvent` | `50` | Records per split, i.e. how long a split lives |
| `payloadBytes` | `256` | Size of each record's random payload |
| `batchSize` | `8` | Records per `fetch()` call; smaller means more queue traffic |
| `queueCapacity` | `2` | `source.element-queue-capacity` |
| `parallelism` | `1` | Source parallelism |
| `fetcherMode` | `shared` | `shared` (Flink's stock manager), `non_reaping`, or `per_split` |
| `reportIntervalMillis` | `5000` | Throughput logging interval; `0` disables |
| `checkpointIntervalMillis` | `10000` | Checkpoint interval; `0` disables |

Leave checkpointing on for long runs. The coordinator's `SplitAssignmentTracker` holds every
assigned split until a checkpoint completes, so with checkpointing off the split objects pile up
by design and drown out anything else that is accumulating.

The default `shared` mode is Flink's stock manager, and it churns fetcher ids all by itself: a
reader holding one split at a time is idle in the gap between splits, `SourceReaderBase` reaps
idle fetchers whenever the element queue drains, and the replacement takes a fresh id. Over 47
seconds `shared` burned 264,097 ids for 251,531 splits. `per_split` burns one per split, and
`non_reaping` — deliberately never letting the fetcher go idle — burns exactly 1. So a connector
gets the churn by default, without doing anything unusual.

### Measuring what is retained

```bash
scripts/leak-check.sh -d 600 -i 60 -x 6g -- recordsPerEvent=200 queueCapacity=1
scripts/leak-check.sh -n patched -c ../flink/flink-connectors/flink-connector-base/target/classes
```

It runs the job unbounded and samples live-object counts. Each sample is a
`jcmd GC.class_histogram`, which forces a full GC first, so every count is live objects; a JFR
recording is written alongside. `-c` is repeatable and puts locally built classes ahead of the
resolved jars. Each run leaves `summary.txt`, `histograms.txt`, `result.env` (the last sample as
key=value) and `run.env` (the exact classpath and arguments it ran with) under
`target/leak-check/<name>/`.

The reading is simple: counts that track *splits currently in flight* are healthy, counts that
track *splits processed since startup* are not.

## Results

`results/` holds the artifacts from one `scripts/run-pr.sh -d 120 -i 60` comparison against
Flink master — [`results/stock/`](results/stock) at the PR's parent commit and
[`results/patched/`](results/patched) at the PR — including the full class histograms.

| | splits | fetcher ids | `ConditionAndFlag` live | per split |
| --- | ---: | ---: | ---: | ---: |
| [stock](results/stock/summary.txt) | 237,861 | 251,278 | 230,606 | 0.97 |
| [patched](results/patched/summary.txt) | 293,933 | 314,883 | **1** | 0.00 |

Per split rather than raw, because the two arms are not the same workload: the patched arm got
through 24% more splits in the same window, no longer dragging a structure sized by every fetcher
ever created through each lookup.

### The histograms

Stock, second sample ([full file](results/stock/histograms.txt)) — the retained state is the
second and third largest thing on the heap, and the producer-indexed array itself is 900 KB:

```
   1:         77749        9295608  [B (java.base@21.0.12)
   2:        230941        5542584  java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject
   3:        230605        5534520  ...synchronization.FutureCompletingBlockingQueue$ConditionAndFlag
   4:          1587        2027056  [Ljdk.internal.vm.FillerElement; (java.base@21.0.12)
   5:         75668        1816032  java.lang.String (java.base@21.0.12)
   ...
   9:             1         922528  [L...FutureCompletingBlockingQueue$ConditionAndFlag;
```

Patched, same point in the run ([full file](results/patched/histograms.txt)) — neither the
condition objects nor the array appear anywhere in the top 30:

```
   1:         80048        9359128  [B (java.base@21.0.12)
   2:          1161        1965904  [Ljdk.internal.vm.FillerElement; (java.base@21.0.12)
   3:         77983        1871592  java.lang.String (java.base@21.0.12)
   ...
  12:         19479         623328  com.example.splitfetcherleaks.RandomEvent
```

The controls in `summary.txt` are what make this readable: `SplitFetcher` pinned at 1 rules out
"fetchers are not being reaped", and `EventSplit` sawtoothing with each checkpoint rules out "the
job is simply behind".

At ten minutes and 6 GB of heap the stock arm reaches ~1.15M live `ConditionAndFlag` against
1.15M splits processed, with retained heap climbing from 95 MB to 1,250 MB — the ratio stays at
0.99 from the first sample to the last, so nothing is ever released.

What the fix does *not* address is the per-split metric groups. `GenericMetricGroup` and
`OperatorQueryScopeInfo` climb in both arms of a long run; `InternalSourceSplitMetricGroup` stays
at 0–1 live, so the split groups themselves are dropped and what survives are the child groups
registered on the parent via `addGroup`. That is a `flink-runtime` concern, and only
`flink-connector-base` is swapped here.

## How it maps to the real connector

| This project | Real connector |
| --- | --- |
| `EventGeneratorEnumerator` | enumerator consuming a notification topic |
| `RandomEvent` | a message pointing at an object in remote storage |
| `EventSplit` | one object as one non-splittable split |
| `RandomDataSplitReader` | reader that opens the object and decodes messages |
| `PerSplitFetcherManager` | per-split fetcher assignment |
| `RandomSourceReader` | `SourceReaderBase` subclass |

`RandomSourceReader` extends `SourceReaderBase` directly rather than
`SingleThreadMultiplexSourceReaderBase`, because the latter only accepts a
`SingleThreadFetcherManager` and would hide the fetcher churn.

Splits are reproducible: a split carries a seed and an emitted count, so a reader restoring from
a checkpoint reseeds, skips what was already emitted, and continues with the identical byte
stream.
