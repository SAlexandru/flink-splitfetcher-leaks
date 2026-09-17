# flink-source-sim

A self-contained Flink job that reproduces the *architecture* of a split-based object-store source
connector without needing the object store, a notification broker, or any other infrastructure.

A random generator produces events, each event becomes one indivisible split, splits are handed to
readers one at a time, each split gets its own fetcher, and the reader synthesizes records instead
of decoding a remote stream. Everything runs on a local `StreamExecutionEnvironment`; the only
dependencies are `org.apache.flink` artifacts from Maven Central.

## Why

The connector this models is hard to run locally — it needs S3 credentials, a Kafka topic carrying
subscription messages, and a packaged runtime. But the parts worth exercising are not the I/O; they
are the `flink-connector-base` machinery that sits around it:

- splits assigned one at a time, with the reader requesting the next only after finishing the
  current one,
- a new `SplitFetcher` per split, so fetchers churn continuously,
- a small handover queue between fetcher threads and the reader, so producers actually block and
  have to be woken up,
- splits that genuinely finish, so per-split metric groups are created and torn down.

That is all reproducible with a random number generator, which is what this project does.

## Running

```bash
mvn package
mvn exec:exec                                   # defaults
mvn exec:exec -Dsim.args="maxEvents=5000 recordsPerEvent=40 queueCapacity=1"
```

`exec:exec` forks a JVM on purpose. Under `exec:java` the mini cluster deserializes the stream graph
with a classloader that cannot see Flink's operator classes and job submission fails with
`ClassNotFoundException`.

## Running against a locally built Flink

Nothing here needs a remote repository — the point of the project is to test Flink changes before
they go anywhere. Two ways, depending on what you are doing.

### Shadowing a single module (fast, for iterating)

Build just the module you changed and put its `target/classes` first on the classpath. No install,
no version juggling, a few seconds per cycle:

```bash
cd ../flink
mvn clean compile -pl flink-connectors/flink-connector-base \
  -DskipTests -Dfast -Pskip-webui-build,java21-target

cd ../flink-source-sim
scripts/leak-check.sh -n patched -c ../flink/flink-connectors/flink-connector-base/target/classes
```

This works as long as your change is confined to one module and does not alter what other modules
were compiled against.

### Installing to the local repository (for a real artifact)

When the change spans modules, or you want something other projects can depend on, install into
`~/.m2` under a version of your own:

```bash
cd ../flink
mvn org.codehaus.mojo:versions-maven-plugin:2.8.1:set \
  -DnewVersion=2.2.1-mypatch-SNAPSHOT -DgenerateBackupPoms=false

mvn clean install -DskipTests -Dfast -Pskip-webui-build,java21-target -T1C \
  -pl flink-connectors/flink-connector-base,flink-clients,flink-runtime,flink-streaming-java -am

cd ../flink-source-sim
mvn exec:exec -Dflink.version=2.2.1-mypatch-SNAPSHOT
scripts/leak-check.sh -v 2.2.1-mypatch-SNAPSHOT
```

`-am` pulls in the modules those four depend on. To undo the version change afterwards:
`git checkout -- '**/pom.xml'`.

### Always pass `clean`

Maven's incremental compiler only recompiles sources that changed relative to whatever is already
in `target/classes`. A tree that was previously built on another branch keeps that branch's classes
and packages them alongside the new ones. The jar links, and then fails at runtime with
`NoSuchMethodError` from the first code path that crosses the seam — for a Flink build, typically
the first checkpoint. Skipping `clean` on one such build produced a `flink-runtime` jar holding
`Checkpoints.class` and `PendingCheckpoint.class` compiled 13 hours and one branch apart.

To check a build after the fact:

```bash
find <module>/target/classes -name '*.class' ! -newermt '<build date>'
```

Anything printed is stale. This project is a cheap way to catch it: it checkpoints within seconds of
starting, so a bad build fails immediately rather than in a downstream consumer.

### Options

All are `key=value` arguments; see `SimulationConfig`.

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
assigned split until a checkpoint completes, so with checkpointing off the split objects pile up by
design and drown out anything else that is accumulating.

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

Splits are reproducible: a split carries a seed and an emitted count, so a reader restoring from a
checkpoint reseeds, skips what was already emitted, and continues with the identical byte stream.

## What a run shows

With `maxEvents=3000 recordsPerEvent=40 queueCapacity=1 batchSize=4`:

```
fetchers churned:   3000
highest fetcher id: 2999
120000 records in 700 ms
```

Fetcher ids come from a monotonic counter and are never reused, so any per-fetcher bookkeeping held
downstream has to be released when the fetcher dies rather than merely abandoned. Counting
`Closing splitFetcher N because it is idle` in the log is the quickest way to see the churn:

```bash
mvn exec:exec -Dsim.args="maxEvents=3000" 2>&1 | grep -c 'because it is idle'
```

Worth noting: Flink's stock manager does **not** avoid the churn, which is why `shared` is the
default mode here rather than a curiosity. A reader that holds one split at a time is idle in the
gap between finishing a split and being handed the next, `SourceReaderBase` reaps idle fetchers
whenever the element queue drains, and the replacement takes a fresh id. Over 47 seconds:

| `fetcherMode` | splits | fetcher ids burned |
| --- | ---: | ---: |
| `shared` (stock) | 251,531 | 264,097 |
| `per_split` | 187,538 | one per split |
| `non_reaping` | 287,357 | **1** |

So a connector gets the churn by default, without doing anything unusual. Only deliberately holding
the fetcher open avoids it.

## Checking what the churn retains

`scripts/leak-check.sh` runs the job unbounded and samples live-object counts, so you can see which
bookkeeping is released as fetchers and splits come and go and which is not:

```bash
scripts/leak-check.sh -d 600 -i 60 -x 6g -- recordsPerEvent=200 queueCapacity=1
```

Each sample comes from `jcmd GC.class_histogram`, which forces a full GC first, so every count is
live objects. A JFR recording is written alongside for the wider picture.

The reading is simple: counts that track *splits currently in flight* are healthy, counts that track
*splits processed since startup* are not. `SplitFetcher` stays at 1 and `EventSplit` sawtooths with
each checkpoint, both correct; anything that climbs in lockstep with the split count is retained
forever.

### Result of a 10-minute run

`-d 600 -i 60 -x 6g -- recordsPerEvent=200 queueCapacity=1`, against stock Flink 2.2.1. Live objects
after a full GC:

| t (s) | splits done | `ConditionAndFlag` | `GenericMetricGroup` | `GenericValueMetricGroup` | `OperatorQueryScopeInfo` | `EventSplit` | `SplitFetcher` |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 60,960 | 59,475 | 59,521 | 59,475 | 118,952 | 33,375 | 1 |
| 123 | 520,590 | 511,860 | 511,916 | 511,870 | 1,023,742 | 507 | 1 |
| 315 | 812,534 | 807,666 | 807,731 | 807,685 | 1,615,372 | 3,309 | 1 |
| 579 | 1,153,334 | 1,145,803 | 1,145,893 | 1,145,847 | 2,291,696 | 8,257 | 1 |

Retained heap went from 95 MB to 1,250 MB, and the ratio to splits processed stays at 0.99 from the
first sample to the last — nothing is ever released. Two distinct things accumulate:

- **`ConditionAndFlag`**, one per fetcher, held by the handover queue. Fetchers themselves are
  released correctly (`SplitFetcher` never exceeds 1 live), so this is state outliving its owner.
- **`GenericMetricGroup` / `GenericValueMetricGroup` / `OperatorQueryScopeInfo`**, the per-split
  metric groups. `InternalSourceSplitMetricGroup` stays at 0-1 live, so the split groups themselves
  are dropped; what survives are the child groups registered on the parent via `addGroup`.

The controls in the same table are what make this readable: `SplitFetcher` pinned at 1 rules out
"fetchers are not being reaped", and `EventSplit` sawtoothing rules out "the job is simply behind".

### Confirming a fix

Point `-c` at a locally built module to run the same workload against patched code without
installing anything:

```bash
scripts/leak-check.sh -n patched -d 600 -i 60 -x 6g \
  -c ../flink/flink-connectors/flink-connector-base/target/classes \
  -- recordsPerEvent=200 queueCapacity=1
```

With a `flink-connector-base` that releases per-producer state when a fetcher shuts down, over
2.3M splits:

| t (s) | splits done | `ConditionAndFlag` | `GenericMetricGroup` | `SplitFetcher` |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 63,442 | 1 | 61,893 | 1 |
| 122 | 597,525 | 1 | 584,282 | 1 |
| 318 | 1,436,223 | 0 | 1,406,224 | 1 |
| 529 | 2,317,085 | 1 | 2,261,886 | 1 |

`ConditionAndFlag` never exceeds 1 — it drops out of JFR's `jdk.ObjectCount` entirely, being far
below the reporting threshold. The metric groups still climb, because that is a `flink-runtime`
concern and only `flink-connector-base` was swapped here; they double as proof the run really is
churning splits.

Throughput roughly doubles as a side effect, 2,000 to 4,400 splits/s, from no longer dragging a
structure sized by every fetcher ever created through each lookup.

### Does the fix remove the need for a workaround?

A connector can dodge the id churn itself by never letting its fetcher go idle (`non_reaping`
above). That works, but it costs a parked thread per subtask and has to be unwound when no more
splits are coming, or the reader can never reach end of input.

Running `shared` — the stock manager, no workaround — against the patched queue answers whether
that is still necessary:

| | splits | fetcher ids burned | `ConditionAndFlag` |
| --- | ---: | ---: | ---: |
| `shared`, stock queue | 251,531 | 264,097 | 244,445 |
| `shared`, patched queue | 262,191 | 278,675 | **1** |

The ids are still burned — that part is inherent to the assignment pattern — but the queue no longer
keeps anything per id. So the workaround becomes unnecessary once the queue releases producer state
on fetcher shutdown, which is what its own documentation predicted.
