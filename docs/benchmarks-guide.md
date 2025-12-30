# Outbox-bench: A Practical Guide to the MySQL Benchmark Harness

If you are evaluating a MySQL-backed outbox implementation, you usually want three answers fast: **how many messages per
second can it move**, **what latency it introduces under load**, and **which database and client-side parameters actually
matter**. This article explains exactly that for the `outbox-bench` harness: what it measures, how it generates load,
how it explores the parameter space (workers, batch size, payload size, partitioning, transactions), and how to
interpret the output files so you can draw correct conclusions and then dig deeper.

The focus is not on benchmarking theory in the abstract. It is on the concrete mechanics of this harness and the
database behaviors it stresses.

---

## 1. What is being benchmarked

### The outbox pattern in one paragraph

The **outbox pattern** stores events (or messages) in a database table as part of the same transactional boundary as
your business changes. A separate process called a **relay** (or dispatcher) reads pending rows from that table and
delivers them to the outside world (Kafka, NATS, HTTP, etc). This allows "write business data and enqueue event" to be
atomic, while message delivery happens asynchronously.

In this repository, the benchmark targets a **MySQL-backed outbox library**:

* **Producers** call `store.Enqueue(...)` to insert outbox entries into MySQL.
* A **relay** (`outbox.NewRelay(...)`) continuously pulls pending rows and invokes a handler.
* The handler in this benchmark is effectively a **no-op** (it does not publish anywhere), so you measure mostly the
  database and relay mechanics.

### The key system boundary

This benchmark measures:

* Insert throughput into the outbox table (enqueue)
* Read and acknowledge throughput (consume)
* The combined behavior under simultaneous insert + consume (mixed), including end-to-end latency computed from producer
  timestamps

It does **not** measure:

* Business transaction complexity (other tables, joins, contention with application reads)
* Real publish cost (network IO, serialization, downstream backpressure)
* Cross-service coordination

It isolates the outbox table plus relay logic so you can reason about the database and the
library with minimal noise.

---

## 2. Modes: consume, enqueue, mixed

The benchmark executable (`./cmd/outbox-bench`) supports three modes:

### Consume mode

Goal: measure how fast the relay can drain an outbox table that is already populated.

Mechanics:

1. **Seed** N outbox rows into the table (`seedEntries(...)`).
2. Start a relay with configurable:

    * `workers` (concurrency)
    * `batchSize` (how many rows per fetch/ack cycle)
    * `partitionWindow` (optional partition pruning hint)
3. The relay runs until a target processed count is reached, then the benchmark cancels the context.

What you learn:

* How worker count and batch size translate into throughput on your hardware and MySQL config
* Whether partition pruning helps or hurts in your data distribution
* How payload size affects read path cost

### Enqueue mode

Goal: measure pure insert rate into the outbox table.

Mechanics:

* Spawn `producers` goroutines.
* Each producer loops, calling `enqueueOnce(...)` until the global record count is reached.
* Optionally wrap each enqueue in its own transaction (`useTx`).

What you learn:

* How expensive per-message transactions are
* Where MySQL becomes the bottleneck (log flush, binlog sync, lock contention, connection pool)

### Mixed mode

Goal: approximate production reality: producers and consumers run at the same time. Measure throughput, backlog, and
end-to-end latency.

Mechanics:

* Start relay in a goroutine.
* Start producer goroutines that enqueue until:

    * a fixed number of records is produced, or
    * a fixed duration expires (`mixedDuration`)
* Producers embed a timestamp in message headers (`{"t": <unix_nanos>}`).
* Consumer reads this timestamp and records **end-to-end latency** as `time.Since(ts)`.

Additional signals:

* `maxLag`: maximum observed `produced - consumed` during the run
* Latency percentiles: p50, p95, p99, max, mean

What you learn:

* Whether consumers can keep up with producers at various concurrency levels
* The latency distribution you would expect from the database + relay loop alone
* How batching and concurrency trade throughput for tail latency

---

## 3. Database shape and partitioning concepts

### Partitioned vs non-partitioned schema

The harness can create the outbox table in two ways:

* **Non-partitioned**: a single InnoDB table.
* **Partitioned**: the same logical table, but split into **RANGE partitions** over time periods (day or month), plus a
  `pmax` catch-all.

Why this matters:

* With a long-lived outbox table, older rows accumulate.
* If your relay queries include a predicate that aligns with the partition key (for example, "only look at the last
  hour/day"), MySQL can use **partition pruning** to avoid scanning partitions that cannot match.

In this harness, partitions are created during table reset using:

* `partitionPeriod`: `day` or `month`
* `partitionAhead`: create future partitions so inserts do not fail or cause DDL at runtime
* `partitionLookback`: create past partitions (useful for seeding historical distributions)

### Partition pruning and the "partition window"

The relay can be configured with a `partitionWindow`:

* `partitionWindow > 0`: relay attempts to restrict work to a recent time window
* `partitionWindow = 0`: no time restriction hint

Guardrail: when using time-distributed seeding (`seed-age`/`seed-days`) with a non-zero `partitionWindow`, the newest
seeded records must still be inside that window. Otherwise the relay will see no rows and the run fails fast. A safe
rule of thumb is `partitionWindow >= max(0, seedAge - (seedDays-1)*24h)`.

The point of the partition-effect phase is to answer a practical question:

> If my outbox contains history, does narrowing the consumer's scan window reduce work enough to matter?

This can be dramatic if your workload mixes "recent traffic" with "old backlog" and the schema is partitioned in a
compatible way. It can also be neutral or even negative if your constraints are not selective, or if partition switching
causes more complexity than it saves.

---

## 4. Workload generation: payloads, timestamps, and seeding

### Payload size and content

Payload generation is deliberately simple:

* Payload is JSON: `{"data":"..."}`
* `payload-bytes` controls the final payload size (bytes), including JSON overhead.
* Content can be:

    * deterministic (`aaaaa...`)
    * randomized (alphanumeric) with a reproducible seed (`payload-seed`)

Why content randomness exists at all:

* Some storage layers, compression, caches, and CPU paths behave differently with low-entropy vs high-entropy data.
* Even if InnoDB does not compress by default, randomization still helps detect artifacts like overly optimistic caching
  or string handling.

### Seeding strategy for consume runs

Consume mode seeds records before measurement. There are two seeding styles:

1. **Default seeding**: insert in batches inside a transaction, commit per batch.
2. **Time-distributed seeding** (used for partition-effect studies): spread inserts across multiple days by controlling
   generated IDs.

The time-distribution mechanism (used only for seeding, not for live enqueue):

* For partition-effect runs, seeding uses an explicit ID generator (`UUIDv7` generator with a custom clock).
* The clock **advances 1 ms per record without real waiting** (the clock is simulated), and it starts at controlled base times spread across days.
* The goal is to produce records whose time component spans many partitions, so pruning behavior is observable.

### Mixed-mode timestamps

In mixed mode, each produced entry includes headers with a timestamp:

* Producer: `headers = {"t": now_unix_nanos}`
* Consumer: parse headers and compute `time.Since(t)`

This gives an "end-to-end latency" measure for "enqueue into MySQL + relay pull + handler invocation", excluding any
real external publish cost.

---

## 5. Harness methodology: how the runner explores the parameter space

The benchmark is not just a single run. The scripts implement a small experimental design:

### Isolation: reset per run

Each run typically:

* Drops and recreates the outbox table (`reset=true`).
* Optionally recreates partitions.

Why:

* It avoids cross-run coupling via fragmentation, history size, or leftover rows.
* It makes comparisons across cases more meaningful.

The harness still records setup time separately:

* `reset_duration`
* `seed_duration`
* `run_duration` (the part used for throughput)

### Warmup runs

There is an optional warmup step per case:

* Warmup uses a smaller record count (`WARMUP_RECORDS`)
* Warmup results are recorded but excluded from the summary aggregation

This reduces distortion from:

* first-time code paths
* MySQL buffer pool warming
* Go runtime initialization
* filesystem cache effects (especially when running in tmpfs)

### Repeats and aggregation

Each case is repeated (`REPEATS`, default 5), and the harness produces:

* raw per-run results (`results.jsonl`)
* aggregated summary per case (`summary.csv`)

The summary includes:

* mean throughput
* median throughput (p50)
* p95 throughput only if there are at least 20 samples (so you do not get fake precision)

### Adaptive selection: screening then focus

A key strategy is: **screen broadly, then zoom in**.

1. **Screen phase**: explores a grid of `workers x batchSize` (payload fixed at 512B).
2. It computes the best performing `(workers, batchSize)` pairs (top 2 by mean throughput).
3. Later phases (payload sweep, large payload, mixed, durable) use only those top combos.

This is a pragmatic way to keep the experiment budget under control while still finding good settings.

### Resumability via checkpoints

Long matrices are fragile. The runner supports resuming partially completed runs:

* Each run produces a `checkpoint_key` string capturing all key parameters.
* If `RESUME=1`, the runner reads existing `results.jsonl` and skips completed keys.

This is useful when:

* you stop the run mid-way
* MySQL restarts
* you want to rerun only missing cases

---

## 6. Environment profiles: why MySQL configuration dominates results

The harness can start MySQL automatically in Docker, with two important profiles:

### Fast profile (default)

Designed for quick iteration:

* Uses tmpfs for `/var/lib/mysql` (memory-backed storage)
* Uses relaxed durability settings:

    * `innodb_flush_log_at_trx_commit=2`
    * `sync_binlog=0`

This profile is great for understanding:

* CPU scaling
* batching effects
* client concurrency effects
* query and lock behavior

But it can be wildly optimistic compared to production disks and strict durability.

### Prod-like profile

Designed to approximate strict durability:

* Disk-backed data directory
* `innodb_flush_log_at_trx_commit=1`
* `sync_binlog=1`

These settings typically increase the cost of commits (log flush and binlog sync), which matters a lot when you do
per-message transactions.

Practical takeaway:

* If your production system requires strict durability on every enqueue transaction, the enqueue mode with `use_tx=true`
  in prod profile is usually the number you should care about.
* If you batch messages in a larger business transaction, you care about a different point in the space.

### Automatic disk cleanup for long runs

Long benchmark runs can fill disks primarily via MySQL binary logs and temporary files. The harness includes safeguards
to limit growth:

* `scripts/mysql-bench.sh` defaults to disabling binlog for `MYSQL_PROFILE=fast` and enables binlog with short retention
  for `MYSQL_PROFILE=prod`.
* `scripts/benchmarks-run.sh` can auto-purge binlogs between phases when running the bundled MySQL container.

Controls:

* `BINLOG=0|1` (mysql-bench.sh): force binlog off/on (default: off for fast, on for prod).
* `BINLOG_EXPIRE_SECONDS=600` (mysql-bench.sh): how long to keep binlogs when enabled.
* `BINLOG_MAX_SIZE=256M` (mysql-bench.sh): binlog rotation size.
* `PURGE_BINLOG=1` (benchmarks-run.sh): run `RESET MASTER` between phases (default: on when `START_MYSQL=1`).

If you run against an external MySQL (`START_MYSQL=0`), binlog purge is disabled by default to avoid permission issues.

---

## 7. What is measured: metrics and how they are computed

### Throughput

Reported as `throughput_msg_per_sec`.

* Consume: `processed / duration_seconds`
* Enqueue: `records / duration_seconds`
* Mixed: `consumed / duration_seconds`

Be aware in mixed mode:

* Duration includes production time plus optional drain time plus shutdown overhead.
* That is intentional if you treat the run as a bounded workload that must fully drain.

### Batch duration stats

The relay reports batch-level timings through the benchmark metrics interface:

* p50, p95, p99, max, mean
* computed via sorting samples and using a nearest-rank style percentile

These help answer:

* Are we doing a few very slow batches that dominate tail behavior?
* Does increasing batch size reduce overhead per message but increase batch latency?

### End-to-end latency (mixed mode)

Latency is measured from producer timestamp to consumer observation:

* p50, p95, p99, max, mean
* sample count

Interpretation:

* This is not network publish latency.
* It is database and relay-loop latency, which is still extremely useful because it often dominates when you are
  database-bound.

### Backlog: max lag

`max_lag` is the maximum observed difference between produced and consumed counts.

Interpretation:

* If max lag is small, consumers track producers closely.
* If max lag grows, you are effectively buffering in the table, which often correlates with rising latency.

### DB pool stats

At the end of a run, it snapshots `database/sql` pool stats:

* `db_wait_count`, `db_wait_duration_ms`
* `db_open_conns`, `db_in_use`, `db_idle`, `db_max_open`

This is a surprisingly high signal metric:

* If `db_wait_count` is high, your connection pool is likely a bottleneck, not MySQL itself.
* If pool is saturated, increasing workers/producers can make throughput worse.

### Process and runtime resource metrics

Each run includes:

* process CPU time (user and system)
* max RSS
* Go memory stats: heap alloc, heap inuse, total alloc, GC count, next GC target, stack stats

The runner also samples once per second:

* max CPU percent and RSS of the benchmark process
* max CPU percent and RSS of the MySQL process (when MySQL is started by the script)

These are best-effort, but very useful for relative comparisons.

---

## 8. Input parameters that matter, and what they usually do

There are two layers of configuration:

1. The `outbox-bench` binary flags
2. The orchestration script environment variables

### Core benchmark flags (binary)

Key flags and their typical effects:

* `-mode=consume|enqueue|mixed`

    * selects which pipeline is active

* `-records=N`

    * size of workload (consume and enqueue) or target count (mixed if `-mixed-duration=0`)

* `-workers=N` (consume and mixed)

    * number of relay worker goroutines
    * tends to improve throughput until you saturate DB CPU, lock contention, or connection pool

* `-batch-size=N` (consume and mixed)

    * more batching often improves throughput by reducing per-message overhead and round trips
    * but can worsen tail latency and increase lock contention

* `-producers=N` (enqueue and mixed)

    * increases write concurrency
    * too high can cause contention (hot indexes, log flush bottlenecks, pool saturation)

* `-use-tx=true|false` (enqueue and mixed)

    * transaction per enqueue (when true)
    * disabling can show a raw insert baseline, but is not recommended for real outbox semantics

* `-payload-bytes=N`, `-payload-random`, `-payload-seed`

    * payload size affects IO, row size, buffer pool behavior, and sometimes CPU
    * random payload avoids overly favorable effects from repeated data

* `-partitioned=true|false`

    * toggles partitioned schema

* `-partition-period=day|month`

    * determines partition granularity

* `-partition-ahead`, `-partition-lookback`

    * controls partition creation range at reset time

* `-partition-window=1h` (or `0`)

    * gives the relay a window hint that can enable pruning and reduce scans

* `-seed-age`, `-seed-days` (consume)

    * controls time distribution during seeding, used to stress partition pruning

* `-producer-interval` (mixed)

    * rate limit each producer, useful for exploring steady-state latency vs burst

* `-mixed-duration`

    * if non-zero, run producers for a fixed time instead of fixed record count

* `-drain-timeout`

    * after producers stop, wait for consumers to catch up
    * controls whether mixed mode ends with backlog or fully drained state

* `-enqueue-timeout`

  * bounds time per enqueue (or per transaction), can expose timeouts under contention

* `-progress` / `-progress-interval`

  * emits a single-line, continuously updated progress status to stderr
  * shows counts, rate, and stall time so hangs are visible immediately
  * keep stdout clean for JSON output

* `-auto-target`

  * when enabled (default), the consume mode automatically lowers the processing target to the number of visible
    pending rows under the current `partitionWindow`
  * prevents indefinite runs when only part of the seeded data falls inside the window
  * if disabled and the requested `records` exceed visible rows, the run fails fast

### Parameters and metrics overview (quick reference)

| Test mode | Config parameter | What it controls | Measured metric | Expected impact (inferred) |
| --- | --- | --- | --- | --- |
| Consume | `-workers` | Parallel relay workers | Throughput (msg/s) | Improves throughput until DB CPU or connection pool saturates |
| Consume | `-batch-size` | Rows fetched and acked per transaction | Throughput and batch latency | Reduces per-message overhead, but can increase tail latency |
| Consume | `-partition-window` | Time window hint for partition pruning | Read efficiency and throughput | Can reduce scanned partitions on large tables |
| Enqueue | `-use-tx` | Wrap each enqueue in a transaction | Insert throughput | Adds commit/fsync overhead; typically lowers insert rate |
| Mixed | `-producers` | Concurrent producer goroutines | Throughput and max lag | More concurrency increases pressure on DB and pool |
| Mixed | `-payload-bytes` | Payload size in bytes | IO/CPU cost and latency | Larger payloads reduce msg/s and raise latency |
| All | `MYSQL_PROFILE` | MySQL durability profile | Throughput vs durability | `fast` is optimistic; `prod` reflects strict durability |
| All | DB pool stats | `db_wait_*`, `db_in_use` | Connection wait time | High waits indicate client pool bottlenecks |

### Harness env vars (runner)

Important environment variables in `scripts/benchmarks-run.sh`:

* `PLAN=mini|full`

    * controls grid sizes, record counts, repeats, and whether mixed mode runs

* `REPEATS`, `WARMUP`, `WARMUP_RECORDS`

    * reliability controls

* `MYSQL_PROFILE=fast|prod`

    * changes durability and storage backing

* `START_MYSQL=1|0`, `DSN=...`

    * choose bundled docker MySQL or external MySQL

* Phase-specific counts like:

    * `CONSUME_RECORDS`, `ENQUEUE_RECORDS`, `MIXED_RECORDS`
    * `PARTITION_EFFECT_RECORDS`, `PARTITION_EFFECT_DAYS`, `PARTITION_EFFECT_SEED_AGE`
    * `LARGE_PAYLOAD_RECORDS`

These are designed to help you run a small experiment quickly, then scale up to "final numbers".

---

## 9. Outputs: what files you get and how to read them

A run writes to:

`docs/benchmarks/results/<RUN_ID>/`

### `metadata.txt`

Contains:

* run id and config
* host CPU info (`lscpu`)
* memory info (`free -h`)
* MySQL version and selected MySQL variables

Use this to make results reproducible and explain differences between machines or profiles.

### `results.jsonl`

One JSON object per run (one line each). Includes:

* all benchmark metrics from the Go program
* plus harness fields: `phase`, `warmup`, `repeat`, `checkpoint_key`
* plus sampled max CPU/RSS metrics for the benchmark and MySQL processes
* consume runs also include `visible_records` and `target_records` when `auto-target` is enabled

This is the source of truth for any custom analysis.

### `results.csv`

CSV form of the JSONL, convenient for quick filtering and plotting.

### `summary.csv`

Aggregated statistics per unique case key:

* mean throughput
* median throughput (p50)
* p95 throughput only when sample count is large enough
* mean of latency, lag, and batch metrics when available

Interpretation tip:

* If you keep repeats small (like 5), do not over-index on p95 throughput. Use mean and median, then increase repeats if
  you need stable tail estimates.

---

## 10. How to interpret results without fooling yourself

### Throughput vs latency trade-offs

* Higher `batch-size` often increases throughput.
* But it can increase:

    * p95 latency in mixed mode
    * batch p95 time
    * max lag (if consumers handle big batches less frequently)

If your real system is latency sensitive, treat throughput wins that increase p95 latency as a trade, not a free lunch.

### Watch connection pool contention

A common failure mode in Go + MySQL benchmarks is saturating the `database/sql` pool rather than MySQL.

Signals:

* `db_wait_count` climbs
* `db_wait_duration_ms` climbs
* throughput flatlines or drops as you increase workers/producers

Fixes:

* Increase max open connections (the benchmark already sets it based on worker+producer counts)
* Reduce concurrency
* Improve query efficiency
* Ensure MySQL can actually handle the parallelism (CPU, IO, locks)

### Understand what "fast profile" means

tmpfs plus relaxed durability makes inserts much cheaper. This can be useful for relative comparisons, but it is not a
production number.

If you care about durable enqueue rate, run:

* `MYSQL_PROFILE=prod`
* enqueue phase with `use_tx=true`
* and consider the throughput as the upper bound for "events per second per MySQL primary" under strict durability

### Partition pruning is workload-dependent

Partitioning helps if:

* your read queries can exclude old partitions
* your data distribution spans many partitions
* and your relay actually limits scans via `partitionWindow` or similar predicates

Partitioning can be neutral or harmful if:

* your workload is only recent anyway
* partitioning adds complexity without reducing scanned pages
* your partition key is not aligned with access patterns

That is why the harness includes explicit partition-effect experiments with time-distributed seeding.

---

## 11. Getting started quickly and extending responsibly

### Quick run patterns

A minimal, quick matrix:

```bash
PLAN=mini ./scripts/benchmarks-run.sh
```

A fuller run:

```bash
PLAN=full ./scripts/benchmarks-run.sh
```

External MySQL:

```bash
START_MYSQL=0 \
DSN='user:pass@tcp(127.0.0.1:3306)/outbox?parseTime=true' \
./scripts/benchmarks-run.sh
```

More production-like durability:

```bash
MYSQL_PROFILE=prod PLAN=full ./scripts/benchmarks-run.sh
```

### Extending the benchmark

If you want to expand this harness, keep two principles in mind:

1. **Change one variable at a time, then re-run repeats.**
   This harness already embodies that idea with phases and screening.

2. **Keep the handler minimal unless you explicitly want to benchmark downstream work.**
   Otherwise you lose visibility into the database and relay mechanics.

Good extensions include:

* adding a simulated handler cost (fixed sleep, CPU burn) to model publish overhead
* adding a second table write inside the enqueue transaction to model business data
* running with realistic poll intervals instead of `pollInterval=0` to see CPU vs latency trade-offs
* capturing MySQL-level metrics (InnoDB row lock time, buffer pool hit rate) alongside the benchmark metrics

---

## Closing mental model

You can think of this harness as a controlled experiment around one bottleneck: the outbox table as a queue inside
MySQL.

* **Consume mode** answers: "How quickly can I drain a queue that already exists?"
* **Enqueue mode** answers: "How quickly can I append to the queue, and what does durability cost?"
* **Mixed mode** answers: "What does steady-state look like under concurrent read and write, and what latency does that
  imply?"

Once you can answer those on your hardware and MySQL settings, you have a solid base for the next layer: adding your
real business transaction and your real publishing pipeline.
