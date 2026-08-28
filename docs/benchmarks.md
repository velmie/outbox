# outbox benchmarks

This document explains the automated research harness for outbox benchmarks, how to run it, and how to interpret the outputs.
For a conceptual walkthrough, see `docs/benchmarks-guide.md`.

## What the research covers

The harness explores the core dimensions that affect throughput and latency:

- Worker count and batch size (screening phase).
- Payload sizes (128B, 512B, 4KB).
- Large payload penalty (16KB, 128KB).
- Mixed workload (concurrent producers + consumers) with end-to-end latency.
- Partitioned vs non-partitioned storage layout under time-distributed data.
- Enqueue throughput with and without per-message transactions.

> **Warning:** Every case launched by `scripts/benchmarks-run.sh` uses `-reset=true` and drops and recreates its table.
> The default table is `outbox_bench`. When using an external MySQL instance, use a disposable, isolated database or
> table and credentials scoped to it. Never point the harness at a production or shared outbox table.

## Quick run (3-5 minutes)

```bash
# Start MySQL and run a reduced matrix.
PLAN=mini ./scripts/benchmarks-run.sh
```

This produces results under `docs/benchmarks/results/<RUN_ID>/`.

## Full run

```bash
PLAN=full ./scripts/benchmarks-run.sh
```

Full runs are longer and meant for final numbers.

## Environment and defaults

The harness assumes MySQL 8.0 and uses Docker + tmpfs by default:

- `scripts/mysql-bench.sh` starts a digest-pinned MySQL 8.4.11 LTS container, publishes the selected port on loopback,
  and uses tmpfs for `/var/lib/mysql`.
- `scripts/mysql-bench-stop.sh` stops the container.

You can skip the bundled MySQL and point to your own instance:

Supply `OUTBOX_DSN` through the process environment or a secret manager before running the command.

```bash
START_MYSQL=0 ./scripts/benchmarks-run.sh
```

### Production-like profile

To benchmark with disk-backed storage and strict durability:

```bash
MYSQL_PROFILE=prod PLAN=full ./scripts/benchmarks-run.sh
```

This switches to a disk-backed data directory and sets `innodb_flush_log_at_trx_commit=1`
and `sync_binlog=1`. The default data directory is `/tmp/outbox-mysql-bench-data`.
Override it with `DATA_DIR=/path/to/data`.

The runner selects this profile with `MYSQL_PROFILE`. When calling `scripts/mysql-bench.sh` directly, use `PROFILE`
instead. The `fast` profile disables the binary log by default; if it is enabled, its expiration and maximum file size
default to 600 seconds and 256M. The `prod` profile enables the binary log by default with 3600 seconds and 1G.

With bundled MySQL, the runner defaults `PURGE_BINLOG=1` and makes a best-effort `RESET MASTER` after each completed
benchmark run and again at phase transitions. This purge is disabled by default for external MySQL.

## Configuration knobs

Environment variables for `scripts/benchmarks-run.sh`:

- `PLAN=mini|full` (default `full`)
- `REPEATS=5` (number of repeats per case)
- `WARMUP=1` and `WARMUP_RECORDS=20000`
- `RESUME=1` (resume from checkpoints when `results.jsonl` exists)
- `PAYLOAD_RANDOM=0` (set to `1` to randomize payload contents)
- `PAYLOAD_SEED=1` (seed for payload generation)
- `ENQUEUE_TIMEOUT=0s` (timeout per enqueue/transaction)
- `MEASURE_LATENCY=1` (enable end-to-end latency collection in mixed mode)
- `AUTO_TARGET=1` (lower the consume target when the full pending backlog contains fewer rows than requested)
- `CONSUME_RECORDS=200000`
- `ENQUEUE_RECORDS=200000`
- `MIXED=1` (enable mixed workload phase)
- `MIXED_RECORDS=200000`
- `MIXED_DURATION=0s` (set duration; overrides records in mixed mode)
- `MIXED_PRODUCER_INTERVAL=0s` (rate-limit per producer)
- `MIXED_DRAIN_TIMEOUT=2m`
- `PARTITION_WINDOW=0s` (deprecated compatibility input; accepted, recorded, and ignored)
- `PARTITION_AHEAD=168h`
- `PARTITION_LOOKBACK=0s` (create partitions in the past; useful for partition-effect runs)
- `PARTITION_EFFECT_RECORDS=2000000`
- `PARTITION_EFFECT_DAYS=90`
- `PARTITION_EFFECT_SEED_AGE=2160h`
- `PARTITION_EFFECT_LOOKBACK=2160h` (defaults to seed age; ensures partitions cover seeded history)
- `LARGE_PAYLOAD_RECORDS=20000` (reduced by default to avoid tmpfs exhaustion for 128KB payloads)
- `OUTBOX_DSN` and MySQL connection vars (`MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DB`)
- `MYSQL_PROFILE=fast|prod` (defaults to `fast`; `prod` uses disk + higher durability)
- `MYSQL_WAIT_SECONDS` (startup wait; default is 60 for fast, 180 for prod)
- `MYSQL_REMOVE_CONTAINER=1` (set to `0` to keep the MySQL container for debugging)
- `PURGE_BINLOG=1|0` (defaults to `1` for bundled MySQL and `0` for external MySQL)

The script embeds default grids:

- workers: 1, 2, 4, 8, 16
- batch size: 10, 50, 100, 200
- payload bytes: 128, 512, 4096
- large payload bytes: 16384, 131072
- producers: 1, 2, 4, 8

## Outputs

Each run writes to `docs/benchmarks/results/<RUN_ID>/`:

- `metadata.txt`: host, MySQL config, and benchmark settings.
- `results.jsonl`: raw per-run JSON (one line per run).
- `results.csv`: CSV version of the JSONL.
- `summary.csv`: mean/p50/p95 throughput per case.
  Mixed-mode runs include mean latency metrics (`mean_latency_p50_ms`, `mean_latency_p95_ms`, `mean_latency_p99_ms`) and `mean_max_lag`.
  `p95` is left blank when there are fewer than 20 samples.

Each JSON row also includes:

- `processed` (actual records processed; throughput uses this value).
- `reset_duration`, `seed_duration`, `run_duration` to separate setup from measured run time.
- `partition_lookback` to capture how far back the reset step created partitions.

Tracked directories under `docs/benchmarks/results/` are immutable, revision-specific evidence. Older datasets may
contain nonzero `partition_window` values produced when that input had different semantics. Keep those artifacts
unchanged and do not combine them with current results without identifying the producing revision.

The runner prints a progress line after each run with completed/total counts, per-phase progress, and ETA.

### Resuming runs (checkpoints)

The runner writes a checkpoint key per run into `results.jsonl`.
To resume a partially completed run:

```bash
RUN_ID=20250101T120000Z RESUME=1 ./scripts/benchmarks-run.sh
```

Use the same `RUN_ID` and configuration, and the script will skip completed runs.
To force a full rerun, set `RESUME=0` or use a new `RUN_ID`.

### CPU / Memory metrics

Each JSON row includes process-level resource usage from the benchmark process itself:

- `process_user_cpu_seconds`, `process_system_cpu_seconds`
- `process_max_rss_kb`
- Go heap stats: `go_heap_*`, `go_stack_*`, `go_total_alloc_bytes`, `go_next_gc_bytes`, `go_num_gc`

The runner also samples host process stats once per second and records the max values:

- `bench_cpu_max_percent`, `bench_rss_kb` (outbox-bench process)
- `mysql_cpu_max_percent`, `mysql_rss_kb` (mysqld PID when `START_MYSQL=1`)

These are best-effort signals (instantaneous samples), useful for relative comparisons across runs.

### DB pool stats

Each run captures `db.Stats()` at the end of the benchmark:

- `db_wait_count`, `db_wait_duration_ms`
- `db_open_conns`, `db_in_use`, `db_idle`, `db_max_open`

### Mixed mode latency

Mixed mode reports end-to-end latency percentiles using producer timestamps embedded in headers.
The report includes p50/p95/p99 and max latency in milliseconds.

## Plotting

A helper script can generate quick graphs.

```bash
# Install optional deps.
python3 -m pip install --user matplotlib pandas

# Render plots.
python3 scripts/benchmarks-plot.py docs/benchmarks/results/<RUN_ID>/summary.csv
```

The script writes PNGs next to the summary file.

## Notes

- The harness recreates the table for every run to keep results isolated.
- The relay and auto-target calculation use the complete pending backlog; they do not apply a time-window cutoff.
- Partition-effect runs seed records across multiple days using `-seed-age`/`-seed-days` to compare partitioned and
  non-partitioned storage layouts under the same historical distribution. Use `PARTITION_EFFECT_LOOKBACK` to ensure
  the partition ranges cover the seeded history.
- `use_tx=false` is for raw insert baseline only; it is not recommended in production.
