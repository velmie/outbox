// Command outbox-bench runs MySQL-backed benchmarks for the outbox library.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

type mode string

const (
	modeConsume mode = "consume"
	modeEnqueue mode = "enqueue"
	modeMixed   mode = "mixed"
)

const (
	defaultRecords          = 100000
	defaultPayloadBytes     = 512
	defaultWorkers          = 4
	defaultProducers        = 4
	defaultBatchSize        = 50
	defaultPartitionAhead   = 7 * 24 * time.Hour
	defaultPartitionWindow  = time.Hour
	defaultDrainTimeout     = 2 * time.Minute
	defaultProgressInterval = 10 * time.Second
	defaultSeedBatchSize    = 1000
	defaultMinDBConns       = 4
	defaultExtraDBConns     = 4
	defaultDrainPoll        = 10 * time.Millisecond
	partitionExtraSlots     = 2
	percentileP50           = 0.50
	percentileP95           = 0.95
	percentileP99           = 0.99
	percentScale            = 100
	microsecondsPerSecond   = 1e6
	hoursPerDay             = 24
	daysPerMonth            = 30
)

var (
	errDSNRequired              = errors.New("outbox-bench: dsn is required")
	errMixedConfigMissing       = errors.New("outbox-bench: mixed mode requires records > 0 or mixed-duration")
	errUnsupportedMode          = errors.New("outbox-bench: unsupported mode")
	errInvalidMode              = errors.New("outbox-bench: invalid mode")
	errInvalidPeriod            = errors.New("outbox-bench: invalid partition period")
	errTableRequired            = errors.New("outbox-bench: table is required")
	errInvalidTableName         = errors.New("outbox-bench: invalid table name")
	errProcessedMismatch        = errors.New("outbox-bench: processed records mismatch")
	errPartitionWindowNoOverlap = errors.New("outbox-bench: partition window does not overlap seeded data")
	errNoVisibleRecords         = errors.New("outbox-bench: no visible records for consume run")
	errTargetMismatch           = errors.New("outbox-bench: requested records exceed visible rows")
)

type result struct {
	Mode                mode          `json:"mode"`
	Records             int           `json:"records"`
	VisibleRecords      int64         `json:"visible_records"`
	TargetRecords       int64         `json:"target_records"`
	Processed           int64         `json:"processed"`
	Duration            time.Duration `json:"duration"`
	ResetDuration       time.Duration `json:"reset_duration"`
	SeedDuration        time.Duration `json:"seed_duration"`
	RunDuration         time.Duration `json:"run_duration"`
	Throughput          float64       `json:"throughput_msg_per_sec"`
	Workers             int           `json:"workers"`
	BatchSize           int           `json:"batch_size"`
	Producers           int           `json:"producers"`
	PayloadBytes        int           `json:"payload_bytes"`
	PayloadRandom       bool          `json:"payload_random"`
	PayloadSeed         int64         `json:"payload_seed"`
	Partitioned         bool          `json:"partitioned"`
	PartitionAhead      time.Duration `json:"partition_ahead"`
	PartitionLookback   time.Duration `json:"partition_lookback"`
	PartitionWindow     time.Duration `json:"partition_window"`
	ProducerInterval    time.Duration `json:"producer_interval"`
	MixedDuration       time.Duration `json:"mixed_duration"`
	DrainTimeout        time.Duration `json:"drain_timeout"`
	EnqueueTimeout      time.Duration `json:"enqueue_timeout"`
	UseTx               bool          `json:"use_tx"`
	SeedAge             time.Duration `json:"seed_age"`
	SeedDays            int           `json:"seed_days"`
	AutoTarget          bool          `json:"auto_target"`
	Produced            int64         `json:"produced"`
	Consumed            int64         `json:"consumed"`
	MaxLag              int64         `json:"max_lag"`
	LatencyP50Ms        float64       `json:"latency_p50_ms"`
	LatencyP95Ms        float64       `json:"latency_p95_ms"`
	LatencyP99Ms        float64       `json:"latency_p99_ms"`
	LatencyMaxMs        float64       `json:"latency_max_ms"`
	LatencyMeanMs       float64       `json:"latency_mean_ms"`
	LatencySamples      int64         `json:"latency_samples"`
	BatchP50Ms          float64       `json:"batch_p50_ms"`
	BatchP95Ms          float64       `json:"batch_p95_ms"`
	BatchP99Ms          float64       `json:"batch_p99_ms"`
	BatchMaxMs          float64       `json:"batch_max_ms"`
	BatchMeanMs         float64       `json:"batch_mean_ms"`
	BatchSamples        int           `json:"batch_samples"`
	DBWaitCount         int64         `json:"db_wait_count"`
	DBWaitDurationMs    float64       `json:"db_wait_duration_ms"`
	DBOpenConns         int           `json:"db_open_conns"`
	DBInUse             int           `json:"db_in_use"`
	DBIdle              int           `json:"db_idle"`
	DBMaxOpen           int           `json:"db_max_open"`
	ProcessUserCPU      float64       `json:"process_user_cpu_seconds"`
	ProcessSystemCPU    float64       `json:"process_system_cpu_seconds"`
	ProcessMaxRSSKB     int64         `json:"process_max_rss_kb"`
	GoHeapAllocBytes    uint64        `json:"go_heap_alloc_bytes"`
	GoHeapSysBytes      uint64        `json:"go_heap_sys_bytes"`
	GoHeapInuseBytes    uint64        `json:"go_heap_inuse_bytes"`
	GoHeapReleasedBytes uint64        `json:"go_heap_released_bytes"`
	GoStackInuseBytes   uint64        `json:"go_stack_inuse_bytes"`
	GoStackSysBytes     uint64        `json:"go_stack_sys_bytes"`
	GoTotalAllocBytes   uint64        `json:"go_total_alloc_bytes"`
	GoNextGCBytes       uint64        `json:"go_next_gc_bytes"`
	GoNumGC             uint32        `json:"go_num_gc"`
}

func main() {
	var (
		dsn               string
		table             string
		runMode           string
		records           int
		payloadBytes      int
		workers           int
		producers         int
		batchSize         int
		partitioned       bool
		partitionPeriod   string
		partitionAhead    time.Duration
		partitionLookback time.Duration
		partitionWindow   time.Duration
		producerInterval  time.Duration
		mixedDuration     time.Duration
		drainTimeout      time.Duration
		enqueueTimeout    time.Duration
		payloadRandom     bool
		payloadSeed       int64
		reset             bool
		resetDuration     time.Duration
		useTx             bool
		seedAge           time.Duration
		seedDays          int
		measureLatency    bool
		progress          bool
		progressInterval  time.Duration
		autoTarget        bool
		jsonOut           bool
	)

	flag.StringVar(&dsn, "dsn", "", "MySQL DSN, e.g. user:pass@tcp(host:3306)/db?parseTime=true")
	flag.StringVar(&table, "table", "outbox_bench", "Outbox table name")
	flag.StringVar(&runMode, "mode", "consume", "Benchmark mode: consume, enqueue, or mixed")
	flag.IntVar(&records, "records", defaultRecords, "Number of records to process")
	flag.IntVar(&payloadBytes, "payload-bytes", defaultPayloadBytes, "Payload size in bytes")
	flag.BoolVar(&payloadRandom, "payload-random", false, "Generate random payload contents")
	flag.Int64Var(&payloadSeed, "payload-seed", 1, "Random seed for payload generation")
	flag.IntVar(&workers, "workers", defaultWorkers, "Relay workers")
	flag.IntVar(&producers, "producers", defaultProducers, "Concurrent producers (enqueue/mixed mode)")
	flag.IntVar(&batchSize, "batch-size", defaultBatchSize, "Batch size for relay")
	flag.BoolVar(&partitioned, "partitioned", true, "Use partitioned schema")
	flag.StringVar(&partitionPeriod, "partition-period", "day", "Partition period: day or month")
	flag.DurationVar(&partitionAhead, "partition-ahead", defaultPartitionAhead, "How far ahead to create partitions")
	flag.DurationVar(&partitionLookback, "partition-lookback", 0, "How far back to create partitions")
	flag.DurationVar(&partitionWindow, "partition-window", defaultPartitionWindow, "Relay partition window (0 disables)")
	flag.DurationVar(&producerInterval, "producer-interval", 0, "Sleep between enqueues per producer (mixed mode)")
	flag.DurationVar(&mixedDuration, "mixed-duration", 0, "Mixed mode duration (0 uses records)")
	flag.DurationVar(&drainTimeout, "drain-timeout", defaultDrainTimeout, "Time to wait for consumers to drain (mixed mode)")
	flag.DurationVar(&enqueueTimeout, "enqueue-timeout", 0, "Timeout per enqueue/transaction")
	flag.BoolVar(&reset, "reset", true, "Drop and recreate the table")
	flag.BoolVar(&useTx, "use-tx", true, "Use transaction per enqueue (enqueue mode)")
	flag.DurationVar(&seedAge, "seed-age", 0, "Seed entries starting at now-seed-age (consume mode)")
	flag.IntVar(&seedDays, "seed-days", 0, "Distribute seeded entries across this many days from seed-age (consume mode)")
	flag.BoolVar(&measureLatency, "measure-latency", true, "Measure end-to-end latency in mixed mode")
	flag.BoolVar(&progress, "progress", true, "Emit progress updates to stderr")
	flag.DurationVar(&progressInterval, "progress-interval", defaultProgressInterval, "Progress update interval")
	flag.BoolVar(&autoTarget, "auto-target", true, "Adjust consume target to visible pending rows")
	flag.BoolVar(&jsonOut, "json", false, "Print JSON result")
	flag.Parse()

	if dsn == "" {
		exitErr(errDSNRequired)
	}

	benchMode, err := parseMode(runMode)
	if err != nil {
		exitErr(err)
	}

	period, err := parsePeriod(partitionPeriod)
	if err != nil {
		exitErr(err)
	}

	table, err = sanitizeTableName(table)
	if err != nil {
		exitErr(err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		exitErr(fmt.Errorf("open db: %w", err))
	}
	defer db.Close()

	db.SetMaxOpenConns(maxInt(defaultMinDBConns, workers+producers+defaultExtraDBConns))
	db.SetMaxIdleConns(maxInt(defaultMinDBConns, workers+producers))

	store, err := mysql.NewStore(db, mysql.WithTable(table), mysql.WithValidateJSON(false))
	if err != nil {
		exitErr(err)
	}

	if reset {
		resetStart := time.Now()
		if err := resetTable(context.Background(), db, table, partitioned, period, partitionAhead, partitionLookback); err != nil {
			exitErr(err)
		}
		resetDuration = time.Since(resetStart)
	}

	// #nosec G404 -- deterministic RNG for benchmark payloads.
	rng := rand.New(rand.NewSource(payloadSeed))
	payload := buildPayload(payloadBytes, payloadRandom, rng)

	var res result
	switch benchMode {
	case modeConsume:
		res, err = runConsume(db, store, benchConfig{
			table:             table,
			records:           records,
			payload:           payload,
			workers:           workers,
			batchSize:         batchSize,
			partitioned:       partitioned,
			partitionAhead:    partitionAhead,
			partitionLookback: partitionLookback,
			partitionPeriod:   period,
			partitionWindow:   partitionWindow,
			resetDuration:     resetDuration,
			seedAge:           seedAge,
			seedDays:          seedDays,
			payloadRandom:     payloadRandom,
			payloadSeed:       payloadSeed,
			enqueueTimeout:    enqueueTimeout,
			progress:          progress,
			progressInterval:  progressInterval,
			autoTarget:        autoTarget,
		})
	case modeEnqueue:
		res, err = runEnqueue(db, store, benchConfig{
			table:             table,
			records:           records,
			payload:           payload,
			workers:           workers,
			producers:         producers,
			batchSize:         batchSize,
			partitioned:       partitioned,
			partitionAhead:    partitionAhead,
			partitionLookback: partitionLookback,
			partitionPeriod:   period,
			resetDuration:     resetDuration,
			payloadRandom:     payloadRandom,
			payloadSeed:       payloadSeed,
			enqueueTimeout:    enqueueTimeout,
			useTx:             useTx,
			progress:          progress,
			progressInterval:  progressInterval,
			autoTarget:        autoTarget,
		})
	case modeMixed:
		res, err = runMixed(db, store, benchConfig{
			table:             table,
			records:           records,
			payload:           payload,
			workers:           workers,
			producers:         producers,
			batchSize:         batchSize,
			partitioned:       partitioned,
			partitionAhead:    partitionAhead,
			partitionLookback: partitionLookback,
			partitionPeriod:   period,
			partitionWindow:   partitionWindow,
			resetDuration:     resetDuration,
			producerInterval:  producerInterval,
			mixedDuration:     mixedDuration,
			drainTimeout:      drainTimeout,
			enqueueTimeout:    enqueueTimeout,
			payloadRandom:     payloadRandom,
			payloadSeed:       payloadSeed,
			useTx:             useTx,
			measureLatency:    measureLatency,
			progress:          progress,
			progressInterval:  progressInterval,
			autoTarget:        autoTarget,
		})
	default:
		err = fmt.Errorf("%w: %s", errUnsupportedMode, runMode)
	}
	if err != nil {
		exitErr(err)
	}

	if jsonOut {
		if err := json.NewEncoder(os.Stdout).Encode(res); err != nil {
			exitErr(err)
		}

		return
	}

	resultFmt := "RESULT mode=%s records=%d duration=%s throughput=%.0f/s " +
		"workers=%d batch=%d producers=%d payload=%dB partitioned=%t " +
		"partition_window=%s use_tx=%t\n"
	fmt.Printf(
		resultFmt,
		res.Mode,
		res.Records,
		res.Duration,
		res.Throughput,
		res.Workers,
		res.BatchSize,
		res.Producers,
		res.PayloadBytes,
		res.Partitioned,
		res.PartitionWindow,
		res.UseTx,
	)
}

type benchConfig struct {
	table             string
	records           int
	payload           []byte
	workers           int
	producers         int
	batchSize         int
	partitioned       bool
	partitionAhead    time.Duration
	partitionPeriod   mysql.PartitionPeriod
	partitionLookback time.Duration
	partitionWindow   time.Duration
	resetDuration     time.Duration
	useTx             bool
	seedAge           time.Duration
	seedDays          int
	producerInterval  time.Duration
	mixedDuration     time.Duration
	drainTimeout      time.Duration
	enqueueTimeout    time.Duration
	payloadRandom     bool
	payloadSeed       int64
	measureLatency    bool
	progress          bool
	progressInterval  time.Duration
	autoTarget        bool
}

func validateConsumeWindow(cfg benchConfig) error {
	if cfg.partitionWindow <= 0 {
		return nil
	}

	seedAge := cfg.seedAge
	if seedAge < 0 {
		seedAge = 0
	}
	seedDays := cfg.seedDays
	if seedDays <= 0 {
		seedDays = 1
	}
	if seedAge == 0 && seedDays <= 1 {
		return nil
	}

	newestAge := seedAge
	if seedDays > 1 {
		newestAge = seedAge - time.Duration(seedDays-1)*time.Duration(hoursPerDay)*time.Hour
		if newestAge < 0 {
			newestAge = 0
		}
	}
	if newestAge > cfg.partitionWindow {
		return fmt.Errorf(
			"%w: partition-window=%s newest-age=%s seed-age=%s seed-days=%d",
			errPartitionWindowNoOverlap,
			cfg.partitionWindow,
			newestAge,
			seedAge,
			seedDays,
		)
	}

	return nil
}

func countVisiblePending(ctx context.Context, db *sql.DB, table string, minCreatedAt time.Time) (int64, error) {
	var count int64
	query := "SELECT COUNT(*) FROM " + table + " WHERE status = ?"
	args := []any{outbox.StatusPending}
	if !minCreatedAt.IsZero() {
		query += " AND created_ts >= ?"
		args = append(args, minCreatedAt.UTC().Unix())
	}
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("outbox-bench: count visible pending failed: %w", err)
	}

	return count, nil
}

func resolveConsumeTargets(ctx context.Context, db *sql.DB, cfg benchConfig) (targetRecords, visibleRecords int64, err error) {
	targetRecords = int64(cfg.records)
	if !cfg.autoTarget && cfg.partitionWindow <= 0 {
		visibleRecords = targetRecords

		return targetRecords, visibleRecords, nil
	}

	minCreatedAt := time.Time{}
	if cfg.partitionWindow > 0 {
		minCreatedAt = time.Now().UTC().Add(-cfg.partitionWindow)
	}
	visibleRecords, err = countVisiblePending(ctx, db, cfg.table, minCreatedAt)
	if err != nil {
		return 0, 0, err
	}
	if visibleRecords == 0 {
		return 0, 0, fmt.Errorf("%w: partition-window=%s", errNoVisibleRecords, cfg.partitionWindow)
	}
	if visibleRecords < int64(cfg.records) {
		if !cfg.autoTarget {
			return 0, 0, fmt.Errorf("%w: requested=%d visible=%d partition-window=%s",
				errTargetMismatch, cfg.records, visibleRecords, cfg.partitionWindow)
		}
		fmt.Fprintf(
			os.Stderr,
			"outbox-bench: auto-target adjusted from %d to %d visible rows (partition-window=%s)\n",
			cfg.records,
			visibleRecords,
			cfg.partitionWindow,
		)
		targetRecords = visibleRecords
	}

	return targetRecords, visibleRecords, nil
}

func runConsume(db *sql.DB, store *mysql.Store, cfg benchConfig) (result, error) {
	ctx := context.Background()
	if err := validateConsumeWindow(cfg); err != nil {
		return result{}, err
	}
	seedStart := time.Now()
	if err := seedEntries(ctx, db, store, cfg.records, cfg.payload, defaultSeedBatchSize, cfg.seedAge, cfg.seedDays); err != nil {
		return result{}, err
	}
	seedDuration := time.Since(seedStart)

	targetRecords, visibleRecords, err := resolveConsumeTargets(ctx, db, cfg)
	if err != nil {
		return result{}, err
	}

	ctx, cancel := context.WithCancel(ctx)
	metrics := &benchMetrics{target: targetRecords, cancel: cancel}
	handler := outbox.HandlerFunc(func(_ context.Context, _ outbox.Record) error {
		return nil
	})

	opts := []outbox.RelayOption{
		outbox.WithWorkers(cfg.workers),
		outbox.WithBatchSize(cfg.batchSize),
		outbox.WithPollInterval(0),
		outbox.WithMetrics(metrics),
	}
	if cfg.partitionWindow > 0 {
		opts = append(opts, outbox.WithPartitionWindow(cfg.partitionWindow))
	}

	relay := outbox.NewRelay(store, handler, opts...)
	progress := newProgressPrinter(cfg.progress, cfg.progressInterval)
	if progress.Enabled() {
		progressCtx, progressCancel := context.WithCancel(context.Background())
		go consumeProgress(progressCtx, progress, cfg, metrics, targetRecords)
		defer func() {
			progressCancel()
			progress.Done(progressConsumeDoneLine(cfg, metrics, targetRecords))
		}()
	}
	usageStart := readResourceUsage()
	start := time.Now()
	err = relay.Run(ctx)
	duration := time.Since(start)
	if err != nil && !errors.Is(err, context.Canceled) {
		return result{}, err
	}
	processed := metrics.Processed()
	if processed < targetRecords {
		return result{}, fmt.Errorf("%w: processed %d records, expected %d", errProcessedMismatch, processed, targetRecords)
	}

	throughput := float64(processed) / duration.Seconds()
	batchSnap := metrics.BatchSnapshot()
	dbSnap := dbStatsSnapshot(db)
	usage := readResourceUsage()
	usageDelta := deltaUsage(usageStart, usage)

	return result{
		Mode:                modeConsume,
		Records:             cfg.records,
		VisibleRecords:      visibleRecords,
		TargetRecords:       targetRecords,
		Processed:           processed,
		Duration:            duration,
		ResetDuration:       cfg.resetDuration,
		RunDuration:         duration,
		SeedDuration:        seedDuration,
		Throughput:          throughput,
		Workers:             cfg.workers,
		BatchSize:           cfg.batchSize,
		PayloadBytes:        len(cfg.payload),
		PayloadRandom:       cfg.payloadRandom,
		PayloadSeed:         cfg.payloadSeed,
		Partitioned:         cfg.partitioned,
		PartitionAhead:      cfg.partitionAhead,
		PartitionLookback:   cfg.partitionLookback,
		PartitionWindow:     cfg.partitionWindow,
		EnqueueTimeout:      cfg.enqueueTimeout,
		SeedAge:             cfg.seedAge,
		SeedDays:            cfg.seedDays,
		AutoTarget:          cfg.autoTarget,
		BatchP50Ms:          msFloat(batchSnap.P50),
		BatchP95Ms:          msFloat(batchSnap.P95),
		BatchP99Ms:          msFloat(batchSnap.P99),
		BatchMaxMs:          msFloat(batchSnap.Max),
		BatchMeanMs:         msFloat(batchSnap.Mean),
		BatchSamples:        batchSnap.Count,
		DBWaitCount:         dbSnap.WaitCount,
		DBWaitDurationMs:    msFloat(dbSnap.WaitDuration),
		DBOpenConns:         dbSnap.OpenConns,
		DBInUse:             dbSnap.InUse,
		DBIdle:              dbSnap.Idle,
		DBMaxOpen:           dbSnap.MaxOpen,
		ProcessUserCPU:      usageDelta.UserCPUSeconds,
		ProcessSystemCPU:    usageDelta.SystemCPUSeconds,
		ProcessMaxRSSKB:     usage.MaxRSSKB,
		GoHeapAllocBytes:    usage.GoHeapAllocBytes,
		GoHeapSysBytes:      usage.GoHeapSysBytes,
		GoHeapInuseBytes:    usage.GoHeapInuseBytes,
		GoHeapReleasedBytes: usage.GoHeapReleasedBytes,
		GoStackInuseBytes:   usage.GoStackInuseBytes,
		GoStackSysBytes:     usage.GoStackSysBytes,
		GoTotalAllocBytes:   usageDelta.GoTotalAllocBytes,
		GoNextGCBytes:       usage.GoNextGCBytes,
		GoNumGC:             usageDelta.GoNumGC,
	}, nil
}

func runEnqueue(db *sql.DB, store *mysql.Store, cfg benchConfig) (result, error) {
	ctx := context.Background()
	entriesPerProducer := int(math.Ceil(float64(cfg.records) / float64(cfg.producers)))

	progress := newProgressPrinter(cfg.progress, cfg.progressInterval)
	var produced int64
	if progress.Enabled() {
		progressCtx, progressCancel := context.WithCancel(context.Background())
		go enqueueProgress(progressCtx, progress, cfg, &produced)
		defer func() {
			progressCancel()
			progress.Done(progressEnqueueDoneLine(cfg, &produced))
		}()
	}

	usageStart := readResourceUsage()
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, cfg.producers)
	for i := 0; i < cfg.producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < entriesPerProducer; j++ {
				n := int(atomic.AddInt64(&produced, 1))
				if n > cfg.records {
					break
				}
				if err := enqueueOnce(ctx, db, store, cfg, entryFromPayload(cfg.payload, nil)); err != nil {
					errCh <- err

					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		return result{}, err
	}
	duration := time.Since(start)

	throughput := float64(cfg.records) / duration.Seconds()
	dbSnap := dbStatsSnapshot(db)
	usage := readResourceUsage()
	usageDelta := deltaUsage(usageStart, usage)

	return result{
		Mode:                modeEnqueue,
		Records:             cfg.records,
		TargetRecords:       int64(cfg.records),
		Duration:            duration,
		ResetDuration:       cfg.resetDuration,
		RunDuration:         duration,
		Throughput:          throughput,
		Producers:           cfg.producers,
		PayloadBytes:        len(cfg.payload),
		PayloadRandom:       cfg.payloadRandom,
		PayloadSeed:         cfg.payloadSeed,
		Partitioned:         cfg.partitioned,
		PartitionAhead:      cfg.partitionAhead,
		PartitionLookback:   cfg.partitionLookback,
		EnqueueTimeout:      cfg.enqueueTimeout,
		UseTx:               cfg.useTx,
		AutoTarget:          cfg.autoTarget,
		DBWaitCount:         dbSnap.WaitCount,
		DBWaitDurationMs:    msFloat(dbSnap.WaitDuration),
		DBOpenConns:         dbSnap.OpenConns,
		DBInUse:             dbSnap.InUse,
		DBIdle:              dbSnap.Idle,
		DBMaxOpen:           dbSnap.MaxOpen,
		ProcessUserCPU:      usageDelta.UserCPUSeconds,
		ProcessSystemCPU:    usageDelta.SystemCPUSeconds,
		ProcessMaxRSSKB:     usage.MaxRSSKB,
		GoHeapAllocBytes:    usage.GoHeapAllocBytes,
		GoHeapSysBytes:      usage.GoHeapSysBytes,
		GoHeapInuseBytes:    usage.GoHeapInuseBytes,
		GoHeapReleasedBytes: usage.GoHeapReleasedBytes,
		GoStackInuseBytes:   usage.GoStackInuseBytes,
		GoStackSysBytes:     usage.GoStackSysBytes,
		GoTotalAllocBytes:   usageDelta.GoTotalAllocBytes,
		GoNextGCBytes:       usage.GoNextGCBytes,
		GoNumGC:             usageDelta.GoNumGC,
	}, nil
}

func runMixed(db *sql.DB, store *mysql.Store, cfg benchConfig) (result, error) {
	if cfg.mixedDuration == 0 && cfg.records <= 0 {
		return result{}, errMixedConfigMissing
	}
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var stage atomic.Int32
	stage.Store(int32(mixedStageProduce))
	state := newMixedState(cfg)
	metrics := &benchMetrics{}
	latency := newLatencyStats()
	handler := mixedHandler(cfg, state, latency)
	errCh := startRelay(ctx, store, cfg, handler, metrics)

	progress := newProgressPrinter(cfg.progress, cfg.progressInterval)
	if progress.Enabled() {
		progressCtx, progressCancel := context.WithCancel(context.Background())
		go mixedProgress(progressCtx, progress, cfg, state, &stage)
		defer func() {
			progressCancel()
			progress.Done(progressMixedDoneLine(cfg, state))
		}()
	}

	usageStart := readResourceUsage()
	start := time.Now()
	if err := runProducers(ctx, db, store, cfg, state); err != nil {
		cancel()

		return result{}, err
	}
	stage.Store(int32(mixedStageDrain))

	if err := drainMixed(ctx, cfg, state); err != nil {
		cancel()

		return result{}, err
	}
	stage.Store(int32(mixedStageDone))

	cancel()
	err := <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		return result{}, err
	}
	duration := time.Since(start)

	throughput := float64(state.consumed) / duration.Seconds()
	batchSnap := metrics.BatchSnapshot()
	latSnap := latency.Snapshot()
	dbSnap := dbStatsSnapshot(db)
	usage := readResourceUsage()
	usageDelta := deltaUsage(usageStart, usage)

	return result{
		Mode:                modeMixed,
		Records:             cfg.records,
		TargetRecords:       int64(cfg.records),
		Duration:            duration,
		ResetDuration:       cfg.resetDuration,
		RunDuration:         duration,
		Throughput:          throughput,
		Workers:             cfg.workers,
		BatchSize:           cfg.batchSize,
		Producers:           cfg.producers,
		PayloadBytes:        len(cfg.payload),
		PayloadRandom:       cfg.payloadRandom,
		PayloadSeed:         cfg.payloadSeed,
		Partitioned:         cfg.partitioned,
		PartitionAhead:      cfg.partitionAhead,
		PartitionLookback:   cfg.partitionLookback,
		PartitionWindow:     cfg.partitionWindow,
		ProducerInterval:    cfg.producerInterval,
		MixedDuration:       cfg.mixedDuration,
		DrainTimeout:        cfg.drainTimeout,
		EnqueueTimeout:      cfg.enqueueTimeout,
		UseTx:               cfg.useTx,
		AutoTarget:          cfg.autoTarget,
		Produced:            state.produced,
		Consumed:            state.consumed,
		MaxLag:              state.maxLag,
		LatencyP50Ms:        msFloat(latSnap.P50),
		LatencyP95Ms:        msFloat(latSnap.P95),
		LatencyP99Ms:        msFloat(latSnap.P99),
		LatencyMaxMs:        msFloat(latSnap.Max),
		LatencyMeanMs:       msFloat(latSnap.Mean),
		LatencySamples:      latSnap.Count,
		BatchP50Ms:          msFloat(batchSnap.P50),
		BatchP95Ms:          msFloat(batchSnap.P95),
		BatchP99Ms:          msFloat(batchSnap.P99),
		BatchMaxMs:          msFloat(batchSnap.Max),
		BatchMeanMs:         msFloat(batchSnap.Mean),
		BatchSamples:        batchSnap.Count,
		DBWaitCount:         dbSnap.WaitCount,
		DBWaitDurationMs:    msFloat(dbSnap.WaitDuration),
		DBOpenConns:         dbSnap.OpenConns,
		DBInUse:             dbSnap.InUse,
		DBIdle:              dbSnap.Idle,
		DBMaxOpen:           dbSnap.MaxOpen,
		ProcessUserCPU:      usageDelta.UserCPUSeconds,
		ProcessSystemCPU:    usageDelta.SystemCPUSeconds,
		ProcessMaxRSSKB:     usage.MaxRSSKB,
		GoHeapAllocBytes:    usage.GoHeapAllocBytes,
		GoHeapSysBytes:      usage.GoHeapSysBytes,
		GoHeapInuseBytes:    usage.GoHeapInuseBytes,
		GoHeapReleasedBytes: usage.GoHeapReleasedBytes,
		GoStackInuseBytes:   usage.GoStackInuseBytes,
		GoStackSysBytes:     usage.GoStackSysBytes,
		GoTotalAllocBytes:   usageDelta.GoTotalAllocBytes,
		GoNextGCBytes:       usage.GoNextGCBytes,
		GoNumGC:             usageDelta.GoNumGC,
	}, nil
}

type mixedState struct {
	produced  int64
	consumed  int64
	maxLag    int64
	remaining int64
}

type mixedStage int32

const (
	mixedStageProduce mixedStage = iota
	mixedStageDrain
	mixedStageDone
)

func newMixedState(cfg benchConfig) *mixedState {
	state := &mixedState{}
	if cfg.mixedDuration == 0 && cfg.records > 0 {
		state.remaining = int64(cfg.records)
	}

	return state
}

func mixedHandler(cfg benchConfig, state *mixedState, latency *latencyStats) outbox.Handler {
	return outbox.HandlerFunc(func(_ context.Context, record outbox.Record) error {
		if cfg.measureLatency {
			ts := recordTimestamp(record)
			delta := time.Since(ts)
			if delta < 0 {
				delta = 0
			}
			latency.Record(delta)
		}
		newConsumed := atomic.AddInt64(&state.consumed, 1)
		updateMaxLag(&state.maxLag, atomic.LoadInt64(&state.produced), newConsumed)

		return nil
	})
}

func startRelay(ctx context.Context, store *mysql.Store, cfg benchConfig, handler outbox.Handler, metrics *benchMetrics) <-chan error {
	opts := []outbox.RelayOption{
		outbox.WithWorkers(cfg.workers),
		outbox.WithBatchSize(cfg.batchSize),
		outbox.WithPollInterval(0),
		outbox.WithMetrics(metrics),
	}
	if cfg.partitionWindow > 0 {
		opts = append(opts, outbox.WithPartitionWindow(cfg.partitionWindow))
	}

	relay := outbox.NewRelay(store, handler, opts...)
	errCh := make(chan error, 1)
	go func() {
		errCh <- relay.Run(ctx)
	}()

	return errCh
}

func runProducers(ctx context.Context, db *sql.DB, store *mysql.Store, cfg benchConfig, state *mixedState) error {
	produceCtx := ctx
	produceCancel := func() {}
	if cfg.mixedDuration > 0 {
		produceCtx, produceCancel = context.WithTimeout(ctx, cfg.mixedDuration)
	}
	defer produceCancel()

	var wg sync.WaitGroup
	prodErrCh := make(chan error, cfg.producers)
	for i := 0; i < cfg.producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			producerLoop(produceCtx, db, store, cfg, state, prodErrCh)
		}()
	}
	wg.Wait()
	close(prodErrCh)
	if err := <-prodErrCh; err != nil {
		return err
	}

	return nil
}

func producerLoop(
	ctx context.Context,
	db *sql.DB,
	store *mysql.Store,
	cfg benchConfig,
	state *mixedState,
	errCh chan<- error,
) {
	var ticker *time.Ticker
	if cfg.producerInterval > 0 {
		ticker = time.NewTicker(cfg.producerInterval)
		defer ticker.Stop()
	}
loop:
	for ctx.Err() == nil {
		if cfg.mixedDuration == 0 && cfg.records > 0 {
			if atomic.AddInt64(&state.remaining, -1) < 0 {
				break
			}
		}
		if ticker != nil {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				break loop
			}
		}
		headers := buildHeaders(time.Now().UnixNano())
		if err := enqueueOnce(ctx, db, store, cfg, entryFromPayload(cfg.payload, headers)); err != nil {
			errCh <- err

			return
		}
		newProduced := atomic.AddInt64(&state.produced, 1)
		updateMaxLag(&state.maxLag, newProduced, atomic.LoadInt64(&state.consumed))
	}
}

func drainMixed(ctx context.Context, cfg benchConfig, state *mixedState) error {
	if cfg.drainTimeout <= 0 {
		return nil
	}
	drainCtx, drainCancel := context.WithTimeout(ctx, cfg.drainTimeout)
	defer drainCancel()
	for atomic.LoadInt64(&state.consumed) < atomic.LoadInt64(&state.produced) {
		select {
		case <-drainCtx.Done():
			return fmt.Errorf("drain timeout exceeded: %w", drainCtx.Err())
		default:
			time.Sleep(defaultDrainPoll)
		}
	}

	return nil
}

func resetTable(
	ctx context.Context,
	db *sql.DB,
	table string,
	partitioned bool,
	period mysql.PartitionPeriod,
	ahead, lookback time.Duration,
) error {
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	if partitioned {
		parts := buildPartitions(period, ahead, lookback)
		stmt, err := mysql.PartitionedSchema(table, parts)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("create partitioned schema: %w", err)
		}

		return nil
	}

	stmt, err := mysql.Schema(table)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	return nil
}

func buildPartitions(period mysql.PartitionPeriod, ahead, lookback time.Duration) []mysql.Partition {
	now := time.Now().UTC()
	if ahead < 0 {
		ahead = 0
	}
	if lookback < 0 {
		lookback = 0
	}
	start := periodStart(now.Add(-lookback), period)
	steps := periodSteps(period, ahead+lookback)
	parts := make([]mysql.Partition, 0, steps+partitionExtraSlots)
	for i := 0; i <= steps; i++ {
		next := nextPeriod(start, period)
		parts = append(parts, mysql.Partition{
			Name:     partitionName(period, start),
			LessThan: strconv.FormatInt(next.Unix(), 10),
		})
		start = next
	}
	parts = append(parts, mysql.Partition{Name: "pmax", LessThan: "MAXVALUE"})

	return parts
}

func seedEntries(
	ctx context.Context,
	db *sql.DB,
	store *mysql.Store,
	total int,
	payload []byte,
	batchSize int,
	seedAge time.Duration,
	seedDays int,
) error {
	return seedEntriesWithOptions(ctx, db, store, total, payload, batchSize, seedAge, seedDays)
}

func seedEntriesWithOptions(
	ctx context.Context,
	db *sql.DB,
	store *mysql.Store,
	total int,
	payload []byte,
	batchSize int,
	seedAge time.Duration,
	seedDays int,
) error {
	if seedAge == 0 && seedDays <= 1 {
		return seedEntriesDefault(ctx, db, store, total, payload, batchSize)
	}
	if seedDays <= 0 {
		seedDays = 1
	}
	if seedAge < 0 {
		seedAge = 0
	}

	base := time.Now().UTC()
	if seedAge > 0 {
		base = base.Add(-seedAge)
	} else if seedDays > 1 {
		base = base.Add(-time.Duration(seedDays-1) * 24 * time.Hour)
	}
	perDay := total / seedDays
	remainder := total % seedDays
	for day := 0; day < seedDays; day++ {
		count := perDay
		if day < remainder {
			count++
		}
		if count == 0 {
			continue
		}
		clock := &stepClock{next: base.Add(time.Duration(day) * time.Duration(hoursPerDay) * time.Hour), step: time.Millisecond}
		gen := outbox.NewUUIDv7Generator(clock)
		if err := seedEntriesWithGenerator(ctx, db, store, count, payload, batchSize, gen); err != nil {
			err = fmt.Errorf("seed entries with generator: %w", err)

			return err
		}
	}

	return nil
}

func seedEntriesDefault(ctx context.Context, db *sql.DB, store *mysql.Store, total int, payload []byte, batchSize int) error {
	inserted := 0
	for inserted < total {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for i := 0; i < batchSize && inserted < total; i++ {
			if _, err := store.Enqueue(ctx, tx, entryFromPayload(payload, nil)); err != nil {
				_ = tx.Rollback()

				return err
			}
			inserted++
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func seedEntriesWithGenerator(
	ctx context.Context,
	db *sql.DB,
	store *mysql.Store,
	total int,
	payload []byte,
	batchSize int,
	gen outbox.IDGenerator,
) error {
	inserted := 0
	for inserted < total {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for i := 0; i < batchSize && inserted < total; i++ {
			id, err := gen.New()
			if err != nil {
				_ = tx.Rollback()
				err = fmt.Errorf("seed entries: generate id failed: %w", err)

				return err
			}
			entry := entryFromPayload(payload, nil)
			entry.ID = id
			if _, err := store.Enqueue(ctx, tx, entry); err != nil {
				_ = tx.Rollback()
				err = fmt.Errorf("seed entries: enqueue failed: %w", err)

				return err
			}
			inserted++
		}
		if err := tx.Commit(); err != nil {
			err = fmt.Errorf("seed entries: commit failed: %w", err)

			return err
		}
	}

	return nil
}

type stepClock struct {
	mu   sync.Mutex
	next time.Time
	step time.Duration
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.next
	c.next = c.next.Add(c.step)

	return t
}

type benchMetrics struct {
	processed int64
	target    int64
	cancel    func()
	batch     batchStats
}

func (m *benchMetrics) ObserveBatchDuration(d time.Duration) {
	m.batch.Add(d)
}

func (m *benchMetrics) AddProcessed(n int) {
	if n == 0 {
		return
	}
	total := atomic.AddInt64(&m.processed, int64(n))
	if m.target > 0 && m.cancel != nil && total >= m.target {
		m.cancel()
	}
}

func (m *benchMetrics) AddErrors(int)  {}
func (m *benchMetrics) AddRetries(int) {}
func (m *benchMetrics) AddDead(int)    {}
func (m *benchMetrics) SetPending(int) {}

func (m *benchMetrics) Processed() int64 {
	return atomic.LoadInt64(&m.processed)
}

func (m *benchMetrics) BatchSnapshot() batchSnapshot {
	return m.batch.Snapshot()
}

type batchStats struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (b *batchStats) Add(d time.Duration) {
	if d <= 0 {
		return
	}
	b.mu.Lock()
	b.samples = append(b.samples, d)
	b.mu.Unlock()
}

func (b *batchStats) Snapshot() batchSnapshot {
	b.mu.Lock()
	samples := append([]time.Duration(nil), b.samples...)
	b.mu.Unlock()
	if len(samples) == 0 {
		return batchSnapshot{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	return batchSnapshot{
		P50:   percentile(samples, percentileP50),
		P95:   percentile(samples, percentileP95),
		P99:   percentile(samples, percentileP99),
		Max:   samples[len(samples)-1],
		Mean:  meanDuration(samples),
		Count: len(samples),
	}
}

type batchSnapshot struct {
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Max   time.Duration
	Mean  time.Duration
	Count int
}

type latencyStats struct {
	mu      sync.Mutex
	samples []time.Duration
}

func newLatencyStats() *latencyStats {
	return &latencyStats{}
}

func (l *latencyStats) Record(d time.Duration) {
	if d <= 0 {
		return
	}
	l.mu.Lock()
	l.samples = append(l.samples, d)
	l.mu.Unlock()
}

func (l *latencyStats) Snapshot() latencySnapshot {
	l.mu.Lock()
	samples := append([]time.Duration(nil), l.samples...)
	l.mu.Unlock()
	if len(samples) == 0 {
		return latencySnapshot{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	return latencySnapshot{
		P50:   percentile(samples, percentileP50),
		P95:   percentile(samples, percentileP95),
		P99:   percentile(samples, percentileP99),
		Max:   samples[len(samples)-1],
		Mean:  meanDuration(samples),
		Count: int64(len(samples)),
	}
}

type latencySnapshot struct {
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Max   time.Duration
	Mean  time.Duration
	Count int64
}

func percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(samples)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(samples) {
		idx = len(samples) - 1
	}

	return samples[idx]
}

func meanDuration(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range samples {
		sum += d
	}

	return sum / time.Duration(len(samples))
}

type resourceUsage struct {
	UserCPUSeconds      float64
	SystemCPUSeconds    float64
	MaxRSSKB            int64
	GoHeapAllocBytes    uint64
	GoHeapSysBytes      uint64
	GoHeapInuseBytes    uint64
	GoHeapReleasedBytes uint64
	GoStackInuseBytes   uint64
	GoStackSysBytes     uint64
	GoTotalAllocBytes   uint64
	GoNextGCBytes       uint64
	GoNumGC             uint32
}

func readResourceUsage() resourceUsage {
	var usage resourceUsage

	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err == nil {
		usage.UserCPUSeconds = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/microsecondsPerSecond
		usage.SystemCPUSeconds = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/microsecondsPerSecond
		usage.MaxRSSKB = ru.Maxrss
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	usage.GoHeapAllocBytes = ms.HeapAlloc
	usage.GoHeapSysBytes = ms.HeapSys
	usage.GoHeapInuseBytes = ms.HeapInuse
	usage.GoHeapReleasedBytes = ms.HeapReleased
	usage.GoStackInuseBytes = ms.StackInuse
	usage.GoStackSysBytes = ms.StackSys
	usage.GoTotalAllocBytes = ms.TotalAlloc
	usage.GoNextGCBytes = ms.NextGC
	usage.GoNumGC = ms.NumGC

	return usage
}

type dbStats struct {
	WaitCount    int64
	WaitDuration time.Duration
	OpenConns    int
	InUse        int
	Idle         int
	MaxOpen      int
}

func dbStatsSnapshot(db *sql.DB) dbStats {
	stats := db.Stats()

	return dbStats{
		WaitCount:    stats.WaitCount,
		WaitDuration: stats.WaitDuration,
		OpenConns:    stats.OpenConnections,
		InUse:        stats.InUse,
		Idle:         stats.Idle,
		MaxOpen:      stats.MaxOpenConnections,
	}
}

type usageDelta struct {
	UserCPUSeconds    float64
	SystemCPUSeconds  float64
	GoTotalAllocBytes uint64
	GoNumGC           uint32
}

func deltaUsage(start, end resourceUsage) usageDelta {
	return usageDelta{
		UserCPUSeconds:    end.UserCPUSeconds - start.UserCPUSeconds,
		SystemCPUSeconds:  end.SystemCPUSeconds - start.SystemCPUSeconds,
		GoTotalAllocBytes: end.GoTotalAllocBytes - start.GoTotalAllocBytes,
		GoNumGC:           end.GoNumGC - start.GoNumGC,
	}
}

type headerTimestamp struct {
	T int64 `json:"t"`
}

func buildHeaders(ts int64) []byte {
	return []byte(fmt.Sprintf("{\"t\":%d}", ts))
}

func recordTimestamp(record outbox.Record) time.Time {
	if len(record.Headers) > 0 {
		var header headerTimestamp
		if err := json.Unmarshal(record.Headers, &header); err == nil && header.T > 0 {
			return time.Unix(0, header.T)
		}
	}
	if !record.CreatedAt.IsZero() {
		return record.CreatedAt
	}

	return time.Now()
}

func enqueueOnce(ctx context.Context, db *sql.DB, store *mysql.Store, cfg benchConfig, entry outbox.Entry) error {
	opCtx := ctx
	cancel := func() {}
	if cfg.enqueueTimeout > 0 {
		opCtx, cancel = context.WithTimeout(ctx, cfg.enqueueTimeout)
	}
	defer cancel()

	if cfg.useTx {
		tx, err := db.BeginTx(opCtx, nil)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		if _, err := store.Enqueue(opCtx, tx, entry); err != nil {
			_ = tx.Rollback()

			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		return nil
	}
	_, err := store.Enqueue(opCtx, db, entry)

	return err
}

func updateMaxLag(maxLag *int64, produced, consumed int64) {
	lag := produced - consumed
	for {
		current := atomic.LoadInt64(maxLag)
		if lag <= current {
			return
		}
		if atomic.CompareAndSwapInt64(maxLag, current, lag) {
			return
		}
	}
}

func msFloat(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func entryFromPayload(payload, headers []byte) outbox.Entry {
	return outbox.Entry{
		AggregateType: "bench",
		AggregateID:   "1",
		EventType:     "bench.created",
		Payload:       payload,
		Headers:       headers,
	}
}

type progressPrinter struct {
	enabled  bool
	interval time.Duration
	isTTY    bool
	mu       sync.Mutex
	lastLen  int
}

func newProgressPrinter(enabled bool, interval time.Duration) *progressPrinter {
	tty := false
	if info, err := os.Stderr.Stat(); err == nil {
		tty = (info.Mode() & os.ModeCharDevice) != 0
	}

	return &progressPrinter{
		enabled:  enabled,
		interval: interval,
		isTTY:    tty,
	}
}

func (p *progressPrinter) Enabled() bool {
	return p.enabled && p.interval > 0
}

func (p *progressPrinter) Print(line string) {
	if !p.Enabled() || line == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.print(line, false)
}

func (p *progressPrinter) Done(line string) {
	if !p.Enabled() || line == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.print(line, true)
}

func (p *progressPrinter) print(line string, final bool) {
	padding := ""
	if p.lastLen > len(line) {
		padding = strings.Repeat(" ", p.lastLen-len(line))
	}
	switch {
	case p.isTTY && final:
		fmt.Fprintf(os.Stderr, "\r%s%s\n", line, padding)
	case p.isTTY:
		fmt.Fprintf(os.Stderr, "\r%s%s", line, padding)
	case final:
		fmt.Fprintf(os.Stderr, "%s\n", line)
	default:
		fmt.Fprintf(os.Stderr, "\r%s", line)
	}
	p.lastLen = len(line)
}

func shortDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}

	return d.Truncate(time.Second).String()
}

func consumeProgress(ctx context.Context, printer *progressPrinter, cfg benchConfig, metrics *benchMetrics, target int64) {
	ticker := time.NewTicker(printer.interval)
	defer ticker.Stop()
	start := time.Now()
	lastCount := metrics.Processed()
	lastChange := start

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			current := metrics.Processed()
			if current != lastCount {
				lastCount = current
				lastChange = now
			}
			elapsed := now.Sub(start)
			rate := 0.0
			if elapsed > 0 {
				rate = float64(current) / elapsed.Seconds()
			}
			percent := 0.0
			if target > 0 {
				percent = float64(current) / float64(target) * percentScale
			}
			line := fmt.Sprintf(
				"consume: %d/%d (%.1f%%) rate=%.0f/s stall=%s workers=%d batch=%d window=%s",
				current,
				target,
				percent,
				rate,
				shortDuration(now.Sub(lastChange)),
				cfg.workers,
				cfg.batchSize,
				shortDuration(cfg.partitionWindow),
			)
			printer.Print(line)
		}
	}
}

func progressConsumeDoneLine(cfg benchConfig, metrics *benchMetrics, target int64) string {
	current := metrics.Processed()
	percent := 0.0
	if target > 0 {
		percent = float64(current) / float64(target) * percentScale
	}

	return fmt.Sprintf(
		"consume: %d/%d (%.1f%%) done workers=%d batch=%d window=%s",
		current,
		target,
		percent,
		cfg.workers,
		cfg.batchSize,
		shortDuration(cfg.partitionWindow),
	)
}

func enqueueProgress(ctx context.Context, printer *progressPrinter, cfg benchConfig, produced *int64) {
	ticker := time.NewTicker(printer.interval)
	defer ticker.Stop()
	start := time.Now()
	lastCount := atomic.LoadInt64(produced)
	lastChange := start

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			current := atomic.LoadInt64(produced)
			if current != lastCount {
				lastCount = current
				lastChange = now
			}
			elapsed := now.Sub(start)
			rate := 0.0
			if elapsed > 0 {
				rate = float64(current) / elapsed.Seconds()
			}
			percent := 0.0
			if cfg.records > 0 {
				percent = float64(current) / float64(cfg.records) * percentScale
			}
			line := fmt.Sprintf(
				"enqueue: %d/%d (%.1f%%) rate=%.0f/s stall=%s producers=%d use_tx=%t",
				current,
				cfg.records,
				percent,
				rate,
				shortDuration(now.Sub(lastChange)),
				cfg.producers,
				cfg.useTx,
			)
			printer.Print(line)
		}
	}
}

func progressEnqueueDoneLine(cfg benchConfig, produced *int64) string {
	current := atomic.LoadInt64(produced)
	percent := 0.0
	if cfg.records > 0 {
		percent = float64(current) / float64(cfg.records) * percentScale
	}

	return fmt.Sprintf(
		"enqueue: %d/%d (%.1f%%) done producers=%d use_tx=%t",
		current,
		cfg.records,
		percent,
		cfg.producers,
		cfg.useTx,
	)
}

func mixedProgress(ctx context.Context, printer *progressPrinter, cfg benchConfig, state *mixedState, stage *atomic.Int32) {
	ticker := time.NewTicker(printer.interval)
	defer ticker.Stop()
	start := time.Now()
	lastCount := atomic.LoadInt64(&state.consumed)
	lastChange := start

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			consumed := atomic.LoadInt64(&state.consumed)
			produced := atomic.LoadInt64(&state.produced)
			lag := produced - consumed
			if consumed != lastCount {
				lastCount = consumed
				lastChange = now
			}
			elapsed := now.Sub(start)
			rate := 0.0
			if elapsed > 0 {
				rate = float64(consumed) / elapsed.Seconds()
			}
			percent := 0.0
			if cfg.records > 0 {
				percent = float64(consumed) / float64(cfg.records) * percentScale
			}
			line := fmt.Sprintf(
				"mixed(%s): produced=%d consumed=%d/%d (%.1f%%) lag=%d rate=%.0f/s stall=%s",
				mixedStageLabel(stage.Load()),
				produced,
				consumed,
				cfg.records,
				percent,
				lag,
				rate,
				shortDuration(now.Sub(lastChange)),
			)
			printer.Print(line)
		}
	}
}

func progressMixedDoneLine(cfg benchConfig, state *mixedState) string {
	consumed := atomic.LoadInt64(&state.consumed)
	produced := atomic.LoadInt64(&state.produced)
	percent := 0.0
	if cfg.records > 0 {
		percent = float64(consumed) / float64(cfg.records) * percentScale
	}

	return fmt.Sprintf(
		"mixed(done): produced=%d consumed=%d/%d (%.1f%%) lag=%d",
		produced,
		consumed,
		cfg.records,
		percent,
		produced-consumed,
	)
}

func mixedStageLabel(stage int32) string {
	switch mixedStage(stage) {
	case mixedStageDrain:
		return "drain"
	case mixedStageDone:
		return "done"
	default:
		return "produce"
	}
}

func buildPayload(size int, random bool, rng *rand.Rand) []byte {
	if size <= 0 {
		return []byte(`{"data":""}`)
	}
	dataSize := maxInt(0, size-len(`{"data":""}`))
	data := make([]byte, dataSize)
	if random {
		if rng == nil {
			// #nosec G404 -- deterministic RNG for benchmark payloads.
			rng = rand.New(rand.NewSource(time.Now().UnixNano()))
		}
		const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		for i := range data {
			data[i] = alphabet[rng.Intn(len(alphabet))]
		}
	} else {
		for i := range data {
			data[i] = 'a'
		}
	}

	return []byte(fmt.Sprintf(`{"data":%q}`, string(data)))
}

func parseMode(value string) (mode, error) {
	switch value {
	case "consume":
		return modeConsume, nil
	case "enqueue":
		return modeEnqueue, nil
	case "mixed":
		return modeMixed, nil
	default:
		return "", fmt.Errorf("%w: %s", errInvalidMode, value)
	}
}

func parsePeriod(value string) (mysql.PartitionPeriod, error) {
	switch value {
	case "day":
		return mysql.PartitionDay, nil
	case "month":
		return mysql.PartitionMonth, nil
	default:
		return 0, fmt.Errorf("%w: %s", errInvalidPeriod, value)
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}

	return b
}

func sanitizeTableName(name string) (string, error) {
	if name == "" {
		return "", errTableRequired
	}
	parts := strings.Split(name, ".")
	for _, part := range parts {
		if part == "" {
			return "", fmt.Errorf("%w: %s", errInvalidTableName, name)
		}
		for _, r := range part {
			if r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				continue
			}

			return "", fmt.Errorf("%w: %s", errInvalidTableName, name)
		}
	}

	return name, nil
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func periodStart(t time.Time, period mysql.PartitionPeriod) time.Time {
	t = t.UTC()
	switch period {
	case mysql.PartitionDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case mysql.PartitionMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	default:
		return t
	}
}

func nextPeriod(t time.Time, period mysql.PartitionPeriod) time.Time {
	switch period {
	case mysql.PartitionDay:
		return t.AddDate(0, 0, 1)
	case mysql.PartitionMonth:
		return t.AddDate(0, 1, 0)
	default:
		return t
	}
}

func periodSteps(period mysql.PartitionPeriod, ahead time.Duration) int {
	if ahead <= 0 {
		return 0
	}
	switch period {
	case mysql.PartitionDay:
		return int(math.Ceil(ahead.Hours() / hoursPerDay))
	case mysql.PartitionMonth:
		return int(math.Ceil(ahead.Hours() / (hoursPerDay * daysPerMonth)))
	default:
		return 0
	}
}

func partitionName(period mysql.PartitionPeriod, start time.Time) string {
	switch period {
	case mysql.PartitionMonth:
		return fmt.Sprintf("p%04d%02d", start.Year(), int(start.Month()))
	default:
		return fmt.Sprintf("p%04d%02d%02d", start.Year(), int(start.Month()), start.Day())
	}
}
