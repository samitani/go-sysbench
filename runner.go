package sysbench

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	histogramSize = 1024
	histogramMin  = 0.001
	histogramMax  = 100000

	nano2mili = 1000000.0
	nano2sec  = 1000000000.0
)

type (
	Benchmark interface {
		// initialize before both Prepare and Event
		Init(context.Context) error
		// finalize after both Prepare and Event
		Done() error
		// when prepare command is issued
		Prepare(context.Context) error
		// when run command is issued, PreEvent() is called once in a benchmark
		PreEvent(context.Context) error
		// when run command is issued, Event() is called in a loop
		Event(context.Context) (numReads, numWrites, numOthers, numIgnoredErros uint64, err error)
	}

	RunnerOpts struct {
		Threads        int    `long:"threads" description:"number of threads to use" default:"1"`
		Events         uint64 `long:"events" description:"limit for total number of events" default:"0"`
		Time           int    `long:"time" description:"limit for total execution time in seconds" default:"10"`
		ReportInterval int    `long:"report-interval" description:"periodically report intermediate statistics with a specified interval in seconds. 0 disables intermediate reports" default:"0"`
		Histogram      string `long:"histogram" choice:"on" choice:"off" description:"print latency histogram in report" default:"off"` //nolint:staticcheck
		Percentile     int    `long:"percentile" description:"percentile to calculate in latency statistics (1-100)" default:"95"`
	}

	Runner struct {
		opts  *RunnerOpts
		bench *benchmarkAdapter
	}

	benchmarkAdapter struct {
		bench Benchmark
	}
)

func (a *benchmarkAdapter) Init(ctx context.Context) error {
	return a.bench.Init(ctx)
}

func (a *benchmarkAdapter) Done() error {
	return a.bench.Done()
}

func (a *benchmarkAdapter) Prepare(ctx context.Context) error {
	return a.bench.Prepare(ctx)
}

func (a *benchmarkAdapter) PreEvent(ctx context.Context) error {
	return a.bench.PreEvent(ctx)
}

func (a *benchmarkAdapter) Event(ctx context.Context) (uint64, uint64, uint64, uint64, error) {
	return a.bench.Event(ctx)
}

func NewRunner(option *RunnerOpts, bench Benchmark) *Runner {
	return &Runner{option, &benchmarkAdapter{bench}}
}

func (r *Runner) Prepare() error {
	ctx := context.Background()

	err := r.bench.Init(ctx)
	if err != nil {
		return err
	}

	err = r.bench.Prepare(ctx)
	if err != nil {
		return err
	}

	err = r.bench.Done()
	if err != nil {
		return err
	}

	return nil
}

func (r *Runner) Run() error {
	// global shared stats
	var totalQueries, totalTransactions atomic.Uint64
	var totalReads, totalWrites, totalOthers, totalIgnoredErrors atomic.Uint64
	var totalEventCalls atomic.Uint64
	var latencyNanoMin atomic.Uint64
	var latencyNanoMax atomic.Uint64
	var latencyNanoSum atomic.Uint64

	// per thread stats
	var pTtotalTransactions []uint64
	var pTlatencyNanoSum []uint64

	// initialize stats
	latencyNanoMin.Store(math.MaxUint64)
	pTtotalTransactions = make([]uint64, r.opts.Threads)
	pTlatencyNanoSum = make([]uint64, r.opts.Threads)

	histogram := NewHistogram(histogramSize, histogramMin, histogramMax)
	intervalHistogram := NewHistogram(histogramSize, histogramMin, histogramMax)

	fmt.Println("Running the test with following options:")
	fmt.Printf("Number of threads: %d\n", r.opts.Threads)

	if r.opts.Percentile > 100 {
		return fmt.Errorf("--percentile should be <= 100")
	}

	if r.opts.ReportInterval > 0 {
		fmt.Printf("Report intermediate results every %d second(s)\n\n\n", r.opts.ReportInterval)
	}

	var percentile = r.opts.Percentile

	err := r.bench.Init(context.Background())
	if err != nil {
		return err
	}

	err = r.bench.PreEvent(context.Background())
	if err != nil {
		return err
	}

	begin := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.opts.Time)*time.Second)
	defer cancel()

	// goroutine for reporting
	if r.opts.ReportInterval > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(r.opts.ReportInterval) * time.Second)
			intervalf := float64(r.opts.ReportInterval)

			defer ticker.Stop()

			var lastQueries, lastTransactions, lastReads, lastWrites, lastOthers, lastIgnoredErrors uint64

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					deltaQueries := totalQueries.Load() - lastQueries
					deltaTransactions := totalTransactions.Load() - lastTransactions
					deltaReads := totalReads.Load() - lastReads
					deltaWrites := totalWrites.Load() - lastWrites
					deltaOthers := totalOthers.Load() - lastOthers
					deltaIgnoredErrors := totalIgnoredErrors.Load() - lastIgnoredErrors

					fmt.Printf("[ %.0fs ] thds: %d tps: %4.2f qps: %4.2f (r/w/o: %4.2f/%4.2f/%4.2f) lat (ms,%d%%): %4.2f err/s %4.2f reconn/s: N/A\n",
						time.Since(begin).Seconds(),
						r.opts.Threads,
						float64(deltaTransactions)/intervalf,
						float64(deltaQueries)/intervalf,
						float64(deltaReads)/intervalf,
						float64(deltaWrites)/intervalf,
						float64(deltaOthers)/intervalf,
						percentile,
						intervalHistogram.GetPercentileAndReset(percentile), // percentile
						float64(deltaIgnoredErrors)/intervalf)

					lastQueries = totalQueries.Load()
					lastTransactions = totalTransactions.Load()
					lastReads = totalReads.Load()
					lastWrites = totalWrites.Load()
					lastOthers = totalOthers.Load()
					lastIgnoredErrors = totalIgnoredErrors.Load()
				}
			}
		}()
	}

	var wg sync.WaitGroup

	for i := 0; i < r.opts.Threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			//var pe error = nil
			var eventBegin time.Time

			for {
				select {
				case <-ctx.Done():
					return
				default:
					totalEventCalls.Add(1)
					// skip execution if number of events reaches the --event
					if r.opts.Events > 0 && totalEventCalls.Load() > r.opts.Events {
						return
					}

					eventBegin = time.Now()
					reads, writes, others, igerrs, err := r.bench.Event(ctx)
					if err != nil && err != context.DeadlineExceeded && err != context.Canceled && err != sql.ErrTxDone {
						fmt.Println(err)
						cancel()
						return
					}
					latency := uint64(time.Since(eventBegin).Nanoseconds())

					totalQueries.Add(reads + writes + others)
					totalReads.Add(reads)
					totalWrites.Add(writes)
					totalOthers.Add(others)
					totalIgnoredErrors.Add(igerrs)

					// count transaction only if all queries are suceeded.
					if igerrs == 0 {
						pTtotalTransactions[i] += 1
						totalTransactions.Add(1)

						pTlatencyNanoSum[i] += latency
						latencyNanoSum.Add(latency)

						if latency < latencyNanoMin.Load() {
							latencyNanoMin.Store(latency)
						}
						if latency > latencyNanoMax.Load() {
							latencyNanoMax.Store(latency)
						}

						intervalHistogram.Add(float64(latency) / nano2mili)
						histogram.Add(float64(latency) / nano2mili)
					}

					// wait until all events finished, then cancel()
					if r.opts.Events > 0 && totalTransactions.Load() == r.opts.Events {
						cancel()
					}
				}
			}
		}()
	}

	// signal handler
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
	case <-sigchan:
		fmt.Println("\nShutdown signal received. Exiting...")
		cancel()
	}

	wg.Wait()
	totalTime := time.Since(begin).Seconds()

	err = r.bench.Done()
	if err != nil {
		return err
	}

	if r.opts.Histogram == "on" {
		fmt.Println("Latency histogram (values are in milliseconds)")
		histogram.Print()
		fmt.Println("")
	}

	fmt.Printf("SQL statistics:\n"+
		"    queries performed:\n"+
		"        read:                            %d\n"+
		"        write:                           %d\n"+
		"        other:                           %d\n"+
		"        total:                           %d\n"+
		"    transactions:                        %-6d (%.2f per sec.)\n"+
		"    queries:                             %-6d (%.2f per sec.)\n"+
		"    ignored errors:                      %-6d (%.2f per sec.)\n"+
		"    reconnects:                          N/A    (N/A per sec.)\n\n",
		totalReads.Load(), totalWrites.Load(), totalOthers.Load(), (totalReads.Load() + totalWrites.Load() + totalOthers.Load()),
		totalTransactions.Load(), float64(totalTransactions.Load())/float64(totalTime), totalQueries.Load(), float64(totalQueries.Load())/float64(totalTime),
		totalIgnoredErrors.Load(), float64(totalIgnoredErrors.Load())/float64(totalTime))

	fmt.Printf("General statistics:\n"+
		"    total time:                          %.4fs\n"+
		"    total number of events:              %d\n\n", totalTime, totalTransactions.Load())

	fmt.Printf("Latency (ms):\n"+
		"         min: %39.2f\n"+
		"         avg: %39.2f\n"+
		"         max: %39.2f\n"+
		"         %dth percentile: %27.2f\n"+
		"         sum: %39.2f\n\n",
		float64(latencyNanoMin.Load())/nano2mili,
		(float64(latencyNanoSum.Load())/nano2mili)/float64(totalTransactions.Load()),
		float64(latencyNanoMax.Load())/nano2mili,
		percentile,
		histogram.Percentile(percentile),
		float64(latencyNanoSum.Load())/nano2mili)

	var transactionsAvg, tranasctionsStddev float64
	var latencyNanoAvg, latencyNanoStddev float64

	transactionsAvg = float64(totalTransactions.Load()) / float64(r.opts.Threads)
	latencyNanoAvg = (float64(latencyNanoSum.Load())) / float64(r.opts.Threads)

	for i := 0; i < r.opts.Threads; i++ {
		diffT := math.Abs(transactionsAvg - float64(pTtotalTransactions[i]))
		tranasctionsStddev += diffT * diffT

		diffL := math.Abs(latencyNanoAvg - float64(pTlatencyNanoSum[i]))
		latencyNanoStddev += diffL * diffL
	}
	tranasctionsStddev = math.Sqrt(tranasctionsStddev / float64(r.opts.Threads))
	latencyNanoStddev = math.Sqrt(latencyNanoStddev / float64(r.opts.Threads))

	fmt.Printf("Threads fairness (Event distribution by threads):\n"+
		"    events (avg/stddev):           %.4f/%3.2f\n"+
		"    execution time (avg/stddev):   %.4f/%3.2f\n", transactionsAvg, tranasctionsStddev, float64(latencyNanoAvg)/nano2sec, float64(latencyNanoStddev)/nano2sec)

	return nil
}
