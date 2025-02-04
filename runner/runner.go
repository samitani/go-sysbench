package runner

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/samitani/go-sysbench/benchmark"
)

const (
	histogramSize = 1024
	histogramMin  = 0.001
	histogramMax  = 100000
)

type (
	RunnerOpts struct {
		Threads        int    `long:"threads" description:"number of threads to use" default:"1"`
		Events         uint64 `long:"events" description:"limit for total number of events" default:"0"`
		Time           int    `long:"time" description:"limit for total execution time in seconds" default:"10"`
		ReportInterval int    `long:"report-interval" description:"periodically report intermediate statistics with a specified interval in seconds. 0 disables intermediate reports" default:"0"`
		Histogram      string `long:"histogram" choice:"on" choice:"off" description:"print latency histogram in report" default:"off"` //nolint:staticcheck
		Percentile     int    `long:"percentile" description:"percentile to calculate in latency statistics (1-100)" default:"95"`
	}

	Runner struct {
		opts *RunnerOpts
	}
)

func NewRunner(option *RunnerOpts) *Runner {
	return &Runner{option}
}

func (r *Runner) Prepare(bench benchmark.Benchmark) error {
	ctx := context.Background()

	err := bench.Init(ctx)
	if err != nil {
		return err
	}

	err = bench.Prepare(ctx)
	if err != nil {
		return err
	}

	err = bench.Done()
	if err != nil {
		return err
	}

	return nil
}

func (r *Runner) Run(bench benchmark.Benchmark) error {
	var totalQueries, lastQueries uint64
	var totalTransactions, lastTransactions uint64
	var totalReads, lastReads uint64
	var totalWrites, lastWrites uint64
	var totalOthers, lastOthers uint64
	var totalErrors, lastErrors uint64

	var latencyNanoMin int64 = math.MaxInt64
	var latencyNanoMax int64 = 0
	var latencyNanoSum int64 = 0

	histogram := NewHistogram(histogramSize, histogramMin, histogramMax)

	var mu sync.Mutex

	fmt.Println("Running the test with following options:")
	fmt.Printf("Number of threads: %d\n", r.opts.Threads)

	if r.opts.ReportInterval > 0 {
		fmt.Printf("Report intermediate results every %d second(s)\n\n\n", r.opts.ReportInterval)
	}

	var percentile = r.opts.Percentile

	err := bench.Init(context.Background())
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

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					mu.Lock()
					deltaQueries := totalQueries - lastQueries
					deltaTransactions := totalTransactions - lastTransactions
					deltaReads := totalReads - lastReads
					deltaWrites := totalWrites - lastWrites
					deltaOthers := totalOthers - lastOthers
					deltaErrors := totalErrors - lastErrors

					fmt.Printf("[ %.0fs ] thds: %d tps: %4.2f qps: %4.2f (r/w/o: %4.2f/%4.2f/%4.2f) lat (ms,%d%%): %4.2f err/s %4.2f reconn/s: N/A\n",
						time.Since(begin).Seconds(),
						r.opts.Threads,
						float64(deltaTransactions)/intervalf,
						float64(deltaQueries)/intervalf,
						float64(deltaReads)/intervalf,
						float64(deltaWrites)/intervalf,
						float64(deltaOthers)/intervalf,
						percentile,
						histogram.Percentile(percentile), // 95p
						float64(deltaErrors)/intervalf)

					lastQueries = totalQueries
					lastTransactions = totalTransactions
					lastReads = totalReads
					lastWrites = totalWrites
					lastOthers = totalOthers
					lastErrors = totalErrors
					mu.Unlock()
				}
			}
		}()
	}

	var wg sync.WaitGroup

	for i := 0; i < r.opts.Threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var pe error = nil
			var eventBegin time.Time

			for {
				select {
				case <-ctx.Done():
					return
				default:
					mu.Lock()
					if r.opts.Events > 0 && totalTransactions >= r.opts.Events {
						cancel()
					}
					mu.Unlock()

					eventBegin = time.Now()
					reads, writes, others, errors, err := bench.Event(ctx)
					if err != nil && err != context.DeadlineExceeded {
						// ignore same error
						if pe == nil || pe.Error() != err.Error() {
							pe = err
							fmt.Println(err.Error())
						}
					}
					latency := time.Since(eventBegin).Nanoseconds()

					mu.Lock()
					latencyNanoSum += latency
					if latency < latencyNanoMin {
						latencyNanoMin = latency
					}
					if latency > latencyNanoMax {
						latencyNanoMax = latency
					}
					histogram.Add(float64(latency) / 1000000.0)

					totalReads = totalReads + reads
					totalWrites = totalWrites + writes
					totalOthers = totalOthers + others
					totalErrors = totalErrors + errors
					totalQueries = totalQueries + reads + writes + others
					totalTransactions = totalTransactions + 1
					mu.Unlock()
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

	err = bench.Done()
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
		totalReads, totalWrites, totalOthers, (totalReads + totalWrites + totalOthers),
		totalTransactions, float64(totalTransactions)/float64(totalTime), totalQueries, float64(totalQueries)/float64(totalTime),
		0, 0.0) // Since --mysql-ignore-errors option is not supprted, it is always 0.

	fmt.Printf("General statistics:\n"+
		"    total time:                          %.4fs\n"+
		"    total number of events:              %d\n\n", totalTime, totalTransactions)

	fmt.Printf("Latency (ms):\n"+
		"         min: %39.2f\n"+
		"         avg: %39.2f\n"+
		"         max: %39.2f\n"+
		"         %dth percentile: %27.2f\n"+
		"         sum: %39.2f\n\n",
		float64(latencyNanoMin)/1000000.0,
		(float64(latencyNanoSum)/1000000.0)/float64(totalTransactions),
		float64(latencyNanoMax)/1000000.0,
		percentile,
		histogram.Percentile(percentile),
		float64(latencyNanoSum)/1000000.0)

	fmt.Printf("Threads fairness:\n"+
		"    events (avg/stddev):           %.4f/%3.2f\n"+
		"    execution time (avg/stddev):   %.4f/%3.2f\n", 0.0, 0.0, 0.0, 0.0)

	//	histogram.Print()

	return nil
}
