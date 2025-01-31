package main

// TODO
// Support PREPARE
// Support CLI options
// Support PostgreSQL
// Calc TPS 
// Calc Latency
// Calc Err
// Calc Reconn

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/samitani/go-sysbench/benchmark/oltp"
)

func main() {
	// MySQL 接続情報
	dsn := "app:Password%123@tcp(127.0.0.1:3570)/"

	// 最大実行時間（秒）
	maxTime := 10

	// スレッド数
	threads := 10

	// レポート出力間隔（秒）
	reportInterval := 3

	// クエリ数カウント
	var totalQueries uint64
	var intervalQueries uint64
	var mu sync.Mutex

	var benchmark = oltp.Oltp{}

	err := benchmark.Prepare(dsn)
	if err != nil {
		log.Fatal(err)
	}

	begin := time.Now()

	// goroutine for reporting
	go func() {
		ticker := time.NewTicker(time.Duration(reportInterval) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				mu.Lock()
				fmt.Printf("[ %.0fs ] thds: %d tps: %4.2f qps: %4.2f (r/w/o: %4.2f/%4.2f/%4.2f) lat (ms,%d%%): %4.2f err/s %4.2f reconn/s: %4.2f\n", time.Since(begin).Seconds(), threads, float32(intervalQueries) / float32(reportInterval), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
				totalQueries += intervalQueries
				intervalQueries = 0
				mu.Unlock()
			}
		}
	}()

	// クエリ実行ゴルーチン
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxTime)*time.Second)
	defer cancel()

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err := benchmark.Event()
					if err != nil {
						log.Fatal(err)
					}
					mu.Lock()
					intervalQueries++
					mu.Unlock()
				}
			}
		}()
	}

	// signal handler
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigchan:
		fmt.Println("\nShutdown signal received. Exiting...")
		cancel()
	}

	wg.Wait()
	fmt.Printf("SQL statistics:\n" +
                   "    queries performed:\n" + 
                   "        read:                            %d\n" +
                   "        write:                           %d\n" +
                   "        other:                           %d\n" +
                   "        total:                           %d\n" +
                   "    transactions:                        %-6d (%.2f per sec.)\n" +
                   "    queries:                             %-6d (%.2f per sec.)\n" +
                   "    ignored errors:                      %-6d (%.2f per sec.)\n" +
                   "    reconnects:                          %-6d (%.2f per sec.)\n", 0,0,0,0, 0,0.0, 0,0.0, 0,0.0, 0,0.0)

}
