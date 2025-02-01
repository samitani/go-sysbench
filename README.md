# go-sysbench

[![Go Report Card](https://goreportcard.com/badge/github.com/samitani/go-sysbench)](https://goreportcard.com/report/github.com/samitani/go-sysbench)
[![License](https://img.shields.io/badge/license-GPLv2-blue.svg)](LICENSE)

Yet another sysbench written in Golang

`go-sysbench` runs the same SQL as `sysbench` and reports the results in the same format as `sysbench`.
`go-sysbench` cannot do as many things as `sysbench`, but it offers more customizability for those who are familiar with Golang.

I would like to thank Peter Zaitsev, Alexy Kopytov and contributors for inventing great tool [sysbench](https://github.com/akopytov/sysbench).

## Usage

### Install

```
go install github.com/samitani/go-sysbench/cmd/go-sysbench@latest
```

### Run benchmark
1. Create a database to run benchmark.
```
mysql> CREATE DATABASE sbtest;
```
2. Execute prepare command to create tables and records.
```
$ go-sysbench --tables=1 --mysql-user=appuser --mysql-password=Password --time=360 --threads=5 --table_size=10000 --report-interval=1 --histogram=on oltp_read_write prepare
```
3. Run benchmark
In this example, It will run the benchmark for 360 seconds with 5 threads.
```
$ go-sysbench --tables=1 --mysql-user=appuser --mysql-password=Password --time=360 --threads=5 --table_size=10000 --report-interval=1 --histogram=on oltp_read_write run
```

### Options

```
Usage:
  go-sysbench [options]... [oltp_read_only|oltp_read_write] [prepare|run]

Application Options:
      --version                         show version
      --tables=                         number of tables (default: 1)
      --table_size=                     number of rows per table (default: 10000)
      --table-size=                     alias of --table_size
      --db-driver=[mysql|pgsql|spanner] specifies database driver to use (default: mysql)
      --db-ps-mode=[auto|disable]       prepared statements usage mode (default: auto)
      --threads=                        number of threads to use (default: 1)
      --events=                         limit for total number of events (default: 0)
      --time=                           limit for total execution time in seconds (default: 10)
      --report-interval=                periodically report intermediate statistics with a specified interval in seconds. 0 disables intermediate reports (default: 0)
      --histogram=[on|off]              print latency histogram in report (default: off)
      --percentile=                     percentile to calculate in latency statistics (1-100) (default: 95)

MySQL:
      --mysql-host=                     MySQL server host (default: localhost)
      --mysql-port=                     MySQL server port (default: 3306)
      --mysql-user=                     MySQL user (default: sbtest)
      --mysql-password=                 MySQL password [$MYSQL_PWD]
      --mysql-db=                       MySQL database name (default: sbtest)
      --mysql-ssl=[on|off]              use SSL connections (default: off)
      --mysql-ignore-errors=            list of errors to ignore, or "all" (default: 1213,1020,1205)

PostgreSQL:
      --pgsql-host=                     PostgreSQL server host (default: localhost)
      --pgsql-port=                     PostgreSQL server port (default: 5432)
      --pgsql-user=                     PostgreSQL user (default: sbtest)
      --pgsql-password=                 PostgreSQL password [$PGPASSWORD]
      --pgsql-db=                       PostgreSQL database name (default: sbtest)
      --pgsql-ssl=[on|off]              use SSL connections (default: off)
      --pgsql-ignore-errors=            list of errors to ignore, or "all" (default: 40P01,23505,40001)

Spanner:
      --spanner-project=                Spanner Google Cloud project name
      --spanner-instance=               Spanner instance id
      --spanner-db=                     Spanner database name (default: sbtest)

Help Options:
  -h, --help                            Show this help message
```

## Incompatibility with sysbench

* `go-sysbench` supports only `oltp_read_only` and `oltp_read_write` database benchmarks. Linux benchmarks such as `fileio`, `cpu`, `memory` are not supported.
* Some options are not implemented. See Options section above.
* Number of reconnects is not reported.
* Lua scripts is not supported. To customize the benchmark scenario, you have to edit the code directly.

## Additional feature

### Google Cloud Spanner 

`go-sysbench` supports Google Cloud Spanner with Google Standard SQL.
Cloud Spanner is prone to lock contention. You need to increase the number of records to avoid lock contention.
```
$ gcloud auth login --update-adc
$ go-sysbench oltp_read_write run --db-driver=spanner --spanner-project=YOUR-PROJECT --spanner-instance=YOUR-INSTANCE-NAME --table_size=1000000
```

In Spanner benchmark, `ErrAbortedDueToConcurrentModification` error is ignored.
