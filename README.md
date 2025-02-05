# go-sysbench

[![Go Report Card](https://goreportcard.com/badge/github.com/samitani/go-sysbench)](https://goreportcard.com/report/github.com/samitani/go-sysbench)
[![License](https://img.shields.io/badge/license-GPLv2-blue.svg)](LICENSE)

Yet another sysbench written in Golang

`sysbench` is a very simple, lightweight, and easy-to-customize benchmarking tool. I've been using `sysbench` for many years. In most cases, it met my requirements and worked well.
But I needed to do more complex and fundamental customization, such as adding drivers. So I decided to make a `sysbench` clone in Golang.

`go-sysbench` runs the same SQL as sysbench and reports the results in the same format as `sysbench`.
`go-sysbench` cannot do as many things as `sysbench`, but it offers more customizability for those who are familiar with Golang.


I would like to thank Peter Zaitsev, Alexy Kopytov and contributors for inventing great tool [sysbench](https://github.com/akopytov/sysbench).

## How to install

```
go install github.com/samitani/go-sysbench/cmd/go-sysbench@main
```

## Incompatibility with sysbench

* `go-sysbench` supports only `oltp_read_only` and `oltp_read_write` database benchmarks. Linux benchmarks such as `fileio`, `cpu`, `memory`, etc. are not supported.
* Some options are not implemented. `go-sysbench oltp_read_write run --help` shows available options.
```
$ go-sysbench oltp_read_write run --help
2025/02/05 22:55:07 Usage:
  go-sysbench [OPTIONS] oltp_read_write run [run-OPTIONS]

Help Options:
  -h, --help                                Show this help message

[run command options]
          --threads=                        number of threads to use (default: 1)
          --events=                         limit for total number of events (default: 0)
          --time=                           limit for total execution time in seconds (default: 10)
          --report-interval=                periodically report intermediate statistics with a specified interval in seconds. 0 disables intermediate reports (default: 0)
          --histogram=[on|off]              print latency histogram in report (default: off)
          --percentile=                     percentile to calculate in latency statistics (1-100) (default: 95)
          --tables=                         number of tables (default: 1)
          --table_size=                     number of rows per table (default: 10000)
          --db-driver=[mysql|pgsql|spanner] specifies database driver to use (default: mysql)
          --mysql-host=                     MySQL server host (default: localhost)
          --mysql-port=                     MySQL server port (default: 3306)
          --mysql-user=                     MySQL user (default: sbtest)
          --mysql-password=                 MySQL password [$MYSQL_PWD]
          --mysql-db=                       MySQL database name (default: sbtest)
          --mysql-ssl=[on|off]              use SSL connections (default: off)
          --pgsql-host=                     PostgreSQL server host (default: localhost)
          --pgsql-port=                     PostgreSQL server port (default: 5432)
          --pgsql-user=                     PostgreSQL user (default: sbtest)
          --pgsql-password=                 PostgreSQL password [$PGPASSWORD]
          --pgsql-db=                       PostgreSQL database name (default: sbtest)
          --spanner-project=                Spanner Google Cloud project name
          --spanner-instance=               Spanner instance id
          --spanner-db=                     Spanner database name (default: sbtest)
```
* Number of reconnects is not reported.
* Lua scripts is not supported. To customize the benchmark scenario, you have to edit the code directly.

## Additional feature

### Google Cloud Spanner 

`go-sysbench` supports Google Cloud Spanner.
```
go-sysbench oltp_read_write run --db-driver=spanner --spanner-project=YOUR-PROJECT --spanner-instance=YOUR-INSTANCE-NAME
```
