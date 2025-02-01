package benchmark

import (
	"context"
	"fmt"
)

const (
	DBDriverMySQL   = "mysql"
	DBDriverPgSQL   = "pgsql"
	DBDriverSpanner = "spanner"
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
	CommonOpts struct {
		Tables         int    `long:"tables" description:"number of tables" default:"1"`
		TableSize      int    `long:"table_size" description:"number of rows per table" default:"10000"`
		TableSizeP     int    `long:"table-size" description:"alias of --table_size"`
		DBDriver       string `long:"db-driver" choice:"mysql" choice:"pgsql" choice:"spanner" description:"specifies database driver to use" default:"mysql"` //nolint:staticcheck
		DBPreparedStmt string `long:"db-ps-mode" choice:"auto" choice:"disable" description:"prepared statements usage mode" default:"auto"`                   //nolint:staticcheck
	}
	BenchmarkOpts struct {
		CommonOpts
		MySQLOpts   `group:"MySQL" description:"MySQL options"`
		PgSQLOpts   `group:"PostgreSQL" description:"PostgreSQL options"`
		SpannerOpts `group:"Spanner" description:"Google Cloud Spanner options"`
	}
)

func BenchmarkFactory(testname string, opt *BenchmarkOpts) (Benchmark, error) {
	if opt.TableSizeP != 0 {
		opt.TableSize = opt.TableSizeP
	}

	if testname == NameOLTPReadOnly {
		return newOLTPBench(opt, rwModeReadOnly), nil
	} else if testname == NameOLTPReadWrite {
		return newOLTPBench(opt, rwModeReadWrite), nil
	}
	return nil, fmt.Errorf("Unknown benchmark: %s", testname)

}

func BenchmarkNames() []string {
	return []string{NameOLTPReadOnly, NameOLTPReadWrite}
}
