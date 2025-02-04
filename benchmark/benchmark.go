package benchmark

import "context"

const (
	DBDriverMySQL   = "mysql"
	DBDriverPgSQL   = "pgsql"
	DBDriverSpanner = "spanner"
)

type (
	Benchmark interface {
		Init(context.Context) error
		Done() error
		Prepare(context.Context) error
		Event(context.Context) (uint64, uint64, uint64, uint64, error)
	}
	CommonOpts struct {
		Tables    int    `long:"tables" description:"number of tables" default:"1"`
		TableSize int    `long:"table_size" description:"number of rows per table" default:"10000"`
		DBDriver  string `long:"db-driver" choice:"mysql" choice:"pgsql" choice:"spanner" description:"specifies database driver to use" default:"mysql"` //nolint:staticcheck
		ReadWrite bool
	}
	BenchmarkOpts struct {
		CommonOpts
		MySQLOpts
		PgSQLOpts
		SpannerOpts
	}
)

func BenchmarkFactory(opt *BenchmarkOpts) Benchmark {
	if opt.DBDriver == DBDriverSpanner {
		return newSpannerOLTP(opt)
	} else {
		return newOLTPBench(opt)
	}
}
