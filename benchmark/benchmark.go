package benchmark

type (
	Benchmark interface {
		Init() error
		Done() error
		Prepare() error
		Event() (uint64, uint64, uint64, uint64, error)
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
		SpannerOpts
	}
)

func BenchmarkFactory(opt *BenchmarkOpts) Benchmark {
	if opt.DBDriver == "spanner" {
		return newSpannerOLTP(opt)
	} else {
		return newMySQLOLTP(opt)
	}
}
