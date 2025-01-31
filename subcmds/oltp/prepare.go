package oltpcmd

import (
	"github.com/jessevdk/go-flags"

	"github.com/samitani/go-sysbench/benchmark"
	"github.com/samitani/go-sysbench/runner"
)

type (
	prepareSubCommand struct {
		runner.RunnerOpts
		benchmark.BenchmarkOpts
	}
)

var _ flags.Commander = (*prepareSubCommand)(nil)

func (s *prepareSubCommand) Execute(_ []string) error {
	r := runner.Runner{}
	err := r.Prepare(benchmark.BenchmarkFactory(&s.BenchmarkOpts))

	if err != nil {
		return err
	}

	return nil
}
