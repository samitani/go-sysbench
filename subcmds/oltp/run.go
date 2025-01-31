package oltpcmd

import (
	"github.com/jessevdk/go-flags"

	"github.com/samitani/go-sysbench/benchmark"
	"github.com/samitani/go-sysbench/runner"
)

type (
	runSubCommand struct {
		runner.RunnerOpts
		benchmark.BenchmarkOpts
	}
)

var _ flags.Commander = (*runSubCommand)(nil)

func (s *runSubCommand) Execute(_ []string) error {
	r := runner.NewRunner(&s.RunnerOpts)
	err := r.Run(benchmark.BenchmarkFactory(&s.BenchmarkOpts))

	if err != nil {
		return err
	}

	return nil
}
