package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/jessevdk/go-flags"

	"github.com/samitani/go-sysbench/benchmark"
	"github.com/samitani/go-sysbench/runner"
)

var version string

type (
	CmdOpts struct {
		Version bool `long:"version" description:"show version"`
		benchmark.BenchmarkOpts
		runner.RunnerOpts
	}
)

func main() {
	opts := CmdOpts{}

	parser := flags.NewParser(&opts, flags.HelpFlag|flags.PassDoubleDash)
	parser.Usage = fmt.Sprintf("[options]... [%s] [prepare|run]", strings.Join(benchmark.BenchmarkNames(), "|"))

	args, err := parser.Parse()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if opts.Version {
		fmt.Printf("go-sysbench %s\n", version)
		os.Exit(0)
	}

	if len(args) != 2 {
		parser.WriteHelp(os.Stdout)
		os.Exit(0)
	}

	testname := args[0]
	command := args[1]

	r := runner.NewRunner(&opts.RunnerOpts)
	bench, err := benchmark.BenchmarkFactory(testname, &opts.BenchmarkOpts)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if command == "run" {
		err = r.Run(bench)
	} else if command == "prepare" {
		err = r.Prepare(bench)
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
