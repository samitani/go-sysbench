package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/jessevdk/go-flags"
	"github.com/samitani/go-sysbench"
)

var version string

type (
	CmdOpts struct {
		sysbench.RunnerOpts
		BenchmarkOpts
		Version bool `long:"version" description:"show version"`
	}
)

func main() {
	opts := CmdOpts{}

	parser := flags.NewParser(&opts, flags.HelpFlag|flags.PassDoubleDash)
	parser.Usage = fmt.Sprintf("[options]... [%s] [prepare|run]", strings.Join(benchmarkNames(), "|"))

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

	bench, err := benchmarkFactory(testname, &opts.BenchmarkOpts)
	r := sysbench.NewRunner(&opts.RunnerOpts, bench)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if command == "run" {
		err = r.Run()
	} else if command == "prepare" {
		err = r.Prepare()
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
