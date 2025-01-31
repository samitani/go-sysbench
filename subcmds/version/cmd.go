package versioncmd

import (
	"fmt"
	"github.com/jessevdk/go-flags"
)

type (
	version struct {
	}
)

var (
	_ flags.Commander = (*version)(nil)
)

func RegisterSubCommand(fp *flags.Parser) error {
	_, err := fp.AddCommand("version", "show version", "show version", &version{})
	if err != nil {
		return err
	}
	return nil
}

func (*version) Execute(args []string) error {
	fmt.Println("0.0.1")
	return nil
}
