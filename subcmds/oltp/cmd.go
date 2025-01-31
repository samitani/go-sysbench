package oltpcmd

import (
	"github.com/jessevdk/go-flags"
)

type (
	oltpCommand struct {
		RunSubCmd     *runSubCommand     `command:"run" description:"run benchmark"`
		PrepareSubCmd *prepareSubCommand `command:"prepare" description:"prepare table and records"`
	}
)

func RegisterSubCommand(fp *flags.Parser) error {

	ro := &runSubCommand{}
	ro.BenchmarkOpts.ReadWrite = false

	_, err := fp.AddCommand("oltp_read_only", "Read-Only OLTP benchmark", "Read-Only OLTP benchmark", &oltpCommand{
		RunSubCmd:     ro,
		PrepareSubCmd: &prepareSubCommand{},
	})
	if err != nil {
		return err
	}

	rw := &runSubCommand{}
	rw.BenchmarkOpts.ReadWrite = true

	_, err = fp.AddCommand("oltp_read_write", "Read/Write OLTP benchmark", "Read/Write OLTP benchmark", &oltpCommand{
		RunSubCmd:     rw,
		PrepareSubCmd: &prepareSubCommand{},
	})
	if err != nil {
		return err
	}

	return nil
}
