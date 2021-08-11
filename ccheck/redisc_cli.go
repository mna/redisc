package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/mna/mainer"
	"github.com/mna/redisc"
)

const binName = "redisc_cli"

var (
	shortUsage = fmt.Sprintf(`
usage: %s [<option>...] <command> [<arg>...]
Run '%[1]s --help' for details.
`, binName)

	longUsage = fmt.Sprintf(`usage: %s [<option>...] <command> [<arg>...]
       %[1]s -h|--help

Interact with a Redis cluster via the redisc package.

Valid flag options are:
       -h --help                 Show this help and exit immediately.
       -a --addrs ADDRS          Comma-separated list of addresses to connect
                                 to the cluster.
       -b --bind KEY             Explicitly bind the connection using this key
                                 instead of binding automatically.
       --hash KEY                Compute and print the hash slot of KEY and
                                 exit immediately.
       -r --read-only            Use a READONLY connection to execute the
                                 command on a replica if possible.
       --retry INT               Execute command with a RetryConn if > 0,
                                 the value indicates the maximum attempts.
       --retry-delay DUR         If --retry is set, use this value as the
                                 duration to wait before retrying a TRYAGAIN
                                 error.

The <command> is the redis command to execute, with the provided <arg>s.
`, binName)
)

type cmd struct {
	Help bool `flag:"h,help"`

	Addrs      string        `flag:"a,addrs"`
	Bind       string        `flag:"b,bind"`
	Hash       string        `flag:"hash"`
	ReadOnly   bool          `flag:"r,read-only"`
	Retry      int           `flag:"retry"`
	RetryDelay time.Duration `flag:"retry-delay"`

	args []string
}

func (c *cmd) SetArgs(args []string) {
	c.args = args
}

func (c *cmd) Validate() error {
	if c.Help || c.Hash != "" {
		return nil
	}

	if c.Addrs == "" {
		return errors.New("--addrs is required")
	}
	if c.Retry < 0 {
		return errors.New("--retry must be >= 0")
	}
	if len(c.args) == 0 {
		return errors.New("no redis command provided")
	}
	return nil
}

func (c *cmd) Main(args []string, stdio mainer.Stdio) mainer.ExitCode {
	var p mainer.Parser
	if err := p.Parse(args, c); err != nil {
		fmt.Fprintln(stdio.Stderr, err)
		return mainer.InvalidArgs
	}

	switch {
	case c.Help:
		fmt.Fprint(stdio.Stdout, longUsage)
		return mainer.Success

	case c.Hash != "":
		slot := redisc.Slot(c.Hash)
		fmt.Fprintf(stdio.Stdout, "slot for %q: %d\n", c.Hash, slot)
		return mainer.Success

	default:
		// run the provided command
	}

	// execute the command...
	return mainer.Success
}

func main() {
	var c cmd
	os.Exit(int(c.Main(os.Args, mainer.CurrentStdio())))
}
