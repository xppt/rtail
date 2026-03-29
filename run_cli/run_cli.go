package run_cli

import (
	"fmt"
	"os"
)

type fatalErr struct {
	msg string
	exitCode int
}

func RunCli(entry func()) {
	defer func() {
		panicVal := recover()
		if panicVal == nil {
			return
		}

		switch typed := panicVal.(type) {
		case fatalErr:
			fmt.Fprintln(os.Stderr, typed.msg)
			os.Exit(typed.exitCode)
		default:
			panic(panicVal)
		}
	}()

	entry()
}

func DieF(format string, args... any) {
	Die(fmt.Sprintf(format, args...))
}

func Die(msg string) {
	panic(fatalErr{msg: msg, exitCode: 1})
}

func DieOnError(err error, msg string) {
	if err != nil {
		DieF("error: %s: %v", msg, err)
	}
}
