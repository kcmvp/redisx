package main

import (
	"fmt"
	"os"

	"github.com/kcmvp/redisx/cmd/_shared/session"
	"github.com/kcmvp/redisx/cmd/redisx/internal"
)

func main() {
	var sess *session.Session
	app := internal.BuildApp(&sess)
	defer func() {
		if sess != nil {
			_ = sess.Close()
		}
	}()
	if err := app.Execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
