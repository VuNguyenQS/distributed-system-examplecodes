package raft

import (
	"fmt"
	"io"
)

// Debugging
const Debug = false

func DFprintf(w io.Writer, format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Fprintf(w, format, a...)
	}
	return
}

func DFprintln(w io.Writer, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Println(w, a)
	}
	return
}
