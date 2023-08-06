package shardkv

import (
	"fmt"
	"io"
)

const Debug = false

func DPrintf(w io.Writer, format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Fprintf(w, format, a...)
	}
	return
}
