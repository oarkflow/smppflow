package main

import (
	"fmt"
	"strings"

	"github.com/oarkflow/smppflow/pkg/protocol"
)

func main() {
	for _, v := range []protocol.Version{protocol.Version33, protocol.Version34, protocol.Version50} {
		var names []string
		for _, op := range protocol.CommandsForVersion(v) {
			names = append(names, op.Name)
		}
		fmt.Printf("SMPP %s: %s\n", v, strings.Join(names, ", "))
	}
}
