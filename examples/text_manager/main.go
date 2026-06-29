package main

import (
	"fmt"
	"github.com/oarkflow/smppflow/message"
	"strings"
)

func main() {
	samples := []string{"hello gsm", "hello €", "Unicode 👋", "zero\u200bwidth", strings.Repeat("hello ", 40)}
	for _, s := range samples {
		a := message.AnalyzeText(s)
		segs, _, _ := message.SegmentText(s, message.SegmentPolicy{})
		fmt.Printf("text=%q encoding=%s parts=%d invisible=%v segments=%d\n", s, a.Encoding, a.Parts, a.Invisible, len(segs))
	}
}
