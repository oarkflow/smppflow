package optout

import (
	"context"
	"testing"
)

func TestProcessor(t *testing.T) {
	st := NewStore()
	p := NewProcessor(st)
	r, err := p.ProcessInbound(context.Background(), "t1", "+9779800000000", " stop ", "mo")
	if err != nil || r.Action != ActionOptOut || !st.Has(context.Background(), "t1", "9779800000000") {
		t.Fatalf("bad stop %#v %v", r, err)
	}
	r, _ = p.ProcessInbound(context.Background(), "t1", "9779800000000", "START", "mo")
	if r.Action != ActionOptIn || st.Has(context.Background(), "t1", "9779800000000") {
		t.Fatalf("bad start %#v", r)
	}
}
