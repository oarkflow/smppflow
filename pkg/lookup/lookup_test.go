package lookup

import (
	"context"
	"testing"
)

func TestStaticAdapterMergesPhoneAndHLR(t *testing.T) {
	s := NewStaticAdapter()
	s.Put("+16502530000", Result{CurrentCarrier: "Example", MCC: "310", MNC: "260", Reachable: true})
	r, err := s.Lookup(context.Background(), "+1 650 253 0000", "US")
	if err != nil {
		t.Fatal(err)
	}
	if r.E164 != "+16502530000" || r.CurrentCarrier != "Example" || !r.Reachable {
		t.Fatalf("bad result: %+v", r)
	}
}
