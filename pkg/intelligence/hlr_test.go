package intelligence

import (
	"context"
	"testing"
	"time"
)

func TestLookupEnginePolicy(t *testing.T) {
	e := NewLookupEngine(LookupPolicy{RequireReachable: true, BlockRoaming: true, MaxFraudScore: 0.7, TTL: time.Minute}, StaticAdapter{Profiles: map[string]NumberProfile{"9779800000000": {MSISDN: "9779800000000", Country: "NP", MCC: "429", MNC: "01", Reachability: ReachabilityReachable, FraudScore: 0.2}}})
	d, err := e.Lookup(context.Background(), "9779800000000")
	if err != nil || !d.Allowed {
		t.Fatalf("allowed=%v err=%v reasons=%v", d.Allowed, err, d.Reasons)
	}
	d, err = e.Lookup(context.Background(), "9779800000000")
	if err != nil || !d.Cached {
		t.Fatalf("expected cached lookup")
	}
}
