package routing

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/provider"
)

func TestAdvancedRouterStrategies(t *testing.T) {
	r := New()
	_ = r.RegisterProvider(provider.Generic34("cheap", "", "", ""))
	_ = r.RegisterProvider(provider.Generic34("fast", "", "", ""))
	r.UpdateProviderStats(ProviderStats{ProviderID: "cheap", QualityScore: 80, CostPerSegment: .001, AvgLatency: 100 * time.Millisecond})
	r.UpdateProviderStats(ProviderStats{ProviderID: "fast", QualityScore: 95, CostPerSegment: .02, AvgLatency: 10 * time.Millisecond})
	d, err := NewAdvanced(r, Policy{Strategy: StrategyLeastCost}).Route(context.Background(), message.SubmitRequest{From: "A", To: "+9779841000000"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Provider.ID != "cheap" {
		t.Fatalf("want cheap got %s", d.Provider.ID)
	}
	d, err = NewAdvanced(r, Policy{Strategy: StrategyFastest}).Route(context.Background(), message.SubmitRequest{From: "A", To: "+9779841000000"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Provider.ID != "fast" {
		t.Fatalf("want fast got %s", d.Provider.ID)
	}
}
