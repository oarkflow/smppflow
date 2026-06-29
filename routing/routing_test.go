package routing

import (
	"context"
	"github.com/oarkflow/smppflow/message"
	"github.com/oarkflow/smppflow/provider"
	"testing"
)

func TestRouteByPrefix(t *testing.T) {
	r := New()
	r.AddProvider(provider.Generic34("np", "addr", "sys", "pw"))
	r.AddRule(Rule{ID: "np", Prefix: "977", ProviderID: "np", Priority: 1})
	d, err := r.Route(context.Background(), message.SubmitRequest{To: "9771"})
	if err != nil || d.Provider.ID != "np" || d.Rule.ID != "np" {
		t.Fatalf("decision %+v err=%v", d, err)
	}
}
