package billing

import (
	"context"
	"testing"

	"github.com/oarkflow/smppflow/pkg/message"
)

func TestRatingAndReservation(t *testing.T) {
	e := NewEngine()
	e.AddRule(PriceRule{TenantID: "t", ProviderID: "p", Country: "US", Currency: "USD", CostPerSegmentMicros: 10000, SellPerSegmentMicros: 20000})
	e.SetBalance("t", NewMoney("USD", 1))
	r, err := e.Rate(context.Background(), message.Message{ID: "m", TenantID: "t"}, "p", "US", "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if r.Sell.AmountMicros != 40000 {
		t.Fatalf("bad sell %+v", r.Sell)
	}
	if err := e.Reserve(context.Background(), "res", r); err != nil {
		t.Fatal(err)
	}
	if err := e.Release(context.Background(), "t", "res"); err != nil {
		t.Fatal(err)
	}
}
