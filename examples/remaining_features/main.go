package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/pkg/billing"
	"github.com/oarkflow/smppflow/pkg/certification"
	"github.com/oarkflow/smppflow/pkg/cluster"
	"github.com/oarkflow/smppflow/pkg/compliance"
	"github.com/oarkflow/smppflow/pkg/lab"
	"github.com/oarkflow/smppflow/pkg/lookup"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/observability"
	"github.com/oarkflow/smppflow/pkg/regulation"
	"github.com/oarkflow/smppflow/pkg/secrets"
)

type fakeHandler struct{}

func (fakeHandler) Handle(ctx context.Context, b []byte) ([]byte, error) {
	if len(b) < 16 {
		return nil, fmt.Errorf("malformed pdu")
	}
	return []byte("ok"), nil
}

type fakeSender struct{}

func (fakeSender) Send(ctx context.Context, seq int) error { return nil }

func main() {
	ctx := context.Background()
	req := message.SubmitRequest{TenantID: "tenant-a", From: "ACME", To: "+16502530000", Text: "Your OTP is 123456. Reply STOP to opt out."}

	pack := certification.GenericPack("generic-carrier")
	report := certification.ValidateSubmit(ctx, pack, req, nil)
	fmt.Println("certification passed:", report.Passed)

	static := lookup.NewStaticAdapter()
	static.Put("+16502530000", lookup.Result{CurrentCarrier: "Example Wireless", MCC: "310", MNC: "260", Reachable: true, ActiveSIM: true, Source: "static-hlr"})
	hlr, _ := static.Lookup(ctx, req.To, "US")
	fmt.Println("lookup carrier:", hlr.CurrentCarrier, hlr.MCC, hlr.MNC)

	rules := regulation.New()
	rules.SetRule(regulation.CountryRule{Country: "US", SenderRegistrationRequired: true, LocalOptOutRequired: true, RequiredFooter: []string{"STOP"}})
	rules.RegisterSender("US", "ACME")
	dec := rules.Check(ctx, req, "US", time.Now())
	fmt.Println("regulatory allowed:", dec.Allowed)

	sec := secrets.NewStore(secrets.NewLocalKMS([]byte("dev-master-key")))
	_ = sec.Put(ctx, "provider/generic/password", []byte("secret-password"))
	plain, _ := sec.Get(ctx, "provider/generic/password")
	fmt.Println("secret redacted:", secrets.Redact(string(plain)))

	coord := cluster.Coordinator{Store: cluster.NewMemoryStore(), Node: cluster.Node{ID: "node-a", State: cluster.NodeActive, Capacity: 10}}
	_ = coord.Heartbeat(ctx)
	lease, ok, _ := coord.AcquireBind(ctx, "generic-carrier", "tenant-a", "trx-1")
	fmt.Println("bind lease acquired:", ok, lease.OwnerNode)

	bill := billing.NewEngine()
	bill.AddRule(billing.PriceRule{ID: "us", TenantID: "tenant-a", ProviderID: "generic-carrier", Country: "US", Currency: "USD", CostPerSegmentMicros: 25000, SellPerSegmentMicros: 45000})
	bill.SetBalance("tenant-a", billing.NewMoney("USD", 10))
	rated, _ := bill.Rate(ctx, message.Message{ID: "m1", TenantID: "tenant-a"}, "generic-carrier", "US", "310", "260", 1)
	_ = bill.Reserve(ctx, "reservation-1", rated)
	fmt.Println("rated sell:", rated.Sell.Float(), rated.Sell.Currency)

	abuse := compliance.New()
	_ = abuse.AddBlockedPattern(`(?i)free\s+money`)
	abuse.VerifySender(compliance.SenderPolicy{TenantID: "tenant-a", Sender: "ACME", Verified: true, AllowedCountries: []string{"US"}})
	mod := abuse.Check(ctx, req, "US")
	fmt.Println("moderation allowed:", mod.Allowed)

	fixtures := []lab.Fixture{{Name: "short malformed", Hex: "0102", Malformed: true}}
	replay := lab.Replay(ctx, fakeHandler{}, fixtures)
	fmt.Println("lab replay passed:", replay[0].Passed)
	soak := lab.Soak(ctx, lab.SoakConfig{TPS: 10, Duration: 250 * time.Millisecond}, fakeSender{})
	fmt.Println("soak sent:", soak.Sent)

	var buf bytes.Buffer
	_ = observability.WriteJSON(&buf, observability.DefaultStack())
	fmt.Println("observability config bytes:", buf.Len())
}
