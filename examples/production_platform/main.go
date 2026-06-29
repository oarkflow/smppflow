package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/pkg/intelligence"
	"github.com/oarkflow/smppflow/pkg/observability"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/provider"
	"github.com/oarkflow/smppflow/pkg/routing"
	"github.com/oarkflow/smppflow/pkg/security"
	"github.com/oarkflow/smppflow/pkg/smppserver"
)

func main() {
	ctx := context.Background()
	_ = ctx
	accounts := smppserver.NewMemoryAccounts()
	accounts.Save(smppserver.Account{TenantID: "tenant-a", SystemID: "esme-a", PasswordHash: security.HashSecret("secret"), Enabled: true, Modes: []protocol.BindMode{protocol.BindModeTransceiver}, MaxBinds: 4, TPS: 100})

	base := routing.New()
	_ = base.RegisterProvider(provider.Modern50("p-fast", "127.0.0.1:2775", "sys", "pw"))
	_ = base.RegisterProvider(provider.Generic34("p-cheap", "127.0.0.1:2776", "sys", "pw"))
	base.UpdateProviderStats(routing.ProviderStats{ProviderID: "p-fast", QualityScore: 98, DeliveryRate: .995, AvgLatency: 20 * time.Millisecond, MaxTPS: 500, CostPerSegment: .02})
	base.UpdateProviderStats(routing.ProviderStats{ProviderID: "p-cheap", QualityScore: 90, DeliveryRate: .98, AvgLatency: 80 * time.Millisecond, MaxTPS: 1000, CostPerSegment: .005})
	ar := routing.NewAdvanced(base, routing.Policy{Strategy: routing.StrategyLeastCost, Cooldown: 30 * time.Second})
	_ = ar

	resolver := intelligence.NewResolverWithRegion([]intelligence.PrefixEntry{{Prefix: "+97798", Country: "Nepal", CountryCode: "NP", CallingCode: "977", MCC: "429", MNC: "01", NNC: "98", NDC: "98", Network: "Nepal mobile", Carrier: "Nepal Telecom", Type: "mobile"}}, "NP")
	fmt.Printf("number=%+v\n", resolver.LookupWithRegion("9841000000", "NP"))

	metrics := observability.New()
	metrics.Inc("smpp_messages_total", map[string]string{"tenant": "tenant-a", "provider": "p-fast"}, 1)
	fmt.Println(metrics.Prometheus())

	_ = accounts
	fmt.Println("production platform components initialized")
}
