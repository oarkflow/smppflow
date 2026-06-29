package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/pkg/account"
	"github.com/oarkflow/smppflow/pkg/provider"
	"github.com/oarkflow/smppflow/pkg/routing"
	"github.com/oarkflow/smppflow/pkg/service"
)

func main() {
	ctx := context.Background()
	accounts := account.NewMemory()
	rt := routing.New()
	svc := service.New(service.Config{Router: rt, AccountStore: accounts})
	_ = svc.RegisterUser(ctx, account.User{ID: "user-1", TenantID: "tenant-a", Username: "alice", Enabled: true, Roles: []string{"sender"}, QuotaPerMinute: 600})
	key, secret, _ := account.NewAPIKey("tenant-a", "user-1", "alice-production")
	_ = accounts.SaveAPIKey(ctx, key)
	u, authKey, _ := svc.AuthenticateAPIKey(ctx, secret)
	fmt.Println("authenticated", u.Username, authKey.Prefix)

	p1 := provider.Generic34("primary-smpp", "127.0.0.1:2775", "", "")
	p1.TPS = 1200
	_ = svc.RegisterProvider(ctx, p1, account.ProviderCredential{TenantID: "tenant-a", ProviderID: "primary-smpp", SystemID: "tenant_a_sys", Password: "tenant_a_secret", Enabled: true})
	p2 := provider.Modern50("backup-smpp5", "127.0.0.1:2776", "", "")
	p2.TPS = 2000
	_ = svc.RegisterProvider(ctx, p2, account.ProviderCredential{TenantID: "tenant-a", ProviderID: "backup-smpp5", SystemID: "tenant_a_v5", Password: "tenant_a_v5_secret", Enabled: true})
	rt.UpdateProviderStats(routing.ProviderStats{ProviderID: "primary-smpp", QualityScore: 93, DeliveryRate: 0.99, AvgLatency: 90 * time.Millisecond, MaxTPS: 1200, CostPerSegment: 0.012})
	rt.UpdateProviderStats(routing.ProviderStats{ProviderID: "backup-smpp5", QualityScore: 88, DeliveryRate: 0.98, AvgLatency: 70 * time.Millisecond, MaxTPS: 2000, CostPerSegment: 0.014})
	_ = rt.RegisterRule(routing.Rule{ID: "tenant-a-nepal", TenantID: "tenant-a", Prefix: "977", ProviderID: "primary-smpp", Priority: 1, MinQuality: 90})
	_ = rt.RegisterRule(routing.Rule{ID: "tenant-a-fallback", TenantID: "tenant-a", ProviderID: "backup-smpp5", Priority: 100})
	creds, _ := accounts.ListProviderCredentials(ctx, "tenant-a")
	for _, c := range creds {
		fmt.Printf("provider credential: %+v\n", account.RedactProviderCredential(c))
	}
	fmt.Println("registered providers", len(rt.Providers()), "rules", len(rt.Rules()))
}
