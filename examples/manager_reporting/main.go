package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/oarkflow/smppflow/pkg/manager"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/provider"
	"github.com/oarkflow/smppflow/pkg/reports"
	"github.com/oarkflow/smppflow/pkg/routing"
	"github.com/oarkflow/smppflow/pkg/storage"
)

func main() {
	ctx := context.Background()
	st := storage.NewMemory()
	r := routing.New()
	ops := manager.NewOperations(nil, st, r, nil, nil)

	_ = ops.UpsertTenant(ctx, "admin", manager.Tenant{ID: "tenant-a", Name: "Tenant A", TPS: 100})
	_ = ops.SaveProvider(ctx, "admin", provider.Generic34("primary", "127.0.0.1:2775", "system", "secret"))
	_ = r.RegisterRule(routing.Rule{ID: "default", TenantID: "tenant-a", ProviderID: "primary", Priority: 1, Weight: 10, Enabled: true})

	msg := &message.Message{ID: "msg-demo", TenantID: "tenant-a", ProviderID: "primary", State: "delivered", ProviderMessageID: "p-1"}
	_ = st.SaveMessage(ctx, msg)

	sim, _ := ops.SimulateRoute(ctx, message.SubmitRequest{TenantID: "tenant-a", From: "INFO", To: "+9779841000000", Text: "Hello"})
	_ = json.NewEncoder(os.Stdout).Encode(sim)

	rep, _ := reports.GenerateProvider(ctx, st, reports.Filter{TenantID: "tenant-a"})
	fmt.Printf("provider_reports=%+v\n", rep)
}
