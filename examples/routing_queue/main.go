package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/provider"
	"github.com/oarkflow/smppflow/pkg/queue"
	"github.com/oarkflow/smppflow/pkg/routing"
)

func main() {
	ctx := context.Background()
	r := routing.New()
	r.AddProvider(provider.Generic34("np", "127.0.0.1:2775", "demo", "secret"))
	r.AddRule(routing.Rule{ID: "np-prefix", Prefix: "977", ProviderID: "np", Priority: 1})
	d, _ := r.Route(ctx, message.SubmitRequest{From: "A", To: "9779800000000"})
	fmt.Println("route", d.Provider.ID, d.Reason)
	q := queue.NewMemory()
	_ = q.Enqueue(ctx, queue.Job{Type: "send_sms", Payload: "hello", Priority: 10, RunAt: time.Now()})
	j, _ := q.Reserve(ctx)
	fmt.Println("reserved", j.Type, j.Payload)
}
