package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/message"
	"github.com/oarkflow/smppflow/provider"
	"github.com/oarkflow/smppflow/routing"
	"github.com/oarkflow/smppflow/server"
	"github.com/oarkflow/smppflow/service"
	"github.com/oarkflow/smppflow/storage"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	smsc := server.New(server.Config{Addr: "127.0.0.1:0", SystemID: "mock", AutoDLR: true})
	go func() { _ = smsc.ListenAndServe(ctx) }()
	time.Sleep(100 * time.Millisecond)
	st := storage.NewMemory()
	r := routing.New()
	r.AddProvider(provider.Modern50("p50", smsc.Addr(), "demo", "secret"))
	r.AddRule(routing.Rule{ID: "all", ProviderID: "p50", Priority: 1})
	svc := service.New(service.Config{Router: r, Store: st})
	svc.StartWorkers(ctx, 2)
	_ = svc.Enqueue(ctx, message.SubmitRequest{From: "SVC", To: "9779800000000", Text: "async worker with SAR TLV multipart " + fmt.Sprint(time.Now()), RegisteredDelivery: 1}, 5, time.Now())
	time.Sleep(time.Second)
	msgs, _ := st.ListMessages(ctx)
	fmt.Println("messages", len(msgs))
	dlrs, _ := st.ListDLR(ctx)
	fmt.Println("dlrs", len(dlrs))
	_ = svc.Close(ctx)
	_ = smsc.Close()
}
