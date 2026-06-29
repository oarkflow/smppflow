package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/smppflow/events"
	"github.com/oarkflow/smppflow/hooks"
	"github.com/oarkflow/smppflow/message"
	"github.com/oarkflow/smppflow/provider"
	"github.com/oarkflow/smppflow/reports"
	"github.com/oarkflow/smppflow/routing"
	"github.com/oarkflow/smppflow/server"
	"github.com/oarkflow/smppflow/service"
	"github.com/oarkflow/smppflow/storage"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	smsc := server.New(server.Config{Addr: "127.0.0.1:0", SystemID: "mock-smsc", AutoDLR: true})
	go func() { _ = smsc.ListenAndServe(ctx) }()
	time.Sleep(100 * time.Millisecond)

	bus := events.NewMemoryBus(false)
	bus.Subscribe("*", func(ctx context.Context, e events.Event) error { fmt.Println("event", e.Topic); return nil })
	hm := hooks.New()
	hm.Register(hooks.BeforeSubmit, 10, time.Second, func(ctx context.Context, payload any) error { fmt.Println("hook BeforeSubmit"); return nil })

	st := storage.NewMemory()
	rt := routing.New()
	rt.AddProvider(provider.Generic34("primary", smsc.Addr(), "demo", "secret"))
	rt.AddRule(routing.Rule{ID: "nepal", Prefix: "977", ProviderID: "primary", Priority: 1})
	svc := service.New(service.Config{Router: rt, Store: st, Bus: bus, Hooks: hm})
	svc.StartWorkers(ctx, 1)

	direct, err := svc.Send(ctx, message.SubmitRequest{TenantID: "tenant-a", From: "OARK", To: "9779800000000", Text: "Hello comprehensive SMPP 👋", RegisteredDelivery: 1})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("direct result: %+v\n", direct)

	if err := svc.Enqueue(ctx, message.SubmitRequest{TenantID: "tenant-a", From: "OARK", To: "9779800000001", Text: "queued multipart " + string(bytes.Repeat([]byte("A"), 200)), RegisteredDelivery: 1}, 10, time.Now()); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	var out bytes.Buffer
	_ = reports.WriteJSON(ctx, &out, st)
	fmt.Println(out.String())
	_ = svc.Close(ctx)
	_ = smsc.Close()
}
