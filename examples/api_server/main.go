package main

import (
	"context"
	"log"
	"net/http"
	"time"

	smppapi "github.com/oarkflow/smppflow/api"
	"github.com/oarkflow/smppflow/provider"
	"github.com/oarkflow/smppflow/routing"
	"github.com/oarkflow/smppflow/server"
	"github.com/oarkflow/smppflow/service"
	"github.com/oarkflow/smppflow/storage"
)

func main() {
	ctx := context.Background()
	smsc := server.New(server.Config{Addr: "127.0.0.1:2776", SystemID: "mock", AutoDLR: true})
	go func() { log.Println(smsc.ListenAndServe(ctx)) }()
	time.Sleep(100 * time.Millisecond)
	st := storage.NewMemory()
	r := routing.New()
	r.AddProvider(provider.Generic34("local", "127.0.0.1:2776", "demo", "secret"))
	r.AddRule(routing.Rule{ID: "default", ProviderID: "local", Priority: 1})
	svc := service.New(service.Config{Router: r, Store: st})
	svc.StartWorkers(ctx, 1)
	api := smppapi.New(svc, st)
	log.Println("API on :8088. POST /v1/messages/send, /v1/messages/enqueue, GET /v1/messages, /v1/dlrs, /v1/reports/summary")
	log.Fatal(http.ListenAndServe(":8088", api.Handler()))
}
