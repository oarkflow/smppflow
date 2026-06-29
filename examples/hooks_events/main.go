package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/events"
	"github.com/oarkflow/smppflow/hooks"
)

func main() {
	ctx := context.Background()
	bus := events.NewMemoryBus(false)
	bus.Subscribe("*", func(ctx context.Context, e events.Event) error {
		fmt.Printf("event topic=%s data=%v\n", e.Topic, e.Data)
		return nil
	})
	hm := hooks.New()
	hm.Register(hooks.BeforeMessageValidate, 1, time.Second, func(ctx context.Context, p any) error { fmt.Println("validate hook", p); return nil })
	_ = bus.Publish(ctx, events.Event{Topic: events.MessageCreated, Data: "msg-1"})
	_ = hm.Run(ctx, hooks.BeforeMessageValidate, "payload")
}
