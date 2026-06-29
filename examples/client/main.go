package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/reports"
	"github.com/oarkflow/smppflow/pkg/server"
	"github.com/oarkflow/smppflow/pkg/session"
	"github.com/oarkflow/smppflow/pkg/storage"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	smsc := server.New(server.Config{Addr: "127.0.0.1:0", SystemID: "mock-smsc", Version: protocol.Version34, AutoDLR: true, Auth: func(b protocol.BindRequest) protocol.CommandStatus {
		if b.SystemID == "demo" && b.Password == "secret" {
			return protocol.ESME_ROK
		}
		return protocol.ESME_RBINDFAIL
	}})
	go func() { _ = smsc.ListenAndServe(ctx) }()
	time.Sleep(100 * time.Millisecond)
	cli := session.NewClient(session.Config{Addr: smsc.Addr(), Bind: protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "demo", Password: "secret", SystemType: "", InterfaceVersion: protocol.Version34}, WindowSize: 8, EnquireInterval: 10 * time.Second})
	if err := cli.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	if err := cli.Bind(ctx); err != nil {
		log.Fatal(err)
	}
	st := storage.NewMemory()
	mgr := message.NewManager(message.ManagerConfig{Client: cli, Store: st, DefaultSourceTON: protocol.TONAlphanumeric, DefaultSourceNPI: protocol.NPIUnknown, DefaultDestTON: protocol.TONInternational, DefaultDestNPI: protocol.NPIISDN})
	mgr.StartInbound(ctx)
	res, err := mgr.Submit(ctx, message.SubmitRequest{TenantID: "tenant-a", From: "OARK", To: "9779800000000", Text: "Hello from smppflow 👋 this message will use UCS2 and split automatically when it becomes long.", RegisteredDelivery: 1})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("submitted: %+v\n", res)
	time.Sleep(time.Second)
	var b bytes.Buffer
	_ = reports.WriteJSON(ctx, &b, st)
	fmt.Println(b.String())
	_ = cli.Close(ctx)
	_ = smsc.Close()
}
