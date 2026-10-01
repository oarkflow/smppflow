package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/server"
	"github.com/oarkflow/smppflow/pkg/session"
	"github.com/oarkflow/smppflow/pkg/storage"
)

func TestClientServerSubmit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := server.New(server.Config{Addr: "127.0.0.1:0", SystemID: "smsc", AutoDLR: true})
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not bind in time")
	}
	cli := session.NewClient(session.Config{Addr: srv.Addr(), Bind: protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "x", Password: "y", InterfaceVersion: protocol.Version34}, EnquireInterval: time.Hour})
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Bind(ctx); err != nil {
		t.Fatal(err)
	}
	st := storage.NewMemory()
	mgr := message.NewManager(message.ManagerConfig{Client: cli, Store: st, DefaultDestNPI: protocol.NPIISDN})
	mgr.StartInbound(ctx)
	res, err := mgr.Submit(ctx, message.SubmitRequest{From: "A", To: "9771", Text: "hello", RegisteredDelivery: 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Parts != 1 {
		t.Fatalf("parts=%d", res.Parts)
	}
	time.Sleep(400 * time.Millisecond)
	dlrs, _ := st.ListDLR(ctx)
	if len(dlrs) == 0 {
		t.Fatal("expected dlr")
	}
	_ = cli.Close(ctx)
	_ = srv.Close()
}
