package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/smppflow/protocol"
	"github.com/oarkflow/smppflow/server"
	"github.com/oarkflow/smppflow/session"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	smsc := server.New(server.Config{Addr: "127.0.0.1:0", SystemID: "mock", AutoDLR: false})
	go func() { _ = smsc.ListenAndServe(ctx) }()
	time.Sleep(100 * time.Millisecond)
	cli := session.NewClient(session.Config{Addr: smsc.Addr(), Bind: protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "demo", Password: "secret", InterfaceVersion: protocol.Version50}, EnquireInterval: time.Hour})
	if err := cli.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	if err := cli.Bind(ctx); err != nil {
		log.Fatal(err)
	}
	src := protocol.Address{TON: protocol.TONAlphanumeric, NPI: protocol.NPIUnknown, Addr: "PROTO"}
	dst := protocol.Address{TON: protocol.TONInternational, NPI: protocol.NPIISDN, Addr: "9779800000000"}
	sm, err := cli.SubmitSM(ctx, protocol.SubmitSMRequest{Source: src, Destination: dst, RegisteredDelivery: 1, DataCoding: protocol.DataCodingDefault, ShortMessage: []byte("submit_sm")})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("submit", sm.MessageID)
	data, err := cli.DataSM(ctx, protocol.SubmitSMRequest{Source: src, Destination: dst, DataCoding: protocol.DataCodingDefault, ShortMessage: []byte("data_sm")})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("data_sm", data.MessageID)
	q, err := cli.QuerySM(ctx, protocol.QuerySMRequest{MessageID: sm.MessageID, Source: src})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("query", q.MessageState)
	_ = cli.ReplaceSM(ctx, protocol.ReplaceSMRequest{MessageID: sm.MessageID, Source: src, ShortMessage: []byte("replacement")})
	_ = cli.CancelSM(ctx, protocol.CancelSMRequest{MessageID: sm.MessageID, Source: src, Destination: dst})
	_ = cli.Close(ctx)
	_ = smsc.Close()
}
