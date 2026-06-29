package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/server"
	"github.com/oarkflow/smppflow/pkg/session"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	smsc := server.New(server.Config{Addr: "127.0.0.1:0", SystemID: "mock-v50", Version: protocol.Version50, AutoDLR: true})
	go func() { _ = smsc.ListenAndServe(ctx) }()
	time.Sleep(100 * time.Millisecond)

	cli := session.NewClient(session.Config{Addr: smsc.Addr(), Bind: protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "demo", Password: "secret", InterfaceVersion: protocol.Version50}, EnquireInterval: time.Hour})
	if err := cli.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	if err := cli.Bind(ctx); err != nil {
		log.Fatal(err)
	}

	src := protocol.Address{TON: protocol.TONAlphanumeric, NPI: protocol.NPIUnknown, Addr: "OPS"}
	dst := protocol.Address{TON: protocol.TONInternational, NPI: protocol.NPIISDN, Addr: "9779800000000"}

	submit, err := cli.SubmitSM(ctx, protocol.SubmitSMRequest{Source: src, Destination: dst, RegisteredDelivery: 1, DataCoding: protocol.DataCodingDefault, ShortMessage: []byte("submit_sm")})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("submit_sm", submit.MessageID)

	multi, err := cli.SubmitMulti(ctx, protocol.SubmitMultiRequest{SubmitSMRequest: protocol.SubmitSMRequest{Source: src, DataCoding: protocol.DataCodingDefault, ShortMessage: []byte("submit_multi")}, Destinations: []protocol.Address{dst}})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("submit_multi", multi.MessageID)

	data, err := cli.DataSM(ctx, protocol.SubmitSMRequest{Source: src, Destination: dst, DataCoding: protocol.DataCodingDefault, ShortMessage: []byte("data_sm")})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("data_sm", data.MessageID)

	q, err := cli.QuerySM(ctx, protocol.QuerySMRequest{MessageID: submit.MessageID, Source: src})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("query_sm", q.MessageState)

	if err := cli.ReplaceSM(ctx, protocol.ReplaceSMRequest{MessageID: submit.MessageID, Source: src, ShortMessage: []byte("replacement")}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("replace_sm ok")
	if err := cli.CancelSM(ctx, protocol.CancelSMRequest{MessageID: submit.MessageID, Source: src, Destination: dst}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("cancel_sm ok")

	bcast, err := cli.BroadcastSM(ctx, protocol.BroadcastSMRequest{ServiceType: "cell", Source: src, MessageID: "bcast-1", DataCoding: protocol.DataCodingDefault, ShortMessage: []byte("broadcast_sm")})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("broadcast_sm", bcast.MessageID)
	qb, err := cli.QueryBroadcastSM(ctx, protocol.QueryBroadcastSMRequest{MessageID: bcast.MessageID, Source: src})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("query_broadcast_sm", qb.MessageState)
	if err := cli.CancelBroadcastSM(ctx, protocol.CancelBroadcastSMRequest{ServiceType: "cell", MessageID: bcast.MessageID, Source: src}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("cancel_broadcast_sm ok")

	// Build/parse the SMSC-initiated operations as part of the runnable coverage.
	outbind, _ := protocol.ParseOutbind(protocol.BuildOutbind(100, protocol.OutbindRequest{SystemID: "mock-v50", Password: "pw"}))
	alert, _ := protocol.ParseAlertNotification(protocol.BuildAlertNotification(101, protocol.AlertNotificationRequest{Source: dst, Esme: src}))
	fmt.Println("outbind", outbind.SystemID)
	fmt.Println("alert_notification", alert.Source.Addr)

	_ = cli.Close(ctx)
	_ = smsc.Close()
}
