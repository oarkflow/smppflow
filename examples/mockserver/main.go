package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/oarkflow/smppflow/protocol"
	"github.com/oarkflow/smppflow/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	s := server.New(server.Config{Addr: ":2775", SystemID: "mock-smsc", Version: protocol.Version34, AutoDLR: true, Auth: func(b protocol.BindRequest) protocol.CommandStatus {
		if b.SystemID == "demo" && b.Password == "secret" {
			return protocol.ESME_ROK
		}
		return protocol.ESME_RBINDFAIL
	}})
	log.Println("mock SMSC listening on :2775")
	if err := s.ListenAndServe(ctx); err != nil {
		log.Println(err)
	}
}
