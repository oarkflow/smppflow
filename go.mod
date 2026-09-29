module github.com/oarkflow/smppflow

go 1.26

replace github.com/oarkflow/broker => ../broker

require github.com/oarkflow/broker v0.0.0-00010101000000-000000000000

require (
	github.com/coder/websocket v1.8.15 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/oarkflow/zlog v0.0.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.29 // indirect
	go.etcd.io/raft/v3 v3.7.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
