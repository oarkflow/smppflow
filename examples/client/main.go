package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/session"
)

var demoAPIKey string

func main() {
	var (
		mode      = flag.String("mode", "all", "demo mode: all, http, smpp")
		httpBase  = flag.String("http", "http://127.0.0.1:8080", "HTTP API base URL")
		apiKey    = flag.String("api-key", os.Getenv("SMPPFLOW_API_KEY"), "HTTP API key (or SMPPFLOW_API_KEY)")
		smppAddr  = flag.String("smpp", "127.0.0.1:2775", "SMPP server address")
		systemID  = flag.String("system-id", "smppclient", "SMPP system_id")
		password  = flag.String("password", "password", "SMPP password")
		tenantID  = flag.String("tenant", "tenant-a", "tenant id")
		from      = flag.String("from", "BRAND", "source address")
		to        = flag.String("to", "9779841000000", "destination address")
		text      = flag.String("text", "Your OTP is 123456", "message text")
		logFormat = flag.String("log-format", "json", "log format: json or text")
	)
	flag.Parse()
	demoAPIKey = *apiKey

	logger := newLogger(*logFormat)
	slog.SetDefault(logger)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	switch strings.ToLower(*mode) {
	case "http":
		runHTTP(ctx, logger, *httpBase, *tenantID, *from, *to, *text)
	case "smpp":
		runSMPP(ctx, logger, *smppAddr, *systemID, *password, *from, *to, *text)
	case "all":
		runHTTP(ctx, logger, *httpBase, *tenantID, *from, *to, *text)
		runSMPP(ctx, logger, *smppAddr, *systemID, *password, *from, *to, *text)
	default:
		logger.Error("unknown mode", "mode", *mode)
		os.Exit(2)
	}
}

func newLogger(format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if strings.EqualFold(format, "text") {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func runHTTP(ctx context.Context, logger *slog.Logger, base, tenant, from, to, text string) {
	logger.Info("http.demo.start", "base", base)
	get(ctx, logger, base+"/healthz")

	sendPayload := message.SubmitRequest{
		TenantID:           tenant,
		From:               from,
		To:                 to,
		Text:               text,
		RegisteredDelivery: 1,
		UseSAR:             true,
		IdempotencyKey:     "demo-key-" + time.Now().Format("150405.000000"),
		CampaignID:         "demo-campaign",
		Meta: map[string]string{
			"country":      "NP",
			"operator":     "NTC",
			"message_type": "otp",
			"user_id":      "demo-user",
			"campaign_id":  "demo-campaign",
		},
	}

	post(ctx, logger, base+"/v1/manager/route/simulate", sendPayload)
	post(ctx, logger, base+"/v1/messages/send", sendPayload)

	enqueuePayload := struct {
		message.SubmitRequest
		Priority int
		RunAt    time.Time
	}{SubmitRequest: sendPayload, Priority: 5, RunAt: time.Now()}
	enqueuePayload.IdempotencyKey = "queued-key-" + time.Now().Format("150405.000000")
	enqueuePayload.Text = text + " queued"
	post(ctx, logger, base+"/v1/messages/enqueue", enqueuePayload)

	time.Sleep(1200 * time.Millisecond)
	get(ctx, logger, base+"/v1/messages")
	get(ctx, logger, base+"/v1/dlrs")
	get(ctx, logger, base+"/v1/reports/summary")
	get(ctx, logger, base+"/v1/reports/delivery?tenant_id="+tenant+"&group_by=day")
	get(ctx, logger, base+"/v1/reports/providers?provider_id=local-smpp")
	get(ctx, logger, base+"/v1/reports/tenants?tenant_id="+tenant)
	get(ctx, logger, base+"/v1/reports/errors?tenant_id="+tenant)
	get(ctx, logger, base+"/v1/manager/queue")
	get(ctx, logger, base+"/v1/manager/dlq")
	get(ctx, logger, base+"/v1/manager/tenants")
	get(ctx, logger, base+"/v1/manager/providers")
	get(ctx, logger, base+"/v1/manager/binds")
	get(ctx, logger, base+"/v1/manager/senders")
	get(ctx, logger, base+"/v1/manager/audit")
	get(ctx, logger, base+"/v1/contacts")
	get(ctx, logger, base+"/v1/optouts")
	get(ctx, logger, base+"/v1/ratelimits")
	get(ctx, logger, base+"/v1/alerts/open")
	get(ctx, logger, base+"/v1/reports/export.ndjson")
	logger.Info("http.demo.complete")
}

func runSMPP(ctx context.Context, logger *slog.Logger, addr, systemID, password, from, to, text string) {
	logger.Info("smpp.demo.start", "addr", addr, "system_id", systemID)
	cli := session.NewClient(session.Config{
		Addr:             addr,
		Bind:             protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: systemID, Password: password, InterfaceVersion: protocol.Version34},
		WindowSize:       8,
		EnquireInterval:  5 * time.Second,
		ResponseTimeout:  5 * time.Second,
		AutoReconnect:    false,
		ReconnectBackoff: time.Second,
	})
	if err := cli.Connect(ctx); err != nil {
		logger.Error("smpp.connect.failed", "error", err)
		return
	}
	defer cli.Close(context.Background())
	if err := cli.Bind(ctx); err != nil {
		logger.Error("smpp.bind.failed", "error", err)
		return
	}
	logger.Info("smpp.bound", "state", string(cli.State()), "window", cli.WindowSize())

	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.After(3 * time.Second)
		for {
			select {
			case p := <-cli.Inbound():
				logger.Info("smpp.inbound_pdu", "command", p.CommandID.String(), "seq", p.SequenceNumber, "status", p.CommandStatus.String(), "bytes", len(p.Body))
				if p.CommandID == protocol.DeliverSM {
					req, err := protocol.ParseSubmitSM(p)
					if err != nil {
						logger.Warn("smpp.deliver_sm.parse_failed", "error", err)
						continue
					}
					logger.Info("smpp.deliver_sm", "from", req.Source.Addr, "to", req.Destination.Addr, "esm_class", req.ESMClass, "text", string(req.ShortMessage))
					_ = cli.DeliverSMResp(context.Background(), p.SequenceNumber, protocol.ESME_ROK)
				}
			case <-deadline:
				return
			}
		}
	}()

	submitReq := protocol.SubmitSMRequest{
		Source:             protocol.Address{Addr: from, TON: protocol.TONAlphanumeric, NPI: protocol.NPIUnknown},
		Destination:        protocol.Address{Addr: to, TON: protocol.TONInternational, NPI: protocol.NPIISDN},
		RegisteredDelivery: 1,
		DataCoding:         protocol.DataCodingDefault,
		ShortMessage:       []byte(text),
	}
	submitResp, err := cli.SubmitSM(ctx, submitReq)
	if err != nil {
		logger.Error("smpp.submit_sm.failed", "error", err)
		return
	}
	logger.Info("smpp.submit_sm.ok", "provider_message_id", submitResp.MessageID)

	q, err := cli.QuerySM(ctx, protocol.QuerySMRequest{MessageID: submitResp.MessageID, Source: protocol.Address{Addr: from, TON: protocol.TONAlphanumeric, NPI: protocol.NPIUnknown}})
	if err != nil {
		logger.Warn("smpp.query_sm.failed", "error", err)
	} else {
		logger.Info("smpp.query_sm.ok", "message_id", q.MessageID, "state", q.MessageState, "error", q.ErrorCode)
	}

	dataResp, err := cli.DataSM(ctx, submitReq)
	if err != nil {
		logger.Warn("smpp.data_sm.failed", "error", err)
	} else {
		logger.Info("smpp.data_sm.ok", "provider_message_id", dataResp.MessageID)
	}

	multiResp, err := cli.SubmitMulti(ctx, protocol.SubmitMultiRequest{SubmitSMRequest: submitReq, Destinations: []protocol.Address{{Addr: to, TON: protocol.TONInternational, NPI: protocol.NPIISDN}, {Addr: "9779841000001", TON: protocol.TONInternational, NPI: protocol.NPIISDN}}})
	if err != nil {
		logger.Warn("smpp.submit_multi.failed", "error", err)
	} else {
		logger.Info("smpp.submit_multi.ok", "provider_message_id", multiResp.MessageID)
	}

	if err := cli.ReplaceSM(ctx, protocol.ReplaceSMRequest{MessageID: submitResp.MessageID, Source: protocol.Address{Addr: from, TON: protocol.TONAlphanumeric, NPI: protocol.NPIUnknown}, RegisteredDelivery: 1, DataCoding: protocol.DataCodingDefault, ShortMessage: []byte("replacement text")}); err != nil {
		logger.Warn("smpp.replace_sm.failed", "error", err)
	} else {
		logger.Info("smpp.replace_sm.ok", "message_id", submitResp.MessageID)
	}

	if err := cli.CancelSM(ctx, protocol.CancelSMRequest{MessageID: submitResp.MessageID, Source: protocol.Address{Addr: from, TON: protocol.TONAlphanumeric, NPI: protocol.NPIUnknown}, Destination: protocol.Address{Addr: to, TON: protocol.TONInternational, NPI: protocol.NPIISDN}}); err != nil {
		logger.Warn("smpp.cancel_sm.failed", "error", err)
	} else {
		logger.Info("smpp.cancel_sm.ok", "message_id", submitResp.MessageID)
	}

	<-done
	logger.Info("smpp.demo.complete")
}

func get(ctx context.Context, logger *slog.Logger, url string) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if demoAPIKey != "" {
		req.Header.Set("Authorization", "Bearer "+demoAPIKey)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Error("http.get.failed", "url", url, "error", err)
		return
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	logger.Info("http.get", "url", url, "status", res.StatusCode, "body", decodeBody(body))
}

func post(ctx context.Context, logger *slog.Logger, url string, payload any) {
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if demoAPIKey != "" {
		req.Header.Set("Authorization", "Bearer "+demoAPIKey)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Error("http.post.failed", "url", url, "error", err)
		return
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	logger.Info("http.post", "url", url, "status", res.StatusCode, "request", decodeBody(b), "body", decodeBody(body))
}

func decodeBody(b []byte) any {
	s := strings.TrimSpace(string(b))
	if s == "" {
		return ""
	}
	var v any
	if err := json.Unmarshal([]byte(s), &v); err == nil {
		return v
	}
	lines := strings.Split(s, "\n")
	if len(lines) > 1 {
		items := make([]any, 0, len(lines))
		allJSON := true
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var item any
			if err := json.Unmarshal([]byte(line), &item); err != nil {
				allJSON = false
				break
			}
			items = append(items, item)
		}
		if allJSON {
			return items
		}
	}
	return s
}

func pretty(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}
