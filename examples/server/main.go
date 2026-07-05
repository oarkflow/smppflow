package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oarkflow/smppflow/pkg/account"
	"github.com/oarkflow/smppflow/pkg/alerts"
	httpapi "github.com/oarkflow/smppflow/pkg/api"
	"github.com/oarkflow/smppflow/pkg/compliance"
	"github.com/oarkflow/smppflow/pkg/contacts"
	"github.com/oarkflow/smppflow/pkg/events"
	"github.com/oarkflow/smppflow/pkg/hooks"
	"github.com/oarkflow/smppflow/pkg/manager"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/optout"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/provider"
	"github.com/oarkflow/smppflow/pkg/queue"
	"github.com/oarkflow/smppflow/pkg/ratelimit"
	"github.com/oarkflow/smppflow/pkg/routing"
	smpp "github.com/oarkflow/smppflow/pkg/server"
	"github.com/oarkflow/smppflow/pkg/service"
	"github.com/oarkflow/smppflow/pkg/session"
	"github.com/oarkflow/smppflow/pkg/storage"
)

type allowCompliance struct{ blocked []string }

func (f allowCompliance) Evaluate(ctx context.Context, req message.SubmitRequest, country string) compliance.RuleDecision {
	_ = ctx
	text := strings.ToLower(req.Text)
	for _, word := range f.blocked {
		if strings.Contains(text, strings.ToLower(word)) {
			return compliance.RuleDecision{Allowed: false, Reasons: []string{"blocked content: " + word}}
		}
	}
	return compliance.RuleDecision{Allowed: true}
}

type staticRating struct{}

func (staticRating) RateRoute(ctx context.Context, req message.SubmitRequest, providerID string, segments int) (routing.RouteRate, error) {
	_ = ctx
	if segments <= 0 {
		segments = 1
	}
	sell := int64(10000 * segments)
	cost := int64(6500 * segments)
	return routing.RouteRate{ProviderID: providerID, Currency: "USD", CostMicros: cost, SellMicros: sell, MarginMicros: sell - cost, Billable: true}, nil
}

type memoryBalance struct{ disabledTenant string }

func newLogger(format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if strings.EqualFold(format, "text") {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func (b memoryBalance) CanReserve(ctx context.Context, tenantID string, rate routing.RouteRate) error {
	_ = ctx
	_ = rate
	if tenantID == b.disabledTenant {
		return errors.New("tenant has insufficient balance")
	}
	return nil
}

func main() {
	var (
		smppAddr       = flag.String("smpp", ":2775", "SMPP listener address")
		httpAddr       = flag.String("http", ":8080", "HTTP API listener address")
		publicSMPPAddr = flag.String("provider-addr", "127.0.0.1:2775", "address used by the HTTP service to connect to the local SMPP server")
		systemID       = flag.String("system-id", "smppclient", "accepted SMPP system_id")
		password       = flag.String("password", "password", "accepted SMPP password")
		tenantID       = flag.String("tenant", "tenant-a", "default demo tenant")
		workers        = flag.Int("workers", 2, "async queue workers")
		routeFile      = flag.String("routes", "./routes.demo.json", "file-backed route repository path")
		logFormat      = flag.String("log-format", "json", "log format: json or text")
	)
	flag.Parse()

	logger := newLogger(*logFormat)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	bus := events.NewMemoryBus(false)
	bus.Subscribe("*", func(ctx context.Context, e events.Event) error {
		logger.Info("event", "topic", e.Topic, "data", fmt.Sprint(e.Data))
		return nil
	})

	hookMgr := hooks.New()
	hookMgr.Register(hooks.BeforeConnect, 10, time.Second, func(ctx context.Context, payload any) error {
		logger.Info("hook.before_connect", "payload", fmt.Sprintf("%T", payload))
		return nil
	})
	hookMgr.Register(hooks.AfterBind, 10, time.Second, func(ctx context.Context, payload any) error {
		logger.Info("hook.after_bind", "payload", fmt.Sprintf("%T", payload))
		return nil
	})
	hookMgr.Register(hooks.AfterSubmit, 10, time.Second, func(ctx context.Context, payload any) error {
		logger.Info("hook.after_submit", "payload", fmt.Sprintf("%T", payload))
		return nil
	})
	hookMgr.Register(hooks.OnDLRReceived, 10, time.Second, func(ctx context.Context, payload any) error {
		logger.Info("hook.dlr_received", "payload", fmt.Sprintf("%+v", payload))
		return nil
	})

	var submitted atomic.Uint64
	smppServer := smpp.New(smpp.Config{
		Addr:     *smppAddr,
		SystemID: "demo-smsc",
		Version:  protocol.Version34,
		Bus:      bus,
		AutoDLR:  true,
		DLRDelay: 500 * time.Millisecond,
		Auth: func(req protocol.BindRequest) protocol.CommandStatus {
			logger.Info("smpp.bind", "system_id", req.SystemID, "mode", req.Mode, "version", req.InterfaceVersion)
			if req.SystemID != *systemID || req.Password != *password {
				logger.Warn("smpp.bind_rejected", "system_id", req.SystemID)
				return protocol.ESME_RINVPASWD
			}
			return protocol.ESME_ROK
		},
		Submit: func(ctx context.Context, req protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			id := fmt.Sprintf("smsc-%06d", submitted.Add(1))
			logger.Info("smpp.submit_sm", "message_id", id, "from", req.Source.Addr, "to", req.Destination.Addr, "coding", req.DataCoding, "registered_delivery", req.RegisteredDelivery, "bytes", len(req.ShortMessage))
			return id, protocol.ESME_ROK
		},
		DataSM: func(ctx context.Context, req protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			id := fmt.Sprintf("data-%06d", submitted.Add(1))
			logger.Info("smpp.data_sm", "message_id", id, "from", req.Source.Addr, "to", req.Destination.Addr)
			return id, protocol.ESME_ROK
		},
		Query: func(ctx context.Context, req protocol.QuerySMRequest) (protocol.QuerySMRespData, protocol.CommandStatus) {
			logger.Info("smpp.query_sm", "message_id", req.MessageID, "source", req.Source.Addr)
			return protocol.QuerySMRespData{MessageID: req.MessageID, MessageState: 2, ErrorCode: 0}, protocol.ESME_ROK
		},
		Cancel: func(ctx context.Context, req any) protocol.CommandStatus {
			logger.Info("smpp.cancel_sm", "request", fmt.Sprintf("%+v", req))
			return protocol.ESME_ROK
		},
		Replace: func(ctx context.Context, req any) protocol.CommandStatus {
			logger.Info("smpp.replace_sm", "request", fmt.Sprintf("%+v", req))
			return protocol.ESME_ROK
		},
		Broadcast: func(ctx context.Context, req protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			id := fmt.Sprintf("broadcast-%06d", submitted.Add(1))
			logger.Info("smpp.broadcast_sm", "message_id", id, "from", req.Source.Addr)
			return id, protocol.ESME_ROK
		},
	})
	go func() {
		logger.Info("smpp.server.start", "addr", *smppAddr, "system_id", *systemID)
		if err := smppServer.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			logger.Error("smpp.server.error", "error", err)
			stop()
		}
	}()

	msgStore := storage.NewMemory()
	q := queue.NewMemory()
	accountStore := account.NewMemory()
	router := routing.New()
	router.SetRouteRepository(routing.NewFileRouteRepository(*routeFile))
	router.SetSnapshotStore(routing.NewMemorySnapshotStore())
	router.SetCapacityManager(routing.NewMemoryCapacityManager())
	router.SetCircuitBreaker(routing.NewCircuitBreaker())
	router.SetSLAStats(routing.NewSLAStats())
	router.SetComplianceFilter(allowCompliance{blocked: []string{"casino", "spam"}})
	router.SetRatingFilter(staticRating{})
	router.SetBalanceFilter(memoryBalance{disabledTenant: "suspended-tenant"})
	router.SetStickyBy("campaign_id")
	router.SetStrategy(routing.StrategyBalanced)

	coverage := routing.NewCoverageMatrix()
	coverage.Upsert(routing.Coverage{ProviderID: "local-smpp", Country: "NP", MessageType: "otp", SupportsAlphaSender: true, SupportsNumericSender: true, SupportsDLR: true, SupportsUnicode: true, SupportsLongSMS: true, Enabled: true})
	coverage.Upsert(routing.Coverage{ProviderID: "local-smpp", Country: "NP", MessageType: "marketing", SupportsAlphaSender: true, SupportsNumericSender: true, SupportsDLR: true, SupportsUnicode: true, SupportsLongSMS: true, Enabled: true})
	router.SetCoverageMatrix(coverage)

	prof := provider.Generic34("local-smpp", *publicSMPPAddr, *systemID, *password)
	prof.WindowSize = 16
	prof.TPS = 100
	prof.ResponseTimeout = 5 * time.Second
	prof.EnquireInterval = 10 * time.Second
	prof.Multipart = provider.MultipartSAR
	if err := router.RegisterProvider(prof); err != nil {
		logger.Error("router.provider.error", "error", err)
		os.Exit(1)
	}
	router.SetProviderState("local-smpp", routing.ProviderActive)
	router.UpdateProviderStats(routing.ProviderStats{ProviderID: "local-smpp", QualityScore: 99, DeliveryRate: 0.995, AvgLatency: 80 * time.Millisecond, MaxTPS: 100, CostPerSegment: 0.01})

	rules := []routing.Rule{
		{ID: "tenant-a-np-otp", TenantID: *tenantID, Prefix: "977", Country: "NP", ProviderID: "local-smpp", Priority: 1, Weight: 100, Objective: routing.ObjectiveHighestDelivery, MessageTypes: []string{"otp"}, MinQuality: 90},
		{ID: "tenant-a-np-marketing", TenantID: *tenantID, Prefix: "977", Country: "NP", ProviderID: "local-smpp", Priority: 2, Weight: 100, Objective: routing.ObjectiveBalanced, MessageTypes: []string{"marketing"}, MaxCost: 20000},
		{ID: "tenant-a-fallback", TenantID: *tenantID, ProviderID: "local-smpp", Priority: 100, Weight: 1, Objective: routing.ObjectiveBalanced},
	}
	for _, rule := range rules {
		if err := router.RegisterRule(rule); err != nil {
			logger.Error("router.rule.error", "rule", rule.ID, "error", err)
			os.Exit(1)
		}
	}
	if issues := router.ValidateAllDetailed(); len(issues) > 0 {
		for _, issue := range issues {
			logger.Info("router.validation", "severity", issue.Severity, "rule", issue.RuleID, "code", issue.Code, "message", issue.Message)
		}
	}
	if err := router.SaveRoutes(ctx); err != nil {
		logger.Warn("router.save_routes.failed", "error", err)
	}

	cred := account.ProviderCredential{TenantID: *tenantID, ProviderID: "local-smpp", SystemID: *systemID, Password: *password, BindMode: string(protocol.BindModeTransceiver), Enabled: true}
	_ = accountStore.SaveProviderCredential(ctx, cred)
	demoUser := account.User{ID: "demo-admin", TenantID: *tenantID, Username: "demo", DisplayName: "Demo Admin", Roles: []string{"admin", "support"}, Enabled: true}
	_ = accountStore.SaveUser(ctx, demoUser)
	apiKey, apiSecret, _ := account.NewAPIKey(*tenantID, demoUser.ID, "demo")
	_ = accountStore.SaveAPIKey(ctx, apiKey)
	logger.Info("demo.api_key", "secret", apiSecret, "note", "demo only; do not log secrets in production")

	svc := service.New(service.Config{Router: router, Store: msgStore, Queue: q, Bus: bus, Hooks: hookMgr, AccountStore: accountStore, RetryDelay: time.Second})
	svc.StartWorkers(ctx, *workers)

	mgrStore := manager.NewStore()
	ops := manager.NewOperations(mgrStore, msgStore, router, q, session.NewManager(nil))
	_ = ops.UpsertTenant(ctx, "bootstrap", manager.Tenant{ID: *tenantID, Name: "Demo Tenant", Status: manager.TenantActive, TPS: 100, AllowedCountries: []string{"NP"}, AllowedSenderIDs: []string{"BRAND", "OTP", "PROMO"}})
	_ = ops.SaveProvider(ctx, "bootstrap", prof)
	_ = ops.SaveBind(ctx, "bootstrap", manager.BindAccount{TenantID: *tenantID, SystemID: *systemID, BindMode: string(protocol.BindModeTransceiver), MaxBinds: 2, TPS: 100, Enabled: true})
	_ = mgrStore.SaveSenderApproval(ctx, manager.SenderApproval{TenantID: *tenantID, Sender: "BRAND", Country: "NP", State: manager.SenderApproved})
	_ = mgrStore.SaveSenderApproval(ctx, manager.SenderApproval{TenantID: *tenantID, Sender: "OTP", Country: "NP", State: manager.SenderApproved})

	optouts := optout.NewStore()
	contactStore := contacts.NewStore()
	_ = contactStore.Upsert(ctx, contacts.Contact{TenantID: *tenantID, MSISDN: "9779841000000", FirstName: "Demo", Country: "NP", Operator: "NTC", Tags: []string{"otp", "demo"}})
	limiter := ratelimit.New()
	_ = limiter.SetPolicy(ratelimit.Policy{Key: "tenant:" + *tenantID, Scope: ratelimit.ScopeTenant, RatePerSecond: 100, Burst: 100, QueueLimit: 1000, Adaptive: true, MaxRatePerSecond: 200})
	alertEngine := alerts.New()
	alertEngine.AddRule(alerts.Rule{ID: "queue-backlog", Name: "Queue backlog", Signal: "queue_depth", Above: 100, Severity: alerts.Warning})

	apiServer := httpapi.New(svc, msgStore)
	apiServer.Ops = ops
	apiServer.OptOut = optouts
	apiServer.Contacts = contactStore
	apiServer.Limiter = limiter
	apiServer.Alerts = alertEngine

	httpSrv := &http.Server{Addr: *httpAddr, Handler: requestLogger(logger, apiServer.Handler()), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("http.server.start", "addr", *httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http.server.error", "error", err)
			stop()
		}
	}()

	printUsage(logger, *httpAddr, *smppAddr, *tenantID, *systemID, *password)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	_ = smppServer.Close()
	logger.Info("shutdown.complete")
}

func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		logger.Info("http.request", "method", r.Method, "path", r.URL.Path, "status", rw.status, "duration", time.Since(started).String(), "remote", r.RemoteAddr)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func printUsage(logger *slog.Logger, httpAddr, smppAddr, tenantID, systemID, password string) {
	httpURL := "http://127.0.0.1" + httpAddr
	if strings.HasPrefix(httpAddr, "127.0.0.1") || strings.HasPrefix(httpAddr, "localhost") {
		httpURL = "http://" + httpAddr
	}
	smppURL := "127.0.0.1" + smppAddr
	if strings.HasPrefix(smppAddr, "127.0.0.1") || strings.HasPrefix(smppAddr, "localhost") {
		smppURL = smppAddr
	}
	logger.Info("ready", "smpp", smppAddr, "http", httpAddr)
	logger.Info("usage.health", "command", "curl -s "+httpURL+"/healthz")
	logger.Info("usage.send", "command", "curl -s -X POST "+httpURL+"/v1/messages/send -H 'Content-Type: application/json' -d '{\"TenantID\":\""+tenantID+"\",\"From\":\"BRAND\",\"To\":\"9779841000000\",\"Text\":\"Your OTP is 123456\",\"RegisteredDelivery\":1,\"Meta\":{\"country\":\"NP\",\"message_type\":\"otp\"}}'")
	logger.Info("usage.route_simulate", "command", "curl -s -X POST "+httpURL+"/v1/manager/route/simulate -H 'Content-Type: application/json' -d '{\"TenantID\":\""+tenantID+"\",\"From\":\"BRAND\",\"To\":\"9779841000000\",\"Text\":\"Your OTP is 123456\",\"Meta\":{\"country\":\"NP\",\"message_type\":\"otp\"}}'")
	logger.Info("usage.client", "command", "go run ./examples/client --http "+httpURL+" --smpp "+smppURL+" --system-id "+systemID+" --password "+password)
}
