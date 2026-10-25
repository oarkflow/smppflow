package main

import (
	"context"
	"crypto/tls"
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
	"github.com/oarkflow/smppflow/pkg/durablequeue"
	"github.com/oarkflow/smppflow/pkg/events"
	"github.com/oarkflow/smppflow/pkg/hooks"
	"github.com/oarkflow/smppflow/pkg/manager"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/observability"
	"github.com/oarkflow/smppflow/pkg/optout"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/provider"
	"github.com/oarkflow/smppflow/pkg/queue"
	"github.com/oarkflow/smppflow/pkg/ratelimit"
	"github.com/oarkflow/smppflow/pkg/rbac"
	"github.com/oarkflow/smppflow/pkg/routing"
	"github.com/oarkflow/smppflow/pkg/security"
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
		smppTLSCert    = flag.String("smpp-tls-cert", "", "TLS certificate file for the SMPP listener (enables SMPPS)")
		smppTLSKey     = flag.String("smpp-tls-key", "", "TLS private key file for the SMPP listener")
		httpAddr       = flag.String("http", ":8080", "HTTP API listener address")
		publicSMPPAddr = flag.String("provider-addr", "127.0.0.1:2775", "address used by the HTTP service to connect to the local SMPP server")
		systemID       = flag.String("system-id", "smppclient", "accepted SMPP system_id")
		password       = flag.String("password", "password", "accepted SMPP password")
		tenantID       = flag.String("tenant", "tenant-a", "default demo tenant")
		workers        = flag.Int("workers", 2, "async queue workers")
		routeFile      = flag.String("routes", "./routes.demo.json", "file-backed route repository path")
		logFormat      = flag.String("log-format", "json", "log format: json or text")
		queueBackend   = flag.String("queue-backend", "memory", "job queue backend: memory, redis (Redis Streams durable queue), or broker (oarkflow/broker WAL-backed durable queue)")
		queueDataDir   = flag.String("queue-data-dir", "./.smppflow-queue", "data directory for --queue-backend=broker's WAL persistence")
		redisAddr      = flag.String("redis-addr", "127.0.0.1:6379", "Redis address for --queue-backend=redis")
		redisPassword  = flag.String("redis-password", os.Getenv("SMPPFLOW_REDIS_PASSWORD"), "Redis password for --queue-backend=redis (or SMPPFLOW_REDIS_PASSWORD)")
		redisStream    = flag.String("redis-stream", "smpp:jobs", "Redis stream name for --queue-backend=redis")
		redisGroup     = flag.String("redis-group", "smppflow", "Redis consumer group for --queue-backend=redis")
		redisConsumer  = flag.String("redis-consumer", "smppflow-1", "Redis consumer name for --queue-backend=redis")
		tlsCert        = flag.String("tls-cert", "", "TLS certificate file for the HTTP API listener (enables HTTPS)")
		tlsKey         = flag.String("tls-key", "", "TLS private key file for the HTTP API listener")
		authMode       = flag.String("auth", "api-key", "HTTP API auth mode: api-key (default) or open (development only)")
		authRateLimit  = flag.Int("auth-rate-limit", 0, "max HTTP API requests per minute per API key (0 disables)")
		healthcheck    = flag.Bool("healthcheck", false, "probe --http's /healthz and exit 0 when healthy, 1 otherwise (for container healthchecks)")
	)
	flag.Parse()

	if *healthcheck {
		os.Exit(runHealthcheck(*httpAddr, *tlsCert != "" && *tlsKey != ""))
	}
	var smppTLS *tls.Config
	if *smppTLSCert != "" || *smppTLSKey != "" {
		if *smppTLSCert == "" || *smppTLSKey == "" {
			logger := newLogger(*logFormat)
			logger.Error("smpp.tls.incomplete", "note", "both --smpp-tls-cert and --smpp-tls-key are required")
			os.Exit(1)
		}
		cert, err := tls.LoadX509KeyPair(*smppTLSCert, *smppTLSKey)
		if err != nil {
			logger := newLogger(*logFormat)
			logger.Error("smpp.tls.load_failed", "error", err)
			os.Exit(1)
		}
		smppTLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}

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

	metrics := observability.New()
	metrics.SetHelp("smpp_submit_total", "Total SMPP submit-style PDUs handled, by kind and status")
	metrics.SetHelp("smpp_bind_total", "Total SMPP bind attempts, by outcome")
	metrics.SetHelp("smpp_pdu_duration_seconds", "SMPP PDU handling latency in seconds, by kind")
	metrics.SetHelp("http_requests_total", "Total HTTP API requests, by path and status")
	metrics.SetHelp("http_request_duration_seconds", "HTTP API request latency in seconds, by path")

	var submitted atomic.Uint64
	bindPasswordHash, err := security.HashPassword(*password)
	if err != nil {
		logger.Error("smpp.password.hash_failed", "error", err)
		os.Exit(1)
	}
	smppServer := smpp.New(smpp.Config{
		Addr:     *smppAddr,
		TLS:      smppTLS,
		SystemID: "demo-smsc",
		Version:  protocol.Version34,
		Bus:      bus,
		AutoDLR:  true,
		DLRDelay: 500 * time.Millisecond,
		Auth: func(req protocol.BindRequest) protocol.CommandStatus {
			logger.Info("smpp.bind", "system_id", req.SystemID, "mode", req.Mode, "version", req.InterfaceVersion)
			if req.SystemID != *systemID || !security.VerifyPassword(req.Password, bindPasswordHash) {
				logger.Warn("smpp.bind_rejected", "system_id", req.SystemID)
				metrics.Inc("smpp_bind_total", map[string]string{"status": "rejected"}, 1)
				return protocol.ESME_RINVPASWD
			}
			metrics.Inc("smpp_bind_total", map[string]string{"status": "ok"}, 1)
			return protocol.ESME_ROK
		},
		Submit: func(ctx context.Context, req protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			started := time.Now()
			id := fmt.Sprintf("smsc-%06d", submitted.Add(1))
			logger.Info("smpp.submit_sm", "message_id", id, "from", req.Source.Addr, "to", req.Destination.Addr, "coding", req.DataCoding, "registered_delivery", req.RegisteredDelivery, "bytes", len(req.ShortMessage))
			metrics.Inc("smpp_submit_total", map[string]string{"kind": "submit_sm", "status": "ok"}, 1)
			metrics.Observe("smpp_pdu_duration_seconds", map[string]string{"kind": "submit_sm"}, time.Since(started))
			return id, protocol.ESME_ROK
		},
		DataSM: func(ctx context.Context, req protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			started := time.Now()
			id := fmt.Sprintf("data-%06d", submitted.Add(1))
			logger.Info("smpp.data_sm", "message_id", id, "from", req.Source.Addr, "to", req.Destination.Addr)
			metrics.Inc("smpp_submit_total", map[string]string{"kind": "data_sm", "status": "ok"}, 1)
			metrics.Observe("smpp_pdu_duration_seconds", map[string]string{"kind": "data_sm"}, time.Since(started))
			return id, protocol.ESME_ROK
		},
		Query: func(ctx context.Context, req protocol.QuerySMRequest) (protocol.QuerySMRespData, protocol.CommandStatus) {
			logger.Info("smpp.query_sm", "message_id", req.MessageID, "source", req.Source.Addr)
			metrics.Inc("smpp_submit_total", map[string]string{"kind": "query_sm", "status": "ok"}, 1)
			return protocol.QuerySMRespData{MessageID: req.MessageID, MessageState: 2, ErrorCode: 0}, protocol.ESME_ROK
		},
		Cancel: func(ctx context.Context, req any) protocol.CommandStatus {
			logger.Info("smpp.cancel_sm", "request", fmt.Sprintf("%+v", req))
			metrics.Inc("smpp_submit_total", map[string]string{"kind": "cancel_sm", "status": "ok"}, 1)
			return protocol.ESME_ROK
		},
		Replace: func(ctx context.Context, req any) protocol.CommandStatus {
			logger.Info("smpp.replace_sm", "request", fmt.Sprintf("%+v", req))
			metrics.Inc("smpp_submit_total", map[string]string{"kind": "replace_sm", "status": "ok"}, 1)
			return protocol.ESME_ROK
		},
		Broadcast: func(ctx context.Context, req protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			id := fmt.Sprintf("broadcast-%06d", submitted.Add(1))
			logger.Info("smpp.broadcast_sm", "message_id", id, "from", req.Source.Addr)
			metrics.Inc("smpp_submit_total", map[string]string{"kind": "broadcast_sm", "status": "ok"}, 1)
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
	var (
		q        queue.Backend
		mgrQueue *queue.Memory // pkg/manager's queue-introspection endpoints (snapshot/DLQ/replay/cancel) only work with the in-memory queue
	)
	switch strings.ToLower(*queueBackend) {
	case "broker":
		bq, err := durablequeue.NewBrokerQueue(ctx, *queueDataDir, "sms.outbound")
		if err != nil {
			logger.Error("queue.broker.init_failed", "error", err)
			os.Exit(1)
		}
		q = bq
		logger.Info("queue.backend", "backend", "broker", "data_dir", *queueDataDir,
			"note", "oarkflow/broker is pre-v1 per its own README; manager queue-introspection endpoints (snapshot/dlq/replay/cancel) are unavailable in this mode")
	case "redis":
		rq := durablequeue.RedisStreamsQueue{
			Addr:     *redisAddr,
			Password: *redisPassword,
			Stream:   *redisStream,
			Group:    *redisGroup,
			Consumer: *redisConsumer,
		}
		if err := rq.EnsureGroup(ctx); err != nil {
			logger.Error("queue.redis.init_failed", "error", err)
			os.Exit(1)
		}
		q = rq
		logger.Info("queue.backend", "backend", "redis", "addr", *redisAddr, "stream", *redisStream, "group", *redisGroup, "consumer", *redisConsumer,
			"note", "Redis Streams is durable across restarts; manager queue-introspection endpoints (snapshot/dlq/replay/cancel) are unavailable in this mode")
	default:
		mem := queue.NewMemory()
		mgrQueue = mem
		q = mem
	}
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

	keyStore := rbac.NewKeyStore()
	apiKey, apiSecret, err := keyStore.Create(*tenantID, "demo", []string{"*"}, 0)
	if err != nil {
		logger.Error("api_key.create_failed", "error", err)
		os.Exit(1)
	}
	_ = accountStore.SaveAPIKey(ctx, account.APIKey{ID: apiKey.ID, TenantID: apiKey.TenantID, UserID: demoUser.ID, Name: apiKey.Name, Prefix: apiSecret[:10], SecretHash: security.HashSecret(apiSecret), Enabled: true, CreatedAt: apiKey.CreatedAt, UpdatedAt: apiKey.CreatedAt})

	svc := service.New(service.Config{Router: router, Store: msgStore, Queue: q, Bus: bus, Hooks: hookMgr, AccountStore: accountStore, RetryDelay: time.Second})
	svc.StartWorkers(ctx, *workers)

	mgrStore := manager.NewStore()
	ops := manager.NewOperations(mgrStore, msgStore, router, mgrQueue, session.NewManager(nil))
	_ = ops.UpsertTenant(ctx, "bootstrap", manager.Tenant{ID: *tenantID, Name: "Demo Tenant", Status: manager.TenantActive, TPS: 100, AllowedCountries: []string{"NP"}, AllowedSenderIDs: []string{"BRAND", "OTP", "PROMO"}})
	_ = ops.SaveProvider(ctx, "bootstrap", prof)
	_ = ops.SaveBind(ctx, "bootstrap", manager.BindAccount{TenantID: *tenantID, SystemID: *systemID, BindMode: string(protocol.BindModeTransceiver), PasswordHash: bindPasswordHash, MaxBinds: 2, TPS: 100, Enabled: true})
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
	apiServer.Keys = keyStore
	if strings.EqualFold(*authMode, "api-key") {
		apiServer.Auth = &httpapi.AuthConfig{
			Keys:                 httpapi.KeyStoreAuth(keyStore),
			Policy:               httpapi.PolicyAuthorize(rbac.NewPolicy()),
			MaxRequestsPerMinute: *authRateLimit,
		}
		logger.Info("http.auth", "mode", "api-key", "note", "all /v1 routes require Authorization: Bearer <key>; /healthz and /metrics are open")
	} else {
		logger.Warn("http.auth", "mode", "open", "note", "HTTP API is UNAUTHENTICATED; never use --auth=open outside local development")
	}

	topMux := http.NewServeMux()
	topMux.Handle("/", instrumentHTTP(metrics, requestLogger(logger, apiServer.Handler())))
	topMux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(metrics.Prometheus()))
	})
	httpSrv := &http.Server{Addr: *httpAddr, Handler: topMux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	scheme := "http"
	if *tlsCert != "" && *tlsKey != "" {
		scheme = "https"
	} else if *tlsCert != "" || *tlsKey != "" {
		logger.Error("http.tls.incomplete", "note", "both --tls-cert and --tls-key are required")
		os.Exit(1)
	}
	go func() {
		logger.Info("http.server.start", "addr", *httpAddr, "scheme", scheme)
		var err error
		if scheme == "https" {
			err = httpSrv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			err = httpSrv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http.server.error", "error", err)
			stop()
		}
	}()

	printUsage(logger, *httpAddr, *smppAddr, *tenantID, *systemID, *password, *queueBackend, scheme, apiSecret, strings.EqualFold(*authMode, "api-key"))
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	_ = smppServer.Close()
	_ = svc.Close(shutdownCtx)
	logger.Info("shutdown.complete")
}

func instrumentHTTP(metrics *observability.Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		labels := map[string]string{"path": r.URL.Path, "status": fmt.Sprintf("%d", rw.status)}
		metrics.Inc("http_requests_total", labels, 1)
		metrics.Observe("http_request_duration_seconds", map[string]string{"path": r.URL.Path}, time.Since(started))
	})
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

// runHealthcheck probes the HTTP API's /healthz endpoint and returns a process
// exit code, so container runtimes can health-check a distroless image that has
// no shell, curl or wget.
func runHealthcheck(httpAddr string, useTLS bool) int {
	host := httpAddr
	if strings.HasPrefix(host, ":") {
		host = "127.0.0.1" + host
	}
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	url := scheme + "://" + host + "/healthz"
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{
		// The probe runs inside the container against its own listener, which
		// commonly presents a self-signed or hostname-mismatched certificate.
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // local loopback probe only
	}}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck: ", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status ", resp.StatusCode)
		return 1
	}
	return 0
}

func printUsage(logger *slog.Logger, httpAddr, smppAddr, tenantID, systemID, password, queueBackend, scheme, apiSecret string, authEnabled bool) {
	httpURL := scheme + "://127.0.0.1" + httpAddr
	if strings.HasPrefix(httpAddr, "127.0.0.1") || strings.HasPrefix(httpAddr, "localhost") {
		httpURL = scheme + "://" + httpAddr
	}
	smppURL := "127.0.0.1" + smppAddr
	if strings.HasPrefix(smppAddr, "127.0.0.1") || strings.HasPrefix(smppAddr, "localhost") {
		smppURL = smppAddr
	}
	authHeader := ""
	if authEnabled {
		// Printed to stdout rather than the structured logger so the demo
		// credential is not shipped to whatever sink logs are written to.
		fmt.Printf("\nDEMO API KEY (shown once, not logged): %s\n", apiSecret)
		fmt.Printf("Add to requests: -H 'Authorization: Bearer %s'\n\n", apiSecret)
		authHeader = "-H 'Authorization: Bearer <DEMO API KEY>' "
	}
	logger.Info("ready", "smpp", smppAddr, "http", httpAddr, "queue_backend", queueBackend, "scheme", scheme, "auth", map[bool]string{true: "api-key", false: "open"}[authEnabled])
	if !authEnabled {
		logger.Warn("usage.auth", "note", "HTTP API is unauthenticated; start with --auth=api-key (the default) before exposing this listener")
	}
	logger.Info("usage.queue_backend", "note", "pass --queue-backend=redis --redis-addr=127.0.0.1:6379 for a Redis Streams durable queue, or --queue-backend=broker --queue-data-dir=./.smppflow-queue for the WAL-backed one, instead of the default in-memory queue")
	logger.Info("usage.health", "command", "curl -sk "+httpURL+"/healthz")
	logger.Info("usage.metrics", "command", "curl -sk "+httpURL+"/metrics")
	logger.Info("usage.send", "command", "curl -sk -X POST "+httpURL+"/v1/messages/send "+authHeader+"-H 'Content-Type: application/json' -d '{\"TenantID\":\""+tenantID+"\",\"From\":\"BRAND\",\"To\":\"9779841000000\",\"Text\":\"Your OTP is 123456\",\"RegisteredDelivery\":1,\"Meta\":{\"country\":\"NP\",\"message_type\":\"otp\"}}'")
	logger.Info("usage.route_simulate", "command", "curl -sk -X POST "+httpURL+"/v1/manager/route/simulate "+authHeader+"-H 'Content-Type: application/json' -d '{\"TenantID\":\""+tenantID+"\",\"From\":\"BRAND\",\"To\":\"9779841000000\",\"Text\":\"Your OTP is 123456\",\"Meta\":{\"country\":\"NP\",\"message_type\":\"otp\"}}'")
	logger.Info("usage.tls", "note", "pass --tls-cert and --tls-key to serve the HTTP API over HTTPS")
	logger.Info("usage.client", "command", "go run ./examples/client --http "+httpURL+" --smpp "+smppURL+" --system-id "+systemID+" --password "+password)
}
