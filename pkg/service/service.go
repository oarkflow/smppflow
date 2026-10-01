package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/account"
	"github.com/oarkflow/smppflow/pkg/events"
	"github.com/oarkflow/smppflow/pkg/hooks"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/observability"
	"github.com/oarkflow/smppflow/pkg/provider"
	"github.com/oarkflow/smppflow/pkg/queue"
	"github.com/oarkflow/smppflow/pkg/routing"
	"github.com/oarkflow/smppflow/pkg/session"
	"github.com/oarkflow/smppflow/pkg/storage"
)

type ClientFactory func(context.Context, provider.Profile) (*session.Client, error)

type Config struct {
	Router        *routing.Router
	Store         message.Store
	Queue         queue.Backend
	Bus           events.Bus
	Hooks         *hooks.Manager
	ClientFactory ClientFactory
	AccountStore  account.Store
	RetryDelay    time.Duration
}

type Service struct {
	cfg     Config
	mu      sync.Mutex
	clients map[string]*session.Client

	workersMu sync.Mutex
	workersWG sync.WaitGroup
	workersCf context.CancelFunc
}

func New(cfg Config) *Service {
	if cfg.Router == nil {
		cfg.Router = routing.New()
	}
	if cfg.Store == nil {
		cfg.Store = storage.NewMemory()
	}
	if cfg.Queue == nil {
		cfg.Queue = queue.NewMemory()
	}
	if cfg.Bus == nil {
		cfg.Bus = events.NewMemoryBus(true)
	}
	if cfg.Hooks == nil {
		cfg.Hooks = hooks.New()
	}
	if cfg.AccountStore == nil {
		cfg.AccountStore = account.NewMemory()
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = time.Second
	}
	s := &Service{cfg: cfg, clients: map[string]*session.Client{}}
	if s.cfg.ClientFactory == nil {
		s.cfg.ClientFactory = s.defaultClientFactory
	}
	return s
}

func (s *Service) RegisterProvider(ctx context.Context, p provider.Profile, cred account.ProviderCredential) error {
	if s.cfg.Router == nil {
		s.cfg.Router = routing.New()
	}
	if cred.ProviderID == "" {
		cred.ProviderID = p.ID
	}
	if cred.TenantID != "" {
		if err := s.cfg.AccountStore.SaveProviderCredential(ctx, cred); err != nil {
			return err
		}
	}
	return s.cfg.Router.RegisterProvider(p)
}

func (s *Service) RegisterUser(ctx context.Context, u account.User) error {
	return s.cfg.AccountStore.SaveUser(ctx, u)
}

func (s *Service) AuthenticateAPIKey(ctx context.Context, secret string) (account.User, account.APIKey, error) {
	return s.cfg.AccountStore.AuthenticateAPIKey(ctx, secret)
}

func (s *Service) applyProviderCredential(ctx context.Context, tenantID string, p provider.Profile) provider.Profile {
	if s.cfg.AccountStore == nil || tenantID == "" {
		return p
	}
	c, err := s.cfg.AccountStore.GetProviderCredential(ctx, tenantID, p.ID)
	if err != nil {
		return p
	}
	p.SystemID = c.SystemID
	p.Password = c.Password
	p.SystemType = c.SystemType
	return p
}

func (s *Service) defaultClientFactory(ctx context.Context, p provider.Profile) (*session.Client, error) {
	cli := session.NewClient(session.Config{Addr: p.Addr, Bind: p.BindRequest(), WindowSize: p.WindowSize, EnquireInterval: p.EnquireInterval, ResponseTimeout: p.ResponseTimeout, Bus: s.cfg.Bus, Hooks: s.cfg.Hooks})
	if err := cli.Connect(ctx); err != nil {
		return nil, err
	}
	if err := cli.Bind(ctx); err != nil {
		return nil, err
	}
	return cli, nil
}

func (s *Service) client(ctx context.Context, p provider.Profile) (*session.Client, error) {
	s.mu.Lock()
	key := p.ID + "\x00" + p.SystemID
	c := s.clients[key]
	s.mu.Unlock()
	if c != nil {
		return c, nil
	}
	c, err := s.cfg.ClientFactory(ctx, p)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.clients[key] = c
	s.mu.Unlock()
	return c, nil
}

func (s *Service) Send(ctx context.Context, req message.SubmitRequest) (message.SubmitResult, error) {
	return s.sendWithMode(ctx, req, routing.PlanOptions{Mode: routing.RouteInitial})
}

func (s *Service) sendWithMode(ctx context.Context, req message.SubmitRequest, po routing.PlanOptions) (message.SubmitResult, error) {
	ctx, span := observability.StartSpan(ctx, "submit")
	span.SetAttr("tenant_id", req.TenantID)
	defer span.End()

	started := time.Now()
	dec, err := s.routeSpan(ctx, req, po)
	if err != nil {
		return message.SubmitResult{}, err
	}
	reservationID := s.cfg.Router.ReservationID(dec)
	prof := s.applyProviderCredential(ctx, req.TenantID, dec.Provider)
	if req.Meta == nil {
		req.Meta = map[string]string{}
	}
	req.ProviderID = prof.ID
	req.Meta["system_id"] = prof.SystemID
	req.Meta["bind_key"] = prof.ID + "|" + req.TenantID + "|" + prof.SystemID + "|" + string(prof.BindMode)
	span.SetAttr("provider_id", prof.ID)
	cli, err := s.client(ctx, prof)
	if err != nil {
		_ = s.cfg.Router.ReleaseReservation(ctx, reservationID)
		s.cfg.Router.MarkRouteFailure(req, prof.ID, time.Since(started))
		return message.SubmitResult{}, err
	}
	useSAR := req.UseSAR || prof.Multipart == provider.MultipartSAR
	if s.cfg.Router != nil && cli != nil {
		s.cfg.Router.UpdateSessionSnapshot(routing.SessionSnapshot{ProviderID: prof.ID, TenantID: req.TenantID, SystemID: prof.SystemID, BindMode: string(prof.BindMode), Healthy: true, WindowUsed: cli.Pending(), WindowSize: cli.WindowSize(), State: string(cli.State())})
	}
	mgr := message.NewManager(message.ManagerConfig{Client: cli, Store: s.cfg.Store, Bus: s.cfg.Bus, Hooks: s.cfg.Hooks, DefaultSourceTON: prof.SourceTON, DefaultSourceNPI: prof.SourceNPI, DefaultDestTON: prof.DestTON, DefaultDestNPI: prof.DestNPI, ProviderID: prof.ID, Taxonomy: message.NewErrorTaxonomy(), UseSAR: useSAR})
	mgr.StartInbound(ctx)

	deliverCtx, deliverSpan := observability.StartSpan(ctx, "deliver")
	res, err := mgr.Submit(deliverCtx, req)
	if err != nil {
		deliverSpan.EndWithError(err)
		_ = s.cfg.Router.ReleaseReservation(ctx, reservationID)
		if s.cfg.Router != nil {
			s.cfg.Router.CooldownProvider(prof.ID, s.cfg.RetryDelay)
			s.cfg.Router.MarkRouteFailure(req, prof.ID, time.Since(started))
		}
		return res, err
	}
	deliverSpan.SetAttr("message_id", string(res.MessageID))
	deliverSpan.End()
	if s.cfg.Router != nil {
		_ = s.cfg.Router.CommitReservation(ctx, reservationID)
		s.cfg.Router.MarkRouteSuccess(req, prof.ID, time.Since(started))
		s.cfg.Router.AttachSnapshotMessageIDPersistent(ctx, dec.Snapshot.ID, res.MessageID)
	}
	return res, nil
}

// routeSpan wraps the router call in its own child span so the routing
// step is separately visible in logs from the overall submit span.
func (s *Service) routeSpan(ctx context.Context, req message.SubmitRequest, po routing.PlanOptions) (routing.Decision, error) {
	ctx, span := observability.StartSpan(ctx, "route")
	defer span.End()
	dec, err := s.cfg.Router.RouteWithMode(ctx, req, po)
	if err != nil {
		span.SetAttr("error", err.Error())
	}
	return dec, err
}

func (s *Service) Enqueue(ctx context.Context, req message.SubmitRequest, priority int, runAt time.Time) error {
	return s.cfg.Queue.Enqueue(ctx, queue.Job{Type: "send_sms", Payload: req, Priority: priority, RunAt: runAt, MaxAttempts: 3})
}

// StartWorkers launches n queue workers. They run until Close is called or ctx
// is cancelled, whichever happens first. Calling StartWorkers again after Close
// starts a fresh worker set.
func (s *Service) StartWorkers(ctx context.Context, n int) {
	if n <= 0 {
		n = 1
	}
	s.workersMu.Lock()
	if s.workersCf != nil {
		s.workersCf()
		s.workersWG.Wait()
	}
	wctx, cancel := context.WithCancel(ctx)
	s.workersCf = cancel
	s.workersMu.Unlock()
	for i := 0; i < n; i++ {
		s.workersWG.Add(1)
		go func(id int) {
			defer s.workersWG.Done()
			s.worker(wctx, id)
		}(i)
	}
}

// coerceSubmitRequest returns j.Payload as a message.SubmitRequest. Backends
// that keep the live Go value across Enqueue/Reserve (e.g. *queue.Memory)
// hit the direct type assertion. Backends that round-trip jobs through JSON
// (BrokerQueue, RedisStreamsQueue, SQLQueue) decode an `any`-typed field back
// as map[string]interface{}, losing the concrete type, so those fall back to
// a JSON re-encode/decode into the expected struct.
func coerceSubmitRequest(payload any) (message.SubmitRequest, error) {
	if req, ok := payload.(message.SubmitRequest); ok {
		return req, nil
	}
	var req message.SubmitRequest
	b, err := json.Marshal(payload)
	if err != nil {
		return req, err
	}
	if err := json.Unmarshal(b, &req); err != nil {
		return req, err
	}
	return req, nil
}

func (s *Service) worker(ctx context.Context, id int) {
	const emptyPollInterval = 100 * time.Millisecond
	const errorBackoff = time.Second
	for {
		j, err := s.cfg.Queue.Reserve(ctx)
		if err != nil {
			// A non-blocking Backend (e.g. BrokerQueue, RedisStreamsQueue) returns
			// ErrNoJob when the queue is momentarily empty; that is not a shutdown
			// signal, so poll again shortly instead of exiting the worker. *Memory's
			// Reserve blocks internally and only returns an error on ctx
			// cancellation, so this branch is a no-op for it in practice.
			backoff := errorBackoff
			if errors.Is(err, queue.ErrNoJob) {
				backoff = emptyPollInterval
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}
		req, err := coerceSubmitRequest(j.Payload)
		if err != nil {
			_ = s.cfg.Queue.Nack(ctx, j, fmt.Errorf("unsupported job payload %T: %w", j.Payload, err), 0)
			continue
		}
		mode := routing.RouteInitial
		if j.Attempts > 1 {
			mode = routing.RouteRetry
		}
		po := routing.PlanOptions{Mode: mode}
		if j.Meta != nil {
			po.OriginalProviderID = j.Meta["original_provider_id"]
			po.ExcludeProviderID = j.Meta["exclude_provider_id"]
			po.AcceptedByProvider = j.Meta["accepted_by_provider"] == "true"
		}
		_, err = s.sendWithMode(ctx, req, po)
		if err != nil {
			_ = s.cfg.Queue.Nack(ctx, j, err, s.cfg.RetryDelay)
			continue
		}
		_ = s.cfg.Queue.Ack(ctx, j.ID)
	}
}

// Close stops the workers and shuts the queue down. It is safe to call more than
// once and blocks until workers have exited.
func (s *Service) Close(ctx context.Context) error {
	s.workersMu.Lock()
	if s.workersCf != nil {
		s.workersCf()
		s.workersCf = nil
	}
	s.workersMu.Unlock()
	s.workersWG.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clients {
		_ = c.Close(ctx)
	}
	s.clients = map[string]*session.Client{}
	return s.cfg.Queue.Shutdown(ctx)
}

func (s *Service) Queue() queue.Backend    { return s.cfg.Queue }
func (s *Service) Router() *routing.Router { return s.cfg.Router }
func (s *Service) Store() message.Store    { return s.cfg.Store }
