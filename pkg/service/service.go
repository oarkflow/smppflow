package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/account"
	"github.com/oarkflow/smppflow/pkg/events"
	"github.com/oarkflow/smppflow/pkg/hooks"
	"github.com/oarkflow/smppflow/pkg/message"
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
	Queue         *queue.Memory
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
	dec, err := s.cfg.Router.Route(ctx, req)
	if err != nil {
		return message.SubmitResult{}, err
	}
	prof := s.applyProviderCredential(ctx, req.TenantID, dec.Provider)
	cli, err := s.client(ctx, prof)
	if err != nil {
		return message.SubmitResult{}, err
	}
	useSAR := req.UseSAR || prof.Multipart == provider.MultipartSAR
	mgr := message.NewManager(message.ManagerConfig{Client: cli, Store: s.cfg.Store, Bus: s.cfg.Bus, Hooks: s.cfg.Hooks, DefaultSourceTON: prof.SourceTON, DefaultSourceNPI: prof.SourceNPI, DefaultDestTON: prof.DestTON, DefaultDestNPI: prof.DestNPI, UseSAR: useSAR})
	mgr.StartInbound(ctx)
	return mgr.Submit(ctx, req)
}

func (s *Service) Enqueue(ctx context.Context, req message.SubmitRequest, priority int, runAt time.Time) error {
	return s.cfg.Queue.Enqueue(ctx, queue.Job{Type: "send_sms", Payload: req, Priority: priority, RunAt: runAt, MaxAttempts: 3})
}

func (s *Service) StartWorkers(ctx context.Context, n int) {
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		go s.worker(ctx, i)
	}
}

func (s *Service) worker(ctx context.Context, id int) {
	for {
		j, err := s.cfg.Queue.Reserve(ctx)
		if err != nil {
			return
		}
		req, ok := j.Payload.(message.SubmitRequest)
		if !ok {
			_ = s.cfg.Queue.Nack(ctx, j, fmt.Errorf("unsupported job payload %T", j.Payload), 0)
			continue
		}
		_, err = s.Send(ctx, req)
		if err != nil {
			_ = s.cfg.Queue.Nack(ctx, j, err, s.cfg.RetryDelay)
			continue
		}
		_ = s.cfg.Queue.Ack(ctx, j.ID)
	}
}

func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clients {
		_ = c.Close(ctx)
	}
	s.cfg.Queue.Close()
	return nil
}
