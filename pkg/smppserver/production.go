package smppserver

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/events"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/security"
	"github.com/oarkflow/smppflow/pkg/server"
)

type Account struct {
	TenantID       string
	SystemID       string
	PasswordHash   string
	Enabled        bool
	Modes          []protocol.BindMode
	MaxBinds       int
	TPS            int
	AllowedSources []string
	IP             security.IPPolicy
}
type AccountStore interface {
	GetBySystemID(context.Context, string) (Account, error)
}
type MemoryAccounts struct {
	mu   sync.RWMutex
	byID map[string]Account
}

func NewMemoryAccounts() *MemoryAccounts { return &MemoryAccounts{byID: map[string]Account{}} }
func (m *MemoryAccounts) Save(a Account) {
	m.mu.Lock()
	if a.MaxBinds == 0 {
		a.MaxBinds = 1
	}
	if a.TPS == 0 {
		a.TPS = 100
	}
	m.byID[a.SystemID] = a
	m.mu.Unlock()
}
func (m *MemoryAccounts) GetBySystemID(ctx context.Context, sid string) (Account, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.byID[sid]
	if !ok {
		return Account{}, errors.New("account not found")
	}
	return a, nil
}

type SessionInfo struct {
	ID         string
	SystemID   string
	TenantID   string
	Mode       protocol.BindMode
	Version    protocol.Version
	RemoteAddr string
	BoundAt    time.Time
}
type Registry struct {
	mu       sync.RWMutex
	sessions map[string]SessionInfo
	counts   map[string]int
}

func NewRegistry() *Registry {
	return &Registry{sessions: map[string]SessionInfo{}, counts: map[string]int{}}
}
func (r *Registry) Add(s SessionInfo, max int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if max > 0 && r.counts[s.SystemID] >= max {
		return errors.New("bind limit exceeded")
	}
	r.sessions[s.ID] = s
	r.counts[s.SystemID]++
	return nil
}
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	if s, ok := r.sessions[id]; ok {
		delete(r.sessions, id)
		if r.counts[s.SystemID] > 0 {
			r.counts[s.SystemID]--
		}
	}
	r.mu.Unlock()
}
func (r *Registry) List() []SessionInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]SessionInfo, 0, len(r.sessions))
	for _, s := range r.sessions {
		out = append(out, s)
	}
	return out
}

type Config struct {
	Addr     string
	TLS      *tls.Config
	SystemID string
	Version  protocol.Version
	Accounts AccountStore
	Registry *Registry
	Bus      events.Bus
	Inner    server.Config
}
type Server struct {
	cfg   Config
	inner *server.Server
}

func New(cfg Config) *Server {
	if cfg.Registry == nil {
		cfg.Registry = NewRegistry()
	}
	if cfg.Bus == nil {
		cfg.Bus = events.NewMemoryBus(true)
	}
	sc := cfg.Inner
	sc.Addr = cfg.Addr
	if cfg.TLS != nil {
		sc.TLS = cfg.TLS
	}
	sc.SystemID = cfg.SystemID
	sc.Version = cfg.Version
	sc.Bus = cfg.Bus
	oldAuth := sc.Auth
	sc.Auth = func(req protocol.BindRequest) protocol.CommandStatus {
		if cfg.Accounts == nil {
			if oldAuth != nil {
				return oldAuth(req)
			}
			return protocol.ESME_ROK
		}
		a, err := cfg.Accounts.GetBySystemID(context.Background(), req.SystemID)
		if err != nil || !a.Enabled {
			return protocol.ESME_RINVSYSID
		}
		if !security.VerifyPassword(req.Password, a.PasswordHash) {
			return protocol.ESME_RINVPASWD
		}
		if len(a.Modes) > 0 {
			ok := false
			for _, m := range a.Modes {
				if m == req.Mode {
					ok = true
				}
			}
			if !ok {
				return protocol.ESME_RBINDFAIL
			}
		}
		return protocol.ESME_ROK
	}
	return &Server{cfg: cfg, inner: server.New(sc)}
}
func (s *Server) ListenAndServe(ctx context.Context) error {
	return s.inner.ListenAndServe(ctx)
}
func (s *Server) Addr() string           { return s.inner.Addr() }
func (s *Server) Ready() <-chan struct{} { return s.inner.Ready() }
func (s *Server) Close() error           { return s.inner.Close() }
func RemoteIP(conn net.Conn) string {
	h, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	return h
}
