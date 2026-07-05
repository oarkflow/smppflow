package session

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/provider"
)

type ManagedProvider struct {
	Profile             provider.Profile
	Healthy             bool
	LastError           string
	LastBind            time.Time
	LastLatency         time.Duration
	WindowUsed          int
	WindowSize          int
	State               string
	Draining            bool
	ConsecutiveFailures int
}
type Manager struct {
	mu       sync.RWMutex
	factory  func(context.Context, provider.Profile) (*Client, error)
	sessions map[string]*Client
	status   map[string]ManagedProvider
}

func NewManager(factory func(context.Context, provider.Profile) (*Client, error)) *Manager {
	return &Manager{factory: factory, sessions: map[string]*Client{}, status: map[string]ManagedProvider{}}
}
func (m *Manager) Register(p provider.Profile) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status[p.ID] = ManagedProvider{Profile: p, Healthy: false}
}
func (m *Manager) Get(ctx context.Context, p provider.Profile) (*Client, error) {
	m.mu.RLock()
	c := m.sessions[p.ID]
	st0 := m.status[p.ID]
	m.mu.RUnlock()
	if c != nil {
		if st0.Draining {
			return nil, errors.New("provider draining")
		}
		return c, nil
	}
	if m.factory == nil {
		return nil, errors.New("nil session factory")
	}
	start := time.Now()
	c, err := m.factory(ctx, p)
	m.mu.Lock()
	st := m.status[p.ID]
	st.Profile = p
	st.LastBind = time.Now()
	st.LastLatency = time.Since(start)
	if err != nil {
		st.Healthy = false
		st.LastError = err.Error()
		st.ConsecutiveFailures++
		m.status[p.ID] = st
		m.mu.Unlock()
		return nil, err
	}
	st.Healthy = true
	st.WindowSize = p.WindowSize
	if c != nil {
		st.WindowUsed = c.Pending()
		st.State = string(c.State())
	}
	st.LastError = ""
	st.ConsecutiveFailures = 0
	m.status[p.ID] = st
	m.sessions[p.ID] = c
	m.mu.Unlock()
	return c, nil
}
func (m *Manager) MarkFailure(id string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.status[id]
	st.Healthy = false
	if err != nil {
		st.LastError = err.Error()
	}
	st.ConsecutiveFailures++
	m.status[id] = st
	delete(m.sessions, id)
}
func (m *Manager) Status() []ManagedProvider {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ManagedProvider, 0, len(m.status))
	for id, s := range m.status {
		if c := m.sessions[id]; c != nil {
			s.WindowUsed = c.Pending()
			s.WindowSize = c.WindowSize()
			s.State = string(c.State())
		}
		out = append(out, s)
	}
	return out
}
func (m *Manager) Close(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, c := range m.sessions {
		_ = c.Close(ctx)
		delete(m.sessions, id)
	}
}

func (m *Manager) Drain(id string) {
	m.mu.Lock()
	st := m.status[id]
	st.Draining = true
	st.State = "draining"
	m.status[id] = st
	m.mu.Unlock()
}
func (m *Manager) Undrain(id string) {
	m.mu.Lock()
	st := m.status[id]
	st.Draining = false
	m.status[id] = st
	m.mu.Unlock()
}
func (m *Manager) Disconnect(ctx context.Context, id string) error {
	m.mu.Lock()
	c := m.sessions[id]
	delete(m.sessions, id)
	st := m.status[id]
	st.Healthy = false
	st.State = "closed"
	m.status[id] = st
	m.mu.Unlock()
	if c != nil {
		return c.Close(ctx)
	}
	return nil
}
func (m *Manager) Rebind(ctx context.Context, p provider.Profile) (*Client, error) {
	_ = m.Disconnect(ctx, p.ID)
	return m.Get(ctx, p)
}
