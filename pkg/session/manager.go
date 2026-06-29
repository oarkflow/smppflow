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
	m.mu.RUnlock()
	if c != nil {
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
	for _, s := range m.status {
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
