package hooks

import (
	"context"
	"sort"
	"sync"
	"time"
)

type Point string

const (
	BeforeConnect         Point = "BeforeConnect"
	AfterConnect          Point = "AfterConnect"
	BeforeBind            Point = "BeforeBind"
	AfterBind             Point = "AfterBind"
	BeforePDUSend         Point = "BeforePDUSend"
	AfterPDUSend          Point = "AfterPDUSend"
	BeforePDUDecode       Point = "BeforePDUDecode"
	AfterPDUDecode        Point = "AfterPDUDecode"
	BeforeMessageValidate Point = "BeforeMessageValidate"
	AfterMessageValidate  Point = "AfterMessageValidate"
	BeforeTextNormalize   Point = "BeforeTextNormalize"
	AfterTextNormalize    Point = "AfterTextNormalize"
	BeforeSegment         Point = "BeforeSegment"
	AfterSegment          Point = "AfterSegment"
	BeforeSubmit          Point = "BeforeSubmit"
	AfterSubmit           Point = "AfterSubmit"
	OnSubmitResp          Point = "OnSubmitResp"
	OnDLRReceived         Point = "OnDLRReceived"
	OnInboundReceived     Point = "OnInboundReceived"
	BeforeStore           Point = "BeforeStore"
	AfterStore            Point = "AfterStore"
)

type Hook func(context.Context, any) error
type registered struct {
	priority int
	timeout  time.Duration
	hook     Hook
}
type Manager struct {
	mu         sync.RWMutex
	hooks      map[Point][]registered
	FailClosed bool
}

func New() *Manager { return &Manager{hooks: map[Point][]registered{}} }
func (m *Manager) Register(p Point, priority int, timeout time.Duration, h Hook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hooks[p] = append(m.hooks[p], registered{priority: priority, timeout: timeout, hook: h})
	sort.Slice(m.hooks[p], func(i, j int) bool { return m.hooks[p][i].priority < m.hooks[p][j].priority })
}
func (m *Manager) Run(ctx context.Context, p Point, payload any) error {
	m.mu.RLock()
	hs := append([]registered(nil), m.hooks[p]...)
	m.mu.RUnlock()
	for _, x := range hs {
		c := ctx
		cancel := func() {}
		if x.timeout > 0 {
			c, cancel = context.WithTimeout(ctx, x.timeout)
		}
		err := safeRun(c, x.hook, payload)
		cancel()
		if err != nil && m.FailClosed {
			return err
		}
	}
	return nil
}
func safeRun(ctx context.Context, h Hook, payload any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = context.Canceled
		}
	}()
	return h(ctx, payload)
}
