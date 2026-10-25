package hooks_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/hooks"
)

func TestRunOrderIsByPriority(t *testing.T) {
	m := hooks.New()
	var order []int
	for _, p := range []struct {
		prio int
		n    int
	}{{2, 2}, {1, 1}, {3, 3}} {
		p := p
		m.Register(hooks.BeforeSubmit, p.prio, 0, func(ctx context.Context, payload any) error {
			order = append(order, p.n)
			return nil
		})
	}
	if err := m.Run(context.Background(), hooks.BeforeSubmit, "x"); err != nil {
		t.Fatal(err)
	}
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("order=%v want [1 2 3]", order)
	}
}

func TestRunWithoutHooksIsNoop(t *testing.T) {
	m := hooks.New()
	if err := m.Run(context.Background(), hooks.AfterBind, nil); err != nil {
		t.Fatalf("empty manager must not fail: %v", err)
	}
}

func TestFailClosedReturnsError(t *testing.T) {
	m := hooks.New()
	m.FailClosed = true
	boom := errors.New("boom")
	ran := false
	m.Register(hooks.BeforeSubmit, 1, 0, func(ctx context.Context, payload any) error {
		ran = true
		return boom
	})
	err := m.Run(context.Background(), hooks.BeforeSubmit, nil)
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v want boom", err)
	}
	if !ran {
		t.Fatal("hook must run")
	}
}

func TestFailOpenSwallowsErrorAndContinues(t *testing.T) {
	m := hooks.New()
	second := false
	m.Register(hooks.BeforeSubmit, 1, 0, func(ctx context.Context, payload any) error {
		return errors.New("ignored")
	})
	m.Register(hooks.BeforeSubmit, 2, 0, func(ctx context.Context, payload any) error {
		second = true
		return nil
	})
	if err := m.Run(context.Background(), hooks.BeforeSubmit, nil); err != nil {
		t.Fatalf("fail-open manager must not return error, got %v", err)
	}
	if !second {
		t.Fatal("later hooks must still run after an error in fail-open mode")
	}
}

func TestPanicIsRecovered(t *testing.T) {
	m := hooks.New()
	m.FailClosed = true
	m.Register(hooks.BeforeSubmit, 1, 0, func(ctx context.Context, payload any) error {
		panic("hook panic")
	})
	err := m.Run(context.Background(), hooks.BeforeSubmit, nil)
	if err == nil {
		t.Fatal("panic must surface as error when FailClosed")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}

	m2 := hooks.New()
	after := false
	m2.Register(hooks.BeforeSubmit, 1, 0, func(ctx context.Context, payload any) error {
		panic("boom")
	})
	m2.Register(hooks.BeforeSubmit, 2, 0, func(ctx context.Context, payload any) error {
		after = true
		return nil
	})
	if err := m2.Run(context.Background(), hooks.BeforeSubmit, nil); err != nil {
		t.Fatalf("fail-open must swallow panics, got %v", err)
	}
	if !after {
		t.Fatal("later hooks must run after a panic in fail-open mode")
	}
}

func TestHookTimeout(t *testing.T) {
	m := hooks.New()
	m.FailClosed = true
	m.Register(hooks.BeforeSubmit, 1, 10*time.Millisecond, func(ctx context.Context, payload any) error {
		<-ctx.Done()
		return ctx.Err()
	})
	err := m.Run(context.Background(), hooks.BeforeSubmit, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v want context.DeadlineExceeded", err)
	}
}

func TestHookPayloadIsPassedThrough(t *testing.T) {
	m := hooks.New()
	var seen any
	m.Register(hooks.OnDLRReceived, 1, 0, func(ctx context.Context, payload any) error {
		seen = payload
		return nil
	})
	if err := m.Run(context.Background(), hooks.OnDLRReceived, "receipt"); err != nil {
		t.Fatal(err)
	}
	if seen != "receipt" {
		t.Fatalf("payload=%v want receipt", seen)
	}
}
