package webhook

import (
	"context"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
)

type RetryConfig struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}
type RetryResult struct {
	Attempts  int
	Delivered bool
	LastError string
	NextDelay time.Duration
}

func (d Dispatcher) SendWithRetry(ctx context.Context, cfg RetryConfig, url, tenant, kind string, msgID message.ID, payload any) RetryResult {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.InitialDelay <= 0 {
		cfg.InitialDelay = 200 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 5 * time.Second
	}
	if cfg.Multiplier <= 1 {
		cfg.Multiplier = 2
	}
	delay := cfg.InitialDelay
	res := RetryResult{}
	for i := 1; i <= cfg.MaxAttempts; i++ {
		res.Attempts = i
		err := d.Send(ctx, url, tenant, kind, msgID, payload)
		if err == nil {
			res.Delivered = true
			return res
		}
		res.LastError = err.Error()
		res.NextDelay = delay
		select {
		case <-ctx.Done():
			res.LastError = ctx.Err().Error()
			return res
		case <-time.After(delay):
		}
		next := time.Duration(float64(delay) * cfg.Multiplier)
		if next > cfg.MaxDelay {
			next = cfg.MaxDelay
		}
		delay = next
	}
	return res
}
