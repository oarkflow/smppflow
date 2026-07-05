package message

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type ErrorClass string

const (
	ErrRetryable      ErrorClass = "retryable"
	ErrPermanent      ErrorClass = "permanent"
	ErrThrottled      ErrorClass = "throttled"
	ErrAuth           ErrorClass = "auth"
	ErrRouting        ErrorClass = "routing"
	ErrInvalidAddress ErrorClass = "invalid_address"
	ErrBlockedContent ErrorClass = "blocked_content"
	ErrProviderDown   ErrorClass = "provider_down"
	ErrUnknown        ErrorClass = "unknown"
)

type ClassifiedError struct {
	Class     ErrorClass
	Code      string
	Retryable bool
	Reason    string
}

type ErrorTaxonomy struct{ codes map[string]ClassifiedError }

func NewErrorTaxonomy() *ErrorTaxonomy {
	t := &ErrorTaxonomy{codes: map[string]ClassifiedError{}}
	for _, c := range []string{"ESME_RTHROTTLED", "0x00000058", "THROTTLED"} {
		t.Register(c, ErrThrottled, true, "provider throttled")
	}
	for _, c := range []string{"ESME_RINVDSTADR", "ESME_RINVSRCADR", "INVALID_DEST", "INVALID_MSISDN"} {
		t.Register(c, ErrInvalidAddress, false, "invalid address")
	}
	for _, c := range []string{"ESME_RINVPASWD", "ESME_RINVSYSID", "AUTH"} {
		t.Register(c, ErrAuth, false, "authentication failed")
	}
	for _, c := range []string{"ESME_RSUBMITFAIL", "TIMEOUT", "NETWORK"} {
		t.Register(c, ErrProviderDown, true, "provider unavailable")
	}
	return t
}
func (t *ErrorTaxonomy) Register(code string, class ErrorClass, retryable bool, reason string) {
	if t.codes == nil {
		t.codes = map[string]ClassifiedError{}
	}
	t.codes[strings.ToUpper(code)] = ClassifiedError{Class: class, Code: code, Retryable: retryable, Reason: reason}
}
func (t *ErrorTaxonomy) Classify(code string, err error) ClassifiedError {
	if t == nil {
		t = NewErrorTaxonomy()
	}
	if c, ok := t.codes[strings.ToUpper(code)]; ok {
		return c
	}
	if err != nil {
		s := strings.ToUpper(err.Error())
		for k, c := range t.codes {
			if strings.Contains(s, k) {
				return c
			}
		}
	}
	return ClassifiedError{Class: ErrUnknown, Code: code, Retryable: true, Reason: "unknown error"}
}

type RetryPolicy struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       time.Duration
}

func (p RetryPolicy) Delay(attempt int) time.Duration {
	if p.InitialDelay <= 0 {
		p.InitialDelay = time.Second
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = time.Minute
	}
	if p.Multiplier <= 1 {
		p.Multiplier = 2
	}
	d := p.InitialDelay
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * p.Multiplier)
		if d > p.MaxDelay {
			return p.MaxDelay
		}
	}
	if p.Jitter > 0 {
		d += time.Duration(time.Now().UnixNano() % int64(p.Jitter))
	}
	if d > p.MaxDelay {
		return p.MaxDelay
	}
	return d
}

type WebhookEvent struct {
	ID          string
	TenantID    string
	URL         string
	Kind        string
	MessageID   ID
	Payload     any
	Attempt     int
	LastError   string
	CreatedAt   time.Time
	DeliveredAt time.Time
}
type WebhookStore interface {
	SaveWebhookAttempt(context.Context, WebhookEvent) error
	ListWebhookAttempts(context.Context, EventFilter) ([]WebhookEvent, error)
}

func PayloadHash(req SubmitRequest) string {
	h := sha256.New()
	_, _ = h.Write([]byte(req.TenantID))
	_, _ = h.Write([]byte("\x00"))
	_, _ = h.Write([]byte(req.From))
	_, _ = h.Write([]byte("\x00"))
	_, _ = h.Write([]byte(req.To))
	_, _ = h.Write([]byte("\x00"))
	_, _ = h.Write([]byte(req.Text))
	return hex.EncodeToString(h.Sum(nil))
}
func IdempotencyConflict(key, oldHash, newHash string) error {
	return fmt.Errorf("idempotency key %q already used with different payload hash %s != %s", key, oldHash, newHash)
}
