package message

import (
	"context"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
)

type IdempotencyRecord struct {
	TenantID    string
	Key         string
	PayloadHash string
	MessageID   ID
	Result      SubmitResult
	CreatedAt   time.Time
	ExpiresAt   time.Time
}

type EventFilter struct {
	TenantID   string
	ProviderID string
	MessageID  ID
	State      lifecycle.MessageState
	From       time.Time
	To         time.Time
	Limit      int
}

type ExtendedStore interface {
	Store
	SaveIdempotency(context.Context, IdempotencyRecord) error
	GetIdempotency(context.Context, string, string) (IdempotencyRecord, error)
	SaveTransition(context.Context, Transition) error
	ListTransitions(context.Context, EventFilter) ([]Transition, error)
	SaveAttempt(context.Context, SubmitAttempt) error
	ListAttempts(context.Context, EventFilter) ([]SubmitAttempt, error)
	SaveInbound(context.Context, *InboundMessage) error
	ListInbound(context.Context, EventFilter) ([]*InboundMessage, error)
}
