package message

import (
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
)

type Transition struct {
	MessageID  ID                     `json:"message_id"`
	SegmentID  string                 `json:"segment_id,omitempty"`
	From       lifecycle.MessageState `json:"from"`
	To         lifecycle.MessageState `json:"to"`
	Reason     string                 `json:"reason,omitempty"`
	Actor      string                 `json:"actor,omitempty"`
	ProviderID string                 `json:"provider_id,omitempty"`
	ErrorCode  string                 `json:"error_code,omitempty"`
	At         time.Time              `json:"at"`
	Meta       map[string]string      `json:"meta,omitempty"`
}

var allowedTransitions = map[lifecycle.MessageState]map[lifecycle.MessageState]bool{
	"":                              {lifecycle.MsgCreated: true},
	lifecycle.MsgCreated:            {lifecycle.MsgValidated: true, lifecycle.MsgRejected: true, lifecycle.MsgSegmented: true, lifecycle.MsgQueued: true, lifecycle.MsgFailed: true},
	lifecycle.MsgValidated:          {lifecycle.MsgEncoded: true, lifecycle.MsgSegmented: true, lifecycle.MsgQueued: true, lifecycle.MsgRejected: true, lifecycle.MsgFailed: true},
	lifecycle.MsgEncoded:            {lifecycle.MsgSegmented: true, lifecycle.MsgQueued: true, lifecycle.MsgFailed: true},
	lifecycle.MsgSegmented:          {lifecycle.MsgQueued: true, lifecycle.MsgSending: true, lifecycle.MsgSubmitted: true, lifecycle.MsgFailed: true},
	lifecycle.MsgQueued:             {lifecycle.MsgSending: true, lifecycle.MsgExpired: true, lifecycle.MsgDeadLettered: true, lifecycle.MsgFailed: true},
	lifecycle.MsgSending:            {lifecycle.MsgSubmitted: true, lifecycle.MsgAccepted: true, lifecycle.MsgThrottled: true, lifecycle.MsgFailed: true, lifecycle.MsgUnknown: true},
	lifecycle.MsgSubmitted:          {lifecycle.MsgAccepted: true, lifecycle.MsgDelivered: true, lifecycle.MsgUndeliverable: true, lifecycle.MsgFailed: true, lifecycle.MsgExpired: true, lifecycle.MsgUnknown: true, lifecycle.MsgPartiallyDelivered: true},
	lifecycle.MsgAccepted:           {lifecycle.MsgDelivered: true, lifecycle.MsgUndeliverable: true, lifecycle.MsgFailed: true, lifecycle.MsgExpired: true, lifecycle.MsgUnknown: true, lifecycle.MsgPartiallyDelivered: true},
	lifecycle.MsgThrottled:          {lifecycle.MsgQueued: true, lifecycle.MsgSending: true, lifecycle.MsgFailed: true, lifecycle.MsgDeadLettered: true},
	lifecycle.MsgUnknown:            {lifecycle.MsgDelivered: true, lifecycle.MsgUndeliverable: true, lifecycle.MsgFailed: true, lifecycle.MsgExpired: true, lifecycle.MsgQueued: true},
	lifecycle.MsgPartiallyDelivered: {lifecycle.MsgDelivered: true, lifecycle.MsgUndeliverable: true, lifecycle.MsgFailed: true, lifecycle.MsgExpired: true},
	lifecycle.MsgDelivered:          {lifecycle.MsgArchived: true},
	lifecycle.MsgUndeliverable:      {lifecycle.MsgArchived: true},
	lifecycle.MsgFailed:             {lifecycle.MsgArchived: true, lifecycle.MsgDeadLettered: true},
	lifecycle.MsgRejected:           {lifecycle.MsgArchived: true},
	lifecycle.MsgExpired:            {lifecycle.MsgArchived: true},
	lifecycle.MsgDeadLettered:       {lifecycle.MsgArchived: true},
}

func CanTransition(from, to lifecycle.MessageState) bool {
	if from == to {
		return true
	}
	return allowedTransitions[from][to]
}

func ApplyTransition(msg *Message, to lifecycle.MessageState, reason, actor, providerID, errorCode string) error {
	if msg == nil {
		return fmt.Errorf("nil message")
	}
	from := msg.State
	if !CanTransition(from, to) {
		return fmt.Errorf("invalid message transition %s -> %s", from, to)
	}
	now := time.Now()
	msg.State = to
	msg.UpdatedAt = now
	msg.Transitions = append(msg.Transitions, Transition{MessageID: msg.ID, From: from, To: to, Reason: reason, Actor: actor, ProviderID: providerID, ErrorCode: errorCode, At: now})
	return nil
}

func AggregateSegmentState(parts []SegmentRecord) lifecycle.MessageState {
	if len(parts) == 0 {
		return lifecycle.MsgUnknown
	}
	delivered, terminal, failed := 0, 0, 0
	for _, p := range parts {
		switch p.State {
		case lifecycle.MsgDelivered:
			delivered++
			terminal++
		case lifecycle.MsgFailed, lifecycle.MsgUndeliverable, lifecycle.MsgExpired, lifecycle.MsgRejected, lifecycle.MsgDeadLettered:
			failed++
			terminal++
		}
	}
	if delivered == len(parts) {
		return lifecycle.MsgDelivered
	}
	if terminal == len(parts) && delivered > 0 {
		return lifecycle.MsgPartiallyDelivered
	}
	if terminal == len(parts) && failed == len(parts) {
		return lifecycle.MsgFailed
	}
	if delivered > 0 {
		return lifecycle.MsgPartiallyDelivered
	}
	return lifecycle.MsgSubmitted
}
