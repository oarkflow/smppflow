package lifecycle_test

import (
	"testing"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
)

func TestMessageStateIsTerminal(t *testing.T) {
	terminal := []lifecycle.MessageState{
		lifecycle.MsgDelivered,
		lifecycle.MsgUndeliverable,
		lifecycle.MsgFailed,
		lifecycle.MsgExpired,
		lifecycle.MsgRejected,
		lifecycle.MsgDeadLettered,
		lifecycle.MsgArchived,
	}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Fatalf("%q must be terminal", s)
		}
	}
	active := []lifecycle.MessageState{
		lifecycle.MsgCreated,
		lifecycle.MsgValidated,
		lifecycle.MsgEncoded,
		lifecycle.MsgSegmented,
		lifecycle.MsgQueued,
		lifecycle.MsgSending,
		lifecycle.MsgSubmitted,
		lifecycle.MsgAccepted,
		lifecycle.MsgThrottled,
		lifecycle.MsgUnknown,
		lifecycle.MsgPartiallyDelivered,
	}
	for _, s := range active {
		if s.IsTerminal() {
			t.Fatalf("%q must not be terminal", s)
		}
	}
}

func TestConnectionStateIsTerminal(t *testing.T) {
	for _, s := range []lifecycle.ConnectionState{lifecycle.ConnClosed, lifecycle.ConnFailed} {
		if !s.IsTerminal() {
			t.Fatalf("%q must be terminal", s)
		}
	}
	for _, s := range []lifecycle.ConnectionState{
		lifecycle.ConnNew,
		lifecycle.ConnConnecting,
		lifecycle.ConnConnected,
		lifecycle.ConnBinding,
		lifecycle.ConnBound,
		lifecycle.ConnActive,
		lifecycle.ConnDraining,
		lifecycle.ConnUnbinding,
		lifecycle.ConnReconnecting,
	} {
		if s.IsTerminal() {
			t.Fatalf("%q must not be terminal", s)
		}
	}
}
