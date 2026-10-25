package message_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
)

func TestErrorTaxonomyClassifyKnownCodes(t *testing.T) {
	tax := message.NewErrorTaxonomy()
	c := tax.Classify("ESME_RTHROTTLED", nil)
	if c.Class != message.ErrThrottled || !c.Retryable {
		t.Fatalf("throttled=%+v", c)
	}
	c = tax.Classify("esme_rinvdstadr", nil)
	if c.Class != message.ErrInvalidAddress || c.Retryable {
		t.Fatalf("invalid address=%+v", c)
	}
	c = tax.Classify("ESME_RINVPASWD", nil)
	if c.Class != message.ErrAuth || c.Retryable {
		t.Fatalf("auth=%+v", c)
	}
	c = tax.Classify("TIMEOUT", nil)
	if c.Class != message.ErrProviderDown || !c.Retryable {
		t.Fatalf("provider down=%+v", c)
	}
}

func TestErrorTaxonomyClassifyByErrorString(t *testing.T) {
	tax := message.NewErrorTaxonomy()
	c := tax.Classify("", errors.New("dial tcp: connection refused NETWORK"))
	if c.Class != message.ErrProviderDown {
		t.Fatalf("class=%q want provider_down", c.Class)
	}
	c = tax.Classify("", errors.New("INVALID_MSISDN"))
	if c.Class != message.ErrInvalidAddress {
		t.Fatalf("class=%q want invalid_address", c.Class)
	}
}

func TestErrorTaxonomyUnknownAndCustom(t *testing.T) {
	tax := message.NewErrorTaxonomy()
	c := tax.Classify("WHO_KNOWS", errors.New("mystery"))
	if c.Class != message.ErrUnknown || !c.Retryable {
		t.Fatalf("unknown=%+v", c)
	}
	tax.Register("CUSTOM_1", message.ErrBlockedContent, false, "blocked")
	c = tax.Classify("custom_1", nil)
	if c.Class != message.ErrBlockedContent || c.Retryable || c.Reason != "blocked" {
		t.Fatalf("custom=%+v", c)
	}

	var nilTax *message.ErrorTaxonomy
	if got := nilTax.Classify("X", nil); got.Class != message.ErrUnknown {
		t.Fatalf("nil taxonomy must classify as unknown, got %+v", got)
	}
}

func TestRetryPolicyDelay(t *testing.T) {
	p := message.RetryPolicy{InitialDelay: 100 * time.Millisecond, MaxDelay: time.Second, Multiplier: 2}
	if got := p.Delay(1); got != 100*time.Millisecond {
		t.Fatalf("delay(1)=%v", got)
	}
	if got := p.Delay(2); got != 200*time.Millisecond {
		t.Fatalf("delay(2)=%v", got)
	}
	if got := p.Delay(3); got != 400*time.Millisecond {
		t.Fatalf("delay(3)=%v", got)
	}
	if got := p.Delay(10); got != time.Second {
		t.Fatalf("delay must cap at MaxDelay, got %v", got)
	}

	zero := message.RetryPolicy{}
	if got := zero.Delay(1); got != time.Second {
		t.Fatalf("default initial delay=%v want 1s", got)
	}
	if got := zero.Delay(20); got != time.Minute {
		t.Fatalf("default max delay=%v want 1m", got)
	}

	jittered := message.RetryPolicy{InitialDelay: 100 * time.Millisecond, MaxDelay: 200 * time.Millisecond, Multiplier: 2, Jitter: 50 * time.Millisecond}
	if got := jittered.Delay(1); got < 100*time.Millisecond || got > 150*time.Millisecond {
		t.Fatalf("jittered delay=%v out of range", got)
	}
	if got := jittered.Delay(5); got != 200*time.Millisecond {
		t.Fatalf("jittered delay must still cap, got %v", got)
	}
}

func TestPayloadHashStableAndDiscriminating(t *testing.T) {
	req := message.SubmitRequest{TenantID: "t", From: "BRAND", To: "9779", Text: "hello"}
	h1 := message.PayloadHash(req)
	h2 := message.PayloadHash(req)
	if h1 != h2 {
		t.Fatal("PayloadHash must be deterministic")
	}
	if len(h1) != 64 {
		t.Fatalf("hash length=%d want 64 (sha256 hex)", len(h1))
	}
	req.Text = "hello2"
	if message.PayloadHash(req) == h1 {
		t.Fatal("different text must change the hash")
	}
	req.Text = "hello"
	req.TenantID = "other"
	if message.PayloadHash(req) == h1 {
		t.Fatal("different tenant must change the hash")
	}
}

func TestIdempotencyConflict(t *testing.T) {
	err := message.IdempotencyConflict("key-1", "old", "new")
	if err == nil {
		t.Fatal("conflict must return an error")
	}
	msg := err.Error()
	for _, want := range []string{"key-1", "old", "new"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q must mention %q", msg, want)
		}
	}
}
