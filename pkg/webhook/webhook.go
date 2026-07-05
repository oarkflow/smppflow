package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
)

type Dispatcher struct {
	Client *http.Client
	Secret string
	Store  message.WebhookStore
	Retry  message.RetryPolicy
}

func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
func (d Dispatcher) Send(ctx context.Context, url, tenant, kind string, msgID message.ID, payload any) error {
	if url == "" {
		return errors.New("webhook url required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	c := d.Client
	if c == nil {
		c = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if d.Secret != "" {
		req.Header.Set("X-SMPPFlow-Signature", Sign(d.Secret, body))
	}
	resp, err := c.Do(req)
	ev := message.WebhookEvent{TenantID: tenant, URL: url, Kind: kind, MessageID: msgID, Payload: payload, CreatedAt: time.Now()}
	if err != nil {
		ev.LastError = err.Error()
		if d.Store != nil {
			_ = d.Store.SaveWebhookAttempt(ctx, ev)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		err = fmt.Errorf("webhook returned %s", resp.Status)
		ev.LastError = err.Error()
		if d.Store != nil {
			_ = d.Store.SaveWebhookAttempt(ctx, ev)
		}
		return err
	}
	ev.DeliveredAt = time.Now()
	if d.Store != nil {
		_ = d.Store.SaveWebhookAttempt(ctx, ev)
	}
	return nil
}
