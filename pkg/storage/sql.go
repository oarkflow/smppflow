package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
)

// SQLStore is a portable database/sql implementation. It is driver agnostic:
// pass a PostgreSQL, MySQL or SQLite *sql.DB from the driver you use in your application.
type SQLStore struct{ DB *sql.DB }

func NewSQL(db *sql.DB) *SQLStore { return &SQLStore{DB: db} }

func (s *SQLStore) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS smpp_messages (id VARCHAR(128) PRIMARY KEY, tenant_id VARCHAR(128), provider_message_id VARCHAR(255), state VARCHAR(64), payload TEXT NOT NULL, created_at TIMESTAMP, updated_at TIMESTAMP)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_messages_tenant ON smpp_messages(tenant_id)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_messages_provider ON smpp_messages(provider_message_id)`,
		`CREATE TABLE IF NOT EXISTS smpp_dlrs (id VARCHAR(128) PRIMARY KEY, provider_message_id VARCHAR(255), message_id VARCHAR(128), state VARCHAR(64), raw TEXT, error_code VARCHAR(32), done_at TIMESTAMP, payload TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_dlrs_provider ON smpp_dlrs(provider_message_id)`,
		`CREATE TABLE IF NOT EXISTS smpp_message_events (id VARCHAR(128) PRIMARY KEY, tenant_id VARCHAR(128), provider_id VARCHAR(128), message_id VARCHAR(128), segment_id VARCHAR(128), from_state VARCHAR(64), to_state VARCHAR(64), reason TEXT, actor VARCHAR(128), error_code VARCHAR(64), payload TEXT, created_at TIMESTAMP)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_message_events_msg ON smpp_message_events(message_id, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_message_events_tenant ON smpp_message_events(tenant_id, created_at)`,
		`CREATE TABLE IF NOT EXISTS smpp_message_attempts (id VARCHAR(128) PRIMARY KEY, tenant_id VARCHAR(128), provider_id VARCHAR(128), message_id VARCHAR(128), segment_id VARCHAR(128), provider_message_id VARCHAR(255), state VARCHAR(64), error_code VARCHAR(64), payload TEXT, started_at TIMESTAMP, finished_at TIMESTAMP)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_message_attempts_msg ON smpp_message_attempts(message_id, started_at)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_message_attempts_provider ON smpp_message_attempts(provider_id, started_at)`,
		`CREATE TABLE IF NOT EXISTS smpp_inbound_messages (id VARCHAR(128) PRIMARY KEY, provider_id VARCHAR(128), source_addr VARCHAR(64), dest_addr VARCHAR(64), text TEXT, payload TEXT, received_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_webhook_attempts (id VARCHAR(128) PRIMARY KEY, tenant_id VARCHAR(128), message_id VARCHAR(128), kind VARCHAR(64), url TEXT, payload TEXT, last_error TEXT, created_at TIMESTAMP, delivered_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_idempotency (tenant_id VARCHAR(128), idem_key VARCHAR(255), payload_hash VARCHAR(128), message_id VARCHAR(128), result TEXT, created_at TIMESTAMP, expires_at TIMESTAMP, PRIMARY KEY(tenant_id, idem_key))`,
		`CREATE TABLE IF NOT EXISTS smpp_provider_sessions (id VARCHAR(128) PRIMARY KEY, provider_id VARCHAR(128), state VARCHAR(64), remote_addr VARCHAR(255), bind_mode VARCHAR(32), window_used INT, window_size INT, tps INT, last_enquire_at TIMESTAMP, updated_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_provider_health (provider_id VARCHAR(128) PRIMARY KEY, healthy BOOLEAN, quality_score DOUBLE PRECISION, delivery_rate DOUBLE PRECISION, current_tps INT, max_tps INT, last_error TEXT, updated_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_tenant_usage_hourly (tenant_id VARCHAR(128), bucket_at TIMESTAMP, submitted BIGINT, delivered BIGINT, failed BIGINT, segments BIGINT, sell_micros BIGINT, PRIMARY KEY(tenant_id,bucket_at))`,
		`CREATE TABLE IF NOT EXISTS smpp_tenant_usage_daily (tenant_id VARCHAR(128), bucket_at DATE, submitted BIGINT, delivered BIGINT, failed BIGINT, segments BIGINT, sell_micros BIGINT, PRIMARY KEY(tenant_id,bucket_at))`,
		`CREATE TABLE IF NOT EXISTS smpp_provider_usage_hourly (provider_id VARCHAR(128), bucket_at TIMESTAMP, submitted BIGINT, delivered BIGINT, failed BIGINT, throttled BIGINT, segments BIGINT, cost_micros BIGINT, PRIMARY KEY(provider_id,bucket_at))`,
		`CREATE TABLE IF NOT EXISTS smpp_billing_ledger (id VARCHAR(128) PRIMARY KEY, tenant_id VARCHAR(128), provider_id VARCHAR(128), message_id VARCHAR(128), segments INT, currency VARCHAR(8), cost_micros BIGINT, sell_micros BIGINT, margin_micros BIGINT, billable BOOLEAN, created_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_sender_approvals (id VARCHAR(128) PRIMARY KEY, tenant_id VARCHAR(128), sender VARCHAR(64), country VARCHAR(8), operator VARCHAR(64), state VARCHAR(32), document_ref TEXT, reason TEXT, created_at TIMESTAMP, updated_at TIMESTAMP, expires_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_optouts (tenant_id VARCHAR(128), msisdn VARCHAR(32), source VARCHAR(64), reason TEXT, created_at TIMESTAMP, PRIMARY KEY(tenant_id,msisdn))`,
		`CREATE TABLE IF NOT EXISTS smpp_campaigns (id VARCHAR(128) PRIMARY KEY, tenant_id VARCHAR(128), name TEXT, state VARCHAR(32), sender VARCHAR(64), template_id VARCHAR(128), tps INT, payload TEXT, created_at TIMESTAMP, updated_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_campaign_recipients (id VARCHAR(128) PRIMARY KEY, campaign_id VARCHAR(128), msisdn VARCHAR(32), message_id VARCHAR(128), state VARCHAR(64), payload TEXT, created_at TIMESTAMP, updated_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_admin_audit_logs (id VARCHAR(128) PRIMARY KEY, tenant_id VARCHAR(128), actor_id VARCHAR(128), action VARCHAR(128), resource VARCHAR(128), resource_id VARCHAR(128), payload TEXT, created_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_contacts (id VARCHAR(128) PRIMARY KEY, tenant_id VARCHAR(128), msisdn VARCHAR(32), first_name TEXT, last_name TEXT, country VARCHAR(8), operator VARCHAR(64), tags TEXT, payload TEXT, created_at TIMESTAMP, updated_at TIMESTAMP)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_contacts_tenant ON smpp_contacts(tenant_id, msisdn)`,
		`CREATE TABLE IF NOT EXISTS smpp_rate_policies (id VARCHAR(128) PRIMARY KEY, scope VARCHAR(32), rate_per_second INT, burst INT, queue_limit INT, adaptive BOOLEAN, payload TEXT, updated_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_alert_rules (id VARCHAR(128) PRIMARY KEY, name TEXT, signal VARCHAR(128), severity VARCHAR(32), payload TEXT, updated_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_incidents (id VARCHAR(128) PRIMARY KEY, rule_id VARCHAR(128), severity VARCHAR(32), signal VARCHAR(128), status VARCHAR(32), payload TEXT, opened_at TIMESTAMP, closed_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_invoices (id VARCHAR(128) PRIMARY KEY, tenant_id VARCHAR(128), period_start TIMESTAMP, period_end TIMESTAMP, currency VARCHAR(8), subtotal_micros BIGINT, tax_micros BIGINT, total_micros BIGINT, payload TEXT, created_at TIMESTAMP)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_invoices_tenant ON smpp_invoices(tenant_id, period_end)`,
	}
	for _, st := range stmts {
		if _, err := s.DB.ExecContext(ctx, st); err != nil {
			return err
		}
	}
	return nil
}
func (s *SQLStore) SaveMessage(ctx context.Context, msg *message.Message) error {
	if s.DB == nil {
		return fmt.Errorf("nil db")
	}
	b, _ := json.Marshal(msg)
	now := time.Now()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	msg.UpdatedAt = now
	_, err := s.DB.ExecContext(ctx, `INSERT INTO smpp_messages(id,tenant_id,provider_message_id,state,payload,created_at,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET tenant_id=excluded.tenant_id, provider_message_id=excluded.provider_message_id, state=excluded.state, payload=excluded.payload, updated_at=excluded.updated_at`, msg.ID, msg.TenantID, msg.ProviderMessageID, string(msg.State), string(b), msg.CreatedAt, msg.UpdatedAt)
	return err
}
func (s *SQLStore) GetMessage(ctx context.Context, id message.ID) (*message.Message, error) {
	row := s.DB.QueryRowContext(ctx, `SELECT payload FROM smpp_messages WHERE id=?`, id)
	var payload string
	if err := row.Scan(&payload); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var msg message.Message
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
func (s *SQLStore) ListMessages(ctx context.Context) ([]*message.Message, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT payload FROM smpp_messages ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*message.Message
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var msg message.Message
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			return nil, err
		}
		out = append(out, &msg)
	}
	return out, rows.Err()
}
func (s *SQLStore) UpdateMessageState(ctx context.Context, id message.ID, st lifecycle.MessageState) error {
	msg, err := s.GetMessage(ctx, id)
	if err != nil {
		return err
	}
	msg.State = st
	return s.SaveMessage(ctx, msg)
}
func (s *SQLStore) SaveDLR(ctx context.Context, d *message.DeliveryReceipt) error {
	b, _ := json.Marshal(d)
	id := fmt.Sprintf("dlr-%s-%d", d.ProviderMessageID, time.Now().UnixNano())
	_, err := s.DB.ExecContext(ctx, `INSERT INTO smpp_dlrs(id,provider_message_id,message_id,state,raw,error_code,done_at,payload) VALUES(?,?,?,?,?,?,?,?)`, id, d.ProviderMessageID, d.MessageID, string(d.State), d.Raw, d.ErrorCode, d.DoneAt, string(b))
	return err
}
func (s *SQLStore) ListDLR(ctx context.Context) ([]*message.DeliveryReceipt, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT payload FROM smpp_dlrs ORDER BY done_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*message.DeliveryReceipt{}
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var d message.DeliveryReceipt
		if err := json.Unmarshal([]byte(payload), &d); err != nil {
			return nil, err
		}
		out = append(out, &d)
	}
	return out, rows.Err()
}
