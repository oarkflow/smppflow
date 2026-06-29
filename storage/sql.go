package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/lifecycle"
	"github.com/oarkflow/smppflow/message"
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
