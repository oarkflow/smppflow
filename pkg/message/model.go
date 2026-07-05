package message

import (
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/protocol"
)

type ID string

type Message struct {
	ID                 ID
	TenantID           string
	ProviderID         string
	CampaignID         string
	TemplateID         string
	IdempotencyKey     string
	PayloadHash        string
	From               protocol.Address
	To                 protocol.Address
	Text               string
	Data               []byte
	Encoding           Encoding
	DataCoding         byte
	RegisteredDelivery byte
	State              lifecycle.MessageState
	ProviderMessageID  string
	Parts              []SegmentRecord
	ScheduledAt        time.Time
	ExpiresAt          time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Transitions        []Transition
	Attempts           []SubmitAttempt
	Meta               map[string]string
}

type SegmentRecord struct {
	ID                string
	Index             int
	Total             int
	Ref               uint16
	ProviderMessageID string
	State             lifecycle.MessageState
	Payload           []byte
	DataCoding        byte
	Sequence          uint32
	Attempt           int
	ErrorCode         string
	LastError         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Meta              map[string]string
}

type DeliveryReceipt struct {
	MessageID         string
	ProviderMessageID string
	ProviderID        string
	SegmentID         string
	State             lifecycle.MessageState
	Raw               string
	DoneAt            time.Time
	ReceivedAt        time.Time
	ErrorCode         string
	TLVs              map[uint16][]byte
}

type SubmitRequest struct {
	TenantID           string
	ProviderID         string
	CampaignID         string
	TemplateID         string
	IdempotencyKey     string
	PayloadHash        string
	From               string
	To                 string
	Text               string
	RegisteredDelivery byte
	UseSAR             bool
	ScheduleAt         time.Time
	ExpiresAt          time.Time
	Meta               map[string]string
}

type SubmitResult struct {
	MessageID   ID
	ProviderIDs []string
	Parts       int
	State       lifecycle.MessageState
	Idempotent  bool
}

type SubmitAttempt struct {
	ID                string                 `json:"id"`
	MessageID         ID                     `json:"message_id"`
	SegmentID         string                 `json:"segment_id,omitempty"`
	ProviderID        string                 `json:"provider_id,omitempty"`
	ProviderMessageID string                 `json:"provider_message_id,omitempty"`
	Sequence          uint32                 `json:"sequence,omitempty"`
	State             lifecycle.MessageState `json:"state"`
	ErrorCode         string                 `json:"error_code,omitempty"`
	Error             string                 `json:"error,omitempty"`
	StartedAt         time.Time              `json:"started_at"`
	FinishedAt        time.Time              `json:"finished_at,omitempty"`
}

type InboundMessage struct {
	ID         string            `json:"id"`
	ProviderID string            `json:"provider_id,omitempty"`
	From       protocol.Address  `json:"from"`
	To         protocol.Address  `json:"to"`
	Text       string            `json:"text,omitempty"`
	Payload    []byte            `json:"payload,omitempty"`
	DataCoding byte              `json:"data_coding,omitempty"`
	ReceivedAt time.Time         `json:"received_at"`
	Raw        map[string]string `json:"raw,omitempty"`
}
