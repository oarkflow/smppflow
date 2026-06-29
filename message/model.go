package message

import (
	"github.com/oarkflow/smppflow/lifecycle"
	"github.com/oarkflow/smppflow/protocol"
	"time"
)

type ID string

type Message struct {
	ID                 ID
	TenantID           string
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
	CreatedAt          time.Time
	UpdatedAt          time.Time
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
	CreatedAt         time.Time
	UpdatedAt         time.Time
}
type DeliveryReceipt struct {
	MessageID         string
	ProviderMessageID string
	State             lifecycle.MessageState
	Raw               string
	DoneAt            time.Time
	ErrorCode         string
}
type SubmitRequest struct {
	TenantID           string
	From               string
	To                 string
	Text               string
	RegisteredDelivery byte
	UseSAR             bool
	Meta               map[string]string
}
type SubmitResult struct {
	MessageID   ID
	ProviderIDs []string
	Parts       int
	State       lifecycle.MessageState
}
