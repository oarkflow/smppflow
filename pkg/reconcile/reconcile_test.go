package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/storage"
)

type fakeQuery struct{}

func (fakeQuery) QuerySM(ctx context.Context, r protocol.QuerySMRequest) (protocol.QuerySMRespData, error) {
	return protocol.QuerySMRespData{MessageID: r.MessageID, MessageState: 3}, nil
}
func TestRunReconciliation(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemory()
	_ = st.SaveMessage(ctx, &message.Message{ID: "m1", ProviderMessageID: "pm1", State: lifecycle.MsgSubmitted, CreatedAt: time.Now().Add(-time.Hour), UpdatedAt: time.Now().Add(-time.Hour)})
	res, err := Run(ctx, Config{Store: st, Client: fakeQuery{}, OlderThan: time.Minute})
	if err != nil || res.Updated != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	m, _ := st.GetMessage(ctx, "m1")
	if m.State != lifecycle.MsgDelivered {
		t.Fatalf("state=%s", m.State)
	}
}
