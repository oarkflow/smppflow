package campaign

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
)

type fakeSubmitter struct{ n int }

func (f *fakeSubmitter) Submit(ctx context.Context, r message.SubmitRequest) (message.SubmitResult, error) {
	f.n++
	return message.SubmitResult{MessageID: message.ID(r.To), Parts: 1, State: lifecycle.MsgSubmitted}, nil
}
func TestRunnerCompletesCampaign(t *testing.T) {
	ctx := context.Background()
	st := NewStore()
	_ = st.SaveCampaign(ctx, Campaign{ID: "c1", TenantID: "t1", Name: "c", Sender: "S", TPS: 1000, Meta: map[string]string{"text": "hi"}})
	_ = st.AddRecipient(ctx, Recipient{CampaignID: "c1", To: "1"})
	_ = st.AddRecipient(ctx, Recipient{CampaignID: "c1", To: "2"})
	fs := &fakeSubmitter{}
	r := NewRunner(RunnerConfig{Store: st, Submitter: fs, DefaultTPS: 1000})
	if err := r.Start(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	cs, _ := st.ListCampaigns(ctx, "")
	if cs[0].State != Completed {
		t.Fatalf("state=%s", cs[0].State)
	}
	if fs.n != 2 {
		t.Fatalf("submitted %d", fs.n)
	}
}
