package reports

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/storage"
)

func TestSchedulerAndAggregates(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemory()
	_ = st.SaveMessage(ctx, &message.Message{ID: "m1", TenantID: "t1", ProviderID: "p1", State: lifecycle.MsgDelivered, CreatedAt: time.Now(), Parts: []message.SegmentRecord{{ID: "s1"}}})
	sch := NewScheduler(st)
	sink := &MemorySink{}
	sch.RegisterSink("mem", sink)
	if err := sch.Upsert(ScheduledReport{ID: "r1", Name: "delivery", Format: ExportJSON, SinkName: "mem", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	ran := sch.RunDue(ctx)
	if len(ran) != 1 || ran[0].LastError != "" {
		t.Fatalf("ran=%+v", ran)
	}
	if len(sink.Objects) != 1 {
		t.Fatalf("objects=%d", len(sink.Objects))
	}
	ag, err := BuildUsageAggregates(ctx, st, Filter{GroupBy: "day"}, "tenant")
	if err != nil || len(ag) != 1 || ag[0].Delivered != 1 {
		t.Fatalf("ag=%+v err=%v", ag, err)
	}
}
