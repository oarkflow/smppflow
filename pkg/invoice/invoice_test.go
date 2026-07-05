package invoice

import (
	"github.com/oarkflow/smppflow/pkg/billing"
	"github.com/oarkflow/smppflow/pkg/message"
	"strings"
	"testing"
	"time"
)

func TestBuildInvoice(t *testing.T) {
	now := time.Now()
	rows := []billing.RatedMessage{{MessageID: message.ID("m1"), TenantID: "t1", ProviderID: "p1", Country: "NP", Segments: 2, Sell: billing.Money{Currency: "USD", AmountMicros: 200000}, RatedAt: now}}
	inv, err := Build("t1", now.Add(-time.Hour), now.Add(time.Hour), rows, 0.1)
	if err != nil || inv.TotalMicros != 220000 || len(inv.Lines) != 1 {
		t.Fatalf("bad invoice %#v %v", inv, err)
	}
	if !strings.Contains(inv.CSV(), "total") {
		t.Fatal("csv missing total")
	}
}
