package contacts

import (
	"context"
	"strings"
	"testing"
)

func TestImportAndSegment(t *testing.T) {
	st := NewStore()
	csv := "msisdn,first_name,country,tags\n+9771,A,NP,vip|promo\n+9772,B,NP,blocked\n"
	n, err := ImportCSV(context.Background(), st, "t1", strings.NewReader(csv))
	if err != nil || n != 2 {
		t.Fatalf("import %d %v", n, err)
	}
	xs := st.Select(context.Background(), Segment{TenantID: "t1", Country: "NP", IncludeTags: []string{"vip"}, ExcludeTags: []string{"blocked"}})
	if len(xs) != 1 || xs[0].MSISDN != "9771" {
		t.Fatalf("bad segment %#v", xs)
	}
}
