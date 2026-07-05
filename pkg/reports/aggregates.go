package reports

import (
	"context"
	"sort"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/storage"
)

type UsageAggregate struct {
	Key        string    `json:"key"`
	TenantID   string    `json:"tenant_id,omitempty"`
	ProviderID string    `json:"provider_id,omitempty"`
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
	Submitted  int       `json:"submitted"`
	Delivered  int       `json:"delivered"`
	Failed     int       `json:"failed"`
	Segments   int       `json:"segments"`
}

func BuildUsageAggregates(ctx context.Context, st storage.Store, f Filter, dimension string) ([]UsageAggregate, error) {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]*UsageAggregate{}
	for _, m := range msgs {
		if !match(m, f) {
			continue
		}
		bucketKey, start, end := bucket(m.CreatedAt, f)
		dim := ""
		switch dimension {
		case "provider":
			dim = m.ProviderID
		case "tenant":
			dim = m.TenantID
		default:
			dim = "all"
		}
		key := dim + "|" + bucketKey
		a := out[key]
		if a == nil {
			a = &UsageAggregate{Key: key, Start: start, End: end}
			if dimension == "provider" {
				a.ProviderID = dim
			} else if dimension == "tenant" {
				a.TenantID = dim
			}
			out[key] = a
		}
		a.Submitted++
		a.Segments += max(1, len(m.Parts))
		switch m.State {
		case lifecycle.MsgDelivered:
			a.Delivered++
		case lifecycle.MsgFailed, lifecycle.MsgUndeliverable, lifecycle.MsgExpired, lifecycle.MsgRejected:
			a.Failed++
		}
	}
	arr := []UsageAggregate{}
	for _, a := range out {
		arr = append(arr, *a)
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].Start.Before(arr[j].Start) || arr[i].Key < arr[j].Key })
	return arr, nil
}
