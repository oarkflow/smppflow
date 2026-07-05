package reconcile

import (
	"context"
	"errors"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/protocol"
)

type QueryClient interface {
	QuerySM(context.Context, protocol.QuerySMRequest) (protocol.QuerySMRespData, error)
}
type Store interface{ message.Store }
type Result struct {
	Checked int
	Updated int
	Unknown int
	Errors  []string
}
type Config struct {
	Store      Store
	Client     QueryClient
	ProviderID string
	OlderThan  time.Duration
	Limit      int
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Store == nil {
		return Result{}, errors.New("store required")
	}
	if cfg.Client == nil {
		return Result{}, errors.New("query client required")
	}
	if cfg.OlderThan <= 0 {
		cfg.OlderThan = 5 * time.Minute
	}
	msgs, err := cfg.Store.ListMessages(ctx)
	if err != nil {
		return Result{}, err
	}
	res := Result{}
	cutoff := time.Now().Add(-cfg.OlderThan)
	for _, m := range msgs {
		if cfg.Limit > 0 && res.Checked >= cfg.Limit {
			break
		}
		if cfg.ProviderID != "" && m.ProviderID != cfg.ProviderID {
			continue
		}
		if m.ProviderMessageID == "" || (m.UpdatedAt.After(cutoff) && m.CreatedAt.After(cutoff)) {
			continue
		}
		if m.State != lifecycle.MsgSubmitted && m.State != lifecycle.MsgAccepted && m.State != lifecycle.MsgUnknown {
			continue
		}
		res.Checked++
		q, err := cfg.Client.QuerySM(ctx, protocol.QuerySMRequest{MessageID: m.ProviderMessageID, Source: m.From})
		if err != nil {
			res.Errors = append(res.Errors, err.Error())
			continue
		}
		st := mapState(q.MessageState)
		if st == lifecycle.MsgUnknown {
			res.Unknown++
			continue
		}
		if err := cfg.Store.UpdateMessageState(ctx, m.ID, st); err != nil {
			res.Errors = append(res.Errors, err.Error())
		} else {
			res.Updated++
		}
	}
	return res, nil
}
func mapState(s byte) lifecycle.MessageState {
	switch s {
	case 1, 2:
		return lifecycle.MsgSubmitted
	case 3:
		return lifecycle.MsgDelivered
	case 4:
		return lifecycle.MsgExpired
	case 5:
		return lifecycle.MsgUndeliverable
	case 6:
		return lifecycle.MsgAccepted
	case 7:
		return lifecycle.MsgUnknown
	case 8:
		return lifecycle.MsgRejected
	default:
		return lifecycle.MsgUnknown
	}
}
