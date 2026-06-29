package reports

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"time"

	"github.com/oarkflow/smppflow/storage"
)

type Summary struct {
	GeneratedAt time.Time      `json:"generated_at"`
	Total       int            `json:"total"`
	ByState     map[string]int `json:"by_state"`
	Parts       int            `json:"parts"`
	DLRs        int            `json:"dlrs"`
}

func GenerateSummary(ctx context.Context, st storage.Store) (Summary, error) {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return Summary{}, err
	}
	dlrs, _ := st.ListDLR(ctx)
	s := Summary{GeneratedAt: time.Now(), ByState: map[string]int{}, DLRs: len(dlrs)}
	for _, m := range msgs {
		s.Total++
		s.ByState[string(m.State)]++
		s.Parts += len(m.Parts)
	}
	return s, nil
}
func WriteJSON(ctx context.Context, w io.Writer, st storage.Store) error {
	s, err := GenerateSummary(ctx, st)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}
func WriteCSV(ctx context.Context, w io.Writer, st storage.Store) error {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return err
	}
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"id", "tenant", "from", "to", "state", "provider_message_id", "parts", "created_at"})
	for _, m := range msgs {
		_ = cw.Write([]string{string(m.ID), m.TenantID, m.From.Addr, m.To.Addr, string(m.State), m.ProviderMessageID, itoa(len(m.Parts)), m.CreatedAt.Format(time.RFC3339Nano)})
	}
	cw.Flush()
	return cw.Error()
}
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
