package reports

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/storage"
)

type ExportFormat string

const (
	ExportJSON   ExportFormat = "json"
	ExportCSV    ExportFormat = "csv"
	ExportNDJSON ExportFormat = "ndjson"
)

type Sink interface {
	Put(ctx context.Context, name string, r io.Reader) error
}
type MemorySink struct {
	mu      sync.Mutex
	Objects map[string][]byte
}

func (s *MemorySink) Put(ctx context.Context, name string, r io.Reader) error {
	_ = ctx
	if s.Objects == nil {
		s.Objects = map[string][]byte{}
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.Objects[name] = b
	s.mu.Unlock()
	return nil
}

type ScheduledReport struct {
	ID        string
	Name      string
	Format    ExportFormat
	Filter    Filter
	Every     time.Duration
	SinkName  string
	Enabled   bool
	LastRunAt time.Time
	LastError string
}
type Scheduler struct {
	mu      sync.Mutex
	Store   storage.Store
	Reports map[string]ScheduledReport
	Sinks   map[string]Sink
	Now     func() time.Time
}

func NewScheduler(st storage.Store) *Scheduler {
	return &Scheduler{Store: st, Reports: map[string]ScheduledReport{}, Sinks: map[string]Sink{}, Now: time.Now}
}
func (s *Scheduler) RegisterSink(name string, sink Sink) {
	s.mu.Lock()
	if s.Sinks == nil {
		s.Sinks = map[string]Sink{}
	}
	s.Sinks[name] = sink
	s.mu.Unlock()
}
func (s *Scheduler) Upsert(r ScheduledReport) error {
	if r.ID == "" {
		return errors.New("report id required")
	}
	if r.Format == "" {
		r.Format = ExportJSON
	}
	s.mu.Lock()
	if s.Reports == nil {
		s.Reports = map[string]ScheduledReport{}
	}
	s.Reports[r.ID] = r
	s.mu.Unlock()
	return nil
}
func (s *Scheduler) RunDue(ctx context.Context) []ScheduledReport {
	s.mu.Lock()
	ids := []string{}
	now := s.Now()
	for id, r := range s.Reports {
		if !r.Enabled {
			continue
		}
		if r.LastRunAt.IsZero() || r.Every <= 0 || now.Sub(r.LastRunAt) >= r.Every {
			ids = append(ids, id)
		}
	}
	s.mu.Unlock()
	out := []ScheduledReport{}
	for _, id := range ids {
		r, _ := s.Run(ctx, id)
		out = append(out, r)
	}
	return out
}
func (s *Scheduler) Run(ctx context.Context, id string) (ScheduledReport, error) {
	s.mu.Lock()
	r, ok := s.Reports[id]
	sink := s.Sinks[r.SinkName]
	s.mu.Unlock()
	if !ok {
		return ScheduledReport{}, errors.New("scheduled report not found")
	}
	if sink == nil {
		return r, errors.New("sink not found")
	}
	var b bytes.Buffer
	err := WriteExport(ctx, &b, s.Store, r.Format, r.Filter)
	if err == nil {
		name := fmt.Sprintf("%s-%s.%s", r.Name, time.Now().Format("20060102-150405"), r.Format)
		err = sink.Put(ctx, name, &b)
	}
	r.LastRunAt = time.Now()
	r.LastError = ""
	if err != nil {
		r.LastError = err.Error()
	}
	s.mu.Lock()
	s.Reports[id] = r
	s.mu.Unlock()
	return r, err
}

func WriteExport(ctx context.Context, w io.Writer, st storage.Store, format ExportFormat, f Filter) error {
	switch format {
	case ExportCSV:
		return WriteCSV(ctx, w, st)
	case ExportNDJSON:
		return WriteNDJSON(ctx, w, st, f)
	default:
		rep, err := GenerateDelivery(ctx, st, f)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
}
