package campaign

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
)

type Submitter interface {
	Submit(context.Context, message.SubmitRequest) (message.SubmitResult, error)
}
type Renderer interface {
	Render(context.Context, string, map[string]string) (string, error)
}

type RunnerConfig struct {
	Store      *Store
	Submitter  Submitter
	Renderer   Renderer
	DefaultTPS int
	Now        func() time.Time
}
type Runner struct {
	cfg     RunnerConfig
	mu      sync.Mutex
	running map[string]context.CancelFunc
}

func NewRunner(cfg RunnerConfig) *Runner {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Runner{cfg: cfg, running: map[string]context.CancelFunc{}}
}

func (r *Runner) Start(ctx context.Context, campaignID string) error {
	if r.cfg.Store == nil {
		return errors.New("campaign store required")
	}
	if r.cfg.Submitter == nil {
		return errors.New("message submitter required")
	}
	cs, err := r.cfg.Store.ListCampaigns(ctx, "")
	if err != nil {
		return err
	}
	var c Campaign
	found := false
	for _, x := range cs {
		if x.ID == campaignID {
			c = x
			found = true
			break
		}
	}
	if !found {
		return errors.New("campaign not found")
	}
	if c.State == Cancelled || c.State == Completed {
		return fmt.Errorf("campaign state %s cannot start", c.State)
	}
	if err := r.cfg.Store.SetState(ctx, campaignID, Running); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	if old := r.running[campaignID]; old != nil {
		old()
	}
	r.running[campaignID] = cancel
	r.mu.Unlock()
	go r.loop(ctx, c)
	return nil
}
func (r *Runner) Pause(ctx context.Context, id string) error {
	r.mu.Lock()
	if c := r.running[id]; c != nil {
		c()
		delete(r.running, id)
	}
	r.mu.Unlock()
	return r.cfg.Store.SetState(ctx, id, Paused)
}
func (r *Runner) Cancel(ctx context.Context, id string) error {
	r.mu.Lock()
	if c := r.running[id]; c != nil {
		c()
		delete(r.running, id)
	}
	r.mu.Unlock()
	return r.cfg.Store.SetState(ctx, id, Cancelled)
}
func (r *Runner) Resume(ctx context.Context, id string) error { return r.Start(ctx, id) }

func (r *Runner) loop(ctx context.Context, c Campaign) {
	tps := c.TPS
	if tps <= 0 {
		tps = r.cfg.DefaultTPS
	}
	if tps <= 0 {
		tps = 1
	}
	interval := time.Second / time.Duration(tps)
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	recipients, _ := r.cfg.Store.ListRecipients(ctx, c.ID)
	for _, rec := range recipients {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if rec.MessageID != "" {
			continue
		}
		text := c.Meta["text"]
		if r.cfg.Renderer != nil && c.TemplateID != "" {
			if rendered, err := r.cfg.Renderer.Render(ctx, c.TemplateID, rec.Variables); err == nil {
				text = rendered
			} else {
				rec.UpdatedAt = r.cfg.Now()
				_ = r.cfg.Store.AddRecipient(ctx, rec)
				continue
			}
		}
		res, err := r.cfg.Submitter.Submit(ctx, message.SubmitRequest{TenantID: c.TenantID, CampaignID: c.ID, TemplateID: c.TemplateID, From: c.Sender, To: rec.To, Text: text, IdempotencyKey: "campaign:" + c.ID + ":" + rec.To, Meta: map[string]string{"campaign": c.Name}})
		rec.UpdatedAt = r.cfg.Now()
		if err == nil {
			rec.MessageID = res.MessageID
			rec.State = res
		} else {
			if rec.Variables == nil {
				rec.Variables = map[string]string{}
			}
			rec.Variables["last_error"] = err.Error()
		}
		_ = r.cfg.Store.AddRecipient(ctx, rec)
	}
	_ = r.cfg.Store.SetState(context.Background(), c.ID, Completed)
	r.mu.Lock()
	delete(r.running, c.ID)
	r.mu.Unlock()
}

type CSVImportResult struct {
	Added   int
	Skipped int
	Errors  []string
}

func ImportCSVLines(ctx context.Context, st *Store, campaignID string, lines []string) CSVImportResult {
	res := CSVImportResult{}
	seen := map[string]bool{}
	for i, line := range lines {
		_ = ctx
		fields := strings.Split(line, ",")
		if len(fields) == 0 || strings.TrimSpace(fields[0]) == "" {
			res.Skipped++
			continue
		}
		to := strings.TrimSpace(fields[0])
		if seen[to] {
			res.Skipped++
			continue
		}
		seen[to] = true
		vars := map[string]string{}
		for _, kv := range fields[1:] {
			p := strings.SplitN(kv, "=", 2)
			if len(p) == 2 {
				vars[strings.TrimSpace(p[0])] = strings.TrimSpace(p[1])
			}
		}
		if err := st.AddRecipient(ctx, Recipient{CampaignID: campaignID, To: to, Variables: vars}); err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("line %d: %v", i+1, err))
		} else {
			res.Added++
		}
	}
	return res
}
