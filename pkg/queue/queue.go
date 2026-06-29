package queue

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type JobState string

const (
	JobQueued       JobState = "queued"
	JobReserved     JobState = "reserved"
	JobDone         JobState = "done"
	JobFailed       JobState = "failed"
	JobDeadLettered JobState = "dead_lettered"
)

type Job struct {
	ID          string
	Type        string
	Payload     any
	Priority    int
	RunAt       time.Time
	Attempts    int
	MaxAttempts int
	LastError   string
	State       JobState
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Meta        map[string]string
}

type Memory struct {
	mu     sync.Mutex
	jobs   []*Job
	dlq    []*Job
	cond   *sync.Cond
	closed bool
}

func NewMemory() *Memory { q := &Memory{}; q.cond = sync.NewCond(&q.mu); return q }
func (q *Memory) Enqueue(ctx context.Context, j Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return errors.New("queue closed")
	}
	if j.ID == "" {
		j.ID = time.Now().Format("20060102150405.000000000")
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now()
	}
	if j.RunAt.IsZero() {
		j.RunAt = time.Now()
	}
	if j.MaxAttempts == 0 {
		j.MaxAttempts = 3
	}
	j.State = JobQueued
	j.UpdatedAt = time.Now()
	cp := j
	q.jobs = append(q.jobs, &cp)
	q.cond.Broadcast()
	return nil
}
func (q *Memory) Reserve(ctx context.Context) (*Job, error) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, errors.New("queue closed")
		}
		now := time.Now()
		idx := -1
		for i, j := range q.jobs {
			if j.State == JobQueued && !j.RunAt.After(now) {
				if idx < 0 || j.Priority > q.jobs[idx].Priority || (j.Priority == q.jobs[idx].Priority && j.CreatedAt.Before(q.jobs[idx].CreatedAt)) {
					idx = i
				}
			}
		}
		if idx >= 0 {
			j := q.jobs[idx]
			q.jobs = append(q.jobs[:idx], q.jobs[idx+1:]...)
			j.State = JobReserved
			j.Attempts++
			j.UpdatedAt = time.Now()
			cp := *j
			q.mu.Unlock()
			return &cp, nil
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func (q *Memory) Ack(ctx context.Context, id string) error { return nil }
func (q *Memory) Nack(ctx context.Context, j *Job, err error, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if j == nil {
		return nil
	}
	cp := *j
	if err != nil {
		cp.LastError = err.Error()
	}
	if cp.Attempts >= cp.MaxAttempts {
		cp.State = JobDeadLettered
		cp.UpdatedAt = time.Now()
		q.dlq = append(q.dlq, &cp)
		return nil
	}
	cp.State = JobQueued
	cp.RunAt = time.Now().Add(delay)
	cp.UpdatedAt = time.Now()
	q.jobs = append(q.jobs, &cp)
	q.cond.Broadcast()
	return nil
}
func (q *Memory) DLQ() []Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Job, 0, len(q.dlq))
	for _, j := range q.dlq {
		out = append(out, *j)
	}
	return out
}
func (q *Memory) Snapshot() []Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	sort.Slice(q.jobs, func(i, j int) bool { return q.jobs[i].CreatedAt.Before(q.jobs[j].CreatedAt) })
	out := make([]Job, 0, len(q.jobs))
	for _, j := range q.jobs {
		out = append(out, *j)
	}
	return out
}
func (q *Memory) Close() { q.mu.Lock(); q.closed = true; q.cond.Broadcast(); q.mu.Unlock() }
