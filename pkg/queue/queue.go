package queue

import (
	"container/heap"
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

// jobHeap is the shared slice backing for the two priority queues below. It
// implements the Len/Swap/Push/Pop parts of container/heap.Interface; each
// concrete heap type only needs to supply Less.
type jobHeap []*Job

func (h jobHeap) Len() int      { return len(h) }
func (h jobHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *jobHeap) Push(x any) {
	*h = append(*h, x.(*Job))
}
func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// readyHeap orders jobs that are already eligible to run (RunAt <= now) by
// highest priority first, then earliest CreatedAt — matching the previous
// linear-scan selection rule, but in O(log n) per push/pop.
type readyHeap struct{ jobHeap }

func (h readyHeap) Less(i, j int) bool {
	a, b := h.jobHeap[i], h.jobHeap[j]
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return a.CreatedAt.Before(b.CreatedAt)
}

// delayedHeap orders not-yet-ready jobs (RunAt in the future) by soonest
// RunAt first, so Reserve can cheaply peek the next time a job becomes
// eligible without scanning the whole queue.
type delayedHeap struct{ jobHeap }

func (h delayedHeap) Less(i, j int) bool {
	return h.jobHeap[i].RunAt.Before(h.jobHeap[j].RunAt)
}

// Memory is an in-memory Backend implementation. Jobs are kept in two
// heaps instead of a flat slice: `ready` (eligible jobs, ordered by
// priority/creation time) and `delayed` (future jobs, ordered by RunAt).
// Reserve() promotes due jobs from `delayed` to `ready` and then pops the
// heap top — O(log n) instead of the previous O(n) linear scan.
type Memory struct {
	mu      sync.Mutex
	ready   readyHeap
	delayed delayedHeap
	dlq     []*Job
	cond    *sync.Cond
	closed  bool
	paused  bool
}

func NewMemory() *Memory {
	q := &Memory{}
	q.cond = sync.NewCond(&q.mu)
	heap.Init(&q.ready)
	heap.Init(&q.delayed)
	return q
}

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
	q.push(&cp, time.Now())
	q.cond.Broadcast()
	return nil
}

// push places a job into the ready heap or the delayed heap depending on
// whether it is already eligible to run relative to `now`. Caller must hold
// q.mu.
func (q *Memory) push(j *Job, now time.Time) {
	if j.RunAt.After(now) {
		heap.Push(&q.delayed, j)
	} else {
		heap.Push(&q.ready, j)
	}
}

// promoteDue moves any jobs from the delayed heap whose RunAt is now due
// into the ready heap. Caller must hold q.mu.
func (q *Memory) promoteDue(now time.Time) {
	for q.delayed.Len() > 0 && !q.delayed.jobHeap[0].RunAt.After(now) {
		j := heap.Pop(&q.delayed).(*Job)
		heap.Push(&q.ready, j)
	}
}

func (q *Memory) Reserve(ctx context.Context) (*Job, error) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, errors.New("queue closed")
		}
		if q.paused {
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		now := time.Now()
		q.promoteDue(now)
		if q.ready.Len() > 0 {
			j := heap.Pop(&q.ready).(*Job)
			j.State = JobReserved
			j.Attempts++
			j.UpdatedAt = time.Now()
			cp := *j
			q.mu.Unlock()
			return &cp, nil
		}
		wait := 100 * time.Millisecond
		if q.delayed.Len() > 0 {
			if d := q.delayed.jobHeap[0].RunAt.Sub(now); d < wait {
				if d < time.Millisecond {
					d = time.Millisecond
				}
				wait = d
			}
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
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
	q.push(&cp, time.Now())
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
	all := make([]*Job, 0, q.ready.Len()+q.delayed.Len())
	all = append(all, q.ready.jobHeap...)
	all = append(all, q.delayed.jobHeap...)
	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt.Before(all[j].CreatedAt) })
	out := make([]Job, 0, len(all))
	for _, j := range all {
		out = append(out, *j)
	}
	return out
}
func (q *Memory) Close() { q.mu.Lock(); q.closed = true; q.cond.Broadcast(); q.mu.Unlock() }

func (q *Memory) Pause()  { q.mu.Lock(); q.paused = true; q.mu.Unlock() }
func (q *Memory) Resume() { q.mu.Lock(); q.paused = false; q.cond.Broadcast(); q.mu.Unlock() }
func (q *Memory) Cancel(ctx context.Context, id string) error {
	_ = ctx
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, j := range q.ready.jobHeap {
		if j.ID == id {
			heap.Remove(&q.ready, i)
			j.State = JobFailed
			j.LastError = "cancelled"
			q.dlq = append(q.dlq, j)
			return nil
		}
	}
	for i, j := range q.delayed.jobHeap {
		if j.ID == id {
			heap.Remove(&q.delayed, i)
			j.State = JobFailed
			j.LastError = "cancelled"
			q.dlq = append(q.dlq, j)
			return nil
		}
	}
	return errors.New("job not found")
}
func (q *Memory) ReplayDLQ(ctx context.Context, ids ...string) error {
	_ = ctx
	q.mu.Lock()
	defer q.mu.Unlock()
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	replayAll := len(want) == 0
	kept := q.dlq[:0]
	now := time.Now()
	for _, j := range q.dlq {
		if replayAll || want[j.ID] {
			cp := *j
			cp.State = JobQueued
			cp.LastError = ""
			cp.RunAt = now
			cp.UpdatedAt = now
			q.push(&cp, now)
		} else {
			kept = append(kept, j)
		}
	}
	q.dlq = kept
	q.cond.Broadcast()
	return nil
}
