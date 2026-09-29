package durablequeue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/broker"

	"github.com/oarkflow/smppflow/pkg/queue"
)

// BrokerQueue is a Backend built on github.com/oarkflow/broker's embedded,
// WAL-backed Store. Rather than depending on an external Redis deployment
// (see RedisStreamsQueue) or a SQL database (see SQLQueue), it runs the
// broker's queue engine in-process and persists every mutation (enqueue,
// lease, ack, nack) to a local write-ahead log before applying it in memory,
// so pending/unacked jobs survive a process restart. Because it is the same
// team's own broker rather than a separate service to operate, it is the
// recommended default backend for production deployments that would
// otherwise need to stand up Redis purely for durable job queueing.
//
// Honest caveat: oarkflow/broker's own README describes itself as "pre-v1":
// "Correctness, durability, protocol conformance, and security hardening are
// still in progress," and the embedded mode specifically is listed as
// "functional, hardening in progress" with "No current mode has a stable
// production-support contract." BrokerQueue inherits that maturity level.
// Treat it as the preferred *option*, not a risk-free default: pilot it,
// monitor it (broker exposes Prometheus metrics and a DLQ), and keep
// RedisStreamsQueue or SQLQueue available as a fallback until broker reaches
// a stable release.
//
// broker's Store interface is pull-based (Lease/Ack/Nack with lease tokens
// and visibility timeouts), which maps directly onto this package's
// Reserve/Ack/Nack contract — BrokerQueue calls Store().Lease/Ack/Nack
// directly rather than going through broker's push-style Subscribe/Handler
// API, so there is no callback-to-channel emulation involved.
//
// Ack and Nack only receive a job ID (matching the Backend interface), but
// broker's AckRequest/NackRequest also require the lease token and consumer
// ID issued at Lease time. BrokerQueue tracks that association internally
// (an in-memory map from broker message ID to lease token, populated by
// Reserve and consumed by Ack/Nack), so lease bookkeeping is invisible to
// callers. That map is process-local: if the process crashes between Reserve
// and Ack, the lease simply expires after VisibilityTimeout and broker
// redelivers the message, the same as any other visibility-timeout based
// queue.
type BrokerQueue struct {
	b     *broker.Broker
	store broker.Store

	queue         string
	consumerID    string
	consumerGroup string
	visibility    time.Duration

	leasesMu sync.Mutex
	leases   map[string]leaseInfo
}

type leaseInfo struct {
	token      string
	consumerID string
}

// BrokerQueueOption configures NewBrokerQueue.
type BrokerQueueOption func(*brokerQueueConfig)

type brokerQueueConfig struct {
	consumerID       string
	consumerGroup    string
	visibility       time.Duration
	maxDepth         int
	dlq              string
	retryPolicy      broker.RetryPolicy
	binaryWAL        bool
	pollInterval     time.Duration
	enablePrometheus bool
}

// WithConsumerID sets the consumer identity used for Reserve/Ack/Nack leases.
// Defaults to "durablequeue".
func WithConsumerID(id string) BrokerQueueOption {
	return func(c *brokerQueueConfig) { c.consumerID = id }
}

// WithConsumerGroup sets the consumer group used for leasing. Defaults to
// "durablequeue".
func WithConsumerGroup(group string) BrokerQueueOption {
	return func(c *brokerQueueConfig) { c.consumerGroup = group }
}

// WithVisibilityTimeout sets how long a leased-but-unacked job stays
// invisible to other Reserve calls before broker redelivers it. Defaults to
// 30s.
func WithVisibilityTimeout(d time.Duration) BrokerQueueOption {
	return func(c *brokerQueueConfig) { c.visibility = d }
}

// WithMaxDepth caps the queue's depth (broker applies its configured
// backpressure policy once reached). 0 means unbounded.
func WithMaxDepth(n int) BrokerQueueOption {
	return func(c *brokerQueueConfig) { c.maxDepth = n }
}

// WithDLQ names the dead-letter queue jobs are moved to when Nack is called
// on a job that has exhausted its attempts. Defaults to "<queue>.dlq".
func WithDLQ(name string) BrokerQueueOption {
	return func(c *brokerQueueConfig) { c.dlq = name }
}

// WithRetryPolicy sets broker's own backoff policy (used for its internal
// bookkeeping/metrics); BrokerQueue's Nack still computes the actual delay
// passed to it by the caller, matching SQLQueue/RedisStreamsQueue behavior.
func WithRetryPolicy(p broker.RetryPolicy) BrokerQueueOption {
	return func(c *brokerQueueConfig) { c.retryPolicy = p }
}

// WithBinaryWAL switches the WAL format from the default JSONL
// (broker.NewFileWALStore) to broker's packed binary BWL1 format
// (broker.NewBinaryFileWALStore), which is more compact and faster to
// replay at the cost of not being human-readable on disk.
func WithBinaryWAL() BrokerQueueOption {
	return func(c *brokerQueueConfig) { c.binaryWAL = true }
}

// WithPollInterval sets broker's internal poll interval (used by its
// timer-driven sweepers, e.g. lease expiry). Defaults to broker's own
// default.
func WithPollInterval(d time.Duration) BrokerQueueOption {
	return func(c *brokerQueueConfig) { c.pollInterval = d }
}

// WithPrometheusMetrics enables broker's built-in Prometheus metrics
// collection for this broker instance.
func WithPrometheusMetrics() BrokerQueueOption {
	return func(c *brokerQueueConfig) { c.enablePrometheus = true }
}

// NewBrokerQueue constructs a BrokerQueue backed by an embedded broker whose
// Store persists to dataDir via a write-ahead log. dataDir is created if it
// does not exist. queueName is declared on the broker (idempotently, so
// reopening the same dataDir across process restarts is safe).
//
// The returned BrokerQueue owns the embedded broker: call Close to stop it
// (flushing/closing the WAL) when done.
func NewBrokerQueue(ctx context.Context, dataDir string, queueName string, opts ...BrokerQueueOption) (*BrokerQueue, error) {
	if dataDir == "" {
		return nil, errors.New("durablequeue: BrokerQueue requires a non-empty dataDir")
	}
	if queueName == "" {
		return nil, errors.New("durablequeue: BrokerQueue requires a non-empty queueName")
	}
	cfg := brokerQueueConfig{
		consumerID:    "durablequeue",
		consumerGroup: "durablequeue",
		visibility:    30 * time.Second,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	var store broker.Store
	var err error
	if cfg.binaryWAL {
		store, err = broker.NewBinaryFileWALStore(dataDir)
	} else {
		store, err = broker.NewFileWALStore(dataDir)
	}
	if err != nil {
		return nil, fmt.Errorf("durablequeue: open broker WAL store at %q: %w", dataDir, err)
	}

	b, err := broker.New(broker.Config{
		Name:              "durablequeue-" + queueName,
		Store:             store,
		DefaultQueue:      queueName,
		DefaultVisibility: cfg.visibility,
		DefaultRetry:      cfg.retryPolicy,
		PollInterval:      cfg.pollInterval,
		EnablePrometheus:  cfg.enablePrometheus,
	})
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("durablequeue: construct broker: %w", err)
	}
	if err := b.Start(ctx); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("durablequeue: start broker: %w", err)
	}

	dlq := cfg.dlq
	if dlq == "" {
		dlq = queueName + ".dlq"
	}
	retry := cfg.retryPolicy
	if retry.MaxAttempts <= 0 {
		retry = broker.DefaultRetryPolicy()
	}
	if err := b.DeclareQueue(ctx, broker.QueueConfig{
		Name:              queueName,
		MaxDepth:          cfg.maxDepth,
		VisibilityTimeout: cfg.visibility,
		AckMode:           broker.AckManual,
		RetryPolicy:       retry,
		DLQ:               dlq,
	}); err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Stop(stopCtx)
		return nil, fmt.Errorf("durablequeue: declare queue %q: %w", queueName, err)
	}

	return &BrokerQueue{
		b:             b,
		store:         b.Store(),
		queue:         queueName,
		consumerID:    cfg.consumerID,
		consumerGroup: cfg.consumerGroup,
		visibility:    cfg.visibility,
		leases:        make(map[string]leaseInfo),
	}, nil
}

// Broker returns the underlying *broker.Broker, for callers that want direct
// access to broker's routing, metrics, DLQ, or admin APIs.
func (q *BrokerQueue) Broker() *broker.Broker { return q.b }

// Close stops the embedded broker, releasing its WAL file handle. Pending
// (unacked) jobs remain durable on disk and are recovered the next time
// NewBrokerQueue is called against the same dataDir.
func (q *BrokerQueue) Close(ctx context.Context) error {
	return q.b.Stop(ctx)
}

// Shutdown implements queue.Backend by delegating to Close.
func (q *BrokerQueue) Shutdown(ctx context.Context) error { return q.Close(ctx) }

var _ queue.Backend = (*BrokerQueue)(nil)

func (q *BrokerQueue) Enqueue(ctx context.Context, j queue.Job) error {
	if j.ID == "" {
		j.ID = time.Now().Format("20060102150405.000000000")
	}
	if j.RunAt.IsZero() {
		j.RunAt = time.Now()
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now()
	}
	if j.MaxAttempts == 0 {
		j.MaxAttempts = 3
	}
	payload, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("brokerqueue: marshal job: %w", err)
	}
	opts := []broker.PublishOption{
		broker.WithQueue(q.queue),
		broker.WithMessageID(j.ID),
		broker.WithConfirmLevel(broker.ConfirmPersisted),
	}
	if !j.RunAt.After(time.Now()) {
		// Available immediately.
	} else {
		opts = append(opts, broker.WithRunAt(j.RunAt))
	}
	if _, err := q.b.Publisher().Publish(ctx, "", payload, opts...); err != nil {
		return fmt.Errorf("brokerqueue: enqueue: %w", err)
	}
	return nil
}

// Reserve leases the next available job from the queue with the configured
// visibility timeout, returning ErrNoJob if nothing is currently available
// (matching RedisStreamsQueue's ErrNoJob sentinel, since callers already
// branch on it).
func (q *BrokerQueue) Reserve(ctx context.Context) (*queue.Job, error) {
	msgs, err := q.store.Lease(ctx, broker.LeaseRequest{
		Queue:             q.queue,
		ConsumerID:        q.consumerID,
		ConsumerGroup:     q.consumerGroup,
		Limit:             1,
		VisibilityTimeout: q.visibility,
	})
	if err != nil {
		if errors.Is(err, broker.ErrQueueNotFound) {
			return nil, ErrNoJob
		}
		return nil, fmt.Errorf("brokerqueue: reserve: %w", err)
	}
	if len(msgs) == 0 {
		return nil, ErrNoJob
	}
	m := msgs[0]

	var j queue.Job
	if err := json.Unmarshal(m.Payload, &j); err != nil {
		return nil, fmt.Errorf("brokerqueue: decode job payload: %w", err)
	}
	if j.Meta == nil {
		j.Meta = map[string]string{}
	}
	// Preserve the application-level job ID (assigned at Enqueue time) but
	// key Ack/Nack off broker's own message ID, since that's what the store
	// needs to look up the lease.
	j.Meta["_job_id"] = j.ID
	j.ID = m.ID
	j.Attempts = m.Attempts
	j.State = queue.JobReserved

	q.leasesMu.Lock()
	q.leases[m.ID] = leaseInfo{token: m.LeaseToken, consumerID: q.consumerID}
	q.leasesMu.Unlock()

	return &j, nil
}

func (q *BrokerQueue) Ack(ctx context.Context, id string) error {
	li, ok := q.takeLease(id)
	if !ok {
		return fmt.Errorf("brokerqueue: ack: no known lease for message %q (already acked, expired, or from a different process)", id)
	}
	if err := q.store.Ack(ctx, broker.AckRequest{
		MessageID:  id,
		Queue:      q.queue,
		LeaseToken: li.token,
		ConsumerID: li.consumerID,
	}); err != nil {
		return fmt.Errorf("brokerqueue: ack: %w", err)
	}
	return nil
}

func (q *BrokerQueue) Nack(ctx context.Context, j *queue.Job, e error, delay time.Duration) error {
	if j == nil {
		return nil
	}
	li, ok := q.takeLease(j.ID)
	if !ok {
		return fmt.Errorf("brokerqueue: nack: no known lease for message %q (already acked, expired, or from a different process)", j.ID)
	}
	action := broker.NackRetry
	if j.MaxAttempts > 0 && j.Attempts >= j.MaxAttempts {
		action = broker.NackDLQ
	}
	msg := ""
	if e != nil {
		msg = e.Error()
	}
	if err := q.store.Nack(ctx, broker.NackRequest{
		MessageID:  j.ID,
		Queue:      q.queue,
		LeaseToken: li.token,
		ConsumerID: li.consumerID,
		Action:     action,
		Reason:     msg,
		Error:      msg,
		Delay:      delay,
	}); err != nil {
		return fmt.Errorf("brokerqueue: nack: %w", err)
	}
	return nil
}

func (q *BrokerQueue) takeLease(id string) (leaseInfo, bool) {
	q.leasesMu.Lock()
	defer q.leasesMu.Unlock()
	li, ok := q.leases[id]
	if ok {
		delete(q.leases, id)
	}
	return li, ok
}
