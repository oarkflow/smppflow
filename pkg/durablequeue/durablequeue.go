// Package durablequeue provides Backend implementations of the queue.Backend
// contract that persist jobs outside process memory: a generic SQL-backed
// queue (database/sql), a Redis Streams consumer-group queue built on a
// minimal hand-rolled RESP client, and a set of "bring your own client"
// adapters for external brokers (Kafka, RabbitMQ, NATS JetStream).
package durablequeue

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/queue"
)

type Backend interface {
	Enqueue(context.Context, queue.Job) error
	Reserve(context.Context) (*queue.Job, error)
	Ack(context.Context, string) error
	Nack(context.Context, *queue.Job, error, time.Duration) error
}

// SQLQueue is a generic database/sql-backed Backend. Pool sizing on the
// injected *sql.DB is applied once per DB handle (see ensurePool) using
// MaxOpenConns/MaxIdleConns/ConnMaxLifetime/ConnMaxIdleTime if set, or
// sensible defaults otherwise. Leave those fields zero to use the defaults.
type SQLQueue struct {
	DB       *sql.DB
	Table    string
	WorkerID string

	// Pool tuning, applied once per *sql.DB the first time this queue is
	// used (Migrate/Enqueue/Reserve). Zero values fall back to defaults of
	// MaxOpenConns=50, MaxIdleConns=10, ConnMaxLifetime=30m,
	// ConnMaxIdleTime=5m.
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// sqlPoolConfigured tracks which *sql.DB handles have already had pool
// limits applied, so sharing one *sql.DB across multiple SQLQueue values
// (e.g. different tables) only configures it once — by whichever SQLQueue
// touches it first.
var sqlPoolConfigured sync.Map // *sql.DB -> struct{}

func (q SQLQueue) ensurePool() {
	if q.DB == nil {
		return
	}
	if _, loaded := sqlPoolConfigured.LoadOrStore(q.DB, struct{}{}); loaded {
		return
	}
	maxOpen := q.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 50
	}
	maxIdle := q.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 10
	}
	lifetime := q.ConnMaxLifetime
	if lifetime <= 0 {
		lifetime = 30 * time.Minute
	}
	idleTime := q.ConnMaxIdleTime
	if idleTime <= 0 {
		idleTime = 5 * time.Minute
	}
	q.DB.SetMaxOpenConns(maxOpen)
	q.DB.SetMaxIdleConns(maxIdle)
	q.DB.SetConnMaxLifetime(lifetime)
	q.DB.SetConnMaxIdleTime(idleTime)
}

func (q SQLQueue) table() string {
	if q.Table == "" {
		return "smpp_jobs"
	}
	return q.Table
}
func (q SQLQueue) Migrate(ctx context.Context) error {
	q.ensurePool()
	_, err := q.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+q.table()+` (id TEXT PRIMARY KEY, type TEXT NOT NULL, payload JSONB, priority INT NOT NULL DEFAULT 0, run_at TIMESTAMPTZ NOT NULL, attempts INT NOT NULL DEFAULT 0, max_attempts INT NOT NULL DEFAULT 3, last_error TEXT, state TEXT NOT NULL, locked_by TEXT, locked_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL, meta JSONB)`)
	return err
}
func (q SQLQueue) Enqueue(ctx context.Context, j queue.Job) error {
	if q.DB == nil {
		return errors.New("sql db required")
	}
	q.ensurePool()
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
	p, _ := json.Marshal(j.Payload)
	m, _ := json.Marshal(j.Meta)
	_, err := q.DB.ExecContext(ctx, `INSERT INTO `+q.table()+` (id,type,payload,priority,run_at,attempts,max_attempts,last_error,state,created_at,updated_at,meta) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, j.ID, j.Type, string(p), j.Priority, j.RunAt, j.Attempts, j.MaxAttempts, j.LastError, queue.JobQueued, j.CreatedAt, time.Now(), string(m))
	if err != nil {
		return fmt.Errorf("sqlqueue: enqueue: %w", err)
	}
	return nil
}
func (q SQLQueue) Reserve(ctx context.Context) (*queue.Job, error) {
	if q.DB == nil {
		return nil, errors.New("sql db required")
	}
	q.ensurePool()
	tx, err := q.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("sqlqueue: begin tx: %w", err)
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `SELECT id,type,payload,priority,run_at,attempts,max_attempts,last_error,state,created_at,updated_at,COALESCE(meta,'{}') FROM `+q.table()+` WHERE state=$1 AND run_at<=NOW() ORDER BY priority DESC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED`, queue.JobQueued)
	var j queue.Job
	var p, m string
	if err := row.Scan(&j.ID, &j.Type, &p, &j.Priority, &j.RunAt, &j.Attempts, &j.MaxAttempts, &j.LastError, &j.State, &j.CreatedAt, &j.UpdatedAt, &m); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		return nil, fmt.Errorf("sqlqueue: reserve scan: %w", err)
	}
	j.Attempts++
	worker := q.WorkerID
	if worker == "" {
		worker = "worker"
	}
	_, err = tx.ExecContext(ctx, `UPDATE `+q.table()+` SET state=$1, attempts=$2, locked_by=$3, locked_at=NOW(), updated_at=NOW() WHERE id=$4`, queue.JobReserved, j.Attempts, worker, j.ID)
	if err != nil {
		return nil, fmt.Errorf("sqlqueue: reserve lock: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlqueue: reserve commit: %w", err)
	}
	_ = json.Unmarshal([]byte(p), &j.Payload)
	_ = json.Unmarshal([]byte(m), &j.Meta)
	j.State = queue.JobReserved
	return &j, nil
}
func (q SQLQueue) Ack(ctx context.Context, id string) error {
	_, err := q.DB.ExecContext(ctx, `UPDATE `+q.table()+` SET state=$1, updated_at=NOW() WHERE id=$2`, queue.JobDone, id)
	if err != nil {
		return fmt.Errorf("sqlqueue: ack: %w", err)
	}
	return nil
}
func (q SQLQueue) Nack(ctx context.Context, j *queue.Job, e error, delay time.Duration) error {
	if j == nil {
		return nil
	}
	state := queue.JobQueued
	if j.Attempts >= j.MaxAttempts {
		state = queue.JobDeadLettered
	}
	msg := ""
	if e != nil {
		msg = e.Error()
	}
	_, err := q.DB.ExecContext(ctx, `UPDATE `+q.table()+` SET state=$1,last_error=$2,run_at=$3,updated_at=NOW() WHERE id=$4`, state, msg, time.Now().Add(delay), j.ID)
	if err != nil {
		return fmt.Errorf("sqlqueue: nack: %w", err)
	}
	return nil
}

// ErrNoJob is returned by RedisStreamsQueue.Reserve when XREADGROUP's BLOCK
// window elapses with no new stream entries, and by BrokerQueue.Reserve when
// nothing is currently leasable. It is an alias of queue.ErrNoJob so worker
// loops built against pkg/queue.Backend can check for it with a single
// errors.Is regardless of which backend is in use.
var ErrNoJob = queue.ErrNoJob

// RedisStreamsQueue is a Backend built on Redis Streams consumer groups,
// using a small hand-rolled RESP2 client (no external Redis driver). It is
// intended for moderate throughput and relies on Redis itself for
// durability; it does not implement the full Redis protocol (e.g. RESP3,
// cluster redirects, TLS) — only what XADD/XACK/XREADGROUP/AUTH need.
//
// Connections are pooled per (Addr, Password) pair and retried with
// exponential backoff on network errors, so concurrent Enqueue/Reserve/Ack
// calls do not serialize through a single connection or pay a fresh
// TCP+AUTH round trip on every command.
type RedisStreamsQueue struct {
	Addr, Password, Stream, Group, Consumer string
	DialTimeout                             time.Duration
	// CommandTimeout bounds each individual command's read/write deadline
	// when ctx has no deadline of its own. Defaults to 10s (or, for
	// Reserve's blocking XREADGROUP, BLOCK time + 5s).
	CommandTimeout time.Duration
	// MaxIdleConns caps pooled idle connections per (Addr,Password). Default 8.
	MaxIdleConns int
	// MaxRetries caps retry attempts on network errors. Default 3.
	MaxRetries int
}

func (r RedisStreamsQueue) Enqueue(ctx context.Context, j queue.Job) error {
	if j.ID == "" {
		j.ID = time.Now().Format("20060102150405.000000000")
	}
	b, _ := json.Marshal(j)
	if _, err := r.cmd(ctx, "XADD", r.stream(), "*", "job", string(b)); err != nil {
		return fmt.Errorf("redisqueue: xadd: %w", err)
	}
	return nil
}

func (r RedisStreamsQueue) Reserve(ctx context.Context) (*queue.Job, error) {
	if r.Group == "" {
		return nil, errors.New("redis group required")
	}
	consumer := r.Consumer
	if consumer == "" {
		consumer = "consumer"
	}
	reply, err := r.cmd(ctx, "XREADGROUP", "GROUP", r.Group, consumer, "COUNT", "1", "BLOCK", "1000", "STREAMS", r.stream(), ">")
	if err != nil {
		return nil, fmt.Errorf("redisqueue: xreadgroup: %w", err)
	}
	entryID, payload, err := firstStreamEntry(reply)
	if err != nil {
		return nil, err
	}
	if entryID == "" {
		return nil, ErrNoJob
	}
	var j queue.Job
	if err := json.Unmarshal([]byte(payload), &j); err != nil {
		return nil, fmt.Errorf("redisqueue: decode job payload: %w", err)
	}
	if j.Meta == nil {
		j.Meta = map[string]string{}
	}
	// Preserve the job's own ID (assigned at Enqueue time) but ack against
	// the stream entry ID, since XACK requires the entry ID rather than an
	// application-level job ID.
	j.Meta["_job_id"] = j.ID
	j.ID = entryID
	j.State = queue.JobReserved
	return &j, nil
}

// firstStreamEntry extracts the (entry ID, "job" field payload) of the
// first entry of the first stream in an XREADGROUP/XREAD reply, whose shape
// is: [ [streamName, [ [entryID, [field,val,field,val,...]], ... ] ], ... ].
// Returns ("", "", nil) when reply is a nil/empty result (BLOCK timeout).
func firstStreamEntry(reply any) (id string, payload string, err error) {
	if reply == nil {
		return "", "", nil
	}
	streams, ok := reply.([]any)
	if !ok || len(streams) == 0 {
		return "", "", nil
	}
	stream0, ok := streams[0].([]any)
	if !ok || len(stream0) != 2 {
		return "", "", errors.New("redisqueue: unexpected xreadgroup reply shape")
	}
	entries, ok := stream0[1].([]any)
	if !ok || len(entries) == 0 {
		return "", "", nil
	}
	entry0, ok := entries[0].([]any)
	if !ok || len(entry0) != 2 {
		return "", "", errors.New("redisqueue: unexpected stream entry shape")
	}
	entryID, _ := entry0[0].(string)
	fields, ok := entry0[1].([]any)
	if !ok {
		return "", "", errors.New("redisqueue: unexpected stream entry fields shape")
	}
	for i := 0; i+1 < len(fields); i += 2 {
		key, _ := fields[i].(string)
		if key == "job" {
			val, _ := fields[i+1].(string)
			return entryID, val, nil
		}
	}
	return "", "", errors.New("redisqueue: job field missing in stream entry")
}

func (r RedisStreamsQueue) Ack(ctx context.Context, id string) error {
	if _, err := r.cmd(ctx, "XACK", r.stream(), r.Group, id); err != nil {
		return fmt.Errorf("redisqueue: xack: %w", err)
	}
	return nil
}

// Nack re-queues the job (optionally delayed) and, when it was delivered
// via a consumer group, acknowledges the original stream entry so it does
// not remain forever pending. Both commands are sent as a single pipelined
// round trip instead of two separate ones.
func (r RedisStreamsQueue) Nack(ctx context.Context, j *queue.Job, e error, delay time.Duration) error {
	if j == nil {
		return nil
	}
	if e != nil {
		j.LastError = e.Error()
	}
	if delay > 0 {
		j.RunAt = time.Now().Add(delay)
	}
	origEntryID := j.ID
	if id, ok := j.Meta["_job_id"]; ok && id != "" {
		j.ID = id
	}
	b, _ := json.Marshal(j)
	cmds := make([][]string, 0, 2)
	if r.Group != "" && origEntryID != "" {
		cmds = append(cmds, []string{"XACK", r.stream(), r.Group, origEntryID})
	}
	cmds = append(cmds, []string{"XADD", r.stream(), "*", "job", string(b)})
	if _, err := r.pipeline(ctx, cmds); err != nil {
		return fmt.Errorf("redisqueue: nack: %w", err)
	}
	return nil
}

func (r RedisStreamsQueue) stream() string {
	if r.Stream == "" {
		return "smpp:jobs"
	}
	return r.Stream
}

func (r RedisStreamsQueue) addr() string {
	if r.Addr == "" {
		return "127.0.0.1:6379"
	}
	return r.Addr
}

func (r RedisStreamsQueue) dialTimeout() time.Duration {
	if r.DialTimeout > 0 {
		return r.DialTimeout
	}
	return 3 * time.Second
}

func (r RedisStreamsQueue) commandTimeout() time.Duration {
	if r.CommandTimeout > 0 {
		return r.CommandTimeout
	}
	return 10 * time.Second
}

func (r RedisStreamsQueue) maxRetries() int {
	if r.MaxRetries > 0 {
		return r.MaxRetries
	}
	return 3
}

func (r RedisStreamsQueue) pool() *redisConnPool {
	return getRedisPool(r.addr(), r.Password, r.dialTimeout(), r.MaxIdleConns)
}

// cmd sends a single command and returns its parsed reply, retrying on
// network errors with exponential backoff.
func (r RedisStreamsQueue) cmd(ctx context.Context, args ...string) (any, error) {
	replies, err := r.pipeline(ctx, [][]string{args})
	if err != nil {
		return nil, err
	}
	if len(replies) == 0 {
		return nil, nil
	}
	return replies[0], nil
}

// pipeline sends multiple commands back-to-back over a single connection
// (one write syscall) and reads their replies in order (Redis pipelining),
// retrying the whole batch on network errors with exponential backoff.
func (r RedisStreamsQueue) pipeline(ctx context.Context, cmds [][]string) ([]any, error) {
	pool := r.pool()
	backoff := 50 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt <= r.maxRetries(); attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
		}
		conn, err := pool.get(ctx)
		if err != nil {
			lastErr = fmt.Errorf("dial: %w", err)
			continue
		}
		deadline := time.Now().Add(r.commandTimeout())
		if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
			deadline = dl
		}
		_ = conn.SetDeadline(deadline)
		replies, err := doPipeline(conn, cmds)
		if err != nil {
			pool.discard(conn)
			if isRetryable(err) {
				lastErr = err
				continue
			}
			return nil, err
		}
		_ = conn.SetDeadline(time.Time{})
		pool.put(conn)
		return replies, nil
	}
	return nil, fmt.Errorf("giving up after %d attempts: %w", r.maxRetries()+1, lastErr)
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// doPipeline writes all commands then reads all replies (including any
// RESP error reply, which is returned as an *error*, matching Redis
// pipelining semantics: an error on one command doesn't stop replies for
// the others from being read).
func doPipeline(c *pooledConn, cmds [][]string) ([]any, error) {
	var buf strings.Builder
	for _, args := range cmds {
		writeCommand(&buf, args)
	}
	if _, err := io.WriteString(c.conn, buf.String()); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	out := make([]any, len(cmds))
	for i := range cmds {
		v, err := readReply(c.r)
		if err != nil {
			return nil, fmt.Errorf("read reply %d/%d: %w", i+1, len(cmds), err)
		}
		out[i] = v
	}
	return out, nil
}

func writeCommand(b *strings.Builder, args []string) {
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		b.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
}

// readReply parses one RESP2 value (simple string, error, integer, bulk
// string or array — recursively for nested arrays), which is all the
// XADD/XACK/XREADGROUP/AUTH commands used here ever return.
func readReply(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, errors.New("redis: empty reply line")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, errors.New("redis: " + line[1:])
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("redis: bad integer reply: %w", err)
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("redis: bad bulk length: %w", err)
		}
		if n < 0 {
			return nil, nil // null bulk string
		}
		buf := make([]byte, n+2) // payload + trailing \r\n
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("redis: read bulk: %w", err)
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("redis: bad array length: %w", err)
		}
		if n < 0 {
			return nil, nil // null array
		}
		arr := make([]any, n)
		for i := 0; i < n; i++ {
			v, err := readReply(r)
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("redis: unknown reply type %q", line[0])
	}
}

// pooledConn pairs a net.Conn with a persistent bufio.Reader so buffered
// bytes survive across pooled reuse.
type pooledConn struct {
	conn net.Conn
	r    *bufio.Reader
}

func (c *pooledConn) SetDeadline(t time.Time) error { return c.conn.SetDeadline(t) }
func (c *pooledConn) Close() error                  { return c.conn.Close() }

// redisConnPool is a small per-(addr,password) idle connection pool so
// concurrent callers don't serialize through one TCP connection, and don't
// pay a fresh dial+AUTH round trip on every command.
type redisConnPool struct {
	addr, password string
	dialTimeout    time.Duration
	maxIdle        int

	mu   sync.Mutex
	idle []*pooledConn
}

var (
	redisPoolsMu sync.Mutex
	redisPools   = map[string]*redisConnPool{}
)

func getRedisPool(addr, password string, dialTimeout time.Duration, maxIdle int) *redisConnPool {
	if maxIdle <= 0 {
		maxIdle = 8
	}
	key := addr + "\x00" + password
	redisPoolsMu.Lock()
	defer redisPoolsMu.Unlock()
	if p, ok := redisPools[key]; ok {
		return p
	}
	p := &redisConnPool{addr: addr, password: password, dialTimeout: dialTimeout, maxIdle: maxIdle}
	redisPools[key] = p
	return p
}

func (p *redisConnPool) get(ctx context.Context) (*pooledConn, error) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		c := p.idle[n-1]
		p.idle[n-1] = nil
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	return p.dial(ctx)
}

func (p *redisConnPool) dial(ctx context.Context) (*pooledConn, error) {
	d := net.Dialer{Timeout: p.dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return nil, fmt.Errorf("redis dial %s: %w", p.addr, err)
	}
	pc := &pooledConn{conn: conn, r: bufio.NewReader(conn)}
	if p.password != "" {
		_ = conn.SetDeadline(time.Now().Add(p.dialTimeout))
		if _, err := doPipeline(pc, [][]string{{"AUTH", p.password}}); err != nil {
			conn.Close()
			return nil, fmt.Errorf("redis auth: %w", err)
		}
		_ = conn.SetDeadline(time.Time{})
	}
	return pc, nil
}

func (p *redisConnPool) put(c *pooledConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idle) >= p.maxIdle {
		c.Close()
		return
	}
	p.idle = append(p.idle, c)
}

func (p *redisConnPool) discard(c *pooledConn) {
	c.Close()
}

// ExternalBackend adapts an externally-supplied client into a
// queue.Backend by delegating to caller-provided func fields. It is a thin
// name-tagging wrapper, not a protocol implementation: Kafka, RabbitMQ and
// NATSJetStream below do not talk to those brokers themselves — the caller
// must supply EnqueueFunc/ReserveFunc/AckFunc/NackFunc backed by a real
// client (e.g. a Kafka producer/consumer, an AMQP channel, a JetStream
// context). Leaving a func nil makes that operation return an error naming
// which one is unconfigured.
type ExternalBackend struct {
	Name        string
	EnqueueFunc func(context.Context, queue.Job) error
	ReserveFunc func(context.Context) (*queue.Job, error)
	AckFunc     func(context.Context, string) error
	NackFunc    func(context.Context, *queue.Job, error, time.Duration) error
}

func (e ExternalBackend) Enqueue(ctx context.Context, j queue.Job) error {
	if e.EnqueueFunc == nil {
		return fmt.Errorf("%s enqueue not configured", e.Name)
	}
	return e.EnqueueFunc(ctx, j)
}
func (e ExternalBackend) Reserve(ctx context.Context) (*queue.Job, error) {
	if e.ReserveFunc == nil {
		return nil, fmt.Errorf("%s reserve not configured", e.Name)
	}
	return e.ReserveFunc(ctx)
}
func (e ExternalBackend) Ack(ctx context.Context, id string) error {
	if e.AckFunc == nil {
		return fmt.Errorf("%s ack not configured", e.Name)
	}
	return e.AckFunc(ctx, id)
}
func (e ExternalBackend) Nack(ctx context.Context, j *queue.Job, err error, d time.Duration) error {
	if e.NackFunc == nil {
		return fmt.Errorf("%s nack not configured", e.Name)
	}
	return e.NackFunc(ctx, j, err, d)
}

// NATSJetStream tags an ExternalBackend as backed by NATS JetStream. This
// is a "bring your own client" adapter: it does not implement the NATS
// wire protocol. `funcs` must already have EnqueueFunc/ReserveFunc/AckFunc/
// NackFunc set to call into a real *nats.Conn/JetStreamContext (or similar)
// supplied by the caller.
func NATSJetStream(name string, funcs ExternalBackend) ExternalBackend {
	funcs.Name = "nats-jetstream:" + name
	return funcs
}

// Kafka tags an ExternalBackend as backed by Kafka. This is a "bring your
// own client" adapter: it does not implement the Kafka wire protocol.
// `funcs` must already have EnqueueFunc/ReserveFunc/AckFunc/NackFunc set to
// call into a real Kafka producer/consumer supplied by the caller.
func Kafka(name string, funcs ExternalBackend) ExternalBackend {
	funcs.Name = "kafka:" + name
	return funcs
}

// RabbitMQ tags an ExternalBackend as backed by RabbitMQ. This is a "bring
// your own client" adapter: it does not implement the AMQP wire protocol.
// `funcs` must already have EnqueueFunc/ReserveFunc/AckFunc/NackFunc set to
// call into a real AMQP channel supplied by the caller.
func RabbitMQ(name string, funcs ExternalBackend) ExternalBackend {
	funcs.Name = "rabbitmq:" + name
	return funcs
}
