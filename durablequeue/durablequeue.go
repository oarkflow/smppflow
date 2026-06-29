package durablequeue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/smppflow/queue"
)

type Backend interface {
	Enqueue(context.Context, queue.Job) error
	Reserve(context.Context) (*queue.Job, error)
	Ack(context.Context, string) error
	Nack(context.Context, *queue.Job, error, time.Duration) error
}

type SQLQueue struct {
	DB       *sql.DB
	Table    string
	WorkerID string
}

func (q SQLQueue) table() string {
	if q.Table == "" {
		return "smpp_jobs"
	}
	return q.Table
}
func (q SQLQueue) Migrate(ctx context.Context) error {
	_, err := q.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+q.table()+` (id TEXT PRIMARY KEY, type TEXT NOT NULL, payload JSONB, priority INT NOT NULL DEFAULT 0, run_at TIMESTAMPTZ NOT NULL, attempts INT NOT NULL DEFAULT 0, max_attempts INT NOT NULL DEFAULT 3, last_error TEXT, state TEXT NOT NULL, locked_by TEXT, locked_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL, meta JSONB)`)
	return err
}
func (q SQLQueue) Enqueue(ctx context.Context, j queue.Job) error {
	if q.DB == nil {
		return errors.New("sql db required")
	}
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
	return err
}
func (q SQLQueue) Reserve(ctx context.Context) (*queue.Job, error) {
	if q.DB == nil {
		return nil, errors.New("sql db required")
	}
	tx, err := q.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `SELECT id,type,payload,priority,run_at,attempts,max_attempts,last_error,state,created_at,updated_at,COALESCE(meta,'{}') FROM `+q.table()+` WHERE state=$1 AND run_at<=NOW() ORDER BY priority DESC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED`, queue.JobQueued)
	var j queue.Job
	var p, m string
	if err := row.Scan(&j.ID, &j.Type, &p, &j.Priority, &j.RunAt, &j.Attempts, &j.MaxAttempts, &j.LastError, &j.State, &j.CreatedAt, &j.UpdatedAt, &m); err != nil {
		return nil, err
	}
	j.Attempts++
	worker := q.WorkerID
	if worker == "" {
		worker = "worker"
	}
	_, err = tx.ExecContext(ctx, `UPDATE `+q.table()+` SET state=$1, attempts=$2, locked_by=$3, locked_at=NOW(), updated_at=NOW() WHERE id=$4`, queue.JobReserved, j.Attempts, worker, j.ID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(p), &j.Payload)
	_ = json.Unmarshal([]byte(m), &j.Meta)
	j.State = queue.JobReserved
	return &j, nil
}
func (q SQLQueue) Ack(ctx context.Context, id string) error {
	_, err := q.DB.ExecContext(ctx, `UPDATE `+q.table()+` SET state=$1, updated_at=NOW() WHERE id=$2`, queue.JobDone, id)
	return err
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
	return err
}

type RedisStreamsQueue struct {
	Addr, Password, Stream, Group, Consumer string
	DialTimeout                             time.Duration
}

func (r RedisStreamsQueue) Enqueue(ctx context.Context, j queue.Job) error {
	if j.ID == "" {
		j.ID = time.Now().Format("20060102150405.000000000")
	}
	b, _ := json.Marshal(j)
	_, err := r.cmd(ctx, "XADD", r.stream(), "*", "job", string(b))
	return err
}
func (r RedisStreamsQueue) Reserve(ctx context.Context) (*queue.Job, error) {
	if r.Group == "" {
		return nil, errors.New("redis group required")
	}
	consumer := r.Consumer
	if consumer == "" {
		consumer = "consumer"
	}
	out, err := r.cmd(ctx, "XREADGROUP", "GROUP", r.Group, consumer, "COUNT", "1", "BLOCK", "1000", "STREAMS", r.stream(), ">")
	if err != nil {
		return nil, err
	}
	idx := strings.Index(out, "{\"ID\"")
	if idx < 0 {
		idx = strings.Index(out, "{\"id\"")
	}
	if idx < 0 {
		return nil, errors.New("no redis stream job in response")
	}
	end := strings.LastIndex(out, "}")
	var j queue.Job
	if err := json.Unmarshal([]byte(out[idx:end+1]), &j); err != nil {
		return nil, err
	}
	j.State = queue.JobReserved
	return &j, nil
}
func (r RedisStreamsQueue) Ack(ctx context.Context, id string) error {
	_, err := r.cmd(ctx, "XACK", r.stream(), r.Group, id)
	return err
}
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
	return r.Enqueue(ctx, *j)
}
func (r RedisStreamsQueue) stream() string {
	if r.Stream == "" {
		return "smpp:jobs"
	}
	return r.Stream
}
func (r RedisStreamsQueue) cmd(ctx context.Context, args ...string) (string, error) {
	addr := r.Addr
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	d := net.Dialer{Timeout: r.DialTimeout}
	if d.Timeout == 0 {
		d.Timeout = 3 * time.Second
	}
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return "", err
	}
	defer c.Close()
	if r.Password != "" {
		if _, err := writeRESP(c, []string{"AUTH", r.Password}); err != nil {
			return "", err
		}
		if _, err := readRESP(c); err != nil {
			return "", err
		}
	}
	if _, err := writeRESP(c, args); err != nil {
		return "", err
	}
	return readRESP(c)
}
func writeRESP(c net.Conn, args []string) (int, error) {
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		b.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	return c.Write([]byte(b.String()))
}
func readRESP(c net.Conn) (string, error) {
	buf := make([]byte, 65536)
	n, err := c.Read(buf)
	if err != nil {
		return "", err
	}
	s := string(buf[:n])
	if strings.HasPrefix(s, "-") {
		return s, errors.New(strings.TrimSpace(s))
	}
	return s, nil
}

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
func NATSJetStream(name string, funcs ExternalBackend) ExternalBackend {
	funcs.Name = "nats-jetstream:" + name
	return funcs
}
func Kafka(name string, funcs ExternalBackend) ExternalBackend {
	funcs.Name = "kafka:" + name
	return funcs
}
func RabbitMQ(name string, funcs ExternalBackend) ExternalBackend {
	funcs.Name = "rabbitmq:" + name
	return funcs
}
