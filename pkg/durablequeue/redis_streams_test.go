package durablequeue

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/queue"
)

type fakeStreamEntry struct {
	id     string
	fields []string
}

type fakeStreamConsumerGroup struct {
	lastID  string
	pending map[string]string
}

type fakeStreamServer struct {
	ln          net.Listener
	requireAuth string

	mu      sync.Mutex
	streams map[string][]fakeStreamEntry
	groups  map[string]*fakeStreamConsumerGroup
	seq     int
	wake    chan struct{}
}

func newFakeStreamServer(t *testing.T, requireAuth string) *fakeStreamServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeStreamServer{
		ln:          ln,
		requireAuth: requireAuth,
		streams:     map[string][]fakeStreamEntry{},
		groups:      map[string]*fakeStreamConsumerGroup{},
		wake:        make(chan struct{}),
	}
	go s.acceptLoop()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *fakeStreamServer) Addr() string { return s.ln.Addr().String() }

func (s *fakeStreamServer) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *fakeStreamServer) handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	authed := s.requireAuth == ""
	for {
		args, err := readFakeCommand(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		if !authed {
			if strings.ToUpper(args[0]) != "AUTH" || len(args) != 2 || args[1] != s.requireAuth {
				_, _ = conn.Write([]byte("-ERR invalid password\r\n"))
				continue
			}
			authed = true
			_, _ = conn.Write([]byte("+OK\r\n"))
			continue
		}
		_, _ = conn.Write(s.dispatch(args))
	}
}

func readFakeCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("fake redis: expected array, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		head, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		head = strings.TrimRight(head, "\r\n")
		if !strings.HasPrefix(head, "$") {
			return nil, fmt.Errorf("fake redis: expected bulk, got %q", head)
		}
		size, err := strconv.Atoi(head[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (s *fakeStreamServer) dispatch(args []string) []byte {
	switch strings.ToUpper(args[0]) {
	case "PING":
		return []byte("+PONG\r\n")
	case "XADD":
		return s.handleXAdd(args)
	case "XGROUP":
		return s.handleXGroup(args)
	case "XREADGROUP":
		return s.handleXReadGroup(args)
	case "XACK":
		return s.handleXAck(args)
	default:
		return []byte("-ERR unknown command '" + args[0] + "'\r\n")
	}
}

func (s *fakeStreamServer) handleXAdd(args []string) []byte {
	if len(args) < 5 || len(args[3:])%2 != 0 {
		return []byte("-ERR wrong number of arguments for 'xadd'\r\n")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	id := fmt.Sprintf("%d-%d", time.Now().UnixMilli(), s.seq)
	s.streams[args[1]] = append(s.streams[args[1]], fakeStreamEntry{id: id, fields: append([]string(nil), args[3:]...)})
	wake := s.wake
	s.wake = make(chan struct{})
	close(wake)
	return bulkReply(id)
}

func (s *fakeStreamServer) handleXGroup(args []string) []byte {
	if len(args) < 5 || strings.ToUpper(args[1]) != "CREATE" {
		return []byte("-ERR unsupported xgroup subcommand\r\n")
	}
	key, group := args[2], args[3]
	s.mu.Lock()
	defer s.mu.Unlock()
	gk := key + "\x00" + group
	if _, ok := s.groups[gk]; ok {
		return []byte("-BUSYGROUP Consumer Group name already exists\r\n")
	}
	start := args[4]
	if start == "$" && len(s.streams[key]) > 0 {
		start = s.streams[key][len(s.streams[key])-1].id
	}
	s.groups[gk] = &fakeStreamConsumerGroup{lastID: start, pending: map[string]string{}}
	return []byte("+OK\r\n")
}

func (s *fakeStreamServer) handleXReadGroup(args []string) []byte {
	group, consumer := "", ""
	count, block := 1, 0
	stream, want := "", ">"
	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "GROUP":
			group, consumer = args[i+1], args[i+2]
			i += 2
		case "COUNT":
			count, _ = strconv.Atoi(args[i+1])
			i++
		case "BLOCK":
			block, _ = strconv.Atoi(args[i+1])
			i++
		case "STREAMS":
			stream, want = args[i+1], args[i+2]
			i += 2
		}
	}
	s.mu.Lock()
	gk := stream + "\x00" + group
	g, ok := s.groups[gk]
	if !ok {
		s.mu.Unlock()
		return []byte("-NOGROUP the consumer group '" + group + "' doesn't exist\r\n")
	}
	deadline := time.Now().Add(time.Duration(block) * time.Millisecond)
	for {
		var found []fakeStreamEntry
		if want == ">" {
			for _, e := range s.streams[stream] {
				if compareStreamIDs(e.id, g.lastID) > 0 {
					found = append(found, e)
				}
			}
		} else {
			for _, e := range s.streams[stream] {
				if _, pending := g.pending[e.id]; pending {
					found = append(found, e)
				}
			}
		}
		if len(found) > 0 || block <= 0 || !time.Now().Before(deadline) {
			if len(found) > count {
				found = found[:count]
			}
			for _, e := range found {
				g.pending[e.id] = consumer
				if want == ">" && compareStreamIDs(e.id, g.lastID) > 0 {
					g.lastID = e.id
				}
			}
			s.mu.Unlock()
			if len(found) == 0 {
				return []byte("*-1\r\n")
			}
			return streamReadReply(stream, found)
		}
		wake := s.wake
		s.mu.Unlock()
		wait := time.Until(deadline)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
		s.mu.Lock()
	}
}

func streamReadReply(stream string, entries []fakeStreamEntry) []byte {
	var b strings.Builder
	b.WriteString("*1\r\n*2\r\n")
	b.WriteString(bulkString(stream))
	b.WriteString(fmt.Sprintf("*%d\r\n", len(entries)))
	for _, e := range entries {
		b.WriteString("*2\r\n")
		b.WriteString(bulkString(e.id))
		b.WriteString(fmt.Sprintf("*%d\r\n", len(e.fields)))
		for _, f := range e.fields {
			b.WriteString(bulkString(f))
		}
	}
	return []byte(b.String())
}

func (s *fakeStreamServer) handleXAck(args []string) []byte {
	if len(args) != 4 {
		return []byte("-ERR wrong number of arguments for 'xack'\r\n")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[args[1]+"\x00"+args[2]]
	if !ok {
		return []byte(":0\r\n")
	}
	if _, pending := g.pending[args[3]]; pending {
		delete(g.pending, args[3])
		return []byte(":1\r\n")
	}
	return []byte(":0\r\n")
}

func bulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func bulkReply(s string) []byte { return []byte(bulkString(s)) }

func compareStreamIDs(a, b string) int {
	parse := func(id string) (int64, int64) {
		parts := strings.SplitN(id, "-", 2)
		ms, _ := strconv.ParseInt(parts[0], 10, 64)
		var seq int64
		if len(parts) == 2 {
			seq, _ = strconv.ParseInt(parts[1], 10, 64)
		}
		return ms, seq
	}
	ams, aseq := parse(a)
	bms, bseq := parse(b)
	switch {
	case ams != bms:
		if ams < bms {
			return -1
		}
		return 1
	case aseq != bseq:
		if aseq < bseq {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func newTestRedisQueue(t *testing.T, srv *fakeStreamServer) RedisStreamsQueue {
	t.Helper()
	return RedisStreamsQueue{
		Addr:     srv.Addr(),
		Password: srv.requireAuth,
		Stream:   "test:jobs",
		Group:    "test-group",
		Consumer: "test-consumer",
	}
}

func TestRedisStreamsQueueEnqueueReserveAck(t *testing.T) {
	srv := newFakeStreamServer(t, "")
	q := newTestRedisQueue(t, srv)
	ctx := context.Background()
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	job := queue.Job{ID: "job-1", Type: "smpp.send", Meta: map[string]string{"k": "v"}}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if got.Type != job.Type {
		t.Fatalf("Type=%q want %q", got.Type, job.Type)
	}
	if got.Meta["k"] != "v" {
		t.Fatalf("Meta=%v want k=v", got.Meta)
	}
	if got.Meta["_job_id"] != "job-1" {
		t.Fatalf("_job_id=%q want job-1", got.Meta["_job_id"])
	}
	if got.State != queue.JobReserved {
		t.Fatalf("State=%v want reserved", got.State)
	}
	if got.ID == "job-1" {
		t.Fatalf("ID must be the stream entry id, got %q", got.ID)
	}
	if err := q.Ack(ctx, got.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestRedisStreamsQueueEnqueueGeneratesID(t *testing.T) {
	srv := newFakeStreamServer(t, "")
	q := newTestRedisQueue(t, srv)
	ctx := context.Background()
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, queue.Job{Type: "t"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if got.Meta["_job_id"] == "" {
		t.Fatal("Enqueue must assign an ID when the job has none")
	}
}

func TestRedisStreamsQueueNackRequeuesAndAcksOriginal(t *testing.T) {
	srv := newFakeStreamServer(t, "")
	q := newTestRedisQueue(t, srv)
	ctx := context.Background()
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, queue.Job{ID: "job-2", Type: "smpp.send"}); err != nil {
		t.Fatal(err)
	}
	got, err := q.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(ctx, got, errors.New("boom"), 0); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	again, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("Reserve after Nack: %v", err)
	}
	if again.Meta["_job_id"] != "job-2" {
		t.Fatalf("_job_id=%q want job-2 (Nack must restore the job id)", again.Meta["_job_id"])
	}
	if again.LastError != "boom" {
		t.Fatalf("LastError=%q want boom", again.LastError)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	g := srv.groups["test:jobs\x00test-group"]
	if len(g.pending) != 1 {
		t.Fatalf("pending=%d want 1 (only the re-enqueued entry)", len(g.pending))
	}
}

func TestRedisStreamsQueueReserveErrNoJobWhenEmpty(t *testing.T) {
	srv := newFakeStreamServer(t, "")
	q := newTestRedisQueue(t, srv)
	ctx := context.Background()
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Reserve(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("Reserve on empty stream: err=%v want ErrNoJob", err)
	}
}

func TestRedisStreamsQueueReserveRequiresGroup(t *testing.T) {
	srv := newFakeStreamServer(t, "")
	ctx := context.Background()
	q := newTestRedisQueue(t, srv)
	q.Group = ""
	if _, err := q.Reserve(ctx); err == nil {
		t.Fatal("Reserve without a group must fail")
	}
	if err := q.EnsureGroup(ctx); err == nil {
		t.Fatal("EnsureGroup without a group must fail")
	}
	q.Group = "missing-group"
	if _, err := q.Reserve(ctx); err == nil || !strings.Contains(err.Error(), "NOGROUP") {
		t.Fatalf("Reserve with unknown group: err=%v want NOGROUP", err)
	}
}

func TestRedisStreamsQueueEnsureGroupIdempotent(t *testing.T) {
	srv := newFakeStreamServer(t, "")
	q := newTestRedisQueue(t, srv)
	ctx := context.Background()
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatalf("first EnsureGroup: %v", err)
	}
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatalf("second EnsureGroup must tolerate BUSYGROUP, got %v", err)
	}
}

func TestRedisStreamsQueueAuth(t *testing.T) {
	srv := newFakeStreamServer(t, "hunter2")
	q := newTestRedisQueue(t, srv)
	ctx := context.Background()
	if err := q.Enqueue(ctx, queue.Job{ID: "job-auth", Type: "t"}); err != nil {
		t.Fatalf("Enqueue with password: %v", err)
	}
	bad := q
	bad.Password = "wrong"
	if err := bad.Enqueue(ctx, queue.Job{ID: "job-auth-2", Type: "t"}); err == nil {
		t.Fatal("Enqueue with wrong password must fail")
	}
}

func TestRedisStreamsQueueShutdown(t *testing.T) {
	srv := newFakeStreamServer(t, "")
	q := newTestRedisQueue(t, srv)
	ctx := context.Background()
	if err := q.Enqueue(ctx, queue.Job{ID: "job-s", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestRedisStreamsQueuePipelineRoundTrip(t *testing.T) {
	srv := newFakeStreamServer(t, "")
	q := newTestRedisQueue(t, srv)
	ctx := context.Background()
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		if err := q.Enqueue(ctx, queue.Job{ID: strconv.Itoa(i), Type: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		got, err := q.Reserve(ctx)
		if err != nil {
			t.Fatalf("Reserve %d: %v", i, err)
		}
		seen[got.Meta["_job_id"]] = true
		if err := q.Ack(ctx, got.ID); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		if !seen[strconv.Itoa(i)] {
			t.Fatalf("job %d was never reserved", i)
		}
	}
}

func TestRedisStreamsQueueRealRedis(t *testing.T) {
	addr := os.Getenv("SMPPFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set SMPPFLOW_TEST_REDIS_ADDR=127.0.0.1:6379 to run against a real Redis")
	}
	q := RedisStreamsQueue{
		Addr:     addr,
		Password: os.Getenv("SMPPFLOW_TEST_REDIS_PASSWORD"),
		Stream:   "smppflow:test:" + strconv.FormatInt(time.Now().UnixNano(), 10),
		Group:    "test-group",
		Consumer: "test-consumer",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup must be idempotent: %v", err)
	}
	if err := q.Enqueue(ctx, queue.Job{ID: "real-1", Type: "smpp.send", Meta: map[string]string{"env": "test"}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if got.Meta["_job_id"] != "real-1" || got.Type != "smpp.send" {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
	if err := q.Nack(ctx, got, errors.New("retry me"), time.Millisecond); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	again, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("Reserve after Nack: %v", err)
	}
	if again.Meta["_job_id"] != "real-1" || again.LastError != "retry me" {
		t.Fatalf("requeued job mismatch: %+v", again)
	}
	if err := q.Ack(ctx, again.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if _, err := q.Reserve(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("Reserve on empty: %v want ErrNoJob", err)
	}
	_ = q.Shutdown(ctx)
}
