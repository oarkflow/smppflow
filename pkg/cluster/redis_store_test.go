package cluster

// Tests for RedisStore run against a minimal, in-process fake Redis server
// (a net.Listen TCP server speaking just enough RESP2 to support SET
// [NX] [PX ms], GET, DEL and SCAN) rather than a real Redis instance, since
// one is not assumed to be available in the test environment.

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- fake Redis server ----

type fakeRedisEntry struct {
	value     string
	expiresAt time.Time // zero means no expiry
}

type fakeRedisServer struct {
	ln       net.Listener
	mu       sync.Mutex
	data     map[string]fakeRedisEntry
	wg       sync.WaitGroup
	closed   chan struct{}
	password string
}

func newFakeRedisServer(t *testing.T) *fakeRedisServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeRedisServer{ln: ln, data: map[string]fakeRedisEntry{}, closed: make(chan struct{})}
	s.wg.Add(1)
	go s.acceptLoop()
	t.Cleanup(s.Close)
	return s
}

func (s *fakeRedisServer) Addr() string { return s.ln.Addr().String() }

func (s *fakeRedisServer) Close() {
	select {
	case <-s.closed:
		return
	default:
		close(s.closed)
	}
	_ = s.ln.Close()
	s.wg.Wait()
}

func (s *fakeRedisServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *fakeRedisServer) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		args, err := readCommandArray(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		resp := s.dispatch(args)
		if _, err := conn.Write([]byte(resp)); err != nil {
			return
		}
	}
}

func readCommandArray(r *bufio.Reader) ([]string, error) {
	reply, err := redisReadReply(r)
	if err != nil {
		return nil, err
	}
	if reply.typ != '*' {
		return nil, nil
	}
	out := make([]string, len(reply.arr))
	for i, item := range reply.arr {
		out[i] = item.str
	}
	return out, nil
}

func (s *fakeRedisServer) dispatch(args []string) string {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "AUTH":
		if s.password == "" || (len(args) > 1 && args[1] == s.password) {
			return "+OK\r\n"
		}
		return "-ERR invalid password\r\n"
	case "SELECT":
		return "+OK\r\n"
	case "PING":
		return "+PONG\r\n"
	case "SET":
		return s.handleSet(args[1:])
	case "GET":
		return s.handleGet(args[1])
	case "DEL":
		return s.handleDel(args[1])
	case "SCAN":
		return s.handleScan(args[1:])
	default:
		return "-ERR unknown command\r\n"
	}
}

func (s *fakeRedisServer) expired(e fakeRedisEntry) bool {
	return !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
}

func (s *fakeRedisServer) handleSet(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong args\r\n"
	}
	key, val := args[0], args[1]
	var nx bool
	var pxMs int64 = -1
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "NX":
			nx = true
		case "PX":
			if i+1 < len(args) {
				pxMs, _ = strconv.ParseInt(args[i+1], 10, 64)
				i++
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.data[key]
	if ok && s.expired(existing) {
		ok = false
	}
	if nx && ok {
		return "$-1\r\n"
	}
	e := fakeRedisEntry{value: val}
	if pxMs >= 0 {
		e.expiresAt = time.Now().Add(time.Duration(pxMs) * time.Millisecond)
	}
	s.data[key] = e
	return "+OK\r\n"
}

func (s *fakeRedisServer) handleGet(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok || s.expired(e) {
		return "$-1\r\n"
	}
	return "$" + strconv.Itoa(len(e.value)) + "\r\n" + e.value + "\r\n"
}

func (s *fakeRedisServer) handleDel(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	if _, ok := s.data[key]; ok {
		delete(s.data, key)
		n = 1
	}
	return ":" + strconv.Itoa(n) + "\r\n"
}

func (s *fakeRedisServer) handleScan(args []string) string {
	pattern := "*"
	for i := 0; i < len(args); i++ {
		if strings.ToUpper(args[i]) == "MATCH" && i+1 < len(args) {
			pattern = args[i+1]
		}
	}
	s.mu.Lock()
	var keys []string
	for k, e := range s.data {
		if s.expired(e) {
			continue
		}
		if matchGlobPrefix(pattern, k) {
			keys = append(keys, k)
		}
	}
	s.mu.Unlock()

	var b strings.Builder
	b.WriteString("*2\r\n$1\r\n0\r\n")
	b.WriteString("*" + strconv.Itoa(len(keys)) + "\r\n")
	for _, k := range keys {
		b.WriteString("$" + strconv.Itoa(len(k)) + "\r\n" + k + "\r\n")
	}
	return b.String()
}

// matchGlobPrefix supports the only glob shape RedisStore uses: "prefix*".
func matchGlobPrefix(pattern, key string) bool {
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(key, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == key
}

// ---- tests ----

func newTestRedisStore(t *testing.T) *RedisStore {
	t.Helper()
	srv := newFakeRedisServer(t)
	s := NewRedisStore(srv.Addr(), "")
	s.SetDialTimeout(2 * time.Second)
	s.NodeTTL = 2 * time.Second
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestRedisStoreBindLeaseOwnership(t *testing.T) {
	st := newTestRedisStore(t)
	ctx := context.Background()

	a := Coordinator{Store: st, Node: Node{ID: "a", State: NodeActive}, TTL: time.Second}
	b := Coordinator{Store: st, Node: Node{ID: "b", State: NodeActive}, TTL: time.Second}

	l, ok, err := a.AcquireBind(ctx, "p", "t", "b1")
	if err != nil || !ok || l.OwnerNode != "a" {
		t.Fatalf("bad acquire %+v %v %v", l, ok, err)
	}
	l2, ok, err := b.AcquireBind(ctx, "p", "t", "b1")
	if err != nil || ok || l2.OwnerNode != "a" {
		t.Fatalf("expected existing lease %+v %v %v", l2, ok, err)
	}
}

func TestRedisStoreRenewAndRelease(t *testing.T) {
	st := newTestRedisStore(t)
	ctx := context.Background()

	l := BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "a"}
	acquired, ok, err := st.TryAcquireBind(ctx, l, time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire failed: %v %v", ok, err)
	}
	if acquired.Version != 1 {
		t.Fatalf("expected version 1, got %d", acquired.Version)
	}

	renewed, err := st.RenewBind(ctx, acquired, time.Second)
	if err != nil {
		t.Fatalf("renew failed: %v", err)
	}
	if renewed.Version != 2 {
		t.Fatalf("expected version 2, got %d", renewed.Version)
	}

	// Renewing as a different owner must fail.
	_, err = st.RenewBind(ctx, BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "other"}, time.Second)
	if err == nil {
		t.Fatalf("expected renew by non-owner to fail")
	}

	// Release by a different owner must fail.
	if err := st.ReleaseBind(ctx, BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "other"}); err == nil {
		t.Fatalf("expected release by non-owner to fail")
	}

	if err := st.ReleaseBind(ctx, renewed); err != nil {
		t.Fatalf("release failed: %v", err)
	}

	leases, err := st.ListLeases(ctx)
	if err != nil {
		t.Fatalf("list leases: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("expected no leases after release, got %+v", leases)
	}

	// Now that it's released, a different node can acquire it.
	l2, ok, err := st.TryAcquireBind(ctx, BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "b"}, time.Second)
	if err != nil || !ok || l2.OwnerNode != "b" {
		t.Fatalf("expected b to acquire freed lease: %+v %v %v", l2, ok, err)
	}
}

func TestRedisStoreDrainingLeaseCanBeStolen(t *testing.T) {
	st := newTestRedisStore(t)
	ctx := context.Background()

	l := BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "a"}
	acquired, ok, err := st.TryAcquireBind(ctx, l, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire failed: %v %v", ok, err)
	}

	acquired.Draining = true
	if _, ok, err := st.TryAcquireBind(ctx, acquired, 10*time.Second); err != nil || !ok {
		t.Fatalf("marking draining failed: %v %v", ok, err)
	}

	stolen, ok, err := st.TryAcquireBind(ctx, BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "b"}, 10*time.Second)
	if err != nil || !ok || stolen.OwnerNode != "b" {
		t.Fatalf("expected b to steal draining lease: %+v %v %v", stolen, ok, err)
	}
}

func TestRedisStoreNodeRegistryAndHeartbeat(t *testing.T) {
	st := newTestRedisStore(t)
	ctx := context.Background()

	if err := st.UpsertNode(ctx, Node{ID: "n1", Addr: "10.0.0.1:2775", State: NodeActive}); err != nil {
		t.Fatalf("upsert node: %v", err)
	}
	nodes, err := st.ListNodes(ctx)
	if err != nil || len(nodes) != 1 || nodes[0].ID != "n1" {
		t.Fatalf("expected 1 node n1, got %+v err=%v", nodes, err)
	}

	// The node's TTL is short (2s); without the background heartbeat
	// goroutine refreshing it, it would expire. Wait past the original
	// TTL and confirm it is still present thanks to the heartbeat loop.
	time.Sleep(2500 * time.Millisecond)
	nodes, err = st.ListNodes(ctx)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("expected node to survive via heartbeat, got %+v err=%v", nodes, err)
	}

	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestRedisStoreListNodesEmpty(t *testing.T) {
	st := newTestRedisStore(t)
	nodes, err := st.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected no nodes, got %+v", nodes)
	}
}
