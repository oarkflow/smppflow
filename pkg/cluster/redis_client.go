package cluster

// A minimal, self-contained Redis RESP2 client scoped to exactly what
// RedisStore needs (SET/GET/DEL/SCAN and friends). This is intentionally
// separate from pkg/durablequeue's hand-rolled Redis client: that package
// is being modified concurrently elsewhere and does not export a reusable
// client type, so rather than depending on its unexported helpers (or
// editing it) this file implements just enough of RESP2 for cluster
// coordination. No third-party dependencies are used.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// redisReply is a minimal RESP2 reply representation covering simple
// strings, errors, integers, bulk strings and arrays (including nil bulk
// strings/arrays), which is all RedisStore's command set produces.
type redisReply struct {
	typ   byte
	str   string
	int   int64
	arr   []redisReply
	isNil bool
}

func (r redisReply) String() string { return r.str }

// redisClient dials a fresh TCP connection per command. This mirrors the
// approach already used by pkg/durablequeue.RedisStreamsQueue and keeps the
// implementation simple and safe for concurrent use without a connection
// pool. Cluster coordination calls (heartbeats every few seconds, lease
// acquire/renew) are low frequency enough that per-call dialing is not a
// meaningful bottleneck.
type redisClient struct {
	Addr        string
	Password    string
	DB          int
	DialTimeout time.Duration
}

func (rc *redisClient) dialTimeout() time.Duration {
	if rc.DialTimeout <= 0 {
		return 3 * time.Second
	}
	return rc.DialTimeout
}

func (rc *redisClient) addr() string {
	if rc.Addr == "" {
		return "127.0.0.1:6379"
	}
	return rc.Addr
}

// do executes a single Redis command over a fresh connection and returns
// its parsed reply.
func (rc *redisClient) do(ctx context.Context, args ...string) (redisReply, error) {
	d := net.Dialer{Timeout: rc.dialTimeout()}
	conn, err := d.DialContext(ctx, "tcp", rc.addr())
	if err != nil {
		return redisReply{}, err
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(rc.dialTimeout() + 5*time.Second))
	}

	r := bufio.NewReader(conn)

	if rc.Password != "" {
		if _, err := redisDo(conn, r, "AUTH", rc.Password); err != nil {
			return redisReply{}, fmt.Errorf("redis auth: %w", err)
		}
	}
	if rc.DB != 0 {
		if _, err := redisDo(conn, r, "SELECT", strconv.Itoa(rc.DB)); err != nil {
			return redisReply{}, fmt.Errorf("redis select: %w", err)
		}
	}
	return redisDo(conn, r, args...)
}

func redisDo(conn net.Conn, r *bufio.Reader, args ...string) (redisReply, error) {
	if err := redisWriteCommand(conn, args); err != nil {
		return redisReply{}, err
	}
	return redisReadReply(r)
}

func redisWriteCommand(w io.Writer, args []string) error {
	var b strings.Builder
	b.WriteString("*")
	b.WriteString(strconv.Itoa(len(args)))
	b.WriteString("\r\n")
	for _, a := range args {
		b.WriteString("$")
		b.WriteString(strconv.Itoa(len(a)))
		b.WriteString("\r\n")
		b.WriteString(a)
		b.WriteString("\r\n")
	}
	_, err := w.Write([]byte(b.String()))
	return err
}

func redisReadReply(r *bufio.Reader) (redisReply, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return redisReply{}, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return redisReply{}, errors.New("redis: empty reply line")
	}
	prefix := line[0]
	body := line[1:]
	switch prefix {
	case '+':
		return redisReply{typ: prefix, str: body}, nil
	case '-':
		return redisReply{}, errors.New("redis: " + body)
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return redisReply{}, err
		}
		return redisReply{typ: prefix, int: n}, nil
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return redisReply{}, err
		}
		if n < 0 {
			return redisReply{typ: prefix, isNil: true}, nil
		}
		buf := make([]byte, n+2) // payload + trailing CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return redisReply{}, err
		}
		return redisReply{typ: prefix, str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return redisReply{}, err
		}
		if n < 0 {
			return redisReply{typ: prefix, isNil: true}, nil
		}
		arr := make([]redisReply, n)
		for i := 0; i < n; i++ {
			item, err := redisReadReply(r)
			if err != nil {
				return redisReply{}, err
			}
			arr[i] = item
		}
		return redisReply{typ: prefix, arr: arr}, nil
	default:
		return redisReply{}, fmt.Errorf("redis: unknown reply prefix %q", string(prefix))
	}
}
