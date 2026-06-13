package bbdb

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type connManager struct {
	addr     string
	dialOpts []grpc.DialOption
	mu       sync.Mutex
	conn     *grpc.ClientConn
}

func newConnManager(addr string, opts []grpc.DialOption) *connManager {
	return &connManager{addr: addr, dialOpts: opts}
}

func (m *connManager) get(ctx context.Context) (*grpc.ClientConn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		return m.conn, nil
	}
	conn, err := grpc.NewClient(m.addr, m.dialOpts...)
	if err != nil {
		return nil, err
	}
	m.conn = conn
	return conn, nil
}

func (m *connManager) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}
}

func (m *connManager) close() {
	m.reset()
}

func defaultDialOpts() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

type backoff struct {
	base    time.Duration
	max     time.Duration
	factor  float64
	current time.Duration
}

func newBackoff(base, max time.Duration, factor float64) *backoff {
	return &backoff{base: base, max: max, factor: factor, current: base}
}

func (b *backoff) next() time.Duration {
	d := b.current
	next := time.Duration(float64(b.current) * b.factor)
	if next > b.max {
		next = b.max
	}
	b.current = next
	// jitter: up to 50% of d, capped so result never exceeds max
	maxJitter := b.max - d
	if maxJitter <= 0 {
		return d
	}
	if maxJitter > d/2 {
		maxJitter = d / 2
	}
	jitter := time.Duration(rand.Int63n(int64(maxJitter)))
	return d + jitter
}

func (b *backoff) reset() {
	b.current = b.base
}
