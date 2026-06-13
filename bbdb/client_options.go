package bbdb

import (
	"time"

	"google.golang.org/grpc"
)

type ClientConfig struct {
	MaxBatchSize       int
	FlushInterval      time.Duration
	QueueDepth         int
	ReconnectBaseDelay time.Duration
	ReconnectMaxDelay  time.Duration
	DialOpts           []grpc.DialOption
}

func defaultClientConfig() ClientConfig {
	return ClientConfig{
		MaxBatchSize:       500,
		FlushInterval:      10 * time.Millisecond,
		QueueDepth:         10_000,
		ReconnectBaseDelay: 100 * time.Millisecond,
		ReconnectMaxDelay:  30 * time.Second,
	}
}

type ClientOption func(*ClientConfig)

func WithMaxBatchSize(n int) ClientOption {
	return func(c *ClientConfig) { c.MaxBatchSize = n }
}

func WithFlushInterval(d time.Duration) ClientOption {
	return func(c *ClientConfig) { c.FlushInterval = d }
}

func WithQueueDepth(n int) ClientOption {
	return func(c *ClientConfig) { c.QueueDepth = n }
}

func WithReconnectDelay(base, max time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.ReconnectBaseDelay = base
		c.ReconnectMaxDelay = max
	}
}

func WithDialOpts(opts ...grpc.DialOption) ClientOption {
	return func(c *ClientConfig) { c.DialOpts = append(c.DialOpts, opts...) }
}
