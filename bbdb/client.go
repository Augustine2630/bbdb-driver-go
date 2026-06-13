package bbdb

import (
	"context"
	"fmt"
	"time"

	bbdbv1 "bbdb-driver-go/gen/bbdb/v1"
)

// QueryRequest is the input for Client.Query.
type QueryRequest struct {
	PartitionKey []byte
	EventType    uint8
	From         time.Time
	To           time.Time
}

// Client is the BBDB driver. Safe for concurrent use.
type Client struct {
	cfg     ClientConfig
	cm      *connManager
	batcher *batcher
	cancel  context.CancelFunc
}

// New creates a Client connected to addr.
func New(addr string, opts ...ClientOption) (*Client, error) {
	cfg := defaultClientConfig()
	for _, o := range opts {
		o(&cfg)
	}

	dialOpts := defaultDialOpts()
	dialOpts = append(dialOpts, cfg.DialOpts...)

	cm := newConnManager(addr, dialOpts)
	b := newBatcher(cm, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	b.start(ctx)

	return &Client{
		cfg:     cfg,
		cm:      cm,
		batcher: b,
		cancel:  cancel,
	}, nil
}

// Write enqueues a single event. Blocks when queue is full (flow control).
func (c *Client) Write(ctx context.Context, e Event) error {
	return c.batcher.enqueue(ctx, e)
}

// WriteSync enqueues an event and waits for server acknowledgement.
func (c *Client) WriteSync(ctx context.Context, e Event) (WriteResult, error) {
	ch, err := c.batcher.enqueueWithResult(ctx, e)
	if err != nil {
		return WriteResult{}, err
	}
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		return WriteResult{}, ctx.Err()
	}
}

// Query executes a query and returns all matching events.
func (c *Client) Query(ctx context.Context, req QueryRequest) (QueryResult, error) {
	if len(req.PartitionKey) == 0 {
		return QueryResult{}, fmt.Errorf("PartitionKey must not be empty")
	}
	if !req.From.Before(req.To) {
		return QueryResult{}, fmt.Errorf("From must be before To")
	}

	conn, err := c.cm.get(ctx)
	if err != nil {
		return QueryResult{}, fmt.Errorf("dial: %w", err)
	}

	qclient := bbdbv1.NewEventQueryClient(conn)
	protoReq := &bbdbv1.QueryRequest{
		PartitionKey: req.PartitionKey,
		EventType:    uint32(req.EventType),
		FromNs:       req.From.UnixNano(),
		ToNs:         req.To.UnixNano(),
	}

	stream, err := qclient.Query(ctx, protoReq)
	if err != nil {
		return QueryResult{}, fmt.Errorf("query RPC: %w", err)
	}

	var result QueryResult
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		if resp.GetError() != nil {
			return QueryResult{}, fmt.Errorf("server error %d: %s", resp.GetError().Code, resp.GetError().Message)
		}
		for _, e := range resp.GetEvents() {
			result.Events = append(result.Events, QueryEvent{
				PartitionKey: e.GetPartitionKey(),
				EventType:    uint8(e.GetEventType()),
				Timestamp:    time.Unix(0, e.GetTimestampNs()).UTC(),
				Payload:      e.GetPayload(),
			})
		}
		if resp.GetIsLast() {
			result.Total = resp.GetTotal()
			break
		}
	}
	return result, nil
}

// Close shuts down the batcher and closes the gRPC connection.
func (c *Client) Close() {
	c.cancel()
	c.cm.close()
}
