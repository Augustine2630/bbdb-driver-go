package bbdb

import (
	"context"
	"fmt"
	"io"
	"time"

	bbdbv1 "github.com/Augustine2630/bbdb-driver-go/gen/bbdb/v1"

	"google.golang.org/grpc"
)

type pendingEvent struct {
	event  Event
	result chan<- WriteResult
}

type batcher struct {
	cm     *connManager
	cfg    ClientConfig
	queue  chan pendingEvent
	stream bbdbv1.EventIngestion_WriteClient
}

func newBatcher(cm *connManager, cfg ClientConfig) *batcher {
	return &batcher{
		cm:    cm,
		cfg:   cfg,
		queue: make(chan pendingEvent, cfg.QueueDepth),
	}
}

func (b *batcher) start(ctx context.Context) {
	go b.drain(ctx)
}

func (b *batcher) enqueue(ctx context.Context, e Event) error {
	select {
	case b.queue <- pendingEvent{event: e}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *batcher) enqueueWithResult(ctx context.Context, e Event) (<-chan WriteResult, error) {
	ch := make(chan WriteResult, 1)
	select {
	case b.queue <- pendingEvent{event: e, result: ch}:
		return ch, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *batcher) drain(ctx context.Context) {
	timer := time.NewTimer(b.cfg.FlushInterval)
	defer timer.Stop()

	var buf []pendingEvent

	flush := func() {
		if len(buf) == 0 {
			return
		}
		b.sendBatch(ctx, buf)
		buf = buf[:0]
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(b.cfg.FlushInterval)
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case e := <-b.queue:
			buf = append(buf, e)
			if len(buf) >= b.cfg.MaxBatchSize {
				flush()
			}
		case <-timer.C:
			flush()
			timer.Reset(b.cfg.FlushInterval)
		}
	}
}

func (b *batcher) sendBatch(ctx context.Context, batch []pendingEvent) {
	req := b.buildRequest(batch)
	bo := newBackoff(b.cfg.ReconnectBaseDelay, b.cfg.ReconnectMaxDelay, 2.0)

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		lastErr = b.send(ctx, req)
		if lastErr == nil {
			bo.reset()
			b.notifyAll(batch, WriteResult{BatchID: req.BatchId})
			return
		}
		b.stream = nil
		b.cm.reset()
		delay := bo.next()
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			lastErr = ctx.Err()
			goto done
		}
	}
done:
	b.notifyAll(batch, WriteResult{Err: lastErr})
}

func (b *batcher) send(ctx context.Context, req *bbdbv1.WriteRequest) error {
	stream, err := b.getStream(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(req); err != nil {
		b.stream = nil
		return fmt.Errorf("send: %w", err)
	}
	resp, err := stream.Recv()
	if err != nil && err != io.EOF {
		b.stream = nil
		return fmt.Errorf("recv: %w", err)
	}
	if resp != nil && resp.Error != nil {
		return fmt.Errorf("server error %d: %s", resp.Error.Code, resp.Error.Message)
	}
	return nil
}

func (b *batcher) getStream(ctx context.Context) (bbdbv1.EventIngestion_WriteClient, error) {
	if b.stream != nil {
		return b.stream, nil
	}
	conn, err := b.cm.get(ctx)
	if err != nil {
		b.cm.reset()
		return nil, fmt.Errorf("dial: %w", err)
	}
	stream, err := bbdbv1.NewEventIngestionClient(conn).Write(ctx, grpc.WaitForReady(true))
	if err != nil {
		b.cm.reset()
		return nil, fmt.Errorf("open stream: %w", err)
	}
	b.stream = stream
	return stream, nil
}

func (b *batcher) buildRequest(batch []pendingEvent) *bbdbv1.WriteRequest {
	events := make([]*bbdbv1.Event, len(batch))
	for i, p := range batch {
		var tsNs int64
		if !p.event.Timestamp.IsZero() {
			tsNs = p.event.Timestamp.UnixNano()
		}
		events[i] = &bbdbv1.Event{
			PartitionKey: p.event.PartitionKey,
			EventType:    uint32(p.event.EventType),
			TimestampNs:  tsNs,
			Payload:      p.event.Payload,
		}
	}
	return &bbdbv1.WriteRequest{
		Events:  events,
		BatchId: fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

func (b *batcher) notifyAll(batch []pendingEvent, result WriteResult) {
	for _, p := range batch {
		if p.result != nil {
			p.result <- result
		}
	}
}
