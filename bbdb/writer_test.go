package bbdb

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	bbdbv1 "github.com/Augustine2630/bbdb-driver-go/gen/bbdb/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeIngestionServer struct {
	bbdbv1.UnimplementedEventIngestionServer
	mu       sync.Mutex
	received []*bbdbv1.WriteRequest
}

func (s *fakeIngestionServer) Write(stream bbdbv1.EventIngestion_WriteServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.received = append(s.received, req)
		s.mu.Unlock()
		_ = stream.Send(&bbdbv1.WriteResponse{
			BatchId:  req.BatchId,
			Accepted: uint32(len(req.Events)),
		})
	}
}

func startFakeServer(t *testing.T, fake *fakeIngestionServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	bbdbv1.RegisterEventIngestionServer(srv, fake)
	go srv.Serve(lis)
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

func TestBatcher_FlushBySize(t *testing.T) {
	fake := &fakeIngestionServer{}
	addr := startFakeServer(t, fake)

	cm := newConnManager(addr, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())})
	cfg := ClientConfig{
		MaxBatchSize:       3,
		FlushInterval:      10 * time.Second,
		QueueDepth:         100,
		ReconnectBaseDelay: 10 * time.Millisecond,
		ReconnectMaxDelay:  500 * time.Millisecond,
	}
	b := newBatcher(cm, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b.start(ctx)

	events := []Event{
		{EventType: 1, Payload: []byte("a")},
		{EventType: 1, Payload: []byte("b")},
		{EventType: 1, Payload: []byte("c")},
	}
	for _, e := range events {
		if err := b.enqueue(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	fake.mu.Lock()
	n := len(fake.received)
	fake.mu.Unlock()
	if n == 0 {
		t.Fatal("expected at least one batch received by server")
	}
}

func TestBatcher_FlushByTimer(t *testing.T) {
	fake := &fakeIngestionServer{}
	addr := startFakeServer(t, fake)

	cm := newConnManager(addr, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())})
	cfg := ClientConfig{
		MaxBatchSize:       1000,
		FlushInterval:      30 * time.Millisecond,
		QueueDepth:         100,
		ReconnectBaseDelay: 10 * time.Millisecond,
		ReconnectMaxDelay:  500 * time.Millisecond,
	}
	b := newBatcher(cm, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b.start(ctx)

	if err := b.enqueue(ctx, Event{EventType: 2, Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	fake.mu.Lock()
	n := len(fake.received)
	fake.mu.Unlock()
	if n == 0 {
		t.Fatal("expected timer-triggered flush")
	}
}
