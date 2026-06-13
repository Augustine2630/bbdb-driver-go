package bbdb_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	bbdbv1 "github.com/Augustine2630/bbdb-driver-go/gen/bbdb/v1"
	"github.com/Augustine2630/bbdb-driver-go/bbdb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeServer struct {
	bbdbv1.UnimplementedEventIngestionServer
	bbdbv1.UnimplementedEventQueryServer
	mu      sync.Mutex
	batches [][]*bbdbv1.Event
}

func (s *fakeServer) Write(stream bbdbv1.EventIngestion_WriteServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.batches = append(s.batches, req.Events)
		s.mu.Unlock()
		_ = stream.Send(&bbdbv1.WriteResponse{
			BatchId:  req.BatchId,
			Accepted: uint32(len(req.Events)),
		})
	}
}

func (s *fakeServer) Query(req *bbdbv1.QueryRequest, stream bbdbv1.EventQuery_QueryServer) error {
	_ = stream.Send(&bbdbv1.QueryResponse{
		Events: []*bbdbv1.Event{
			{PartitionKey: req.PartitionKey, TimestampNs: 1000, Payload: []byte("data")},
		},
		Total:  1,
		IsLast: true,
	})
	return nil
}

func startServer(t *testing.T, fake *fakeServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	bbdbv1.RegisterEventIngestionServer(srv, fake)
	bbdbv1.RegisterEventQueryServer(srv, fake)
	go srv.Serve(lis)
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

func TestClient_Write(t *testing.T) {
	fake := &fakeServer{}
	addr := startServer(t, fake)

	client, err := bbdb.New(addr,
		bbdb.WithMaxBatchSize(10),
		bbdb.WithFlushInterval(20*time.Millisecond),
		bbdb.WithDialOpts(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	err = client.Write(ctx, bbdb.Event{
		EventType: 1,
		Payload:   []byte(`{"key":"value"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	fake.mu.Lock()
	total := 0
	for _, b := range fake.batches {
		total += len(b)
	}
	fake.mu.Unlock()
	if total != 1 {
		t.Fatalf("expected 1 event delivered, got %d", total)
	}
}

func TestClient_Query(t *testing.T) {
	fake := &fakeServer{}
	addr := startServer(t, fake)

	client, err := bbdb.New(addr,
		bbdb.WithDialOpts(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	now := time.Now()
	result, err := client.Query(ctx, bbdb.QueryRequest{
		PartitionKey: []byte("pk"),
		From:         now.Add(-time.Hour),
		To:           now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 {
		t.Fatalf("expected total=1, got %d", result.Total)
	}
	if len(result.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(result.Events))
	}
}

func TestClient_Close(t *testing.T) {
	fake := &fakeServer{}
	addr := startServer(t, fake)
	client, err := bbdb.New(addr,
		bbdb.WithDialOpts(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
}
