package bbdb_test

import (
	"context"
	"net"
	"testing"
	"time"

	bbdbv1 "bbdb-driver-go/gen/bbdb/v1"
	"bbdb-driver-go/bbdb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestClient_ReconnectsAfterServerRestart(t *testing.T) {
	fake := &fakeServer{}

	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis1.Addr().String()
	srv1 := grpc.NewServer()
	bbdbv1.RegisterEventIngestionServer(srv1, fake)
	bbdbv1.RegisterEventQueryServer(srv1, fake)
	go srv1.Serve(lis1)

	client, err := bbdb.New(addr,
		bbdb.WithFlushInterval(20*time.Millisecond),
		bbdb.WithMaxBatchSize(100),
		bbdb.WithReconnectDelay(10*time.Millisecond, 500*time.Millisecond),
		bbdb.WithDialOpts(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	if err := client.Write(ctx, bbdb.Event{EventType: 1, Payload: []byte("before")}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	srv1.Stop()
	time.Sleep(50 * time.Millisecond)

	lis2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("re-listen on %s: %v", addr, err)
	}
	fake2 := &fakeServer{}
	srv2 := grpc.NewServer()
	bbdbv1.RegisterEventIngestionServer(srv2, fake2)
	bbdbv1.RegisterEventQueryServer(srv2, fake2)
	go srv2.Serve(lis2)
	defer srv2.Stop()

	time.Sleep(200 * time.Millisecond)
	if err := client.Write(ctx, bbdb.Event{EventType: 1, Payload: []byte("after")}); err != nil {
		t.Fatalf("write after restart: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	fake2.mu.Lock()
	var total int
	for _, b := range fake2.batches {
		total += len(b)
	}
	fake2.mu.Unlock()

	if total == 0 {
		t.Error("expected at least one event delivered after server restart")
	}
}
