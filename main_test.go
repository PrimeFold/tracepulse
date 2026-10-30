package main

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	pb "tracepulse/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestTracePulseEndToEnd(t *testing.T) {
	// 1. Boot internal pipeline and store
	store := NewTraceStore()
	pipeline := NewPipeline(store, 10000, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pipeline.Start(ctx)

	// 2. Start in-memory listener on random free port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	srv := NewServer(pipeline)
	pb.RegisterTelemetryCollectorServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// 3. Dial as a client
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewTelemetryCollectorClient(conn)

	// 4. Send 1,000 spans across 10 traces
	totalSpans := 1000
	var batch []*pb.Span
	for i := 0; i < totalSpans; i++ {
		batch = append(batch, &pb.Span{
			TraceId:       fmt.Sprintf("trace-%d", i%10),
			SpanId:        fmt.Sprintf("span-%d", i),
			ServiceName:   "payment-service",
			OperationName: "ProcessPayment",
			StatusCode:    "OK",
			DurationMs:    int64(i % 50),
		})
	}

	resp, err := client.IngestSpans(context.Background(), &pb.IngestReqest{Spans: batch})
	if err != nil {
		t.Fatalf("IngestSpans failed: %v", err)
	}

	if !resp.Success || resp.ReceivedCount != int32(totalSpans) {
		t.Fatalf("expected %d spans, got %d (success=%v)", totalSpans, resp.ReceivedCount, resp.Success)
	}

	// Give workers a moment to drain the queue into store
	time.Sleep(200 * time.Millisecond)

	// 5. Verify the store received all spans for trace-0
	trace0 := store.GetTrace("trace-0")
	if len(trace0) != totalSpans/10 {
		t.Fatalf("expected %d spans for trace-0, got %d", totalSpans/10, len(trace0))
	}
}



// BenchmarkPipelineSubmit measures the raw throughput of submitting spans
// into the non-blocking pipeline queue.
func BenchmarkPipelineSubmit(b *testing.B) {
	store := NewTraceStore()
	pipeline := NewPipeline(store, 100000, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pipeline.Start(ctx)

	span := &pb.Span{
		TraceId:       "trace-bench",
		SpanId:        "span-bench",
		ServiceName:   "auth-service",
		OperationName: "VerifyToken",
		StatusCode:    "OK",
		DurationMs:    12,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pipeline.Submit(span)
	}
}

// BenchmarkEndToEndGRPC measures complete serialization, network transit,
// and pipeline ingestion over a real TCP connection.
func BenchmarkEndToEndGRPC(b *testing.B) {
	store := NewTraceStore()
	pipeline := NewPipeline(store, 500000, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pipeline.Start(ctx)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	srv := NewServer(pipeline)
	pb.RegisterTelemetryCollectorServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		b.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewTelemetryCollectorClient(conn)

	req := &pb.IngestReqest{
		Spans: []*pb.Span{
			{
				TraceId:       "trace-1",
				SpanId:        "span-1",
				ServiceName:   "order-service",
				OperationName: "CreateOrder",
				StatusCode:    "OK",
				DurationMs:    5,
			},
		},
	}

	callCtx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.IngestSpans(callCtx, req)
		if err != nil {
			b.Fatalf("IngestSpans error: %v", err)
		}
	}
}