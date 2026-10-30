# ⚡ TracePulse

A high-throughput, distributed trace telemetry collector and span ingestion pipeline written in Go, modeled after the internal ingestion mechanics of **Jaeger** and the **OpenTelemetry Collector**.

TracePulse receives serialized Protocol Buffer spans over gRPC, buffers them through a non-blocking queue, and drains them concurrently across a worker pool into a thread-safe trace index.

---

## 🏗️ Architecture

```
                 ┌───────────────────────────┐
                 │ gRPC Microservice Clients │
                 └─────────────┬─────────────┘
                               │ Protobuf over HTTP/2
                               ▼
                 ┌───────────────────────────┐
                 │    gRPC Ingest Server     │
                 └─────────────┬─────────────┘
                               │ Non-blocking Submit()
                               ▼
                 ┌───────────────────────────┐
                 │ Buffered Pipeline Channel │ (Capacity: 10,000 Spans)
                 └─────────────┬─────────────┘
                               │
                ┌──────────────┼──────────────┐
                ▼              ▼              ▼
           [Worker 1]     [Worker 2]     [Worker N]   (Concurrent Worker Pool)
                │              │              │
                └──────────────┼──────────────┘
                               │ StoreSpan()
                               ▼
                 ┌───────────────────────────┐
                 │        TraceStore         │ (sync.RWMutex In-Memory Index)
                 └───────────────────────────┘
```

---

## 📊 Benchmark Results

Benchmarked on an **Intel® Core™ Ultra 5 125H (18 cores)** under Linux/WSL:

| Benchmark | Operations | Latency | Memory / Op | Allocs / Op |
| :--- | :--- | :--- | :--- | :--- |
| **`BenchmarkPipelineSubmit`** | **278,717,304** | **4.12 ns/op** | **0 B/op** | **0 allocs/op** |
| **`BenchmarkEndToEndGRPC`** | **11,476** | **112 µs/op** | **9,291 B/op** | **154 allocs/op** |

* **Zero-Allocation Pipeline:** Span queue submissions achieve **4.12 ns/op** with **zero heap allocations**, sustaining **>240M spans/sec** internal pipeline throughput.
* **Non-Blocking Ingestion:** Protects gRPC network threads from starvation during downstream database latency or traffic spikes.
* **Race-Free Concurrency:** Validated end-to-end under the Go race detector (`go test -race`).
* **Graceful Drain:** Automated context propagation and `sync.WaitGroup` barrier drain all queued spans on `SIGINT` / `SIGTERM` before exit.

---

## 🛠️ Protobuf Schema

```protobuf
syntax = "proto3";

package trackpulse;

message Span {
    string trace_id = 1;
    string span_id = 2;
    string parent_span_id = 3;
    string service_name = 4;
    string operation_name = 5;
    int64 start_time_unix_nano = 6;
    int64 duration_ms = 7;
    string status_code = 8;
    map<string, string> tags = 9;
}

service TelemetryCollector {
    rpc IngestSpans (IngestReqest) returns (IngestResponse);
}
```

---

## 🚀 Getting Started

### 1. Run the Collector
```bash
go run .
```

### 2. Run Test Suite with Race Detector
```bash
go test -v -race .
```

### 3. Run Performance Benchmarks
```bash
go test -bench=. -benchmem -run=^$ .
```
