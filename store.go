package main

import (
	"sync"
	pb "tracepulse/proto"
)

type TraceStore struct{
	mu sync.RWMutex
	traces map[string][]*pb.Span
}

func NewTraceStore() *TraceStore{
	return &TraceStore{
		traces: make(map[string][]*pb.Span),
	}
}


func (ts *TraceStore) StoreSpan(span *pb.Span){
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.traces[span.TraceId] = append(ts.traces[span.TraceId],span)
}


func (ts *TraceStore) GetTrace(traceId string) []*pb.Span{
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return ts.traces[traceId]
}


