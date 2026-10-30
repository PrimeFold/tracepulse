package main

import (
	"context"
	"sync"
	pb "tracepulse/proto"
)

type Pipeline struct{
	store *TraceStore
	queue chan *pb.Span
	workerCount int
	wg sync.WaitGroup
}

func NewPipeline( store *TraceStore , queueCap int , workerCount int) *Pipeline{
	return &Pipeline{
		store:store,
		queue: make(chan *pb.Span , queueCap ),
		workerCount: workerCount,
	}
}

func (p *Pipeline) Submit(span *pb.Span) bool {
	select{
	case p.queue<- span:
		return true
	default:
		return false
	}
}

func (p *Pipeline) Start(ctx context.Context){
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go func (workerId int)  {
			defer p.wg.Done()
			p.StartWorker(ctx,workerId)
		}(i)
	}
}



func (p *Pipeline) Stop(){
	close(p.queue)
	p.wg.Wait()
}