package main

import "context"

func (p *Pipeline) StartWorker(ctx context.Context , id int) {
	for{
		select{
		case<-ctx.Done():
			return
		case span,ok:= <-p.queue:
			if !ok{
				return
			}
			p.store.StoreSpan(span)
		}
	}
}