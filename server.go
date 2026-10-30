package main

import (
	"context"
	pb "tracepulse/proto"
)


type Server struct{
	pb.UnimplementedTelemetryCollectorServer
	pipeline *Pipeline
}


func NewServer(p *Pipeline) *Server {
	return &Server{
		pipeline: p,
	}
}

func (s *Server) IngestSpans(ctx context.Context , req *pb.IngestReqest) (*pb.IngestResponse , error){
	var accepted int
	for _,i := range req.Spans{
		ok := s.pipeline.Submit(i)
		if ok{
			accepted++
		}	
	}
	return &pb.IngestResponse{
		ReceivedCount: int32(accepted),
		Success: true,
	},nil
}


