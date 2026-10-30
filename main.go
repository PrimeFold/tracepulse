package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	pb "tracepulse/proto"

	"google.golang.org/grpc"
)


func main(){

	//Starting the Trace Store and feeding it to the pipeline

	ts:= NewTraceStore()
	p:= NewPipeline(ts,10000,8)


	// Initializing context..
	ctx,cancel:= context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)


	listener,err:= net.Listen("tcp",":50051")
	if err != nil{
		log.Fatalf("Couldn't listen to the port 50051 : %v",err)
	}

	grpcServer:= grpc.NewServer()

	server:= NewServer(p)
	pb.RegisterTelemetryCollectorServer(grpcServer,server)

	log.Println("TracePulse collector running on :50051...")	


	sigCh:= make(chan os.Signal,1)

	signal.Notify(sigCh,os.Interrupt,syscall.SIGTERM)

	go func ()  {
		<-sigCh
		log.Println("\nShutting down TracePulse gracefully...")
		grpcServer.GracefulStop()
		cancel()
		p.Stop()
		log.Println("TracePulse stopped.")
		os.Exit(0)
	}()
	

	if err := grpcServer.Serve(listener); err != nil {
    	log.Fatalf("failed to serve: %v", err)
	}	
	
}