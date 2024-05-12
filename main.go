package main

import (
	"context"
	"net"

	"github.com/larkiee/distributed_logger/api/v1"
	"github.com/larkiee/distributed_logger/pkg/log"
	"github.com/larkiee/distributed_logger/pkg/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	
)

func main() {
	lst, _ := net.Listen("tcp", ":0")
	var lgr *log.Log
	s, _, _ := server.NewGRPCServer(lgr)

	go func() {
		cc, _ := grpc.Dial(lst.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		client := api.NewLogClient(cc)
		client.Produce(context.Background(), &api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("Helloooo !!!"),
			},
		})
		client.Consume(context.Background(), &api.ConsumeRequest{Offset: 0})
	}()

	s.Serve(lst)
}
