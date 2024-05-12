package server

import (
	"context"
	"fmt"
	logger "log"
	"net"
	"testing"

	"github.com/larkiee/distributed_logger/api/v1"
	"github.com/larkiee/distributed_logger/pkg/config"
	"github.com/larkiee/distributed_logger/pkg/log"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServer(t *testing.T) {
	logger.Println("Here...")
	testCases := []struct {
		name string
		fn func(*testing.T, api.LogClient)
	}{
		{name: "produce/consume test", fn: testProduceConsume},
		{name: "produce/consume stream succeeds", fn: testProduceConsumeStream},
	}

	for _, tc := range testCases {
		addr, cleanupServer := setupServer(t)
		logger.Println("Server Address :", addr)
		client, cleanupClient := setupClient(t, addr)
		defer func ()  {
			cleanupClient()
			cleanupServer()
		}()
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, client)
		})
	}
}


func setupServer(t *testing.T) (addr string, cleanup func()){
	logger.Println("Here...")
	var l *log.Log
	ip := viper.GetString("server.ip")
	port := viper.GetInt("server.port")
	lst, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	serverOpts := make([]grpc.ServerOption, 0)
	if viper.GetBool("tls.use") {
		tlsCfg, err := config.GetTLSConfig(config.TLSRequest{ServerAddr: ip, IsServer: true})
		require.NoError(t, err)
		crends := credentials.NewTLS(tlsCfg)
		serverOpts = append(serverOpts, 
			grpc.Creds(crends),
		)
	}
	logger.Println(":::", err)
	require.NoError(t, err)
	s, lc,  err := NewGRPCServer(l, serverOpts...)
	require.NoError(t, err)
	go func ()  {
		s.Serve(lst)
	}()

	cleanup = func(){
		lc()
		s.Stop()
	}
	return lst.Addr().String(), cleanup
}

func setupClient(t *testing.T, addr string) (client api.LogClient, cleanup func()) {
	t.Helper()
	clientOpts := []grpc.DialOption{}
	if viper.GetBool("tls.use") {
		ip := viper.GetString("server.ip")
		tlsCfg, err := config.GetTLSConfig(config.TLSRequest{ServerAddr: ip, IsServer: false})
		require.NoError(t, err)
		crends := credentials.NewTLS(tlsCfg)
		clientOpts = append(clientOpts, grpc.WithTransportCredentials(crends))
	} else {
		clientOpts = append(clientOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	
	logger.Println()
	cc, err := grpc.Dial(addr, clientOpts...)
	require.NoError(t, err)
	client = api.NewLogClient(cc)
	cleanup = func ()  {
		cc.Close()
	}
	return client, cleanup
}

func testProduceConsume(t *testing.T, client api.LogClient){
	ctx := context.Background()

	pRes, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("Hello grpc !!!"),
		},
	})
	require.NoError(t, err)
	logger.Printf("produce response: %v\n", pRes)

	cRes, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)
	require.Equal(t, cRes.Record.Offset, uint64(0))

	_, err = client.Consume(ctx, &api.ConsumeRequest{Offset: 1})
	require.Error(t, err)
}


func testProduceConsumeStream(t *testing.T, client api.LogClient){
	ctx := context.Background()
	messages := []*api.Record{
		{Value: []byte("Hi grpc !"), Offset: 0},
		{Value: []byte("Hi again grpc !!"), Offset: 1},
		{Value: []byte("seriously hi :)"), Offset: 2},
	}

	for _, mes := range messages {
		stream , err := client.ProduceStream(ctx)
		require.NoError(t, err)
		err = stream.Send(&api.ProduceRequest{Record: mes})
		require.NoError(t, err)
		rRes, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, rRes.Offset, mes.Offset)
	}


	consumeStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)
	for i := uint64(0); i < uint64(len(messages)); i++ {
		cRes, err := consumeStream.Recv()
		require.NoError(t, err)
		require.Equal(t, cRes.Record.Offset, i)
		require.Equal(t, messages[i].Value, cRes.Record.Value)
	}
}
