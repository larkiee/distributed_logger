package agent

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/larkiee/distributed_logger/api/v1"
	"github.com/larkiee/distributed_logger/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestAgent(t *testing.T) {
	var agents []*Agent

	for i := 0; i < 3; i++ {
		port := dynaport.Get(2)
		addr := "127.0.0.1"
		bindAddr := fmt.Sprintf("%s:%d", addr, port[0])

		dir, err := os.MkdirTemp("", fmt.Sprintf("agent-0%d-", i))
		require.NoError(t, err)

		srvTLSConf, err := config.GetTLSConfig(config.TLSRequest{
			IsServer:   true,
			ServerAddr: addr,
		})
		require.NoError(t, err)
		peerTLSConf, err := config.GetTLSConfig(config.TLSRequest{
			IsServer:   false,
			ServerAddr: addr,
		})
		require.NoError(t, err)
		c := Config{
			ServerTLSConfig: srvTLSConf,
			PerrTLSConfig:   peerTLSConf,
			NodeName:        fmt.Sprintf("%d", i),
			DataDir:         dir,
			BindAddr:        bindAddr,
			RPCPort:         port[1],
		}

		if i != 0 {
			c.StartJoinAddrs = append(c.StartJoinAddrs,
				agents[0].BindAddr,
			)
		}

		a, err := New(c)
		require.NoError(t, err)
		agents = append(agents, a)
	}

	defer func() {
		for _, a := range agents {
			err := a.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(a.DataDir))
		}
	}()

	time.Sleep(3 * time.Second)
	
	leadership := client(t, agents[0])

	pRes, err := leadership.Produce(context.Background(), &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("Hiii !!!"),
		},
	})
	require.NoError(t, err)
	require.Equal(t, pRes.Offset, uint64(0))

	cRes, err := leadership.Consume(context.Background(), &api.ConsumeRequest{
		Offset: 0,
	})

	require.NoError(t, err)
	require.Equal(t, cRes.Record.Value, []byte("Hiii !!!"))
	time.Sleep(3*time.Second)

	// follower := client(t, agents[1])
	// cRes, err = follower.Consume(context.Background(), &api.ConsumeRequest{
	// 	Offset: 0,
	// })
	
	// require.NoError(t, err)
	// require.Equal(t, cRes.Record.Value, []byte("Hiii !!!"))
}


func client(t *testing.T, a *Agent) api.LogClient {
	rpcAddr, err := a.RPCAddr()
	require.NoError(t, err)
	tlsConfig, err := config.GetTLSConfig(config.TLSRequest{
		IsServer: false,
		ServerAddr: "127.0.0.1",
	})
	require.NoError(t, err)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(
			credentials.NewTLS(tlsConfig),
		),
	}
	cc, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)

	client := api.NewLogClient(cc)
	return client
}