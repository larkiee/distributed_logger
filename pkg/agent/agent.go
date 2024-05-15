package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/larkiee/distributed_logger/api/v1"
	"github.com/larkiee/distributed_logger/pkg/config"
	"github.com/larkiee/distributed_logger/pkg/discovery"
	"github.com/larkiee/distributed_logger/pkg/log"
	"github.com/larkiee/distributed_logger/pkg/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	ServerTLSConfig *tls.Config
	PerrTLSConfig *tls.Config
	DataDir string
	BindAddr string
	RPCPort int
	NodeName string
	StartJoinAddrs []string
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

type Agent struct {
	Config
	log *log.Log
	server *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown bool
	shutdowns chan struct {}
	shutdownLock sync.Mutex
}

func New(c Config) (*Agent, error){
	a := &Agent{
		Config: c,
	}
	a.shutdowns = make(chan struct{})

	setups := []func() error{
		a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setups {
		err := fn()
		if err != nil {
			return nil, err
		}
	}

	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	l, err := log.NewLog(a.DataDir, log.Config{})
	if err != nil {
		return err
	}
	a.log = l
	return nil
}

func (a *Agent) setupServer() error {
	opts := []grpc.ServerOption{}
	host, _, _ := net.SplitHostPort(a.BindAddr)
	if a.ServerTLSConfig != nil {
		tlsCfg, err := config.GetTLSConfig(config.TLSRequest{
			IsServer: true,
			ServerAddr: host,
		})
		if err != nil {
			return err
		}
		tlsCrends := credentials.NewTLS(tlsCfg)
		opts = append(opts, grpc.Creds(tlsCrends))
	}
	s, _, err := server.NewGRPCServer(a.log, opts...)
	if err != nil {
		return err
	}
	a.server = s

	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func(){
		if err := s.Serve(l); err != nil {
			a.Shutdown()
		}
	}()
	return nil
}

func (a *Agent) setupMembership() error{
	opts := []grpc.DialOption{}

	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	host, _, _ := net.SplitHostPort(a.BindAddr)
	if a.PerrTLSConfig != nil {
		tlsConfig, err := config.GetTLSConfig(config.TLSRequest{
			IsServer: false,
			ServerAddr: host,
		})
		if err != nil {
			return err
		}
		tlsCrends := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.WithTransportCredentials(tlsCrends))
	}

	cc, err := grpc.Dial(rpcAddr, opts...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(cc)
	replicator := &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}
	a.replicator = replicator

	c := discovery.Config{
		NodeName: a.NodeName,
		BindAddr: a.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.StartJoinAddrs,
	}

	a.membership, err = discovery.NewMembership(c, a.replicator)
	if err != nil {
		return err
	}
	return nil
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.shutdown = true
	close(a.shutdowns)
	fns := []func() error {
		a.membership.Leave,
		a.log.Close,
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
	}

	for _, fn := range fns {
		err := fn()
		if err != nil {
			return err
		}
	}
	return nil
}