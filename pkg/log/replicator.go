package log

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/larkiee/distributed_logger/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	mu          sync.Mutex
	logger      *zap.Logger
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) Join(name, addr string) error {
	fmt.Println("GGGGGGGGGGG", addr, name)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	if _, OK := r.servers[name]; OK {
		// already replicating this server
		return nil
	}
	ch := make(chan struct{})
	r.servers[name] = ch
	go r.replicate(addr, ch)
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "error in replication", "addr", addr)
	}
	fmt.Println("HiiiByee")
	client := api.NewLogClient(cc)
	records := make(chan *api.Record)

	go func() {
		i := 0
		stream, err := client.ConsumeStream(context.Background(), &api.ConsumeRequest{Offset: 0})
		if err != nil {
			r.logError(err, "failed got stream", "addr", addr)
		}

		for {
			res, err := stream.Recv()
			if i != 0 {
				os.Exit(1)
			}
			s, _ := status.FromError(err)

			if err != nil && strings.Contains(s.Message(), "range") {
				continue
			} else if err != nil {
				return

			}

			records <- res.Record
		}
	}()

	for {
		select {
		case <-leave:
			return
		case <-r.close:
			return
		case rec := <-records:
			_, err = r.LocalServer.Produce(context.Background(), &api.ProduceRequest{
				Record: rec,
			})
			if err != nil {
				r.logError(
					err,
					"failed to produce",
					"addr",
					addr,
				)
			}
		}
	}
}

func (r *Replicator) logError(err error, msg string, args ...string) {
	fields := []zap.Field{zap.Error(err)}
	for i := 0; i < len(args); i += 2 {
		k := args[i]
		v := args[i+1]
		fields = append(fields, zap.String(k, v))

	}
	r.logger.Error(
		msg,
		fields...,
	)
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, OK := r.servers[name]; !OK {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}
