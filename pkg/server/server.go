package server

import (
	"context"
	"errors"
	"os"
	"path"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/larkiee/distributed_logger/api/v1"
	"github.com/larkiee/distributed_logger/pkg/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	tracer = otel.Tracer("Producer")
)

func NewGRPCServer(l Logger, srvOpts ...grpc.ServerOption) (*grpc.Server, func(), error) {
	var logger Logger
	var err error
	switch v := l.(type) {
	case *log.Log:
		
		if v == nil {
			dir := path.Join(os.TempDir(), "server-test")
			os.RemoveAll(dir)
			err = os.Mkdir(dir, 0777)
			if err != nil {
				return nil, nil, err
			}
			logger, err = log.NewLog(dir, log.Config{})
			if err != nil {
				return nil, nil, err
			}
		}
	default:
		return nil, nil, errors.New("not recognisable logger")
	}
	zl, _ := zap.NewDevelopment()
	te, _ := stdouttrace.New(stdouttrace.WithPrettyPrint())
	tp := trace.NewTracerProvider(trace.WithBatcher(te,
		// Default is 5s. Set to 1s for demonstrative purposes.
		trace.WithBatchTimeout(time.Second)))
	srvOpts = append(srvOpts,
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(tp))),
		grpc.ChainUnaryInterceptor(
			logging.UnaryServerInterceptor(interceptorLogger(zl), loggingMiddlewareOpts...),
		),
		grpc.ChainStreamInterceptor(
			otelgrpc.StreamServerInterceptor(),
			logging.StreamServerInterceptor(interceptorLogger(zl), loggingMiddlewareOpts...),
		),
	)
	gsrv := grpc.NewServer(srvOpts...)

	srv, err := newgrpcServer(logger)
	if err != nil {
		return nil, nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	cleanup := func ()  {
		logger.Remove()
	}
	return gsrv, cleanup, nil
}

type Logger interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
	Remove() error
}

type grpcServer struct {
	api.UnimplementedLogServer
	Logger
}

func newgrpcServer(l Logger) (*grpcServer, error) {

	s := &grpcServer{Logger: l}
	return s, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	off, err := s.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: off}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	_, span := tracer.Start(ctx, "producer")
	defer span.End()
	r, err := s.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Record: r}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		off, err := s.Append(req.Record)
		if err != nil {
			return err
		}

		err = stream.Send(&api.ProduceResponse{Offset: off})
		if err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			default:
				return err
			}
			err = stream.Send(res)
			if err != nil {
				return err
			}
			req.Offset++
		}
	}
}
