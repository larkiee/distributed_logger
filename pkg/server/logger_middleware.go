package server

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"go.uber.org/zap"
)

var loggingMiddlewareOpts = []logging.Option{
	logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
}

func interceptorLogger(l *zap.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, level logging.Level, msg string, fields ...any) {
		f := make([]zap.Field, 0, len(fields)  / 2)

		for i := 0; i < len(fields); i += 2 {
			k, _ := fields[i].(string)
			val := fields[i+1]
			switch v := val.(type){
			case string:
				f = append(f, zap.String(k, v))
			case int:
				f = append(f, zap.Int(k, v))
			case bool:
				f = append(f, zap.Bool(k, v))
			default:
				f = append(f, zap.Any(k, v))
			}

			cl := l.WithOptions(zap.AddCallerSkip(1)).With(f...)

			switch level {
			case logging.LevelDebug:
				cl.Debug(msg)
			case logging.LevelInfo:
				cl.Info(msg)
			case logging.LevelWarn:
				cl.Warn(msg)
			case logging.LevelError:
				cl.Error(msg)
			default:
				panic("invalid log lvl")
			}
		}
	})
}

