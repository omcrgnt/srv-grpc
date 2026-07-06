package srvgrpc

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/omcrgnt/app"
	common "github.com/omcrgnt/proto/gen/go/common/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

// GRPCRegistrar registers service implementations on a gRPC server.
type GRPCRegistrar interface {
	RegisterGRPC(*grpc.Server)
}

// Config is the gRPC server spec (Label, Host, Port); ecfg fills before Build.
type Config[T GRPCRegistrar] struct {
	Label common.Label
	Host  common.Host
	Port  common.Port
}

func (cfg *Config[T]) Build() (any, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host.Value, cfg.Port.Value))
	if err != nil {
		return nil, err
	}
	return &Server[T]{
		label:    cfg.Label.GetValue(),
		listener: listener,
	}, nil
}

// Server is the gRPC server resource bound to handler type T.
// Catalog field: *Server[T] (Configurable); materialized *Server[T] is the runtime instance after [Config].Build.
// Runtime methods: Start, Close, HealthCheck, ProbeReady.
type Server[T GRPCRegistrar] struct {
	grpc     *grpc.Server
	listener net.Listener
	handler  T
	metrics  *GRPCMetrics
	label    string
	err      atomic.Error
}

func (*Server[T]) BuildConfig() (app.Materializer, error) {
	return &Config[T]{}, nil
}

func (r *Server[T]) Deps() []any {
	var t T
	return []any{
		t,
		(*GRPCMetrics)(nil),
	}
}

func (r *Server[T]) Inject(args []any) {
	for _, arg := range args {
		switch v := arg.(type) {
		case T:
			r.handler = v
		case *GRPCMetrics:
			r.metrics = v
		}
	}
}

func (t *Server[T]) Start(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		opts := []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(t.metrics.UnaryServerInterceptor()),
			grpc.ChainStreamInterceptor(t.metrics.StreamServerInterceptor()),
			grpc.StatsHandler(otelgrpc.NewServerHandler(
				otelgrpc.WithMetricAttributes(attribute.String("srv", t.label)),
			)),
		}
		t.grpc = grpc.NewServer(opts...)
		t.handler.RegisterGRPC(t.grpc)
		t.metrics.InitializeMetrics(t.grpc)

		go func() {
			if err := t.grpc.Serve(t.listener); err != nil {
				t.err.Store(err)
			}
		}()
		return nil
	}
}

func (t *Server[T]) Close(ctx context.Context) error {
	if t.grpc == nil {
		return nil
	}

	stopped := make(chan struct{})
	go func() {
		t.grpc.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		t.grpc.Stop()
		if errors.Is(ctx.Err(), context.Canceled) {
			return context.Canceled
		}
		return ctx.Err()
	}
}

func (t *Server[T]) HealthCheck(_ context.Context) error {
	return t.err.Load()
}

// ProbeReady reports traffic readiness (SDI duck typing; no ops import).
// v1: same as HealthCheck — non-nil if Serve failed after Start.
func (t *Server[T]) ProbeReady(ctx context.Context) error {
	return t.HealthCheck(ctx)
}
