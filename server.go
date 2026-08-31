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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
// Runtime methods: Start (returns a cleanup that stops the server), HealthCheck, ProbeReady.
type Server[T GRPCRegistrar] struct {
	grpc         *grpc.Server
	listener     net.Listener
	handler      T
	metrics      *GRPCMetrics
	gate         gate
	gateDisabled bool
	label        string
	err          atomic.Error
}

// gate reports whether traffic should be let through — no runner import;
// duck-typed against runner.Gate's Ready() bool.
type gate interface{ Ready() bool }

// gateUnaryInterceptor answers codes.Unavailable instead of calling handler
// while g reports not ready.
func gateUnaryInterceptor(g gate) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !g.Ready() {
			return nil, status.Error(codes.Unavailable, "not ready")
		}
		return handler(ctx, req)
	}
}

// gateStreamInterceptor is gateUnaryInterceptor for streaming RPCs.
func gateStreamInterceptor(g gate) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !g.Ready() {
			return status.Error(codes.Unavailable, "not ready")
		}
		return handler(srv, ss)
	}
}

func (*Server[T]) BuildConfig() (app.Materializer, error) {
	return &Config[T]{}, nil
}

// DisableGate permanently turns off gate-checking for this instance — for
// servers that must never be traffic-gated (e.g. ops's own readiness
// endpoint, which would otherwise mask its own status behind a blanket
// codes.Unavailable, and could false-fail a liveness check sharing the same
// listener).
func (r *Server[T]) DisableGate() { r.gateDisabled = true }

func (r *Server[T]) Deps() []any {
	var t T
	return []any{
		t,
		(*GRPCMetrics)(nil),
		(*gate)(nil),
	}
}

func (r *Server[T]) Inject(args []any) {
	for _, arg := range args {
		switch v := arg.(type) {
		case T:
			r.handler = v
		case *GRPCMetrics:
			r.metrics = v
		case gate:
			r.gate = v
		}
	}
}

func (t *Server[T]) Start(ctx context.Context) (func(context.Context) error, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		unaryInterceptors := []grpc.UnaryServerInterceptor{t.metrics.UnaryServerInterceptor()}
		streamInterceptors := []grpc.StreamServerInterceptor{t.metrics.StreamServerInterceptor()}
		if t.gate != nil && !t.gateDisabled {
			unaryInterceptors = append(unaryInterceptors, gateUnaryInterceptor(t.gate))
			streamInterceptors = append(streamInterceptors, gateStreamInterceptor(t.gate))
		}

		opts := []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(unaryInterceptors...),
			grpc.ChainStreamInterceptor(streamInterceptors...),
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
		return t.stop, nil
	}
}

func (t *Server[T]) stop(ctx context.Context) error {
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
