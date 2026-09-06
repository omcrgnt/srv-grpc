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

	// extraUnary carries New's options through to Build — unexported, so
	// ecfg's reflection-based walker can't set it and leaves it alone.
	extraUnary []grpc.UnaryServerInterceptor
}

func (cfg *Config[T]) Build() (any, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host.Value, cfg.Port.Value))
	if err != nil {
		return nil, err
	}
	return &Server[T]{
		label:      cfg.Label.GetValue(),
		listener:   listener,
		extraUnary: cfg.extraUnary,
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

	// extraUnary is set via New/WithUnaryServerInterceptors, carried through
	// BuildConfig -> Config -> Build — see client-grpc's Client.New doc
	// comment for why the catalog field must be constructed non-nil for
	// this to survive at all.
	extraUnary []grpc.UnaryServerInterceptor
}

// Option configures a Server at construction time, for values ecfg can't
// fill (e.g. interceptor functions) — see New.
type Option[T GRPCRegistrar] func(*Server[T])

// WithUnaryServerInterceptors appends interceptors run in addition to (not
// instead of) the metrics/gate interceptors this package always installs,
// in the order given (grpc.ChainUnaryInterceptor semantics: first listed
// runs outermost).
func WithUnaryServerInterceptors[T GRPCRegistrar](in ...grpc.UnaryServerInterceptor) Option[T] {
	return func(s *Server[T]) { s.extraUnary = append(s.extraUnary, in...) }
}

// New constructs a Server[T] with the given options applied. The catalog
// field holding it must be assigned this (non-nil) in the app's resources
// literal — left nil (the zero-value default), these options are lost when
// Build constructs a fresh instance. See client-grpc's Client.New.
func New[T GRPCRegistrar](opts ...Option[T]) *Server[T] {
	s := &Server[T]{}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BuildConfig returns the config spec for materialize, carrying over
// whatever options New was called with.
func (t *Server[T]) BuildConfig() (app.Materializer, error) {
	return &Config[T]{extraUnary: t.extraUnary}, nil
}

// gate reports whether traffic should be let through — no runner import;
// duck-typed against runner.Gate's Ready() bool.
type gate interface{ Ready() bool }

// gateCheckErr returns a codes.Unavailable error if g reports not ready,
// nil otherwise — shared by both interceptors below so the error format
// only needs to change in one place.
func gateCheckErr(g gate) error {
	if !g.Ready() {
		return status.Error(codes.Unavailable, "not ready")
	}
	return nil
}

// gateUnaryInterceptor answers codes.Unavailable instead of calling handler
// while g reports not ready.
func gateUnaryInterceptor(g gate) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := gateCheckErr(g); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// gateStreamInterceptor is gateUnaryInterceptor for streaming RPCs.
func gateStreamInterceptor(g gate) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := gateCheckErr(g); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// DisableGate permanently turns off gate-checking for this instance — for
// servers that must never be traffic-gated (e.g. ops's own readiness
// endpoint, which would otherwise mask its own status behind a blanket
// codes.Unavailable, and could false-fail a liveness check sharing the same
// listener).
//
// Must be called before Start, synchronously by the same caller that wires
// this Server: Start reads t.gate/t.gateDisabled once, while building the
// interceptor chain, and never again — a call after Start has already run
// is a silent no-op, not an error.
func (t *Server[T]) DisableGate() { t.gateDisabled = true }

func (t *Server[T]) Deps() []any {
	var handler T
	return []any{
		handler,
		(*GRPCMetrics)(nil),
		(*gate)(nil),
	}
}

func (t *Server[T]) Inject(args []any) {
	for _, arg := range args {
		switch v := arg.(type) {
		case T:
			t.handler = v
		case *GRPCMetrics:
			t.metrics = v
		case gate:
			t.gate = v
		}
	}
}

func (t *Server[T]) Start(ctx context.Context) (func(context.Context) error, error) {
	select {
	case <-ctx.Done():
		// Build already opened t.listener; on this early-return path nothing
		// else will ever close it (Start returns no cleanup on failure), so
		// it must be closed here or the fd leaks.
		_ = t.listener.Close()
		return nil, ctx.Err()
	default:
		// metrics, then gate: metrics is the outer wrapper, so a request the
		// gate rejects (Unavailable, handler never called) is still observed
		// by the metrics interceptor as a handled RPC — with that status and
		// a near-zero duration, not silently dropped. Expected during
		// startup (the gate opens once, after every runner.Starter has
		// started), but worth knowing if you own grpc_server_handled_total
		// dashboards/alerts: a startup window can show a burst of
		// Unavailable that never reached real handler logic.
		var unaryInterceptors []grpc.UnaryServerInterceptor
		var streamInterceptors []grpc.StreamServerInterceptor
		if t.metrics != nil {
			unaryInterceptors = append(unaryInterceptors, t.metrics.UnaryServerInterceptor())
			streamInterceptors = append(streamInterceptors, t.metrics.StreamServerInterceptor())
		}
		if t.gate != nil && !t.gateDisabled {
			unaryInterceptors = append(unaryInterceptors, gateUnaryInterceptor(t.gate))
			streamInterceptors = append(streamInterceptors, gateStreamInterceptor(t.gate))
		}
		unaryInterceptors = append(unaryInterceptors, t.extraUnary...)

		opts := []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(unaryInterceptors...),
			grpc.ChainStreamInterceptor(streamInterceptors...),
			grpc.StatsHandler(otelgrpc.NewServerHandler(
				otelgrpc.WithMetricAttributes(attribute.String("srv", t.label)),
			)),
		}
		t.grpc = grpc.NewServer(opts...)
		t.handler.RegisterGRPC(t.grpc)
		if t.metrics != nil {
			t.metrics.InitializeMetrics(t.grpc)
		}

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

// ProbeReady reports traffic readiness (SDI duck typing; no ops import):
// non-nil if Serve failed after Start (same as HealthCheck), or if a gate
// is wired, enabled, and not yet open — a caller relying only on
// HealthCheck would see this Server as ready while every real RPC is still
// answered Unavailable by the gate interceptor.
func (t *Server[T]) ProbeReady(ctx context.Context) error {
	if err := t.HealthCheck(ctx); err != nil {
		return err
	}
	if t.gate != nil && !t.gateDisabled && !t.gate.Ready() {
		return errors.New("srvgrpc: gate not ready")
	}
	return nil
}
