package srvgrpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	common "github.com/omcrgnt/proto/gen/go/common/v1"
	"github.com/omcrgnt/res/gate"
	"github.com/omcrgnt/res/unique"
	"github.com/omcrgnt/sdi"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type healthAPI struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (h *healthAPI) RegisterGRPC(s *grpc.Server) {
	grpc_health_v1.RegisterHealthServer(s, h)
}

func (h *healthAPI) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func testGRPCMetrics(t *testing.T) (*GRPCMetrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := &GRPCMetrics{}
	if err := m.RegisterMetrics(reg); err != nil {
		t.Fatal(err)
	}
	return m, reg
}

func TestConfig_Build_integration(t *testing.T) {
	spanExporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(spanExporter))
	otel.SetTracerProvider(tp)

	metrics, reg := testGRPCMetrics(t)
	api := &healthAPI{}

	cfg := Config[*healthAPI]{
		Label: common.Label{Value: "test_srv"},
		Host:  common.Host{Value: "127.0.0.1"},
		Port:  common.Port{Value: 0},
	}

	built, err := cfg.Build()
	if err != nil {
		t.Fatal(err)
	}

	server := built.(*Server[*healthAPI])
	server.Inject([]any{api, metrics})

	stop, err := server.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stop(context.Background())
	})

	addr := server.listener.Addr().String()

	time.Sleep(50 * time.Millisecond)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := grpc_health_v1.NewHealthClient(conn)
	resp, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("expected SERVING, got %v", resp.GetStatus())
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	metricsStr := formatMetricFamilies(mfs)
	if !strings.Contains(metricsStr, "grpc_server_handled_total") {
		t.Error("metrics: missing grpc_server_handled_total")
	}
	if !strings.Contains(metricsStr, `grpc_service="grpc.health.v1.Health"`) {
		t.Errorf("metrics: want grpc.health.v1.Health service label, got excerpt: %.200s", metricsStr)
	}

	spans := spanExporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("no trace spans recorded")
	}
	found := false
	for _, span := range spans {
		if strings.Contains(span.Name, "Health/Check") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no Health/Check span among %d spans", len(spans))
	}

	if err := stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := server.HealthCheck(t.Context()); err != nil {
		t.Errorf("HealthCheck after graceful close: %v", err)
	}
}

func formatMetricFamilies(mfs []*dto.MetricFamily) string {
	var b strings.Builder
	for _, mf := range mfs {
		fmt.Fprintf(&b, "%s ", mf.GetName())
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				fmt.Fprintf(&b, `%s="%s" `, lp.GetName(), lp.GetValue())
			}
		}
	}
	return b.String()
}

func TestInject(t *testing.T) {
	api := &healthAPI{}
	metrics, _ := testGRPCMetrics(t)

	s := &Server[*healthAPI]{}
	deps := s.Deps()

	if got, want := reflect.TypeOf(deps[0]), reflect.TypeOf((*healthAPI)(nil)); got != want {
		t.Errorf("Deps()[0] type = %v, want %v", got, want)
	}
	if got, want := reflect.TypeOf(deps[1]), reflect.TypeOf((*GRPCMetrics)(nil)); got != want {
		t.Errorf("Deps()[1] type = %v, want %v", got, want)
	}
	if got, want := reflect.TypeOf(deps[2]), reflect.TypeOf((*gate.Gate)(nil)); got != want {
		t.Errorf("Deps()[2] type = %v, want %v", got, want)
	}

	// ready:false is deliberate: a not-yet-wired gate.Switch reports Ready()
	// == true (see gate.Switch's own doc), so this value is the only one
	// that can distinguish "gate wired" from "gate never wired" here —
	// Switch has no exported way to compare identity directly.
	fg := &fakeGate{ready: false}
	s.Inject([]any{api, metrics, gate.Gate(fg)})

	if s.gate.Ready() {
		t.Error("Inject: gate not set (Switch still reports Ready with no gate wired)")
	}
}

type fakeGate struct{ ready bool }

func (g *fakeGate) Ready() bool { return g.ready }

// TestServer_SDIResolve_requiresGate pins the consequence of Deps declaring
// (*gate)(nil) as a single, required dependency (not a many-dep slice):
// sdi.Resolve must fail outright if nothing implementing gate is
// registered, not silently wire a nil gate. Every other test in this file
// only exercises manual Inject, which can't catch this — sdi.Resolve is a
// separate code path with its own dependency-satisfaction checks.
func TestServer_SDIResolve_requiresGate(t *testing.T) {
	reg := unique.New()
	reg.MustAddFixed(&GRPCMetrics{})
	reg.MustAddFixed(&healthAPI{})

	cfg := &Config[*healthAPI]{
		Label: common.Label{Value: "test_srv"},
		Host:  common.Host{Value: "127.0.0.1"},
		Port:  common.Port{Value: 0},
	}
	built, err := cfg.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = built.(*Server[*healthAPI]).listener.Close() })
	reg.MustAddReplaceable(built)

	if err := sdi.Resolve(reg); err == nil {
		t.Fatal("sdi.Resolve: expected an error — no gate-compatible resource registered")
	}
}

// TestServer_SDIResolve_withGate is the successful counterpart: a real
// sdi.Resolve pass actually wires the gate dependency through, not just
// manual Inject.
func TestServer_SDIResolve_withGate(t *testing.T) {
	reg := unique.New()
	reg.MustAddFixed(&GRPCMetrics{})
	reg.MustAddFixed(&healthAPI{})
	reg.MustAddFixed(&fakeGate{ready: true})

	cfg := &Config[*healthAPI]{
		Label: common.Label{Value: "test_srv"},
		Host:  common.Host{Value: "127.0.0.1"},
		Port:  common.Port{Value: 0},
	}
	built, err := cfg.Build()
	if err != nil {
		t.Fatal(err)
	}
	server := built.(*Server[*healthAPI])
	t.Cleanup(func() { _ = server.listener.Close() })
	reg.MustAddReplaceable(built)

	if err := sdi.Resolve(reg); err != nil {
		t.Fatalf("sdi.Resolve: %v", err)
	}
	if !server.gate.Ready() {
		t.Fatal("gate was not wired by sdi.Resolve (or was wired but reports not ready)")
	}
}

func TestConfig_Build_gate_stream(t *testing.T) {
	table := []struct {
		name     string
		gate     *fakeGate
		wantCode codes.Code
	}{
		// healthAPI embeds UnimplementedHealthServer: once the gate lets the
		// call through, Watch itself answers Unimplemented — proving the
		// interceptor invoked the real handler instead of short-circuiting.
		{name: "gate not ready", gate: &fakeGate{ready: false}, wantCode: codes.Unavailable},
		{name: "gate ready", gate: &fakeGate{ready: true}, wantCode: codes.Unimplemented},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			metrics, _ := testGRPCMetrics(t)
			api := &healthAPI{}

			cfg := Config[*healthAPI]{
				Label: common.Label{Value: "test_srv"},
				Host:  common.Host{Value: "127.0.0.1"},
				Port:  common.Port{Value: 0},
			}
			built, err := cfg.Build()
			if err != nil {
				t.Fatal(err)
			}
			server := built.(*Server[*healthAPI])
			server.Inject([]any{api, metrics, gate.Gate(tc.gate)})

			stop, err := server.Start(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = stop(context.Background()) })

			addr := server.listener.Addr().String()
			time.Sleep(50 * time.Millisecond)

			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			client := grpc_health_v1.NewHealthClient(conn)
			stream, err := client.Watch(t.Context(), &grpc_health_v1.HealthCheckRequest{})
			if err != nil {
				t.Fatal(err)
			}
			// Both outcomes below surface server-side, on the first Recv,
			// not on the initial Watch call itself.
			_, err = stream.Recv()
			if gotCode := status.Code(err); gotCode != tc.wantCode {
				t.Errorf("code = %v, want %v", gotCode, tc.wantCode)
			}
		})
	}
}

func TestConfig_Build_gate(t *testing.T) {
	table := []struct {
		name     string
		gate     *fakeGate
		wantCode codes.Code
	}{
		{name: "no gate wired", gate: nil, wantCode: codes.OK},
		{name: "gate not ready", gate: &fakeGate{ready: false}, wantCode: codes.Unavailable},
		{name: "gate ready", gate: &fakeGate{ready: true}, wantCode: codes.OK},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			metrics, _ := testGRPCMetrics(t)
			api := &healthAPI{}

			cfg := Config[*healthAPI]{
				Label: common.Label{Value: "test_srv"},
				Host:  common.Host{Value: "127.0.0.1"},
				Port:  common.Port{Value: 0},
			}
			built, err := cfg.Build()
			if err != nil {
				t.Fatal(err)
			}
			server := built.(*Server[*healthAPI])

			deps := []any{api, metrics}
			if tc.gate != nil {
				deps = append(deps, gate.Gate(tc.gate))
			}
			server.Inject(deps)

			stop, err := server.Start(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = stop(context.Background()) })

			addr := server.listener.Addr().String()
			time.Sleep(50 * time.Millisecond)

			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			client := grpc_health_v1.NewHealthClient(conn)
			_, err = client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
			if gotCode := status.Code(err); gotCode != tc.wantCode {
				t.Errorf("code = %v, want %v", gotCode, tc.wantCode)
			}
		})
	}
}

func TestConfig_Build_gate_disabled(t *testing.T) {
	// A gate-not-ready would normally answer Unavailable (see
	// TestConfig_Build_gate) — this proves DisableGate suppresses that
	// check even with a gate injected, the case ops needs its own
	// readiness endpoint to never be blocked by.
	metrics, _ := testGRPCMetrics(t)
	api := &healthAPI{}

	cfg := Config[*healthAPI]{
		Label: common.Label{Value: "test_srv"},
		Host:  common.Host{Value: "127.0.0.1"},
		Port:  common.Port{Value: 0},
	}
	built, err := cfg.Build()
	if err != nil {
		t.Fatal(err)
	}
	server := built.(*Server[*healthAPI])
	server.DisableGate()
	server.Inject([]any{api, metrics, gate.Gate(&fakeGate{ready: false})})

	stop, err := server.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stop(context.Background()) })

	addr := server.listener.Addr().String()
	time.Sleep(50 * time.Millisecond)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := grpc_health_v1.NewHealthClient(conn)
	_, err = client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	if gotCode := status.Code(err); gotCode != codes.OK {
		t.Errorf("code = %v, want %v (DisableGate should suppress the gate check)", gotCode, codes.OK)
	}
}

func TestStart_cancelledContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &Server[*healthAPI]{
		listener: ln,
	}

	cleanup, err := s.Start(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Start: got %v, want context.Canceled", err)
	}
	if cleanup != nil {
		t.Error("Start: expected nil cleanup on failure")
	}
	// Start returns no cleanup on this path, so the listener Build already
	// opened would otherwise never be closed by anything — Accept must fail
	// on a closed listener. Bounded deadline instead of a bare blocking
	// Accept: if this regresses to leaking the listener again, Accept would
	// otherwise block forever instead of failing the test.
	_ = ln.(*net.TCPListener).SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := ln.Accept(); err == nil {
		t.Error("Start: listener was not closed on the already-cancelled-ctx path — fd leak")
	} else if !strings.Contains(err.Error(), "use of closed network connection") {
		t.Errorf("Start: Accept error = %v, want \"use of closed network connection\" (got a deadline timeout instead — listener was never closed)", err)
	}
}

// TestStart_nilMetrics_doesNotPanic: Deps declares *GRPCMetrics as required,
// so under normal sdi.Resolve wiring it's never nil at Start — but Start
// shouldn't panic if a caller bypasses that (as this test deliberately
// does), matching the existing nil-guard on t.gate one line below in the
// real code.
func TestStart_nilMetrics_doesNotPanic(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	api := &healthAPI{}
	s := &Server[*healthAPI]{listener: ln}
	s.Inject([]any{api}) // deliberately no metrics

	stop, err := s.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stop(context.Background()) })
}

func TestHealthCheck_serveError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	metrics, _ := testGRPCMetrics(t)
	api := &healthAPI{}

	s := &Server[*healthAPI]{
		listener: ln,
	}
	s.Inject([]any{api, metrics})

	if _, err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.HealthCheck(context.Background()); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("HealthCheck: expected serve error, got nil")
}

func TestProbeReady_serveError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	metrics, _ := testGRPCMetrics(t)
	api := &healthAPI{}

	s := &Server[*healthAPI]{
		listener: ln,
	}
	s.Inject([]any{api, metrics})

	if _, err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.ProbeReady(context.Background()); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("ProbeReady: expected serve error, got nil")
}

func TestProbeReady_matchesHealthCheck(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	metrics, _ := testGRPCMetrics(t)
	api := &healthAPI{}

	s := &Server[*healthAPI]{
		listener: ln,
	}
	s.Inject([]any{api, metrics})

	ctx := context.Background()
	stop, err := s.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stop(context.Background()) })

	if err := s.ProbeReady(ctx); err != nil {
		t.Fatalf("ProbeReady after Start: %v", err)
	}
	if err := s.HealthCheck(ctx); err != nil {
		t.Fatalf("HealthCheck after Start: %v", err)
	}
}

// TestProbeReady_gateNotReady: HealthCheck alone (t.err) can't see this —
// Serve hasn't failed, only the gate hasn't opened yet. Without ProbeReady
// also consulting the gate, a k8s readiness probe would mark this pod Ready
// while every real RPC is still answered Unavailable by the gate
// interceptor.
func TestProbeReady_gateNotReady(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	metrics, _ := testGRPCMetrics(t)
	api := &healthAPI{}

	s := &Server[*healthAPI]{listener: ln}
	s.Inject([]any{api, metrics, gate.Gate(&fakeGate{ready: false})})

	ctx := context.Background()
	stop, err := s.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stop(context.Background()) })

	if err := s.ProbeReady(ctx); err == nil {
		t.Fatal("ProbeReady: expected error while gate is not ready, got nil")
	}
	// HealthCheck alone is unaffected — Serve itself hasn't failed.
	if err := s.HealthCheck(ctx); err != nil {
		t.Fatalf("HealthCheck: got %v, want nil (gate readiness is ProbeReady's concern, not HealthCheck's)", err)
	}
}

// TestProbeReady_gateDisabled_ignoresNotReadyGate mirrors
// TestConfig_Build_gate_disabled at the ProbeReady level: DisableGate must
// suppress the gate check here too, not just in the RPC interceptors.
func TestProbeReady_gateDisabled_ignoresNotReadyGate(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	metrics, _ := testGRPCMetrics(t)
	api := &healthAPI{}

	s := &Server[*healthAPI]{listener: ln}
	s.DisableGate()
	s.Inject([]any{api, metrics, gate.Gate(&fakeGate{ready: false})})

	ctx := context.Background()
	stop, err := s.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stop(context.Background()) })

	if err := s.ProbeReady(ctx); err != nil {
		t.Fatalf("ProbeReady: got %v, want nil (DisableGate should suppress the gate check)", err)
	}
}

func TestStop_cancelledContext(t *testing.T) {
	metrics, _ := testGRPCMetrics(t)

	hold := make(chan struct{})
	release := make(chan struct{})

	api := &blockingHealthAPI{
		hold:    hold,
		release: release,
	}

	cfg := Config[*blockingHealthAPI]{
		Label: common.Label{Value: "test_srv"},
		Host:  common.Host{Value: "127.0.0.1"},
		Port:  common.Port{Value: 0},
	}

	built, err := cfg.Build()
	if err != nil {
		t.Fatal(err)
	}

	server := built.(*Server[*blockingHealthAPI])
	server.Inject([]any{api, metrics})

	stop, err := server.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		_ = stop(context.Background())
	})

	addr := server.listener.Addr().String()
	go func() {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		defer conn.Close()
		client := grpc_health_v1.NewHealthClient(conn)
		stream, err := client.Watch(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			return
		}
		_, _ = stream.Recv()
	}()

	select {
	case <-hold:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Watch connection")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("stop with cancelled context: got %v, want context.Canceled", err)
	}
}

type blockingHealthAPI struct {
	grpc_health_v1.UnimplementedHealthServer
	hold    chan struct{}
	release chan struct{}
}

func (h *blockingHealthAPI) RegisterGRPC(s *grpc.Server) {
	grpc_health_v1.RegisterHealthServer(s, h)
}

func (h *blockingHealthAPI) Watch(_ *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	close(h.hold)
	select {
	case <-h.release:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	return stream.Send(&grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING})
}

func TestServer_BuildConfig(t *testing.T) {
	slot := &Server[*healthAPI]{}
	mat, err := slot.BuildConfig()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := mat.(*Config[*healthAPI]); !ok {
		t.Fatalf("BuildConfig: got %T, want *Config[*healthAPI]", mat)
	}
}

func TestConfig_Build_listenError(t *testing.T) {
	cfg := Config[*healthAPI]{
		Host: common.Host{Value: "127.0.0.1"},
		Port: common.Port{Value: 99999},
	}

	_, err := cfg.Build()
	if err == nil {
		t.Fatal("Build: expected listen error for invalid port")
	}
}
