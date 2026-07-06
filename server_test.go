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
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
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

	if err := server.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Close(context.Background())
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

	if err := server.Close(t.Context()); err != nil {
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

	s.Inject([]any{api, metrics})
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

	err = s.Start(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Start: got %v, want context.Canceled", err)
	}
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

	if err := s.Start(context.Background()); err != nil {
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

	if err := s.Start(context.Background()); err != nil {
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
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	if err := s.ProbeReady(ctx); err != nil {
		t.Fatalf("ProbeReady after Start: %v", err)
	}
	if err := s.HealthCheck(ctx); err != nil {
		t.Fatalf("HealthCheck after Start: %v", err)
	}
}

func TestClose_cancelledContext(t *testing.T) {
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

	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		_ = server.Close(context.Background())
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

	if err := server.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close with cancelled context: got %v, want context.Canceled", err)
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
