package srvgrpc

import (
	"github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/omcrgnt/res/unique"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// MetricsContributor registers gRPC metrics collectors into a shared registry.
// Same method signature as github.com/omcrgnt/ops/metrics.MetricsContributor.
type MetricsContributor interface {
	RegisterMetrics(reg *prom.Registry) error
}

// GRPCMetrics is a singleton pool resource: contributor + shared prometheus interceptors for all srv-grpc servers.
type GRPCMetrics struct {
	srv *prometheus.ServerMetrics
}

func (m *GRPCMetrics) RegisterMetrics(reg *prom.Registry) error {
	m.srv = prometheus.NewServerMetrics()
	reg.MustRegister(m.srv)
	return nil
}

func (m *GRPCMetrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return m.srv.UnaryServerInterceptor()
}

func (m *GRPCMetrics) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return m.srv.StreamServerInterceptor()
}

func (m *GRPCMetrics) InitializeMetrics(s *grpc.Server) {
	m.srv.InitializeMetrics(s)
}

var _ MetricsContributor = (*GRPCMetrics)(nil)

func init() {
	unique.MustAddFixed(&GRPCMetrics{})
}
