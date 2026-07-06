package srvgrpc_test

import (
	"reflect"
	"testing"

	srvgrpc "github.com/omcrgnt/srv-grpc"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestGRPCMetrics_RegisterMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := &srvgrpc.GRPCMetrics{}

	if err := m.RegisterMetrics(reg); err != nil {
		t.Fatal(err)
	}

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(m.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(m.StreamServerInterceptor()),
	)
	grpc_health_v1.RegisterHealthServer(s, &grpc_health_v1.UnimplementedHealthServer{})
	m.InitializeMetrics(s)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(mfs) == 0 {
		t.Fatal("expected collectors registered")
	}
}

func TestGRPCMetrics_implementsContributor(t *testing.T) {
	var m srvgrpc.GRPCMetrics
	var _ srvgrpc.MetricsContributor = &m
	if typ := reflect.TypeOf((*srvgrpc.MetricsContributor)(nil)).Elem(); typ.NumMethod() != 1 {
		t.Fatalf("MetricsContributor methods = %d", typ.NumMethod())
	}
}
