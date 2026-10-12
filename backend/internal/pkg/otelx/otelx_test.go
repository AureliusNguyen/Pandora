package otelx_test

import (
	"context"
	"testing"

	"github.com/aureliusnguyen/pandora/backend/internal/pkg/otelx"
)

func TestSetupExportsOTelMetricsToRegistry(t *testing.T) {
	ctx := context.Background()
	tel, err := otelx.Setup(ctx, otelx.Options{ServiceName: "otelx-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tel.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	c, err := tel.MeterProvider.Meter("test").Int64Counter("pandora_test_hits")
	if err != nil {
		t.Fatal(err)
	}
	c.Add(ctx, 3)
	mfs, err := tel.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, want := range []string{"pandora_test_hits_total", "go_goroutines"} {
		if !names[want] {
			t.Errorf("metric %s missing; have %v", want, names)
		}
	}
}

func TestSetupRequiresServiceName(t *testing.T) {
	if _, err := otelx.Setup(context.Background(), otelx.Options{}); err == nil {
		t.Fatal("want error for empty ServiceName")
	}
}
