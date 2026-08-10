package main

import (
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// installTestOTel points the globals at in-memory SDKs and rebuilds the
// process-wide instrument set against them. Not parallel-safe (it mutates
// globals), hence the single test below.
func installTestOTel(t *testing.T) (*tracetest.SpanRecorder, *sdkmetric.ManualReader) {
	t.Helper()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	tel = newTelemetry()

	t.Cleanup(func() {
		_ = tp.Shutdown(t.Context())
		_ = mp.Shutdown(t.Context())
	})
	return sr, reader
}

func spanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

func spanAttr(s sdktrace.ReadOnlySpan, key attribute.Key) string {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value.String()
		}
	}
	return ""
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]metricdata.Aggregation{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m.Data
		}
	}
	return out
}

// TestSaveTelemetry drives a real git save and asserts the span tree and the
// metric data points that come out of it.
func TestSaveTelemetry(t *testing.T) {
	sr, reader := installTestOTel(t)

	remote, _ := seededRemote(t)
	dir := t.TempDir()
	b := mustBackend(t, testCfg(dir, remote))

	if err := os.WriteFile(filepath.Join(dir, "model.sql"), []byte("select 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx := withTrigger(t.Context(), triggerAutosave)
	resp, err := b.save(ctx)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := resp.GetStatus(); got != "created" {
		t.Fatalf("status = %q, want created", got)
	}

	// ── spans ──
	spans := sr.Ended()
	save := spanByName(spans, "snapshot.save")
	if save == nil {
		t.Fatal("no snapshot.save span recorded")
	}
	for _, tc := range []struct {
		key  attribute.Key
		want string
	}{
		{attrBackend, backendGit},
		{attrTrigger, triggerAutosave},
		{attrStatus, "created"},
	} {
		if got := spanAttr(save, tc.key); got != tc.want {
			t.Errorf("save span %s = %q, want %q", tc.key, got, tc.want)
		}
	}
	if spanAttr(save, attrHash) == "" {
		t.Error("save span missing snapshot.hash")
	}

	push := spanByName(spans, "git.push")
	if push == nil {
		t.Fatal("no git.push span recorded")
	}
	if push.Parent().SpanID() != save.SpanContext().SpanID() {
		t.Error("git.push is not a child of snapshot.save")
	}

	// ── metrics ──
	metrics := collect(t, reader)

	hist, ok := metrics["snapshot.save.duration"].(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("snapshot.save.duration missing or wrong type: %T", metrics["snapshot.save.duration"])
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("save.duration data points = %d, want 1", len(hist.DataPoints))
	}
	dp := hist.DataPoints[0]
	if dp.Count != 1 {
		t.Errorf("save.duration count = %d, want 1", dp.Count)
	}
	if v, ok := dp.Attributes.Value(attrStatus); !ok || v.String() != "created" {
		t.Errorf("save.duration status attribute = %q, want created", v.String())
	}
	// error.type is only attached on failure.
	if _, ok := dp.Attributes.Value(attrErrorType); ok {
		t.Error("save.duration carries error.type on a successful save")
	}

	gauge, ok := metrics["snapshot.git.state"].(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("snapshot.git.state missing or wrong type: %T", metrics["snapshot.git.state"])
	}
	if len(gauge.DataPoints) != len(gitStates) {
		t.Fatalf("git.state data points = %d, want %d", len(gauge.DataPoints), len(gitStates))
	}
	var current []string
	for _, dp := range gauge.DataPoints {
		if dp.Value == 1 {
			v, _ := dp.Attributes.Value(attrGitState)
			current = append(current, v.String())
		}
	}
	if len(current) != 1 || current[0] != "in-sync" {
		t.Errorf("git.state reports %v as current, want exactly [in-sync]", current)
	}
}
