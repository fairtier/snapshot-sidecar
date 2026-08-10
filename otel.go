package main

// OpenTelemetry wiring. The sidecar is a long-lived, low-traffic process, so
// the instrumentation is deliberately small: one span per save/sync/restore
// (plus a child per remote round-trip), a handful of histograms keyed by
// backend and outcome, and a gauge for the git backend's sync state.
//
// Everything hangs off the OTel *global* providers, which are no-op until
// setupOTel installs the SDK. The global instruments created in newTelemetry
// delegate to the real ones once that happens, so package-level construction
// (and tests that never call setupOTel) are safe.
//
// Export is off unless an OTLP endpoint is configured — see setupOTel.

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// scopeName identifies this package's instrumentation scope.
const scopeName = "github.com/fairtier/snapshot-sidecar"

// Attribute keys. The snapshot.* namespace is ours; error.type follows the
// OTel semantic conventions.
const (
	attrBackend    = attribute.Key("snapshot.backend")     // s3 | git
	attrStatus     = attribute.Key("snapshot.status")      // created | unchanged | busy | remote_changed
	attrTrigger    = attribute.Key("snapshot.trigger")     // rpc | autosave | shutdown | restore
	attrSyncResult = attribute.Key("snapshot.sync.result") // up-to-date | pulled | skipped
	attrGitState   = attribute.Key("snapshot.git.state")   // in-sync | dirty | ahead | behind | diverged
	attrKey        = attribute.Key("snapshot.key")
	attrHash       = attribute.Key("snapshot.hash")
	attrErrorType  = attribute.Key("error.type")
)

// Trigger values, carried on the context by withTrigger.
const (
	triggerRPC      = "rpc"
	triggerAutosave = "autosave"
	triggerShutdown = "shutdown"
	triggerRestore  = "restore"
)

// gitStates is the closed set reported by the snapshot.git.state gauge; each
// is emitted every collection cycle (1 for the current one, 0 for the rest)
// so dashboards never see a stale series.
var gitStates = []string{"in-sync", "dirty", "ahead", "behind", "diverged"}

// tel is the process-wide instrument set. Safe to use before (and without)
// setupOTel — the global providers are no-op until the SDK is installed.
var tel = newTelemetry()

type telemetry struct {
	tracer trace.Tracer
	meter  metric.Meter

	saveDuration    metric.Float64Histogram
	syncDuration    metric.Float64Histogram
	restoreDuration metric.Float64Histogram
	archiveSize     metric.Int64Histogram
	archiveFiles    metric.Int64Histogram
}

func newTelemetry() *telemetry {
	t := &telemetry{
		tracer: otel.Tracer(scopeName),
		meter:  otel.Meter(scopeName),
	}

	t.saveDuration = mustHistogram(t.meter.Float64Histogram(
		"snapshot.save.duration",
		metric.WithDescription("Duration of a save (commit+push, or tar+upload), by outcome."),
		metric.WithUnit("s"),
	))
	t.syncDuration = mustHistogram(t.meter.Float64Histogram(
		"snapshot.sync.duration",
		metric.WithDescription("Duration of a remote sync attempt (git backend)."),
		metric.WithUnit("s"),
	))
	t.restoreDuration = mustHistogram(t.meter.Float64Histogram(
		"snapshot.restore.duration",
		metric.WithDescription("Duration of the restore subcommand (init container)."),
		metric.WithUnit("s"),
	))
	t.archiveSize = mustHistogram(t.meter.Int64Histogram(
		"snapshot.archive.size",
		metric.WithDescription("Compressed size of a snapshot archive (s3 backend)."),
		metric.WithUnit("By"),
	))
	t.archiveFiles = mustHistogram(t.meter.Int64Histogram(
		"snapshot.archive.files",
		metric.WithDescription("Number of entries written into a snapshot archive (s3 backend)."),
		metric.WithUnit("{file}"),
	))

	return t
}

// mustHistogram unwraps instrument construction: the global (and SDK)
// providers only fail on a bad instrument name, which is a programming error.
func mustHistogram[H any](h H, err error) H {
	if err != nil {
		panic("otel: create instrument: " + err.Error())
	}
	return h
}

// observeGitState registers the snapshot.git.state gauge against a live
// classifier. Returns the unregister func (unused in practice — the backend
// outlives the process).
func (t *telemetry) observeGitState(current func() string) error {
	gauge, err := t.meter.Int64ObservableGauge(
		"snapshot.git.state",
		metric.WithDescription("1 for the working clone's current state vs the remote, 0 for the others."),
	)
	if err != nil {
		return err
	}
	_, err = t.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		now := current()
		for _, s := range gitStates {
			var v int64
			if s == now {
				v = 1
			}
			o.ObserveInt64(gauge, v, metric.WithAttributes(attrGitState.String(s)))
		}
		return nil
	}, gauge)
	return err
}

// ── trigger propagation ──────────────────────────────────────────────────────

type triggerKey struct{}

// withTrigger tags a context with what asked for the save, so save() can
// attribute its span and metrics without every caller threading a parameter.
func withTrigger(ctx context.Context, trigger string) context.Context {
	return context.WithValue(ctx, triggerKey{}, trigger)
}

// triggerFrom defaults to rpc: the RPC handlers are the only save callers that
// do not set it explicitly (their context comes from the ConnectRPC stack).
func triggerFrom(ctx context.Context) string {
	if v, ok := ctx.Value(triggerKey{}).(string); ok {
		return v
	}
	return triggerRPC
}

// ── helpers ──────────────────────────────────────────────────────────────────

// recordErr marks a span failed (no-op when err is nil), so callers can defer
// it on the happy path too.
func recordErr(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// withErrType appends error.type only on failure: an always-present empty
// attribute would widen every series for nothing.
func withErrType(attrs []attribute.KeyValue, err error) []attribute.KeyValue {
	if err != nil {
		attrs = append(attrs, attrErrorType.String(errorType(err)))
	}
	return attrs
}

// errorType keeps error.type at low cardinality: a few well-known classes
// plus the semconv fallback, never the message (which carries paths, hashes
// and remote URLs).
func errorType(err error) string {
	if err == nil {
		return ""
	}
	var terr interface{ Timeout() bool }
	switch {
	case errors.Is(err, context.Canceled):
		return "context.Canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context.DeadlineExceeded"
	case errors.As(err, &terr) && terr.Timeout():
		return "timeout"
	}
	return "_OTHER"
}

// ── setup ────────────────────────────────────────────────────────────────────

// setupOTel installs the trace and metric SDKs when an OTLP endpoint is
// configured, and returns a shutdown func that flushes pending telemetry.
//
// Configuration is the standard OTEL_* environment (OTEL_EXPORTER_OTLP_*,
// OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES, OTEL_SDK_DISABLED). With no
// endpoint set, the global providers stay no-op and this is a cheap no-op
// itself — the sidecar runs identically without a collector.
func setupOTel(ctx context.Context, logger *slog.Logger) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }

	if !otelEnabled() {
		logger.Debug("otel export disabled (no OTLP endpoint configured)")
		return noop, nil
	}

	// Exporter errors must not take the sidecar down (or spam stderr through
	// the default handler) — the snapshot job matters, the telemetry does not.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Warn("otel error", "err", err)
	}))

	res, err := resource.New(ctx,
		resource.WithFromEnv(),      // OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES
		resource.WithTelemetrySDK(), // telemetry.sdk.*
		resource.WithHost(),
		resource.WithContainer(),
		resource.WithProcessRuntimeName(),
		resource.WithProcessRuntimeVersion(),
		resource.WithAttributes(
			semconv.ServiceName("snapshot-sidecar"), // overridden by OTEL_SERVICE_NAME
			semconv.ServiceVersion(buildVersion()),
		),
	)
	// A partial resource (e.g. no container id) is still usable.
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return noop, err
	}

	traceExp, err := otlptracehttp.New(ctx)
	if err != nil {
		return noop, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExp),
	)

	metricExp, err := otlpmetrichttp.New(ctx)
	if err != nil {
		_ = tp.Shutdown(ctx)
		return noop, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
	)

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	logger.Info("otel export enabled", "endpoint", otlpEndpoint())

	return func(ctx context.Context) error {
		// Flush metrics first: the final save's data points are the ones
		// most likely to explain a bad shutdown.
		return errors.Join(mp.Shutdown(ctx), tp.Shutdown(ctx))
	}, nil
}

func otelEnabled() bool {
	if os.Getenv("OTEL_SDK_DISABLED") == "true" {
		return false
	}
	return otlpEndpoint() != ""
}

func otlpEndpoint() string {
	for _, k := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// shutdownTimeout bounds the final flush so a wedged collector cannot delay
// pod termination.
const shutdownTimeout = 5 * time.Second
