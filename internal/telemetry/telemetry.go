// Package telemetry supplies low-cardinality scheduler metrics and tracing.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"

	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	Transitions           = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_transitions_total", Help: "Committed job state transitions."}, []string{"status"})
	LeaseLosses           = prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_lease_losses_total", Help: "Executions canceled after losing their lease."})
	Latency               = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "scheduler_job_duration_seconds", Help: "Handler execution duration.", Buckets: prometheus.ExponentialBuckets(0.01, 4, 10)})
	QueueDepth            = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "scheduler_jobs", Help: "Persisted jobs by status."}, []string{"status"})
	OutboxLag             = prometheus.NewGauge(prometheus.GaugeOpts{Name: "scheduler_outbox_lag_seconds", Help: "Age of the oldest unpublished outbox message."})
	OutboxDepth           = prometheus.NewGauge(prometheus.GaugeOpts{Name: "scheduler_outbox_pending", Help: "Unpublished outbox messages."})
	HandlerErrors         = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_handler_failures_total", Help: "Handler failures by error category."}, []string{"kind"})
	OutboxPublishFailures = prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_outbox_publish_failures_total", Help: "Failed outbox publish attempts."})
	OutboxDeadLettered    = prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_outbox_dead_lettered_total", Help: "Outbox announcements moved to the dead letter."})
	OutboxExhaustedJobs   = prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_outbox_exhausted_jobs_total", Help: "Jobs terminalized after their dispatch announcements exhausted the dead-letter cap."})
	NoWorkerDeferrals     = prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_no_worker_deferrals_total", Help: "Announcements refused because no worker queue exists for the job name; the job is retried later."})
	NoWorkerDeadJobs      = prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_no_worker_dead_jobs_total", Help: "Jobs dead after no worker handled their job name within NO_WORKER_TIMEOUT."})
	StatsSampleFailures   = prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_stats_sample_failures_total", Help: "Failed persistent metrics samples."})
	StatsLastSuccess      = prometheus.NewGauge(prometheus.GaugeOpts{Name: "scheduler_stats_last_success_timestamp_seconds", Help: "Unix time of the last successful persistent metrics sample, or zero if none."})
)

func init() {
	prometheus.MustRegister(Transitions, LeaseLosses, Latency, QueueDepth, OutboxLag, OutboxDepth,
		HandlerErrors, OutboxPublishFailures, OutboxDeadLettered, OutboxExhaustedJobs, NoWorkerDeferrals, NoWorkerDeadJobs, StatsSampleFailures, StatsLastSuccess)
	for _, kind := range []string{"error", "panic", "canceled", "timeout"} {
		HandlerErrors.WithLabelValues(kind)
	}
}

// Init configures JSON logging and optional OTLP export via standard OTEL env vars.
func Init(ctx context.Context, name string) (func(context.Context) error, error) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		slog.Error("telemetry error", "error", err)
	}))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	options := []sdktrace.TracerProviderOption{sdktrace.WithResource(resource.NewWithAttributes("", attribute.String("service.name", name)))}
	if endpoint := otlpTraceEndpoint(); endpoint != "" {
		if err := validateOTLPTraceTLS(os.Getenv("APP_ENV") == "development"); err != nil {
			return nil, err
		}
		exporter, err := otlptracegrpc.New(ctx)
		if err != nil {
			return nil, err
		}
		options = append(options, sdktrace.WithBatcher(exporter))
	}
	provider := sdktrace.NewTracerProvider(append(options, sdktrace.WithSampler(sdktrace.ParentBased(traceSampleRatio())))...)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

// traceSampleRatio reads OTEL_TRACES_SAMPLER_ARG (a ratio in [0,1]);
// it defaults to full sampling, mirroring the OTEL spec's default sampler.
func traceSampleRatio() sdktrace.Sampler {
	arg := os.Getenv("OTEL_TRACES_SAMPLER_ARG")
	if arg == "" {
		return sdktrace.AlwaysSample()
	}
	ratio, err := strconv.ParseFloat(arg, 64)
	if err != nil || ratio < 0 || ratio > 1 {
		slog.Warn("invalid OTEL_TRACES_SAMPLER_ARG; sampling everything", "value", arg)
		return sdktrace.AlwaysSample()
	}
	return sdktrace.TraceIDRatioBased(ratio)
}

func otlpTraceEndpoint() string {
	if endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
}

func validateOTLPTraceTLS(development bool) error {
	if development {
		return nil
	}
	// The SDK processes generic variables first, then trace-specific variables.
	// Reject malformed endpoints as well as any sources of plaintext settings.
	for _, key := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"} {
		if endpoint := os.Getenv(key); endpoint != "" {
			u, err := url.Parse(endpoint)
			if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil {
				return fmt.Errorf("%s requires an HTTPS endpoint outside development", key)
			}
		}
	}
	for _, key := range []string{"OTEL_EXPORTER_OTLP_INSECURE", "OTEL_EXPORTER_OTLP_TRACES_INSECURE"} {
		if value := os.Getenv(key); value != "" && value != "false" {
			return fmt.Errorf("%s cannot disable TLS outside development", key)
		}
	}
	return nil
}
