package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	appinfra "github.com/el10savio/pqrt/src/infra/minio"
	appkafka "github.com/el10savio/pqrt/src/kafka"
	"github.com/el10savio/pqrt/src/lib"
	"github.com/el10savio/pqrt/src/pkg/logger"
	"github.com/el10savio/pqrt/src/pkg/metrics"
	"github.com/el10savio/pqrt/src/query"
	"github.com/el10savio/pqrt/src/writer"
	"github.com/olekukonko/tablewriter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// publishLatencyBudget is a generous worst-case for a single synchronous
	// publish (broker round-trip). It feeds publisherPoolSize via Little's Law;
	// over-estimating it just over-provisions the pool, which is free because the
	// rate limiter, not the pool, caps throughput.
	publishLatencyBudget = 20 * time.Millisecond
	kafkaGroupID         = "logs-consumer"
	// flushSize is the row count per Parquet file. Larger files mean far fewer
	// objects in MinIO, which keeps query-time S3 round-trips (one footer read
	// per file) low. ~50k rows lands each file around a couple MB.
	flushSize    = 50000
	batchSize    = 1000
	batchTimeout = 400 * time.Millisecond
)

type config struct {
	KafkaBrokers         string `env:"KAFKA_BROKERS"           envDefault:"localhost:9092"`
	ProducerRateLimitRPS int    `env:"PRODUCER_RATE_LIMIT_RPS" envDefault:"0"`
	MaxEvents            int64  `env:"MAX_EVENTS"              envDefault:"0"`
}

func setupOTel() (*sdkmetric.MeterProvider, error) {
	exporter, err := otelprom.New()
	if err != nil {
		return nil, fmt.Errorf("otel prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(provider)
	if err := metrics.Init(provider.Meter("prqt")); err != nil {
		return nil, fmt.Errorf("init metrics: %w", err)
	}
	return provider, nil
}

// publisherPoolSize derives how many concurrent publishers are needed to
// sustain rateRPS. Because each publish blocks for up to publishLatencyBudget,
// Little's Law gives the in-flight count to saturate the rate as
// rate × latency, rounded up. The rate limiter still caps throughput, so this
// only needs to be "enough"; we over-provide rather than risk undershooting.
// A non-positive rate means unbounded, so fall back to a fixed pool.
func publisherPoolSize(rateRPS int) int {
	if rateRPS <= 0 {
		return 50
	}
	n := (rateRPS*int(publishLatencyBudget.Milliseconds()) + 999) / 1000
	if n < 1 {
		n = 1
	}
	return n
}

func mustBootstrap() (config, *zap.Logger) {
	cfg := config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	zapLogger, err := logger.NewLogger()
	if err != nil {
		log.Fatalf("init logger: %v", err)
	}
	return cfg, zapLogger
}

func newProducerCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "producer",
		Short: "Generate fake HTTP log events and publish to Kafka",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, zapLogger := mustBootstrap()
			defer zapLogger.Sync()

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
			defer stop()

			provider, err := setupOTel()
			if err != nil {
				return err
			}
			defer provider.Shutdown(ctx)

			go func() {
				http.ListenAndServe(":2112", promhttp.Handler())
			}()

			producer := appkafka.NewProducer(cfg.KafkaBrokers, cfg.ProducerRateLimitRPS, zapLogger)
			defer producer.Close()

			g, gctx := errgroup.WithContext(ctx)
			g.SetLimit(publisherPoolSize(cfg.ProducerRateLimitRPS))

			var published atomic.Int64
			for n := int64(0); cfg.MaxEvents == 0 || n < cfg.MaxEvents; n++ {
				if gctx.Err() != nil {
					break
				}
				g.Go(func() error {
					event := lib.CreateLogEvent()
					if err := producer.Publish(gctx, event); err != nil {
						// Tolerate transient publish errors: log and move on
						// rather than aborting the whole run via the group.
						if gctx.Err() == nil {
							zapLogger.Warn("publish error", zap.Error(err))
						}
						return nil
					}
					published.Add(1)
					return nil
				})
			}

			if err := g.Wait(); err != nil {
				return err
			}
			zapLogger.Info("producer stopped", zap.Int64("published", published.Load()))
			return nil
		},
	}
}

func newConsumerCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "consumer",
		Short: "Consume log events from Kafka and write Parquet files to MinIO",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, zapLogger := mustBootstrap()
			defer zapLogger.Sync()

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
			defer stop()

			provider, err := setupOTel()
			if err != nil {
				return err
			}
			defer provider.Shutdown(ctx)

			go func() {
				http.ListenAndServe(":2113", promhttp.Handler())
			}()

			minioClient, err := appinfra.NewClient()
			if err != nil {
				return fmt.Errorf("minio client: %w", err)
			}

			w := writer.NewWriter(minioClient, "logs", flushSize)

			consumer := appkafka.NewConsumer(cfg.KafkaBrokers, kafkaGroupID, batchSize, batchTimeout, zapLogger)
			defer consumer.Close()

			return consumer.Run(ctx, w)
		},
	}
}

func newQueryCmd() *cobra.Command {
	queryCmd := &cobra.Command{
		Use:   "query",
		Short: "Query Parquet files in MinIO via DuckDB",
	}
	queryCmd.AddCommand(
		newTopPathsCmd(),
		newErrorRateCmd(),
		newRequestVolumeCmd(),
		newBytesTransferredCmd(),
		newStatusCodesCmd(),
		newTopClientsCmd(),
	)
	return queryCmd
}

func renderTable(rows [][]string) {
	if len(rows) < 2 {
		fmt.Println("(no results)")
		return
	}
	t := tablewriter.NewWriter(os.Stdout)
	headerAny := make([]any, len(rows[0]))
	for i, h := range rows[0] {
		headerAny[i] = h
	}
	t.Header(headerAny...)
	for _, row := range rows[1:] {
		rowAny := make([]any, len(row))
		for i, v := range row {
			rowAny[i] = v
		}
		t.Append(rowAny...)
	}
	t.Render()
}

func newTopPathsCmd() *cobra.Command {
	var limit int
	var window string
	cmd := &cobra.Command{
		Use:   "top-paths",
		Short: "Top URIs by request count",
		RunE: func(cmd *cobra.Command, args []string) error {
			dur, err := time.ParseDuration(window)
			if err != nil {
				return fmt.Errorf("invalid --window %q: %w", window, err)
			}
			q, err := query.NewQuerier()
			if err != nil {
				return err
			}
			defer q.Close()
			rows, err := q.TopPaths(cmd.Context(), limit, int64(dur.Seconds()))
			if err != nil {
				return err
			}
			renderTable(rows)
			return nil
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 10, "Number of results to return")
	cmd.Flags().StringVar(&window, "window", "1h", "Time window to query (e.g. 30m, 1h, 24h)")
	return cmd
}

func newErrorRateCmd() *cobra.Command {
	var granularity, window string
	cmd := &cobra.Command{
		Use:   "error-rate",
		Short: "Error rate percentage over time",
		RunE: func(cmd *cobra.Command, args []string) error {
			dur, err := time.ParseDuration(window)
			if err != nil {
				return fmt.Errorf("invalid --window %q: %w", window, err)
			}
			q, err := query.NewQuerier()
			if err != nil {
				return err
			}
			defer q.Close()
			rows, err := q.ErrorRate(cmd.Context(), granularity, int64(dur.Seconds()))
			if err != nil {
				return err
			}
			renderTable(rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&granularity, "granularity", "hour", "Time granularity (day, hour, minute)")
	cmd.Flags().StringVar(&window, "window", "24h", "Time window to query (e.g. 1h, 6h, 24h)")
	return cmd
}

func newRequestVolumeCmd() *cobra.Command {
	var granularity, window string
	cmd := &cobra.Command{
		Use:   "request-volume",
		Short: "Request count over time",
		RunE: func(cmd *cobra.Command, args []string) error {
			dur, err := time.ParseDuration(window)
			if err != nil {
				return fmt.Errorf("invalid --window %q: %w", window, err)
			}
			q, err := query.NewQuerier()
			if err != nil {
				return err
			}
			defer q.Close()
			rows, err := q.RequestVolume(cmd.Context(), granularity, int64(dur.Seconds()))
			if err != nil {
				return err
			}
			renderTable(rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&granularity, "granularity", "hour", "Time granularity (day, hour, minute)")
	cmd.Flags().StringVar(&window, "window", "24h", "Time window to query (e.g. 1h, 6h, 24h)")
	return cmd
}

func newBytesTransferredCmd() *cobra.Command {
	var granularity, window string
	cmd := &cobra.Command{
		Use:   "bytes-transferred",
		Short: "MB transferred over time",
		RunE: func(cmd *cobra.Command, args []string) error {
			dur, err := time.ParseDuration(window)
			if err != nil {
				return fmt.Errorf("invalid --window %q: %w", window, err)
			}
			q, err := query.NewQuerier()
			if err != nil {
				return err
			}
			defer q.Close()
			rows, err := q.BytesTransferred(cmd.Context(), granularity, int64(dur.Seconds()))
			if err != nil {
				return err
			}
			renderTable(rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&granularity, "granularity", "hour", "Time granularity (day, hour, minute)")
	cmd.Flags().StringVar(&window, "window", "24h", "Time window to query (e.g. 1h, 6h, 24h)")
	return cmd
}

func newStatusCodesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status-codes",
		Short: "Request count per HTTP status code",
		RunE: func(cmd *cobra.Command, args []string) error {
			q, err := query.NewQuerier()
			if err != nil {
				return err
			}
			defer q.Close()
			rows, err := q.StatusCodes(cmd.Context())
			if err != nil {
				return err
			}
			renderTable(rows)
			return nil
		},
	}
}

func newTopClientsCmd() *cobra.Command {
	var limit int
	var window string
	cmd := &cobra.Command{
		Use:   "top-clients",
		Short: "Top client IDs by request count",
		RunE: func(cmd *cobra.Command, args []string) error {
			dur, err := time.ParseDuration(window)
			if err != nil {
				return fmt.Errorf("invalid --window %q: %w", window, err)
			}
			q, err := query.NewQuerier()
			if err != nil {
				return err
			}
			defer q.Close()
			rows, err := q.TopClients(cmd.Context(), limit, int64(dur.Seconds()))
			if err != nil {
				return err
			}
			renderTable(rows)
			return nil
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 10, "Number of results to return")
	cmd.Flags().StringVar(&window, "window", "1h", "Time window to query (e.g. 30m, 1h, 24h)")
	return cmd
}

func main() {
	rootCmd := &cobra.Command{
		Use:          "pqrt",
		Short:        "Streaming log analytics pipeline",
		SilenceUsage: true,
	}
	rootCmd.AddCommand(newProducerCmd())
	rootCmd.AddCommand(newConsumerCmd())
	rootCmd.AddCommand(newQueryCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
