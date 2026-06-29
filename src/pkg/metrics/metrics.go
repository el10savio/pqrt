package metrics

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	// Producer
	EventsPublished     metric.Int64Counter
	KafkaPublishLatency metric.Float64Histogram
	PublishErrors       metric.Int64Counter

	// Consumer
	EventsConsumed metric.Int64Counter
	KafkaLag       metric.Float64Gauge
	ConsumerErrors metric.Int64Counter

	// Writer
	BatchFlushSize      metric.Float64Histogram
	ParquetWriteLatency metric.Float64Histogram
	MinioUploadLatency  metric.Float64Histogram
	UploadErrors        metric.Int64Counter
)

func init() {
	_ = Init(otel.GetMeterProvider().Meter("prqt"))
}

func Init(meter metric.Meter) (err error) {
	latencyBuckets := metric.WithExplicitBucketBoundaries(
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
	)

	if EventsPublished, err = meter.Int64Counter("producer_events_published_total",
		metric.WithDescription("Total Kafka messages successfully published"),
	); err != nil {
		return
	}
	if KafkaPublishLatency, err = meter.Float64Histogram("producer_kafka_publish_latency_seconds",
		metric.WithDescription("Latency of Kafka publish calls"),
		metric.WithUnit("s"),
		latencyBuckets,
	); err != nil {
		return
	}
	if PublishErrors, err = meter.Int64Counter("producer_publish_errors_total",
		metric.WithDescription("Total Kafka publish errors"),
	); err != nil {
		return
	}
	if EventsConsumed, err = meter.Int64Counter("consumer_events_consumed_total",
		metric.WithDescription("Total Kafka messages consumed"),
	); err != nil {
		return
	}
	if KafkaLag, err = meter.Float64Gauge("consumer_kafka_lag",
		metric.WithDescription("Difference between latest and committed Kafka offset"),
	); err != nil {
		return
	}
	if ConsumerErrors, err = meter.Int64Counter("consumer_errors_total",
		metric.WithDescription("Total consumer processing errors"),
	); err != nil {
		return
	}
	if BatchFlushSize, err = meter.Float64Histogram("consumer_batch_flush_size",
		metric.WithDescription("Number of rows per Parquet flush"),
		metric.WithExplicitBucketBoundaries(100, 500, 1000, 2500, 5000, 7500, 10000),
	); err != nil {
		return
	}
	if ParquetWriteLatency, err = meter.Float64Histogram("consumer_parquet_write_latency_seconds",
		metric.WithDescription("Latency of writing rows to a Parquet temp file"),
		metric.WithUnit("s"),
		latencyBuckets,
	); err != nil {
		return
	}
	if MinioUploadLatency, err = meter.Float64Histogram("consumer_minio_upload_latency_seconds",
		metric.WithDescription("Latency of uploading a Parquet file to MinIO"),
		metric.WithUnit("s"),
		latencyBuckets,
	); err != nil {
		return
	}
	if UploadErrors, err = meter.Int64Counter("consumer_upload_errors_total",
		metric.WithDescription("Total MinIO upload errors"),
	); err != nil {
		return
	}
	return nil
}
