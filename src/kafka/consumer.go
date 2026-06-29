package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/el10savio/pqrt/src/lib"
	"github.com/el10savio/pqrt/src/pkg/metrics"
	"github.com/el10savio/pqrt/src/writer"
	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Consumer struct {
	reader       *kafkago.Reader
	logger       *zap.Logger
	batchSize    int
	batchTimeout time.Duration
}

func NewConsumer(brokers, groupID string, batchSize int, batchTimeout time.Duration, logger *zap.Logger) *Consumer {
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  strings.Split(brokers, ","),
		Topic:    TopicIngest,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	logger.Info("kafka consumer initialised",
		zap.String("brokers", brokers),
		zap.String("topic", TopicIngest),
		zap.String("group_id", groupID),
		zap.Int("batch_size", batchSize),
		zap.Duration("batch_timeout", batchTimeout),
	)
	return &Consumer{reader: r, logger: logger, batchSize: batchSize, batchTimeout: batchTimeout}
}

func (c *Consumer) Run(ctx context.Context, w *writer.Writer) error {
	go c.trackLag(ctx)

	for {
		msgs, events := c.collectBatch(ctx)
		if ctx.Err() != nil {
			return w.FlushAll(ctx)
		}
		if len(msgs) == 0 {
			continue
		}

		for _, event := range events {
			if err := w.Add(event.ToParquetRow()); err != nil {
				c.logger.Error("writer add failed", zap.Error(err))
				metrics.ConsumerErrors.Add(ctx, 1)
			}
		}
		metrics.EventsConsumed.Add(ctx, int64(len(events)))

		if err := c.reader.CommitMessages(ctx, msgs...); err != nil {
			c.logger.Error("kafka commit failed", zap.Error(err))
		}
	}
}

// collectBatch fetches up to batchSize messages within batchTimeout.
// Messages that fail JSON parsing are committed immediately so they do not
// block the batch; only successfully parsed events are returned.
func (c *Consumer) collectBatch(ctx context.Context) (msgs []kafkago.Message, events []LogEvent) {
	deadline := time.Now().Add(c.batchTimeout)

	for len(events) < c.batchSize {
		fetchCtx, cancel := context.WithDeadline(ctx, deadline)
		msg, err := c.reader.FetchMessage(fetchCtx)
		cancel()

		if err != nil {
			return
		}

		event, ok := c.parseMessage(msg)
		if !ok {
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		msgs = append(msgs, msg)
		events = append(events, event)
	}
	return
}

func (c *Consumer) parseMessage(msg kafkago.Message) (LogEvent, bool) {
	var event lib.LogEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.logger.Error("decode event failed", zap.Error(err), zap.ByteString("value", msg.Value))
		metrics.ConsumerErrors.Add(context.Background(), 1)
		return LogEvent{}, false
	}
	return event, true
}

func (c *Consumer) trackLag(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.KafkaLag.Record(ctx, float64(c.reader.Stats().Lag))
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
