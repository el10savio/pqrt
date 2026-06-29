package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/el10savio/pqrt/src/pkg/metrics"
	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type Producer struct {
	writer  *kafkago.Writer
	logger  *zap.Logger
	limiter *rate.Limiter
}

func NewProducer(brokers string, rateLimitRPS int, logger *zap.Logger) *Producer {
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(strings.Split(brokers, ",")...),
		Topic:        TopicIngest,
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireOne,
		Async:        false,
		BatchTimeout: 1 * time.Millisecond,
		BatchSize:    1000,
	}
	var lim *rate.Limiter
	if rateLimitRPS > 0 {
		lim = rate.NewLimiter(rate.Limit(rateLimitRPS), rateLimitRPS/10)
	}
	logger.Info("kafka producer initialised",
		zap.String("brokers", brokers),
		zap.String("topic", TopicIngest),
		zap.Int("rate_limit_rps", rateLimitRPS),
	)
	return &Producer{writer: w, logger: logger, limiter: lim}
}

func (p *Producer) Publish(ctx context.Context, event LogEvent) error {
	if p.limiter != nil {
		if err := p.limiter.Wait(ctx); err != nil {
			return err
		}
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	start := time.Now()
	err = p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(event.LogID),
		Value: payload,
	})
	metrics.KafkaPublishLatency.Record(ctx, time.Since(start).Seconds())

	if err != nil {
		metrics.PublishErrors.Add(ctx, 1)
		p.logger.Error("kafka publish failed", zap.Error(err))
		return err
	}

	metrics.EventsPublished.Add(ctx, 1)
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
