package kafka

import (
	"context"

	"github.com/el10savio/pqrt/src/lib"
)

const TopicIngest = "http.logs"

type LogEvent = lib.LogEvent

type EventPublisher interface {
	Publish(ctx context.Context, event LogEvent) error
}
