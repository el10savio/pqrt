package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// parseMessage must reject anything that is not a valid JSON object, returning a
// zero-value event, while a valid payload is decoded onto our struct tags.
func TestParseMessage(t *testing.T) {
	c := &Consumer{logger: zap.NewNop()}

	cases := []struct {
		name   string
		value  []byte
		wantOK bool
	}{
		{"valid object", []byte(`{"log_id":"abc","method":"GET"}`), true},
		{"empty object decodes to zero value", []byte("{}"), true},
		{"malformed json", []byte("{not valid json"), false},
		{"nil payload", nil, false},
		{"empty payload", []byte(""), false},
		{"non-object json", []byte("12345"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event, ok := c.parseMessage(kafkago.Message{Value: tc.value})
			assert.Equal(t, tc.wantOK, ok)
			if !tc.wantOK {
				assert.Equal(t, LogEvent{}, event, "failed parse returns a zero-value event")
			}
		})
	}
}

func TestParseMessageMapsFields(t *testing.T) {
	c := &Consumer{logger: zap.NewNop()}

	event, ok := c.parseMessage(kafkago.Message{
		Value: []byte(`{"log_id":"abc","method":"GET","status_code":200,"bytes_sent":512}`),
	})
	assert.True(t, ok)
	assert.Equal(t, "abc", event.LogID)
	assert.Equal(t, "GET", event.Method)
	assert.Equal(t, int32(200), event.StatusCode)
	assert.Equal(t, int64(512), event.BytesSent)
}
