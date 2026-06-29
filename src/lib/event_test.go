package lib

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToParquetRow(t *testing.T) {
	ts := time.Date(2024, 5, 1, 12, 30, 45, 123_000_000, time.UTC)
	event := LogEvent{
		LogID:      "abc-123",
		Timestamp:  ts,
		Method:     "POST",
		URI:        "/checkout",
		Protocol:   "HTTP/2.0",
		StatusCode: 201,
		BytesSent:  4096,
		UserAgent:  "curl/8.0",
		ClientID:   "42US",
	}

	row := event.ToParquetRow()

	assert.Equal(t, ts.UnixMilli(), row.Timestamp)
	assert.Equal(t, event.Method, row.Method)
	assert.Equal(t, event.URI, row.URI)
	assert.Equal(t, event.Protocol, row.Protocol)
	assert.Equal(t, event.StatusCode, row.StatusCode)
	assert.Equal(t, event.BytesSent, row.BytesSent)
	assert.Equal(t, event.UserAgent, row.UserAgent)
	assert.Equal(t, event.ClientID, row.ClientID)
}

func TestToParquetRows(t *testing.T) {
	events := []LogEvent{
		{LogID: "1", Method: "GET", URI: "/a", BytesSent: 1},
		{LogID: "2", Method: "POST", URI: "/b", BytesSent: 2},
		{LogID: "3", Method: "PUT", URI: "/c", BytesSent: 3},
	}

	rows := ToParquetRows(events)

	require.Len(t, rows, len(events))
	for i, event := range events {
		assert.Equal(t, event.ToParquetRow(), rows[i], "row order should be preserved")
	}
}

func TestToParquetRowsEmpty(t *testing.T) {
	rows := ToParquetRows(nil)
	assert.NotNil(t, rows, "should return a non-nil (empty) slice")
	assert.Empty(t, rows)

	rows = ToParquetRows([]LogEvent{})
	assert.Empty(t, rows)
}
