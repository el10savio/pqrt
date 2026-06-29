package writer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/el10savio/pqrt/src/lib"
)

func TestPartitionKey(t *testing.T) {
	cases := []struct {
		name string
		ts   time.Time
		want string
	}{
		{
			name: "utc afternoon",
			ts:   time.Date(2024, 5, 1, 14, 23, 5, 0, time.UTC),
			want: "year=2024/month=05/day=01/hour=14",
		},
		{
			name: "single digit month and day are zero padded",
			ts:   time.Date(2023, 2, 3, 4, 0, 0, 0, time.UTC),
			want: "year=2023/month=02/day=03/hour=04",
		},
		{
			name: "epoch",
			ts:   time.UnixMilli(0).UTC(),
			want: "year=1970/month=01/day=01/hour=00",
		},
		{
			name: "end of year midnight",
			ts:   time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
			want: "year=2025/month=12/day=31/hour=00",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, partitionKey(tc.ts.UnixMilli()))
		})
	}
}

func TestPartitionKeyConvertsToUTC(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	local := time.Date(2024, 1, 1, 23, 30, 0, 0, loc)
	assert.Equal(t, "year=2024/month=01/day=02/hour=04", partitionKey(local.UnixMilli()))
}

func rowAt(t time.Time) lib.ParquetRow {
	return lib.ParquetRow{Timestamp: t.UnixMilli()}
}

func TestAddBuffersBelowFlushSize(t *testing.T) {
	w := NewWriter(nil, "logs", 1000)
	ts := time.Date(2024, 5, 1, 14, 0, 0, 0, time.UTC)
	key := partitionKey(ts.UnixMilli())

	for i := 0; i < 5; i++ {
		require.NoError(t, w.Add(rowAt(ts)))
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	assert.Len(t, w.buffers[key], 5, "rows should accumulate in the partition buffer")
	assert.Equal(t, 0, w.fileCounter[key], "no flush should have occurred yet")
}

func TestAddSeparatesPartitions(t *testing.T) {
	w := NewWriter(nil, "logs", 1000)
	hourA := time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC)
	hourB := time.Date(2024, 5, 1, 11, 0, 0, 0, time.UTC)

	require.NoError(t, w.Add(rowAt(hourA)))
	require.NoError(t, w.Add(rowAt(hourB)))
	require.NoError(t, w.Add(rowAt(hourB)))

	w.mu.Lock()
	defer w.mu.Unlock()
	assert.Len(t, w.buffers[partitionKey(hourA.UnixMilli())], 1)
	assert.Len(t, w.buffers[partitionKey(hourB.UnixMilli())], 2)
}

func TestFlushAllClearsBuffersWithoutData(t *testing.T) {
	w := NewWriter(nil, "logs", 1000)

	w.mu.Lock()
	w.buffers["year=2024/month=05/day=01/hour=00"] = nil
	w.mu.Unlock()

	require.NoError(t, w.FlushAll(context.Background()))

	w.mu.Lock()
	defer w.mu.Unlock()
	assert.Empty(t, w.buffers, "FlushAll should clear all buffer keys")
}
