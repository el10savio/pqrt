package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/el10savio/pqrt/src/lib"
)

func ptr[T any](v T) *T { return &v }

// newTestQuerier returns a Querier backed by an in-memory DuckDB instance,
// bypassing the httpfs/S3 setup that NewQuerier performs (which requires
// network access and a running MinIO).
func newTestQuerier(t *testing.T) *Querier {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return &Querier{db: db, table: baseTable}
}

func TestExec(t *testing.T) {
	q := newTestQuerier(t)
	ctx := context.Background()

	cases := []struct {
		name string
		sql  string
		args []any
		want [][]string // including the header row
	}{
		{
			name: "header and single row",
			sql:  `SELECT 'GET' AS method, 200 AS status`,
			want: [][]string{{"method", "status"}, {"GET", "200"}},
		},
		{
			name: "multiple rows preserve order",
			sql:  `SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(n, label) ORDER BY n`,
			want: [][]string{{"n", "label"}, {"1", "a"}, {"2", "b"}, {"3", "c"}},
		},
		{
			name: "null rendered as NULL",
			sql:  `SELECT NULL AS missing, 'present' AS value`,
			want: [][]string{{"missing", "value"}, {"NULL", "present"}},
		},
		{
			name: "no data rows yields header only",
			sql:  `SELECT 1 AS n WHERE 1 = 0`,
			want: [][]string{{"n"}},
		},
		{
			name: "args are passed through",
			sql:  `SELECT ? AS echoed`,
			args: []any{42},
			want: [][]string{{"echoed"}, {"42"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := q.Exec(ctx, tc.sql, tc.args...)
			require.NoError(t, err)
			assert.Equal(t, tc.want, rows)
		})
	}
}

func TestExecInvalidSQLReturnsError(t *testing.T) {
	q := newTestQuerier(t)
	_, err := q.Exec(context.Background(), `SELECT FROM WHERE bad`)
	require.Error(t, err)
}

// writeParquet writes rows to a temp parquet file using the same generic
// writer (and therefore the same logical-type schema) as the writer package,
// and returns a read_parquet() table expression pointing at it.
func writeParquet(t *testing.T, rows []lib.ParquetRow) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.parquet")
	f, err := os.Create(path)
	require.NoError(t, err)

	pw := parquet.NewGenericWriter[lib.ParquetRow](f)
	_, err = pw.Write(rows)
	require.NoError(t, err)
	require.NoError(t, pw.Close())
	require.NoError(t, f.Close())

	return fmt.Sprintf("read_parquet('%s')", path)
}

// TestReportQueriesAgainstRealSchema exercises every window-scoped report
// method against a parquet file written with the real ParquetRow schema. The
// timestamp column carries a timestamp(millisecond) logical type, so DuckDB
// surfaces it as TIMESTAMP WITH TIME ZONE — this guards against the queries
// mishandling that type (e.g. treating it as raw epoch millis).
func TestReportQueriesAgainstRealSchema(t *testing.T) {
	now := time.Now().UTC()
	rows := []lib.ParquetRow{
		// Recent rows, inside a generous window.
		{Timestamp: now.Add(-1 * time.Minute).UnixMilli(), Method: "GET", URI: "/a", StatusCode: 200, BytesSent: 1000, ClientID: "c1"},
		{Timestamp: now.Add(-2 * time.Minute).UnixMilli(), Method: "GET", URI: "/a", StatusCode: 500, BytesSent: 2000, ClientID: "c1"},
		{Timestamp: now.Add(-3 * time.Minute).UnixMilli(), Method: "GET", URI: "/b", StatusCode: 200, BytesSent: 500, ClientID: "c2"},
		// Old row, outside the window — should be excluded by window filters.
		{Timestamp: now.Add(-48 * time.Hour).UnixMilli(), Method: "GET", URI: "/old", StatusCode: 200, BytesSent: 9999, ClientID: "c3"},
	}

	q := newTestQuerier(t)
	q.table = writeParquet(t, rows)
	ctx := context.Background()
	const windowSecs = int64(3600) // 1 hour

	t.Run("TopPaths", func(t *testing.T) {
		got, err := q.TopPaths(ctx, 10, windowSecs)
		require.NoError(t, err)
		assert.Equal(t, []string{"uri", "requests"}, got[0])
		// /a (2 hits) ranks above /b (1 hit); /old is outside the window.
		assert.Equal(t, [][]string{{"uri", "requests"}, {"/a", "2"}, {"/b", "1"}}, got)
	})

	t.Run("TopClients", func(t *testing.T) {
		got, err := q.TopClients(ctx, 10, windowSecs)
		require.NoError(t, err)
		assert.Equal(t, [][]string{{"client_id", "requests"}, {"c1", "2"}, {"c2", "1"}}, got)
	})

	t.Run("ErrorRate", func(t *testing.T) {
		got, err := q.ErrorRate(ctx, "hour", windowSecs)
		require.NoError(t, err)
		require.Len(t, got, 2) // header + one bucket
		// 1 of 3 in-window requests is a 5xx.
		assert.Equal(t, "33.333333333333336", got[1][1])
	})

	t.Run("RequestVolume", func(t *testing.T) {
		got, err := q.RequestVolume(ctx, "hour", windowSecs)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "3", got[1][1])
	})

	t.Run("BytesTransferred", func(t *testing.T) {
		got, err := q.BytesTransferred(ctx, "hour", windowSecs)
		require.NoError(t, err)
		require.Len(t, got, 2)
		// (1000 + 2000 + 500) / 1e6 MB.
		assert.Equal(t, "0.0035", got[1][1])
	})

	t.Run("StatusCodes", func(t *testing.T) {
		// StatusCodes has no window filter, so the old row is included.
		got, err := q.StatusCodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"status_code", "requests"}, got[0])
		assert.Equal(t, [][]string{{"status_code", "requests"}, {"200", "3"}, {"500", "1"}}, got)
	})
}
