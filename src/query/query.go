package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// baseTable reads every Parquet file under the Hive-partitioned prefix.
// hive_types_autocast=0 keeps year/month/day/hour as zero-padded VARCHAR so
// partitionPrune can match them against the exact path text.
const baseTable = `read_parquet('s3://logs/data/**/*.parquet', hive_partitioning=true, hive_types_autocast=0)`

// maxPruneHours caps how many hour-partition clauses partitionPrune will emit.
// Beyond this it falls back to a full scan rather than building a huge predicate.
const maxPruneHours = 24 * 31

type Querier struct {
	db *sql.DB
	// table is the SQL table expression the report methods read from.
	// Defaults to baseTable; overridable in tests to point at local data.
	table string
	// hivePartitioned enables partition pruning. Only true for the real S3
	// base table; tests that override table point at a flat file with no
	// year/month/day/hour columns, so pruning must stay off for them.
	hivePartitioned bool
}

func NewQuerier() (*Querier, error) {
	endpoint := getenv("MINIO_ENDPOINT", "minio:9000")
	accessKey := getenv("MINIO_ACCESS_KEY", "minioadmin")
	secretKey := getenv("MINIO_SECRET_KEY", "minioadmin")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	for _, stmt := range []string{
		`INSTALL httpfs`,
		`LOAD httpfs`,
		fmt.Sprintf(`CREATE OR REPLACE SECRET minio (
			TYPE s3,
			KEY_ID '%s',
			SECRET '%s',
			ENDPOINT '%s',
			USE_SSL false,
			URL_STYLE 'path'
		)`, accessKey, secretKey, endpoint),
	} {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("duckdb init: %w", err)
		}
	}

	return &Querier{db: db, table: baseTable, hivePartitioned: true}, nil
}

func (q *Querier) Close() error { return q.db.Close() }

// partitionPrune returns a SQL fragment that restricts the scan to the
// year/month/day/hour partitions overlapping [now-window, now] in UTC, so
// DuckDB skips Parquet files outside the window instead of opening every
// footer. It returns "" when pruning does not apply (non-hive table, or a
// window so wide it would emit more than maxPruneHours clauses). The fragment
// is intentionally permissive at the edges — the timestamp predicate in the
// caller still trims results to the exact window.
func (q *Querier) partitionPrune(windowSecs int64) string {
	if !q.hivePartitioned || windowSecs <= 0 {
		return ""
	}
	now := time.Now().UTC()
	start := now.Add(-time.Duration(windowSecs) * time.Second).Truncate(time.Hour)

	var clauses []string
	for t := start; !t.After(now); t = t.Add(time.Hour) {
		if len(clauses) >= maxPruneHours {
			return ""
		}
		clauses = append(clauses, fmt.Sprintf(
			"(year='%04d' AND month='%02d' AND day='%02d' AND hour='%02d')",
			t.Year(), int(t.Month()), t.Day(), t.Hour()))
	}
	if len(clauses) == 0 {
		return ""
	}
	return "\n\t\t\tAND (" + strings.Join(clauses, " OR ") + ")"
}

// Exec runs a query and returns results as [][]string.
// The first row contains column names.
func (q *Querier) Exec(ctx context.Context, query string, args ...any) ([][]string, error) {
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	result := [][]string{cols}

	scanBuf := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range scanBuf {
		scanPtrs[i] = &scanBuf[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		row := make([]string, len(cols))
		for i, v := range scanBuf {
			if v == nil {
				row[i] = "NULL"
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// TopPaths returns the top N URIs by request count within the given window.
func (q *Querier) TopPaths(ctx context.Context, limit int, windowSecs int64) ([][]string, error) {
	return q.Exec(ctx, fmt.Sprintf(`
		SELECT uri, count(*) AS requests
		FROM %s
		WHERE timestamp >= now()::TIMESTAMP - (? * INTERVAL '1 second')%s
		GROUP BY uri ORDER BY requests DESC LIMIT ?`, q.table, q.partitionPrune(windowSecs)), windowSecs, limit)
}

// ErrorRate returns error rate percentage grouped by the given granularity.
func (q *Querier) ErrorRate(ctx context.Context, granularity string, windowSecs int64) ([][]string, error) {
	return q.Exec(ctx, fmt.Sprintf(`
		SELECT date_trunc('%s', timestamp::TIMESTAMP) AS period,
		       count_if(status_code >= 400) * 100.0 / count(*) AS error_rate_pct
		FROM %s
		WHERE timestamp >= now()::TIMESTAMP - (? * INTERVAL '1 second')%s
		GROUP BY period ORDER BY period`, granularity, q.table, q.partitionPrune(windowSecs)), windowSecs)
}

// RequestVolume returns request count grouped by the given granularity within the given window.
func (q *Querier) RequestVolume(ctx context.Context, granularity string, windowSecs int64) ([][]string, error) {
	return q.Exec(ctx, fmt.Sprintf(`
		SELECT date_trunc('%s', timestamp::TIMESTAMP) AS period,
		       count(*) AS requests
		FROM %s
		WHERE timestamp >= now()::TIMESTAMP - (? * INTERVAL '1 second')%s
		GROUP BY period ORDER BY period`, granularity, q.table, q.partitionPrune(windowSecs)), windowSecs)
}

// BytesTransferred returns MB transferred grouped by the given granularity within the given window.
func (q *Querier) BytesTransferred(ctx context.Context, granularity string, windowSecs int64) ([][]string, error) {
	return q.Exec(ctx, fmt.Sprintf(`
		SELECT date_trunc('%s', timestamp::TIMESTAMP) AS period,
		       sum(bytes_sent) / 1e6 AS mb_transferred
		FROM %s
		WHERE timestamp >= now()::TIMESTAMP - (? * INTERVAL '1 second')%s
		GROUP BY period ORDER BY period`, granularity, q.table, q.partitionPrune(windowSecs)), windowSecs)
}

// StatusCodes returns request count per status code.
func (q *Querier) StatusCodes(ctx context.Context) ([][]string, error) {
	return q.Exec(ctx, fmt.Sprintf(`
		SELECT status_code, count(*) AS requests
		FROM %s
		GROUP BY status_code ORDER BY requests DESC`, q.table))
}

// TopClients returns the top N client IDs by request count within the given window.
func (q *Querier) TopClients(ctx context.Context, limit int, windowSecs int64) ([][]string, error) {
	return q.Exec(ctx, fmt.Sprintf(`
		SELECT client_id, count(*) AS requests
		FROM %s
		WHERE timestamp >= now()::TIMESTAMP - (? * INTERVAL '1 second')%s
		GROUP BY client_id ORDER BY requests DESC LIMIT ?`, q.table, q.partitionPrune(windowSecs)), windowSecs, limit)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
