package writer

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"golang.org/x/sync/errgroup"

	"github.com/el10savio/pqrt/src/lib"
	"github.com/el10savio/pqrt/src/pkg/metrics"
)

type Writer struct {
	minioClient *miniogo.Client
	bucket      string
	flushSize   int
	buffers     map[string][]lib.ParquetRow
	mu          sync.Mutex
	fileCounter map[string]int
	eg          *errgroup.Group
	egCtx       context.Context
}

func NewWriter(minioClient *miniogo.Client, bucket string, flushSize int) *Writer {
	eg, ctx := errgroup.WithContext(context.Background())
	return &Writer{
		minioClient: minioClient,
		bucket:      bucket,
		flushSize:   flushSize,
		buffers:     make(map[string][]lib.ParquetRow),
		fileCounter: make(map[string]int),
		eg:          eg,
		egCtx:       ctx,
	}
}

func (w *Writer) Add(row lib.ParquetRow) error {
	key := partitionKey(row.Timestamp)

	w.mu.Lock()
	w.buffers[key] = append(w.buffers[key], row)
	full := len(w.buffers[key]) >= w.flushSize
	var batch []lib.ParquetRow
	if full {
		batch = w.buffers[key]
		w.buffers[key] = nil
	}
	w.mu.Unlock()

	if full {
		w.flushAsync(key, batch)
	}
	return nil
}

func (w *Writer) FlushAll(ctx context.Context) error {
	w.mu.Lock()
	pending := make(map[string][]lib.ParquetRow, len(w.buffers))
	for k, v := range w.buffers {
		if len(v) > 0 {
			pending[k] = v
		}
		delete(w.buffers, k)
	}
	w.mu.Unlock()

	for key, batch := range pending {
		w.flushAsync(key, batch)
	}
	return w.eg.Wait()
}

func (w *Writer) flushAsync(key string, batch []lib.ParquetRow) {
	w.mu.Lock()
	n := w.fileCounter[key]
	w.fileCounter[key]++
	w.mu.Unlock()

	w.eg.Go(func() error {
		return w.flush(key, n, batch)
	})
}

func (w *Writer) flush(key string, n int, batch []lib.ParquetRow) error {
	tmpFile, err := os.CreateTemp("", "parquet-*.parquet")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	writeStart := time.Now()
	pw := parquet.NewGenericWriter[lib.ParquetRow](tmpFile,
		parquet.Compression(&snappy.Codec{}),
	)

	ctx := w.egCtx
	if _, err := pw.Write(batch); err != nil {
		tmpFile.Close()
		metrics.UploadErrors.Add(ctx, 1)
		return fmt.Errorf("parquet write: %w", err)
	}
	if err := pw.Close(); err != nil {
		tmpFile.Close()
		metrics.UploadErrors.Add(ctx, 1)
		return fmt.Errorf("parquet close: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		metrics.UploadErrors.Add(ctx, 1)
		return fmt.Errorf("close temp file: %w", err)
	}
	metrics.ParquetWriteLatency.Record(ctx, time.Since(writeStart).Seconds())
	metrics.BatchFlushSize.Record(ctx, float64(len(batch)))

	objectName := fmt.Sprintf("data/%s/part-%04d.parquet", key, n)
	uploadStart := time.Now()
	_, err = w.minioClient.FPutObject(ctx, w.bucket, objectName, tmpPath, miniogo.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		metrics.UploadErrors.Add(ctx, 1)
		return fmt.Errorf("minio upload %s: %w", objectName, err)
	}
	metrics.MinioUploadLatency.Record(ctx, time.Since(uploadStart).Seconds())
	return nil
}

func partitionKey(tsMillis int64) string {
	t := time.UnixMilli(tsMillis).UTC()
	return fmt.Sprintf("year=%04d/month=%02d/day=%02d/hour=%02d",
		t.Year(), t.Month(), t.Day(), t.Hour())
}
