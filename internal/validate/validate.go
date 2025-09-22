package validate

import (
	"context"
	"fmt"
	"io"
	"time"

	parquetreader "github.com/hexatiles/hexatiles/internal/parquet"
)

// Options configures a validation run.
type Options struct {
	InputPath       string
	MinResolution   int
	MaxResolution   int
	SampleLimit     int
	ReaderBatchSize int
	ReaderParallel  int
}

// Issue captures an invalid row sample.
type Issue struct {
	RowNumber int64
	H3        string
	Message   string
}

// Result summarises validation findings for a single file.
type Result struct {
	TotalRows           int64
	ValidRows           int64
	InvalidCells        int64
	ResolutionFiltered  int64
	ResolutionHistogram map[int]int64
	InvalidSamples      []Issue
	MinResolutionSeen   int
	MaxResolutionSeen   int
	Duration            time.Duration
}

// Run executes validation on a single Parquet file.
func Run(ctx context.Context, opts Options) (*Result, error) {
	if opts.SampleLimit <= 0 {
		opts.SampleLimit = 10
	}
	if opts.ReaderBatchSize <= 0 {
		opts.ReaderBatchSize = 2048
	}
	if opts.ReaderParallel <= 0 {
		opts.ReaderParallel = 1
	}

	reader, err := parquetreader.NewReader(opts.InputPath, parquetreader.ReaderOptions{BatchSize: opts.ReaderBatchSize, Parallel: opts.ReaderParallel})
	if err != nil {
		return nil, fmt.Errorf("open parquet reader: %w", err)
	}
	defer reader.Close()

	res := &Result{
		ResolutionHistogram: make(map[int]int64),
		MinResolutionSeen:   -1,
		MaxResolutionSeen:   -1,
	}

	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		row, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read parquet row: %w", err)
		}

		res.TotalRows++

		if row.Err != nil {
			res.InvalidCells++
			if len(res.InvalidSamples) < opts.SampleLimit {
				res.InvalidSamples = append(res.InvalidSamples, Issue{
					RowNumber: row.RowNumber,
					H3:        row.CellString,
					Message:   row.Err.Error(),
				})
			}
			continue
		}

		if opts.MinResolution >= 0 && row.Resolution < opts.MinResolution {
			res.ResolutionFiltered++
			continue
		}
		if opts.MaxResolution >= 0 && row.Resolution > opts.MaxResolution {
			res.ResolutionFiltered++
			continue
		}

		res.ValidRows++
		res.ResolutionHistogram[row.Resolution]++
		if res.MinResolutionSeen == -1 || row.Resolution < res.MinResolutionSeen {
			res.MinResolutionSeen = row.Resolution
		}
		if res.MaxResolutionSeen == -1 || row.Resolution > res.MaxResolutionSeen {
			res.MaxResolutionSeen = row.Resolution
		}
	}

	res.Duration = time.Since(start)
	return res, nil
}
