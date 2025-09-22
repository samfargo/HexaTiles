package build

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	h3geom "github.com/hexatiles/hexatiles/internal/h3"
	"github.com/hexatiles/hexatiles/internal/ndjson"
	parquetreader "github.com/hexatiles/hexatiles/internal/parquet"
	"github.com/hexatiles/hexatiles/internal/props"
	"github.com/hexatiles/hexatiles/internal/report"
	"github.com/hexatiles/hexatiles/internal/tiler"
)

// Options describe a build invocation.
type Options struct {
	InputPath       string
	OutputPMTiles   string
	KeepNDJSON      bool
	MinZoom         int
	MaxZoom         int
	MinResolution   int
	MaxResolution   int
	PropertyInclude []string
	PropertyDrop    []string
	QuantizeSpec    string
	Simplify        bool
	Threads         int
	PropertyByteCap int
	TippecanoePath  string
	PMTilesPath     string
	Metadata        map[string]string
}

// Result contains the report produced by the build.
type Result struct {
	Report *report.Report
}

// Run executes the Parquet → NDJSON → PMTiles pipeline according to Options.
func Run(ctx context.Context, opts Options) (*Result, error) {
	if err := validateOptions(opts); err != nil {
		return nil, err
	}

	threads := opts.Threads
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	propertyCap := opts.PropertyByteCap
	if propertyCap <= 0 {
		propertyCap = 2 * 1024 // 2 KB default cap
	}

	absInput, err := filepath.Abs(opts.InputPath)
	if err != nil {
		return nil, fmt.Errorf("resolve input path: %w", err)
	}

	absOutput, err := filepath.Abs(opts.OutputPMTiles)
	if err != nil {
		return nil, fmt.Errorf("resolve output path: %w", err)
	}

	outDir := filepath.Dir(absOutput)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}

	ndjsonPath := filepath.Join(outDir, "xyz.ndjson")
	mbtilesPath := filepath.Join(outDir, "tiles.mbtiles")

	if err := removeIfExists(absOutput); err != nil {
		return nil, err
	}
	if err := removeIfExists(mbtilesPath); err != nil {
		return nil, err
	}
	if err := removeIfExists(ndjsonPath); err != nil {
		return nil, err
	}

	rep := &report.Report{
		Config: report.Config{
			InputPath:        absInput,
			OutputPMTiles:    absOutput,
			KeepNDJSON:       opts.KeepNDJSON,
			MinZoom:          opts.MinZoom,
			MaxZoom:          opts.MaxZoom,
			MinZoomDerived:   opts.MinZoom < 0,
			MaxZoomDerived:   opts.MaxZoom < 0,
			MinResolution:    opts.MinResolution,
			MaxResolution:    opts.MaxResolution,
			ResolutionFilter: opts.MinResolution >= 0 || opts.MaxResolution >= 0,
			QuantizeSpec:     opts.QuantizeSpec,
			PropsKeep:        append([]string(nil), opts.PropertyInclude...),
			PropsDrop:        append([]string(nil), opts.PropertyDrop...),
			Threads:          threads,
			Simplify:         opts.Simplify,
			PropertyByteCap:  propertyCap,
		},
		Metrics: report.Metrics{
			StartedAt: time.Now(),
		},
	}

	quantizer, err := props.Parse(opts.QuantizeSpec)
	if err != nil {
		return nil, fmt.Errorf("parse quantize spec: %w", err)
	}

    // Default per SPEC: --props whitelist; default none (keep none). Drop patterns still applied.
    // We still add system fields (h3, resolution) later in buildFeature.
    filter := props.NewFilter(opts.PropertyInclude, opts.PropertyDrop, false)

	reader, err := parquetreader.NewReader(absInput, parquetreader.ReaderOptions{BatchSize: 4096, Parallel: threads})
	if err != nil {
		return nil, fmt.Errorf("open parquet reader: %w", err)
	}
	defer reader.Close()

	writer, err := ndjson.NewWriter(ndjsonPath)
	if err != nil {
		return nil, fmt.Errorf("create NDJSON writer: %w", err)
	}
	defer writer.Close()

	err = processRows(ctx, reader, writer, processConfig{
		Options:     opts,
		Threads:     threads,
		PropertyCap: propertyCap,
		Quantizer:   quantizer,
		Filter:      filter,
		Report:      rep,
	})
	if err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close NDJSON writer: %w", err)
	}

	if info, statErr := os.Stat(ndjsonPath); statErr == nil {
		rep.Metrics.NDJSONPath = ndjsonPath
		rep.Metrics.NDJSONSize = info.Size()
	}

	tippecanoeRunner, err := tiler.NewTippecanoeRunner(opts.TippecanoePath)
	if err != nil {
		return nil, err
	}

	pmtilesConverter, err := tiler.NewPMTilesConverter(opts.PMTilesPath)
	if err != nil {
		return nil, err
	}

	minZoom, maxZoom := deriveZooms(opts, rep)

	tipOpts := tiler.TippecanoeOptions{
		MinZoom:   minZoom,
		MaxZoom:   maxZoom,
		Simplify:  opts.Simplify,
		SortBy:    "h3",
		Threads:   threads,
		LayerName: "h3",
		Metadata:  opts.Metadata,
		Attributes: deriveAttributes(filter),
	}

	rep.Config.MinZoom = minZoom
	rep.Config.MaxZoom = maxZoom
	rep.Config.MinZoomDerived = opts.MinZoom < 0
	rep.Config.MaxZoomDerived = opts.MaxZoom < 0

	tipStart := time.Now()
	tipOutput, tipArgs, err := tippecanoeRunner.Run(ctx, ndjsonPath, mbtilesPath, tipOpts)
	rep.Metrics.TilingDuration += time.Since(tipStart)
	rep.Metrics.TippecanoeCommand = append([]string(nil), tipArgs...)
	rep.Metrics.TippecanoeOutput = tipOutput
	if err != nil {
		return nil, err
	}

	if info, statErr := os.Stat(mbtilesPath); statErr == nil {
		rep.Metrics.MBTilesPath = mbtilesPath
		rep.Metrics.MBTilesSize = info.Size()
	}

	convertStart := time.Now()
	pmOutput, err := pmtilesConverter.Convert(ctx, mbtilesPath, absOutput)
	rep.Metrics.TilingDuration += time.Since(convertStart)
	if err != nil {
		rep.Metrics.TippecanoeOutput += "\n" + pmOutput
		return nil, err
	}

	if info, statErr := os.Stat(absOutput); statErr == nil {
		rep.Metrics.PMTilesPath = absOutput
		rep.Metrics.PMTilesSize = info.Size()
	}

	pmMeta, pmRaw, infoErr := pmtilesConverter.Info(ctx, absOutput)
	if infoErr == nil {
		rep.Metrics.PMTilesInfo = pmMeta
	} else if pmRaw != "" {
		rep.AddWarning(fmt.Sprintf("pmtiles info: %v", infoErr))
	}

	if !opts.KeepNDJSON {
		_ = os.Remove(ndjsonPath)
		rep.Metrics.NDJSONPath = ""
	}
	_ = os.Remove(mbtilesPath)

	rep.Metrics.FinishedAt = time.Now()
	rep.Metrics.Duration = time.Since(rep.Metrics.StartedAt)

	if err := rep.WriteHTML(filepath.Join(outDir, "report.html")); err != nil {
		return nil, err
	}

	return &Result{Report: rep}, nil
}

// Additional helper functions and types will go here.

type processConfig struct {
	Options     Options
	Threads     int
	PropertyCap int
	Quantizer   props.Quantizer
	Filter      *props.Filter
	Report      *report.Report
}

func processRows(ctx context.Context, reader *parquetreader.Reader, writer *ndjson.Writer, cfg processConfig) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()

	jobs := make(chan *parquetreader.Row)
	results := make(chan featureResult, cfg.Threads*2)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerLoop(ctx, jobs, results, cfg)
		}()
	}

	go func() {
		defer close(jobs)
		for {
			row, err := reader.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				select {
				case results <- featureResult{Err: fmt.Errorf("read parquet: %w", err)}:
				case <-ctx.Done():
				}
				return
			}

			select {
			case <-ctx.Done():
				return
			case jobs <- row:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	const (
		propertyWarningLimit       = 20
		propertyWarnCountThreshold = 15
		propertyWarnBytesThreshold = 20 * 1024
		invalidSampleLimit         = 10
	)

	expected := int64(1)
	pending := make(map[int64]featureResult)
	propertyWarnings := 0
	invalidSamples := make([]string, 0, invalidSampleLimit)
	minResSeen := 0
	maxResSeen := 0
	resInitialised := false

	for res := range results {
		if res.Err != nil {
			cancel()
			wg.Wait()
			return res.Err
		}

		pending[res.RowNumber] = res

		for {
			fr, ok := pending[expected]
			if !ok {
				break
			}
			delete(pending, expected)
			expected++

			cfg.Report.Metrics.TotalRows++
			if fr.Resolution >= 0 {
				cfg.Report.IncrementHistogram(fr.Resolution)
				if !resInitialised {
					minResSeen, maxResSeen = fr.Resolution, fr.Resolution
					resInitialised = true
				} else {
					if fr.Resolution < minResSeen {
						minResSeen = fr.Resolution
					}
					if fr.Resolution > maxResSeen {
						maxResSeen = fr.Resolution
					}
				}
			}

			if fr.Dropped {
				switch fr.DropReason {
				case "resolution":
					cfg.Report.Metrics.DroppedResolution++
				case "property_cap":
					cfg.Report.Metrics.DroppedPropertyCap++
					if propertyWarnings < propertyWarningLimit {
						cfg.Report.AddPropertyWarning(report.PropertyWarning{
							RowNumber:     fr.RowNumber,
							H3:            fr.CellString,
							PropertyCount: fr.PropertyCount,
							PropertyBytes: fr.PropertyBytes,
							Message:       fmt.Sprintf("dropped: property payload %d bytes exceeds cap %d bytes", fr.PropertyBytes, cfg.PropertyCap),
						})
					}
					propertyWarnings++
				case "invalid_h3":
					cfg.Report.Metrics.DroppedInvalidH3++
					if len(invalidSamples) < invalidSampleLimit {
						detail := fr.DropDetail
						if detail == "" {
							detail = "invalid H3 cell"
						}
						entry := fmt.Sprintf("row %d (%s): %s", fr.RowNumber, fr.CellString, detail)
						invalidSamples = append(invalidSamples, entry)
					}
				default:
					cfg.Report.Metrics.DroppedOther++
				}
				continue
			}

			if err := writer.WriteFeature(fr.Feature); err != nil {
				cancel()
				wg.Wait()
				return fmt.Errorf("write NDJSON feature: %w", err)
			}

			cfg.Report.Metrics.EmittedFeatures++
			if fr.QuantResult.Changes > 0 {
				cfg.Report.Metrics.QuantizeApplied = true
				cfg.Report.Metrics.QuantizeChanges += int64(fr.QuantResult.Changes)
				cfg.Report.Metrics.QuantizeTotalError += fr.QuantResult.TotalAbsError
			}

			if fr.PropertyCount > propertyWarnCountThreshold || fr.PropertyBytes > propertyWarnBytesThreshold {
				if propertyWarnings < propertyWarningLimit {
					cfg.Report.AddPropertyWarning(report.PropertyWarning{
						RowNumber:     fr.RowNumber,
						H3:            fr.CellString,
						PropertyCount: fr.PropertyCount,
						PropertyBytes: fr.PropertyBytes,
						Message:       fmt.Sprintf("large property payload (%d props, %d bytes)", fr.PropertyCount, fr.PropertyBytes),
					})
				}
				propertyWarnings++
			}
		}
	}

	cancel()
	wg.Wait()

	if resInitialised {
		cfg.Report.Metrics.MinResolutionSeen = minResSeen
		cfg.Report.Metrics.MaxResolutionSeen = maxResSeen
		if maxResSeen-minResSeen >= 5 {
			cfg.Report.AddWarning(fmt.Sprintf("mixed H3 resolutions detected: r%d-r%d", minResSeen, maxResSeen))
		}
	}
	if len(invalidSamples) > 0 {
		msg := fmt.Sprintf("invalid H3 cells encountered: %s", strings.Join(invalidSamples, "; "))
		if cfg.Report.Metrics.DroppedInvalidH3 > int64(len(invalidSamples)) {
			msg += fmt.Sprintf(" (and %d more)", cfg.Report.Metrics.DroppedInvalidH3-int64(len(invalidSamples)))
		}
		cfg.Report.AddWarning(msg)
	}
	if propertyWarnings > propertyWarningLimit {
		cfg.Report.AddWarning(fmt.Sprintf("property warnings truncated (%d total)", propertyWarnings))
	}
	cfg.Report.Metrics.NDJSONDuration = time.Since(start)

	if len(pending) != 0 {
		return fmt.Errorf("incomplete processing: %d features pending", len(pending))
	}

	return nil
}

func validateOptions(opts Options) error {
	if opts.InputPath == "" {
		return fmt.Errorf("input path is required")
	}
	if opts.OutputPMTiles == "" {
		return fmt.Errorf("output path is required")
	}
	if _, err := os.Stat(opts.InputPath); err != nil {
		return fmt.Errorf("input file: %w", err)
	}
	return nil
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", path, err)
	}
	return nil
}

func deriveZooms(opts Options, report *report.Report) (int, int) {
	minZoom := opts.MinZoom
	if minZoom < 0 {
		minZoom = 0
	}

	maxZoom := opts.MaxZoom
	if maxZoom < 0 {
		maxRes := report.Metrics.MaxResolutionSeen
		if maxRes <= 0 {
			maxZoom = 12
		} else {
			computed := maxRes + 2
			if computed < 12 {
				computed = 12
			}
			if computed > 15 {
				computed = 15
			}
			maxZoom = computed
		}
	}

	if maxZoom < minZoom {
		maxZoom = minZoom
	}
	if maxZoom > 15 {
		maxZoom = 15
	}

	return minZoom, maxZoom
}

func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func deriveAttributes(f *props.Filter) []string {
    // Always include system fields used downstream
    base := []string{"h3", "resolution"}
    if f == nil {
        return base
    }
    keys := f.Keys()
    if len(keys) == 0 {
        return base
    }
    out := make([]string, 0, len(base)+len(keys))
    out = append(out, base...)
    out = append(out, keys...)
    return out
}

type featureResult struct {
	RowNumber     int64
	CellString    string
	Resolution    int
	Feature       ndjson.Feature
	PropertyBytes int
	PropertyCount int
	QuantResult   props.Result
	Dropped       bool
	DropReason    string
	DropDetail    string
	Err           error
}

func workerLoop(ctx context.Context, jobs <-chan *parquetreader.Row, results chan<- featureResult, cfg processConfig) {
	for {
		select {
		case <-ctx.Done():
			return
		case row, ok := <-jobs:
			if !ok {
				return
			}
			fr := buildFeature(row, cfg)
			select {
			case results <- fr:
			case <-ctx.Done():
				return
			}
		}
	}
}

func buildFeature(row *parquetreader.Row, cfg processConfig) featureResult {
	result := featureResult{
		RowNumber:  row.RowNumber,
		CellString: row.CellString,
		Resolution: row.Resolution,
	}

	if row.Err != nil {
		result.Resolution = -1
		result.Dropped = true
		result.DropReason = "invalid_h3"
		result.DropDetail = row.Err.Error()
		return result
	}

	if cfg.Options.MinResolution >= 0 && row.Resolution < cfg.Options.MinResolution {
		result.Dropped = true
		result.DropReason = "resolution"
		return result
	}
	if cfg.Options.MaxResolution >= 0 && row.Resolution > cfg.Options.MaxResolution {
		result.Dropped = true
		result.DropReason = "resolution"
		return result
	}

    propsMap := cloneMap(row.Properties)
    filtered := propsMap
    if cfg.Filter != nil {
        filtered = cfg.Filter.Apply(propsMap)
    }
	if filtered == nil {
		filtered = make(map[string]any)
	}

    // System fields always included regardless of filter
    filtered["h3"] = row.CellString
    filtered["resolution"] = row.Resolution

	quantResult := cfg.Quantizer.Apply(filtered)

	propJSON, err := json.Marshal(filtered)
	if err != nil {
		result.Err = fmt.Errorf("marshal properties: %w", err)
		return result
	}

	result.PropertyBytes = len(propJSON)
	result.PropertyCount = len(filtered)
	result.QuantResult = quantResult

	if cfg.PropertyCap > 0 && result.PropertyBytes > cfg.PropertyCap {
		result.Dropped = true
		result.DropReason = "property_cap"
		return result
	}

	polygon, err := h3geom.PolygonFromCell(row.Cell)
	if err != nil {
		result.Err = fmt.Errorf("polygonize %s: %w", row.CellString, err)
		return result
	}

	bound := polygon.Bound()
	result.Feature = ndjson.Feature{
		ID:         row.CellString,
		Geometry:   polygon,
		Properties: filtered,
		BBox:       &bound,
	}

	return result
}
