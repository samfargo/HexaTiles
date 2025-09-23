package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"sort"
	"strings"
	"time"
)

// Config summarises the build configuration used for a run.
type Config struct {
	InputPath        string
	OutputPMTiles    string
	KeepNDJSON       bool
	MinZoom          int
	MaxZoom          int
	MinZoomDerived   bool
	MaxZoomDerived   bool
	MinResolution    int
	MaxResolution    int
	ResolutionFilter bool
	QuantizeSpec     string
	PropsKeep        []string
	PropsDrop        []string
	Threads          int
	Simplify         bool
	PropertyByteCap  int
}

// PropertyWarning captures over-sized property payloads.
type PropertyWarning struct {
	RowNumber     int64
	H3            string
	PropertyCount int
	PropertyBytes int
	Message       string
}

// HistogramEntry is used to render deterministic resolution histograms.
type HistogramEntry struct {
	Resolution int
	Count      int64
}

// Metrics holds runtime statistics gathered during a build.
type Metrics struct {
	StartedAt           time.Time
	FinishedAt          time.Time
	Duration            time.Duration
	NDJSONDuration      time.Duration
	TilingDuration      time.Duration
	TotalRows           int64
	EmittedFeatures     int64
	DroppedInvalidH3    int64
	DroppedResolution   int64
	DroppedPropertyCap  int64
	DroppedOther        int64
	PropertyWarnings    []PropertyWarning
	MinResolutionSeen   int
	MaxResolutionSeen   int
	ResolutionHistogram map[int]int64
	ResolutionEntries   []HistogramEntry
	QuantizeApplied     bool
	QuantizeChanges     int64
	QuantizeTotalError  float64
	NDJSONPath          string
	NDJSONSize          int64
	MBTilesPath         string
	MBTilesSize         int64
	PMTilesPath         string
	PMTilesSize         int64
	TippecanoeCommand   []string
	TippecanoeOutput    string
	PMTilesInfo         map[string]any
	Warnings            []string
}

// Report ties together configuration and metrics.
type Report struct {
	Config  Config
	Metrics Metrics
}

// AddWarning appends a human-readable warning to the report.
func (r *Report) AddWarning(message string) {
	r.Metrics.Warnings = append(r.Metrics.Warnings, message)
}

// AddPropertyWarning appends a property warning to the report.
func (r *Report) AddPropertyWarning(w PropertyWarning) {
	r.Metrics.PropertyWarnings = append(r.Metrics.PropertyWarnings, w)
}

// IncrementHistogram increments the resolution histogram.
func (r *Report) IncrementHistogram(resolution int) {
	if r.Metrics.ResolutionHistogram == nil {
		r.Metrics.ResolutionHistogram = make(map[int]int64)
	}
	r.Metrics.ResolutionHistogram[resolution]++
}

// Prepare final derived metrics (called before rendering).
func (r *Report) prepare() {
	if len(r.Metrics.ResolutionHistogram) > 0 {
		keys := make([]int, 0, len(r.Metrics.ResolutionHistogram))
		for k := range r.Metrics.ResolutionHistogram {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		r.Metrics.ResolutionEntries = make([]HistogramEntry, 0, len(keys))
		for _, k := range keys {
			r.Metrics.ResolutionEntries = append(r.Metrics.ResolutionEntries, HistogramEntry{Resolution: k, Count: r.Metrics.ResolutionHistogram[k]})
		}
	}
}

// WriteHTML renders the report as an HTML file at the given path.
func (r *Report) WriteHTML(path string) error {
	r.prepare()

	funcMap := template.FuncMap{
		"FormatBytes": formatBytes,
		"FormatDuration": func(d time.Duration) string {
			if d <= 0 {
				return "n/a"
			}
			return d.Truncate(time.Millisecond).String()
		},
		"FormatJSON": func(v any) string {
			if v == nil {
				return "{}"
			}
			buf, err := json.MarshalIndent(v, "", "  ")
			if err != nil {
				return fmt.Sprintf("(error: %v)", err)
			}
			return string(buf)
		},
		"Join": strings.Join,
		"int64": func(i int) int64 {
			return int64(i)
		},
	}

	tpl, err := template.New("report").Funcs(funcMap).Parse(htmlTemplate)
	if err != nil {
		return fmt.Errorf("parse report template: %w", err)
	}

	var buf bytes.Buffer
	if err := tpl.Execute(&buf, r); err != nil {
		return fmt.Errorf("execute report template: %w", err)
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	return nil
}

func formatBytes(value int64) string {
	if value <= 0 {
		return "0 B"
	}

	units := []string{"B", "KB", "MB", "GB", "TB"}
	f := float64(value)
	idx := 0
	for f >= 1024 && idx < len(units)-1 {
		f /= 1024
		idx++
	}
	if f >= 10 || idx == 0 {
		return fmt.Sprintf("%.0f %s", f, units[idx])
	}
	return fmt.Sprintf("%.1f %s", f, units[idx])
}

const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>HexaTiles Build Report</title>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 40px; color: #1f2933; }
header { margin-bottom: 32px; }
h1 { font-size: 28px; margin: 0; }
section { margin-bottom: 36px; }
h2 { font-size: 20px; border-bottom: 1px solid #e1e4e8; padding-bottom: 4px; margin-bottom: 16px; }
table { border-collapse: collapse; width: 100%; margin-bottom: 16px; }
th, td { border: 1px solid #d9e2ec; padding: 8px 12px; text-align: left; font-size: 14px; }
th { background: #f0f4f8; }
code { background: #f1f5f9; padding: 2px 4px; border-radius: 4px; }
ul { padding-left: 20px; }
.warning { color: #b43403; }
pre { background: #0f172a; color: #e2e8f0; padding: 16px; border-radius: 6px; overflow-x: auto; font-size: 13px; }
</style>
</head>
<body>
<header>
  <h1>HexaTiles Build Report</h1>
  <p>Input: <code>{{ .Config.InputPath }}</code> &middot; Output: <code>{{ .Config.OutputPMTiles }}</code></p>
  <p>Started {{ .Metrics.StartedAt.Format "2006-01-02 15:04:05" }} &middot; Duration {{ FormatDuration .Metrics.Duration }}</p>
</header>

<section>
  <h2>Configuration</h2>
  <table>
    <tr><th>Keep NDJSON</th><td>{{ if .Config.KeepNDJSON }}yes{{ else }}no{{ end }}</td></tr>
    <tr><th>Zooms</th><td>{{ .Config.MinZoom }} &rarr; {{ .Config.MaxZoom }}{{ if .Config.MinZoomDerived }} (min derived){{ end }}{{ if .Config.MaxZoomDerived }} (max derived){{ end }}</td></tr>
    <tr><th>Resolution Filter</th><td>{{ if .Config.ResolutionFilter }}r{{ .Config.MinResolution }} &rarr; r{{ .Config.MaxResolution }}{{ else }}none{{ end }}</td></tr>
    <tr><th>Quantization</th><td>{{ if .Config.QuantizeSpec }}{{ .Config.QuantizeSpec }}{{ else }}disabled{{ end }}</td></tr>
    <tr><th>Property Cap</th><td>{{ if gt .Config.PropertyByteCap 0 }}{{ FormatBytes (int64 .Config.PropertyByteCap) }}{{ else }}not set{{ end }}</td></tr>
    <tr><th>Threads</th><td>{{ .Config.Threads }}</td></tr>
    <tr><th>Simplify</th><td>{{ if .Config.Simplify }}enabled{{ else }}disabled{{ end }}</td></tr>
    <tr><th>Keep Properties</th><td>{{ if .Config.PropsKeep }}{{ Join .Config.PropsKeep ", " }}{{ else }}all{{ end }}</td></tr>
    <tr><th>Drop Patterns</th><td>{{ if .Config.PropsDrop }}{{ Join .Config.PropsDrop ", " }}{{ else }}none{{ end }}</td></tr>
  </table>
</section>

<section>
  <h2>Dataset</h2>
  <table>
    <tr><th>Total rows</th><td>{{ .Metrics.TotalRows }}</td></tr>
    <tr><th>Features emitted</th><td>{{ .Metrics.EmittedFeatures }}</td></tr>
    <tr><th>Dropped (invalid H3)</th><td>{{ .Metrics.DroppedInvalidH3 }}</td></tr>
    <tr><th>Dropped (resolution filter)</th><td>{{ .Metrics.DroppedResolution }}</td></tr>
    <tr><th>Dropped (property cap)</th><td>{{ .Metrics.DroppedPropertyCap }}</td></tr>
    <tr><th>Resolution span</th><td>{{ if gt .Metrics.TotalRows 0 }}r{{ .Metrics.MinResolutionSeen }} → r{{ .Metrics.MaxResolutionSeen }}{{ else }}n/a{{ end }}</td></tr>
  </table>
  {{ if .Metrics.ResolutionEntries }}
  <h3>Resolution histogram</h3>
  <table>
    <tr><th>Resolution</th><th>Rows</th></tr>
    {{ range .Metrics.ResolutionEntries }}
    <tr><td>r{{ .Resolution }}</td><td>{{ .Count }}</td></tr>
    {{ end }}
  </table>
  {{ end }}
</section>

<section>
  <h2>Artifacts</h2>
  <table>
    <tr><th>NDJSON</th><td>{{ if .Metrics.NDJSONPath }}<code>{{ .Metrics.NDJSONPath }}</code> ({{ FormatBytes .Metrics.NDJSONSize }}){{ else }}not kept{{ end }}</td></tr>
    <tr><th>MBTiles</th><td>{{ if .Metrics.MBTilesPath }}<code>{{ .Metrics.MBTilesPath }}</code> ({{ FormatBytes .Metrics.MBTilesSize }}){{ else }}temporary{{ end }}</td></tr>
    <tr><th>PMTiles</th><td><code>{{ .Metrics.PMTilesPath }}</code> ({{ FormatBytes .Metrics.PMTilesSize }})</td></tr>
  </table>
</section>

{{ if or .Metrics.PropertyWarnings .Metrics.Warnings }}
<section>
  <h2>Warnings</h2>
  {{ if .Metrics.Warnings }}
  <ul>
    {{ range .Metrics.Warnings }}<li class="warning">{{ . }}</li>{{ end }}
  </ul>
  {{ end }}
  {{ if .Metrics.PropertyWarnings }}
  <table>
    <tr><th>Row</th><th>H3</th><th>Properties</th><th>Bytes</th><th>Details</th></tr>
    {{ range .Metrics.PropertyWarnings }}
    <tr><td>{{ .RowNumber }}</td><td><code>{{ .H3 }}</code></td><td>{{ .PropertyCount }}</td><td>{{ .PropertyBytes }}</td><td>{{ .Message }}</td></tr>
    {{ end }}
  </table>
  {{ end }}
</section>
{{ end }}

<section>
  <h2>Quantization</h2>
  <table>
    <tr><th>Applied</th><td>{{ if .Metrics.QuantizeApplied }}yes ({{ .Metrics.QuantizeChanges }} adjustments, total error {{ printf "%.4f" .Metrics.QuantizeTotalError }}){{ else }}no{{ end }}</td></tr>
  </table>
</section>

<section>
  <h2>Tippecanoe</h2>
  <table>
    <tr><th>Command</th><td>{{ if .Metrics.TippecanoeCommand }}<code>{{ Join .Metrics.TippecanoeCommand " " }}</code>{{ else }}n/a{{ end }}</td></tr>
    <tr><th>Duration</th><td>{{ FormatDuration .Metrics.TilingDuration }}</td></tr>
  </table>
  {{ if .Metrics.TippecanoeOutput }}
  <pre>{{ .Metrics.TippecanoeOutput }}</pre>
  {{ end }}
</section>

<section>
  <h2>PMTiles Metadata</h2>
  <pre>{{ FormatJSON .Metrics.PMTilesInfo }}</pre>
</section>

<footer>
  <p>HexaTiles — static H3 tiling pipeline. Generated {{ .Metrics.FinishedAt.Format "2006-01-02 15:04:05" }}.</p>
</footer>
</body>
</html>`
