package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/parquet-go/parquet-go"
	h3 "github.com/uber/h3-go/v4"

	"github.com/hexatiles/hexatiles/internal/build"
	"github.com/hexatiles/hexatiles/internal/tiler"
	"github.com/hexatiles/hexatiles/internal/validate"
)

// These variables are set via ldflags during build
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	if err := newRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hexatiles",
		Short: "HexaTiles: Parquet → H3 polygons → PMTiles in one command",
		Long:  "HexaTiles converts H3-indexed Parquet datasets into PMTiles vector tilesets with deterministic defaults.",
		RunE: func(cmd *cobra.Command, args []string) error {
			showVersion, _ := cmd.Flags().GetBool("version")
			if showVersion {
				fmt.Printf("hexatiles version %s (commit: %s, built: %s)\n", version, commit, date)
				return nil
			}
			return cmd.Help()
		},
	}

	cmd.Flags().BoolP("version", "v", false, "Show version information")
	cmd.AddCommand(newBuildCommand())
	cmd.AddCommand(newValidateCommand())
	cmd.AddCommand(newInspectCommand())
	cmd.AddCommand(newPreviewCommand())
	cmd.AddCommand(newSchemaCommand())
	cmd.AddCommand(newSampleCommand())

	return cmd
}

func newBuildCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Convert Parquet files with H3 columns into PMTiles",
		RunE: func(cmd *cobra.Command, args []string) error {
			input, _ := cmd.Flags().GetString("in")
			output, _ := cmd.Flags().GetString("out")
			keepNDJSON, _ := cmd.Flags().GetBool("keep-ndjson")
			minZoom, _ := cmd.Flags().GetInt("minzoom")
			maxZoom, _ := cmd.Flags().GetInt("maxzoom")
			minRes, _ := cmd.Flags().GetInt("min-res")
			maxRes, _ := cmd.Flags().GetInt("max-res")
			propsKeepStr, _ := cmd.Flags().GetString("props")
			propsDropStr, _ := cmd.Flags().GetString("props-drop")
			quantizeSpec, _ := cmd.Flags().GetString("quantize")
			simplify, _ := cmd.Flags().GetBool("simplify")
			threads, _ := cmd.Flags().GetInt("threads")
			propertyCap, _ := cmd.Flags().GetInt("property-cap")
			tippecanoeBin, _ := cmd.Flags().GetString("tippecanoe-bin")
			pmtilesBin, _ := cmd.Flags().GetString("pmtiles-bin")
            name, _ := cmd.Flags().GetString("name")
            description, _ := cmd.Flags().GetString("description")
            attribution, _ := cmd.Flags().GetString("attribution")
            version, _ := cmd.Flags().GetString("tileset-version")

			opts := build.Options{
				InputPath:       input,
				OutputPMTiles:   output,
				KeepNDJSON:      keepNDJSON,
				MinZoom:         minZoom,
				MaxZoom:         maxZoom,
				MinResolution:   minRes,
				MaxResolution:   maxRes,
				PropertyInclude: parseList(propsKeepStr),
				PropertyDrop:    parseList(propsDropStr),
				QuantizeSpec:    quantizeSpec,
				Simplify:        simplify,
				Threads:         threads,
				PropertyByteCap: propertyCap,
				TippecanoePath:  tippecanoeBin,
				PMTilesPath:     pmtilesBin,
                Metadata: map[string]string{
                    "name":        name,
                    "description": description,
                    "attribution": attribution,
                    "version":     version,
                },
			}

			result, err := build.Run(cmd.Context(), opts)
			if err != nil {
				return err
			}

			rep := result.Report
			dropped := rep.Metrics.TotalRows - rep.Metrics.EmittedFeatures
			fmt.Fprintf(cmd.OutOrStdout(), "✔ build complete in %s\n", formatDuration(rep.Metrics.Duration))
			fmt.Fprintf(cmd.OutOrStdout(), "  tiles: %s (%s)\n", rep.Metrics.PMTilesPath, formatBytes(rep.Metrics.PMTilesSize))
			fmt.Fprintf(cmd.OutOrStdout(), "  features: %d emitted, %d dropped\n", rep.Metrics.EmittedFeatures, dropped)
			fmt.Fprintf(cmd.OutOrStdout(), "  report: %s\n", filepath.Join(filepath.Dir(rep.Config.OutputPMTiles), "report.html"))

			return nil
		},
	}

	cmd.SilenceUsage = true

	cmd.Flags().String("in", "", "Input Parquet file")
	cmd.Flags().String("out", "", "Output PMTiles file path")
	cmd.Flags().Bool("keep-ndjson", false, "Keep intermediate NDJSON output")
	cmd.Flags().Int("minzoom", -1, "Minimum zoom level (default: derived)")
	cmd.Flags().Int("maxzoom", -1, "Maximum zoom level (default: derived)")
	cmd.Flags().Int("min-res", -1, "Minimum allowed H3 resolution")
	cmd.Flags().Int("max-res", -1, "Maximum allowed H3 resolution")
	cmd.Flags().String("props", "", "Comma-separated whitelist of properties to keep")
	cmd.Flags().String("props-drop", "", "Glob pattern of properties to drop")
	cmd.Flags().String("quantize", "", "Quantization directives (float=0.01,int=1)")
	cmd.Flags().Bool("simplify", false, "Simplify polygons (default false)")
	cmd.Flags().Int("threads", 0, "Number of worker threads (default: runtime.NumCPU())")
	cmd.Flags().Int("property-cap", 2048, "Maximum property bytes per feature (0 to disable)")
	cmd.Flags().String("tippecanoe-bin", "", "Override tippecanoe binary path")
	cmd.Flags().String("pmtiles-bin", "", "Override pmtiles binary path")
	cmd.Flags().String("name", "", "Tileset name (metadata)")
	cmd.Flags().String("description", "", "Tileset description (metadata)")
	cmd.Flags().String("attribution", "", "Tileset attribution (metadata)")
	cmd.Flags().String("tileset-version", "", "Tileset semantic version (metadata)")

	cmd.MarkFlagRequired("in")
	cmd.MarkFlagRequired("out")

	return cmd
}

func newValidateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate H3 Parquet input files",
		RunE: func(cmd *cobra.Command, args []string) error {
			inputs, _ := cmd.Flags().GetStringArray("in")
			if len(inputs) == 0 {
				return fmt.Errorf("no input files provided")
			}
			minRes, _ := cmd.Flags().GetInt("min-res")
			maxRes, _ := cmd.Flags().GetInt("max-res")
			sampleLimit, _ := cmd.Flags().GetInt("sample")

			hasErrors := false

			for _, path := range inputs {
				opts := validate.Options{
					InputPath:     path,
					MinResolution: minRes,
					MaxResolution: maxRes,
					SampleLimit:   sampleLimit,
				}

				res, err := validate.Run(cmd.Context(), opts)
				if err != nil {
					return fmt.Errorf("%s: %w", path, err)
				}

				fmt.Fprintf(cmd.OutOrStdout(), "%s\n", path)
				fmt.Fprintf(cmd.OutOrStdout(), "  rows: %d valid: %d invalid: %d filtered: %d\n", res.TotalRows, res.ValidRows, res.InvalidCells, res.ResolutionFiltered)
				if res.MinResolutionSeen >= 0 {
					fmt.Fprintf(cmd.OutOrStdout(), "  resolutions: r%d -> r%d\n", res.MinResolutionSeen, res.MaxResolutionSeen)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "  duration: %s\n", formatDuration(res.Duration))

				if res.InvalidCells > 0 {
					hasErrors = true
					fmt.Fprintf(cmd.OutOrStdout(), "  invalid samples:\n")
					for _, sample := range res.InvalidSamples {
						fmt.Fprintf(cmd.OutOrStdout(), "    row %d (%s): %s\n", sample.RowNumber, sample.H3, sample.Message)
					}
					if int64(len(res.InvalidSamples)) < res.InvalidCells {
						fmt.Fprintf(cmd.OutOrStdout(), "    ... %d more\n", res.InvalidCells-int64(len(res.InvalidSamples)))
					}
				}
			}

			if hasErrors {
				return fmt.Errorf("validation failed: invalid H3 cells detected")
			}

			return nil
		},
	}

	cmd.SilenceUsage = true

	cmd.Flags().StringArray("in", nil, "Input Parquet files (glob supported by shell)")
	cmd.Flags().Int("min-res", -1, "Minimum allowed H3 resolution")
	cmd.Flags().Int("max-res", -1, "Maximum allowed H3 resolution")
	cmd.Flags().Int("sample", 5, "Number of invalid samples to display")
	cmd.MarkFlagRequired("in")

	return cmd
}

func newInspectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect a PMTiles archive",
		RunE: func(cmd *cobra.Command, args []string) error {
			input, _ := cmd.Flags().GetString("in")
			binPath, _ := cmd.Flags().GetString("pmtiles-bin")
			converter, err := tiler.NewPMTilesConverter(binPath)
			if err != nil {
				return err
			}

			info, raw, err := converter.Info(cmd.Context(), input)
			if err != nil {
				if raw != "" {
					fmt.Fprintln(cmd.ErrOrStderr(), raw)
				}
				return err
			}

			if info == nil {
				if raw != "" {
					fmt.Fprintln(cmd.OutOrStdout(), raw)
				}
				return nil
			}

			pretty, err := json.MarshalIndent(info, "", "  ")
			if err != nil {
				return fmt.Errorf("format pmtiles info: %w", err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), string(pretty))
			return nil
		},
	}

	cmd.SilenceUsage = true

	cmd.Flags().String("in", "", "PMTiles file to inspect")
	cmd.Flags().String("pmtiles-bin", "", "Override pmtiles binary path")
	cmd.MarkFlagRequired("in")
	return cmd
}

func parseList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	fields := strings.FieldsFunc(value, func(r rune) bool {
		switch r {
		case ',', ';':
			return true
		default:
			return false
		}
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		trimmed := strings.TrimSpace(f)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func newSampleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sample",
		Short: "Generate a sample Parquet file with H3 hexagons for testing",
		Long:  "Generate a sample Parquet file containing H3 hexagons around Boston Common with demo data (score, category).",
		RunE: func(cmd *cobra.Command, args []string) error {
			output, _ := cmd.Flags().GetString("out")
			count, _ := cmd.Flags().GetInt("count")
			resolution, _ := cmd.Flags().GetInt("resolution")
			
			return generateSampleData(output, count, resolution)
		},
	}

	cmd.Flags().StringP("out", "o", "dist/sample.parquet", "Output Parquet file path")
	cmd.Flags().IntP("count", "c", 5, "Number of rings around center point")
	cmd.Flags().IntP("resolution", "r", 8, "H3 resolution (0-15)")

	return cmd
}

// SampleRow represents a row in the sample Parquet file
type SampleRow struct {
	H3       string
	Score    float64
	Category string
}

func generateSampleData(outputPath string, ringCount int, resolution int) error {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate H3 hexagons around Boston Common (42.355, -71.065)
	lat, lng := 42.355, -71.065
	centerHex, err := h3.LatLngToCell(h3.LatLng{Lat: lat, Lng: lng}, resolution)
	if err != nil {
		return fmt.Errorf("failed to convert lat/lng to H3 cell: %w", err)
	}
	
	// Get hexagons in rings around the center
	hexes, err := h3.GridDisk(centerHex, ringCount)
	if err != nil {
		return fmt.Errorf("failed to generate H3 grid disk: %w", err)
	}
	
	// Create sample data rows
	rows := make([]SampleRow, 0, len(hexes))
	for i, hex := range hexes {
		score := float64(i%10) * 0.1
		category := "demo"
		if i%2 == 1 {
			category = "test"
		}
		rows = append(rows, SampleRow{
			H3:       h3.IndexToString(uint64(hex)),
			Score:    score,
			Category: category,
		})
	}

	// Create Parquet file
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %w", err)
	}
	defer file.Close()

	// Create Parquet writer with explicit schema
	schema := parquet.SchemaOf(SampleRow{})
	writer := parquet.NewGenericWriter[SampleRow](file, schema)

	// Write rows to Parquet
	_, err = writer.Write(rows)
	if err != nil {
		return fmt.Errorf("failed to write parquet data: %w", err)
	}

	// Close writer
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}

	fmt.Printf("Generated sample data with %d H3 hexagons at resolution %d\n", len(rows), resolution)
	fmt.Printf("Written to: %s\n", outputPath)

	return nil
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "n/a"
	}
	return d.Truncate(time.Millisecond).String()
}

func formatBytes(size int64) string {
	if size <= 0 {
		return "0 B"
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	f := float64(size)
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
