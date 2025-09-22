package main

import (
	"fmt"
	"io"
	"sort"

	"github.com/spf13/cobra"

	parquetreader "github.com/hexatiles/hexatiles/internal/parquet"
)

type propertyInfo struct {
	Type    string
	Example string
	Count   int
	Mixed   bool
}

func newSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Inspect the schema of a Parquet file",
		RunE: func(cmd *cobra.Command, args []string) error {
			input, _ := cmd.Flags().GetString("in")
			sampleLimit, _ := cmd.Flags().GetInt("sample")
			reader, err := parquetreader.NewReader(input, parquetreader.ReaderOptions{BatchSize: sampleLimit, Parallel: 1})
			if err != nil {
				return fmt.Errorf("open parquet reader: %w", err)
			}
			defer reader.Close()

			totalRows := reader.TotalRows()

			props := make(map[string]*propertyInfo)
			resHistogram := make(map[int]int64)
			invalidSamples := make([]string, 0, 5)
			invalidRows := int64(0)
			sampled := 0

			for sampled < sampleLimit {
				row, err := reader.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("read parquet row: %w", err)
				}

				sampled++

				if row.Err != nil {
					invalidRows++
					if len(invalidSamples) < cap(invalidSamples) {
						invalidSamples = append(invalidSamples, row.Err.Error())
					}
					continue
				}

				resHistogram[row.Resolution]++

				for key, value := range row.Properties {
					info := props[key]
					valueType := detectType(value)
					if info == nil {
						info = &propertyInfo{Type: valueType}
						props[key] = info
					}
					if info.Type != valueType {
						info.Mixed = true
					}
					info.Count++
					if info.Example == "" && value != nil {
						info.Example = formatExample(value)
					}
				}
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", input)
			fmt.Fprintf(cmd.OutOrStdout(), "  total rows: %d\n", totalRows)
			fmt.Fprintf(cmd.OutOrStdout(), "  sampled rows: %d (limit %d)\n", sampled, sampleLimit)
			fmt.Fprintf(cmd.OutOrStdout(), "  invalid rows: %d\n", invalidRows)
			if len(invalidSamples) > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "  invalid samples:\n")
				for _, sample := range invalidSamples {
					fmt.Fprintf(cmd.OutOrStdout(), "    %s\n", sample)
				}
				if invalidRows > int64(len(invalidSamples)) {
					fmt.Fprintf(cmd.OutOrStdout(), "    ... %d more\n", invalidRows-int64(len(invalidSamples)))
				}
			}

			if len(props) > 0 {
				names := make([]string, 0, len(props))
				for name := range props {
					names = append(names, name)
				}
				sort.Strings(names)

				fmt.Fprintf(cmd.OutOrStdout(), "  properties:\n")
				for _, name := range names {
					info := props[name]
					typ := info.Type
					if info.Mixed {
						typ = typ + " (mixed)"
					}
					example := info.Example
					if example == "" {
						example = "n/a"
					}
					fmt.Fprintf(cmd.OutOrStdout(), "    %s: %s (%d samples, example %s)\n", name, typ, info.Count, example)
				}
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "  properties: none\n")
			}

			if len(resHistogram) > 0 {
				keys := make([]int, 0, len(resHistogram))
				for res := range resHistogram {
					keys = append(keys, res)
				}
				sort.Ints(keys)
				fmt.Fprintf(cmd.OutOrStdout(), "  resolutions:\n")
				for _, res := range keys {
					fmt.Fprintf(cmd.OutOrStdout(), "    r%d: %d\n", res, resHistogram[res])
				}
			}

			return nil
		},
	}

	cmd.SilenceUsage = true

	cmd.Flags().String("in", "", "Input Parquet file")
	cmd.Flags().Int("sample", 5000, "Number of rows to sample for schema detection")
	cmd.MarkFlagRequired("in")
	return cmd
}

func detectType(value any) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "bool"
	case int, int32, int64, uint, uint32, uint64:
		return "int"
	case float32, float64:
		return "float"
	default:
		return fmt.Sprintf("%T", v)
	}
}

func formatExample(value any) string {
	switch v := value.(type) {
	case string:
		if len(v) > 48 {
			return fmt.Sprintf("%q", v[:45]+"...")
		}
		return fmt.Sprintf("%q", v)
	default:
		s := fmt.Sprintf("%v", value)
		if len(s) > 48 {
			return s[:45] + "..."
		}
		return s
	}
}
