package parquet

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	h3 "github.com/uber/h3-go/v4"
	"github.com/parquet-go/parquet-go"
)

// ReaderOptions controls how Parquet rows are streamed.
type ReaderOptions struct {
	// BatchSize controls how many rows are fetched per request.
	BatchSize int
	// Parallel controls the number of goroutines spawned by parquet-go when decoding row groups.
	Parallel int
}

// Row represents a fully decoded Parquet row that contains an H3 index and optional properties.
type Row struct {
	RowNumber  int64
	Cell       h3.Cell
	CellString string
	Resolution int
	Properties map[string]any
	Err        error
}

// Reader streams H3 rows from a Parquet file.
type Reader struct {
	opts      ReaderOptions
	filePath  string
	reader    *parquet.Reader
	totalRows int64

	mu     sync.Mutex
	buffer []*Row
	cursor int
	read   int64
}

// NewReader opens a Parquet file and prepares it for streaming rows.
func NewReader(path string, opts ReaderOptions) (*Reader, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 4096
	}
	if opts.Parallel <= 0 {
		opts.Parallel = runtime.NumCPU()
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	reader := parquet.NewReader(file)

	// Get total rows from metadata
	total := reader.NumRows()

	r := &Reader{
		opts:      opts,
		filePath:  filepath.Clean(path),
		reader:    reader,
		totalRows: total,
	}

	return r, nil
}

// Close releases Parquet reader resources.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.reader != nil {
		r.reader.Close()
		r.reader = nil
	}
	r.buffer = nil
	return nil
}

// ErrNoH3Column is returned when the Parquet file does not contain a recognizable H3 column.
var ErrNoH3Column = errors.New("parquet file missing required H3 column")

// Next returns the next decoded H3 row. It returns io.EOF when all rows are consumed.
func (r *Reader) Next() (*Row, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.reader == nil {
		return nil, fmt.Errorf("reader closed")
	}

	if r.cursor >= len(r.buffer) {
		if err := r.fillBuffer(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, err
		}
	}

	if r.cursor >= len(r.buffer) {
		return nil, io.EOF
	}

	row := r.buffer[r.cursor]
	r.cursor++
	return row, nil
}

func (r *Reader) fillBuffer() error {
	if r.read >= r.totalRows {
		return io.EOF
	}

	remaining := int(r.totalRows - r.read)
	toRead := r.opts.BatchSize
	if toRead > remaining {
		toRead = remaining
	}

	// Read rows using the new parquet library
	rows := make([]parquet.Row, toRead)
	n, err := r.reader.ReadRows(rows)
	if err != nil && err != io.EOF {
		return fmt.Errorf("read parquet rows: %w", err)
	}

	if n == 0 {
		return io.EOF
	}

	r.buffer = r.buffer[:0]
	r.cursor = 0

	// Get schema to understand column structure
	schema := r.reader.Schema()
	
	for i := 0; i < n; i++ {
		rowNumber := r.read + 1
		
		// Convert parquet.Row to map[string]any
		rowMap := make(map[string]any)
		for j, value := range rows[i] {
			if j < len(schema.Fields()) {
				field := schema.Fields()[j]
				rowMap[field.Name()] = value
			}
		}

		props := extractProperties(rowMap)
		cell, cellString, cellErr := extractCell(rowMap)
		
		if cellErr != nil {
			r.buffer = append(r.buffer, &Row{
				RowNumber:  rowNumber,
				CellString: cellString,
				Resolution: -1,
				Properties: props,
				Err:        fmt.Errorf("row %d: %w", rowNumber, cellErr),
			})
			r.read++
			continue
		}

		if cell == 0 {
			r.buffer = append(r.buffer, &Row{
				RowNumber:  rowNumber,
				CellString: cellString,
				Resolution: -1,
				Properties: props,
				Err:        fmt.Errorf("row %d: %w", rowNumber, ErrNoH3Column),
			})
			r.read++
			continue
		}

		if cellString == "" {
			cellString = h3.IndexToString(uint64(cell))
		}

		r.buffer = append(r.buffer, &Row{
			RowNumber:  rowNumber,
			Cell:       cell,
			CellString: cellString,
			Resolution: cell.Resolution(),
			Properties: props,
		})
		r.read++
	}

	return nil
}

// TotalRows returns the number of rows reported by the Parquet footer.
func (r *Reader) TotalRows() int64 {
	return r.totalRows
}

var possibleH3Names = []string{"h3", "h3_id", "h3index", "h3_index", "h3id", "cell", "cell_id"}

func extractCell(row map[string]any) (h3.Cell, string, error) {
	if len(row) == 0 {
		return 0, "", nil
	}

	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if isH3Column(key) {
			idx, cellString, err := parseCell(row[key])
			if err != nil {
				return 0, cellString, fmt.Errorf("column %s: %w", key, err)
			}
			if idx != 0 {
				if !idx.IsValid() {
					if cellString == "" {
						cellString = h3.IndexToString(uint64(idx))
					}
					return 0, cellString, fmt.Errorf("column %s: invalid H3 cell", key)
				}
				if cellString == "" {
					cellString = h3.IndexToString(uint64(idx))
				}
				return idx, cellString, nil
			}
		}
	}

	return 0, "", ErrNoH3Column
}

func isH3Column(name string) bool {
	lname := strings.ToLower(name)
	for _, candidate := range possibleH3Names {
		if lname == candidate {
			return true
		}
	}
	return false
}

func parseCell(value any) (h3.Cell, string, error) {
	switch v := value.(type) {
	case nil:
		return 0, "", nil
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, "", nil
		}
		cell, err := stringToCell(trimmed)
		if err != nil {
			return 0, trimmed, err
		}
		return cell, h3.IndexToString(uint64(cell)), nil
	case []byte:
		trimmed := strings.TrimSpace(string(v))
		if trimmed == "" {
			return 0, "", nil
		}
		cell, err := stringToCell(trimmed)
		if err != nil {
			return 0, trimmed, err
		}
		return cell, h3.IndexToString(uint64(cell)), nil
	case fmt.Stringer:
		trimmed := strings.TrimSpace(v.String())
		if trimmed == "" {
			return 0, "", nil
		}
		cell, err := stringToCell(trimmed)
		if err != nil {
			return 0, trimmed, err
		}
		return cell, h3.IndexToString(uint64(cell)), nil
	case int:
		if v < 0 {
			return 0, fmt.Sprint(v), fmt.Errorf("negative integer %d", v)
		}
		cell := h3.Cell(uint64(v))
		return cell, h3.IndexToString(uint64(cell)), nil
	case int32:
		if v < 0 {
			return 0, fmt.Sprint(v), fmt.Errorf("negative integer %d", v)
		}
		cell := h3.Cell(uint64(v))
		return cell, h3.IndexToString(uint64(cell)), nil
	case int64:
		if v < 0 {
			return 0, fmt.Sprint(v), fmt.Errorf("negative integer %d", v)
		}
		cell := h3.Cell(uint64(v))
		return cell, h3.IndexToString(uint64(cell)), nil
	case uint:
		cell := h3.Cell(uint64(v))
		return cell, h3.IndexToString(uint64(cell)), nil
	case uint32:
		cell := h3.Cell(uint64(v))
		return cell, h3.IndexToString(uint64(cell)), nil
	case uint64:
		cell := h3.Cell(v)
		return cell, h3.IndexToString(uint64(cell)), nil
	case float32:
		if v < 0 {
			return 0, fmt.Sprint(v), fmt.Errorf("negative float %f", v)
		}
		cell := h3.Cell(uint64(v))
		return cell, h3.IndexToString(uint64(cell)), nil
	case float64:
		if v < 0 {
			return 0, fmt.Sprint(v), fmt.Errorf("negative float %f", v)
		}
		cell := h3.Cell(uint64(v))
		return cell, h3.IndexToString(uint64(cell)), nil
	default:
		s := strings.TrimSpace(fmt.Sprint(v))
		if s == "" {
			return 0, "", nil
		}
		cell, err := stringToCell(s)
		if err != nil {
			return 0, s, err
		}
		return cell, h3.IndexToString(uint64(cell)), nil
	}
}

func stringToCell(s string) (h3.Cell, error) {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		s = s[2:]
	}
	value, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		// User may have passed a decimal value; fall back once.
		value, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse H3 string %q: %w", s, err)
		}
	}
	return h3.Cell(value), nil
}

func extractProperties(row map[string]any) map[string]any {
	props := make(map[string]any, len(row))
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if isH3Column(key) {
			continue
		}
		props[key] = normalizeValue(row[key])
	}

	return props
}

func normalizeValue(v any) any {
	switch val := v.(type) {
	case []byte:
		return string(val)
	default:
		return val
	}
}
