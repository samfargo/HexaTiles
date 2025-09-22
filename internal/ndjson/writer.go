package ndjson

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

// Feature represents a GeoJSON feature destined for NDJSON output.
type Feature struct {
	ID         string
	Geometry   orb.Geometry
	Properties map[string]any
	BBox       *orb.Bound
}

// Writer streams GeoJSON features as newline-delimited JSON.
type Writer struct {
	mu           sync.Mutex
	file         *os.File
	encoder      *json.Encoder
	path         string
	count        int64
	bytesWritten int64
}

// NewWriter creates a writer that outputs to the specified path, creating parent directories as needed.
func NewWriter(path string) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create NDJSON directory: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create NDJSON file: %w", err)
	}

	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)

	return &Writer{file: f, encoder: enc, path: path}, nil
}

// Close flushes and closes the underlying file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	err := w.file.Close()
	w.file = nil
	return err
}

// Path returns the destination file path.
func (w *Writer) Path() string {
	return w.path
}

// Count returns how many features have been written.
func (w *Writer) Count() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count
}

// Bytes returns the total bytes written so far (best-effort).
func (w *Writer) Bytes() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytesWritten
}

// WriteFeature appends a feature as a single NDJSON line.
func (w *Writer) WriteFeature(feature Feature) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.encoder == nil {
		return fmt.Errorf("writer closed")
	}

	payload := geojson.NewFeature(feature.Geometry)
	if feature.Properties != nil {
		payload.Properties = feature.Properties
	}
	if feature.BBox != nil {
		bbox := geojson.NewBBox(*feature.BBox)
		payload.BBox = bbox
	}
	if feature.ID != "" {
		payload.ID = feature.ID
	}

	if err := w.encoder.Encode(payload); err != nil {
		return fmt.Errorf("encode feature: %w", err)
	}

	w.count++

	if w.file != nil {
		if info, err := w.file.Stat(); err == nil {
			w.bytesWritten = info.Size()
		}
	}

	return nil
}

// MarshalFeature returns the JSON encoding of a feature suitable for diagnostics or size estimation.
func MarshalFeature(feature Feature) ([]byte, error) {
	payload := geojson.NewFeature(feature.Geometry)
	if feature.Properties != nil {
		payload.Properties = feature.Properties
	}
	if feature.BBox != nil {
		bbox := geojson.NewBBox(*feature.BBox)
		payload.BBox = bbox
	}
	if feature.ID != "" {
		payload.ID = feature.ID
	}
	return json.Marshal(payload)
}
