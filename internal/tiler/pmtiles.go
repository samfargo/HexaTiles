package tiler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
)

// PMTilesConverter wraps the pmtiles CLI for MBTiles→PMTiles conversion and inspection.
type PMTilesConverter struct {
	Binary string
}

// NewPMTilesConverter resolves the pmtiles binary from PATH or explicit override.
func NewPMTilesConverter(pathOverride string) (*PMTilesConverter, error) {
	candidate := pathOverride
	if candidate == "" {
		candidate = os.Getenv("PMTILES_PATH")
	}
	if candidate == "" {
		candidate = "pmtiles"
	}

	resolved, err := exec.LookPath(candidate)
	if err != nil {
		return nil, fmt.Errorf("pmtiles CLI not found (%s): %w", candidate, err)
	}

	return &PMTilesConverter{Binary: resolved}, nil
}

// Convert invokes `pmtiles convert` and returns combined stdout/stderr output.
func (c *PMTilesConverter) Convert(ctx context.Context, inputMBTiles, outputPMTiles string) (string, error) {
	if c == nil || c.Binary == "" {
		return "", fmt.Errorf("pmtiles converter is not initialised")
	}

	args := []string{"convert", inputMBTiles, outputPMTiles}
	cmd := exec.CommandContext(ctx, c.Binary, args...)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Run(); err != nil {
		return output.String(), fmt.Errorf("pmtiles convert failed: %w", err)
	}

	return output.String(), nil
}

// Info returns metadata from `pmtiles info --json` as a generic map.
func (c *PMTilesConverter) Info(ctx context.Context, pmtilesPath string) (map[string]any, string, error) {
	if c == nil || c.Binary == "" {
		return nil, "", fmt.Errorf("pmtiles converter is not initialised")
	}

	args := []string{"info", "--json", pmtilesPath}
	cmd := exec.CommandContext(ctx, c.Binary, args...)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Run(); err != nil {
		return nil, output.String(), fmt.Errorf("pmtiles info failed: %w", err)
	}

	data := make(map[string]any)
	if err := json.Unmarshal(output.Bytes(), &data); err != nil {
		return nil, output.String(), fmt.Errorf("decode pmtiles info: %w", err)
	}

	return data, output.String(), nil
}
