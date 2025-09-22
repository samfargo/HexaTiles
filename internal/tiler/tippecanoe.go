package tiler

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// TippecanoeOptions configures a tippecanoe invocation.
type TippecanoeOptions struct {
	MinZoom   int
	MaxZoom   int
	Simplify  bool
	SortBy    string
	Threads   int
	LayerName string
	Metadata  map[string]string
    Attributes []string
}

// TippecanoeRunner wraps calls to the tippecanoe CLI.
type TippecanoeRunner struct {
	Binary string
}

// NewTippecanoeRunner resolves the tippecanoe binary from PATH or an explicit override.
func NewTippecanoeRunner(pathOverride string) (*TippecanoeRunner, error) {
	candidate := pathOverride
	if candidate == "" {
		candidate = os.Getenv("TIPPECANOE_PATH")
	}
	if candidate == "" {
		candidate = "tippecanoe"
	}

    resolved, err := exec.LookPath(candidate)
    if err != nil {
        return nil, fmt.Errorf("tippecanoe CLI not found. Install via: macOS 'brew install tippecanoe', Ubuntu 'sudo apt install tippecanoe', or see https://github.com/felt/tippecanoe")
    }

	return &TippecanoeRunner{Binary: resolved}, nil
}

// Run executes tippecanoe with deterministic defaults. It returns combined stdout/stderr output and the exact argument list.
func (r *TippecanoeRunner) Run(ctx context.Context, inputNDJSON, outputMBTiles string, opts TippecanoeOptions) (string, []string, error) {
	if r == nil || r.Binary == "" {
		return "", nil, fmt.Errorf("tippecanoe runner is not initialised")
	}

	layer := opts.LayerName
	if layer == "" {
		layer = "h3"
	}

	sortBy := opts.SortBy
	if sortBy == "" {
		sortBy = "h3"
	}

	args := []string{
		"-o", outputMBTiles,
		"--force",
		"--layer", layer,
		"--drop-densest-as-needed",
		"--extend-zooms-if-still-dropping",
		"--coalesce-densest-as-needed",
		"--no-feature-limit",
		"--no-tile-size-limit",
		"--sort-by", sortBy,
	}

	if !opts.Simplify {
		args = append(args, "--no-line-simplification")
	}

	if opts.MinZoom >= 0 {
		args = append(args, "--minimum-zoom", strconv.Itoa(opts.MinZoom))
	}
	if opts.MaxZoom >= 0 {
		args = append(args, "--maximum-zoom", strconv.Itoa(opts.MaxZoom))
	}

    if len(opts.Attributes) > 0 {
        args = append(args, "--attributes", strings.Join(opts.Attributes, ","))
    }

	for key, value := range opts.Metadata {
		if strings.TrimSpace(value) == "" {
			continue
		}
		switch strings.ToLower(key) {
		case "name":
			args = append(args, "--name", value)
		case "description":
			args = append(args, "--description", value)
		case "attribution":
			args = append(args, "--attribution", value)
		case "version":
			args = append(args, "--version", value)
		}
	}

	args = append(args, inputNDJSON)

	cmd := exec.CommandContext(ctx, r.Binary, args...)

	env := os.Environ()
	if opts.Threads > 0 {
		env = append(env, fmt.Sprintf("TIPPECANOE_MAX_THREADS=%d", opts.Threads))
	}
	cmd.Env = env

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Run(); err != nil {
		return output.String(), cmd.Args, fmt.Errorf("tippecanoe failed: %w", err)
	}

	return output.String(), cmd.Args, nil
}
