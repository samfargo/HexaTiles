package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"github.com/spf13/cobra"
)

func newPreviewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preview",
		Short: "Preview PMTiles locally",
		RunE: func(cmd *cobra.Command, args []string) error {
			pmtiles, _ := cmd.Flags().GetString("pmtiles")
			port, _ := cmd.Flags().GetInt("port")
			autoOpen, _ := cmd.Flags().GetBool("open")
			return startPreview(cmd.Context(), pmtiles, port, autoOpen, cmd.OutOrStdout())
		},
	}

	cmd.SilenceUsage = true

	cmd.Flags().String("pmtiles", "", "PMTiles file to preview")
	cmd.Flags().Int("port", 0, "Port for the preview server (0 selects a random port)")
	cmd.Flags().Bool("open", false, "Open the preview in your default browser")
	cmd.MarkFlagRequired("pmtiles")
	return cmd
}

func startPreview(parentCtx context.Context, pmtilesPath string, port int, autoOpen bool, out io.Writer) error {
	absPath, err := filepath.Abs(pmtilesPath)
	if err != nil {
		return fmt.Errorf("resolve pmtiles path: %w", err)
	}
	if _, err := os.Stat(absPath); err != nil {
		return fmt.Errorf("pmtiles file: %w", err)
	}

	ctx, stop := signal.NotifyContext(parentCtx, os.Interrupt)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if err := previewTemplate.Execute(w, map[string]string{
			"TilesPath": "/tiles.pmtiles",
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/tiles.pmtiles", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, absPath)
	})

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	server := &http.Server{Handler: mux}

	errCh := make(chan error, 1)
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
		close(errCh)
	}()

	url := fmt.Sprintf("http://%s", listener.Addr().String())
	fmt.Fprintf(out, "Preview available at %s\n", url)

	if autoOpen {
		if err := openBrowser(url); err != nil {
			fmt.Fprintf(out, "(failed to open browser: %v)\n", err)
		}
	}

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	case serveErr := <-errCh:
		if serveErr != nil {
			return serveErr
		}
	}

	return nil
}

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", url).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	default:
		return exec.Command("xdg-open", url).Start()
	}
}

var previewTemplate = template.Must(template.New("preview").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>HexaTiles Preview</title>
<link href="https://unpkg.com/maplibre-gl@2.4.0/dist/maplibre-gl.css" rel="stylesheet" />
<script src="https://unpkg.com/maplibre-gl@2.4.0/dist/maplibre-gl.js"></script>
<script src="https://unpkg.com/pmtiles@2.10.0/dist/pmtiles.js"></script>
<style>
  html, body { height: 100%; margin: 0; }
  #map { height: 100%; width: 100%; }
</style>
</head>
<body>
<div id="map"></div>
<script>
(async function() {
  const protocol = new pmtiles.Protocol();
  maplibregl.addProtocol("pmtiles", protocol.tile);

  const tilesUrl = window.location.origin + "{{.TilesPath}}";
  const pmtiles = new pmtiles.PMTiles(tilesUrl);
  protocol.add(pmtiles);

  const map = new maplibregl.Map({
    container: "map",
    style: {
      version: 8,
      sources: {
        h3: {
          type: "vector",
          url: "pmtiles://" + tilesUrl
        }
      },
      layers: [
        {
          id: "h3-fill",
          type: "fill",
          source: "h3",
          "source-layer": "h3",
          paint: {
            "fill-color": "#277da1",
            "fill-opacity": 0.65,
            "fill-outline-color": "#1d3557"
          }
        }
      ]
    },
    center: [0, 0],
    zoom: 2
  });

  map.addControl(new maplibregl.NavigationControl());

  try {
    const metadata = await pmtiles.getMetadata();
    if (metadata && metadata.center && metadata.center.length >= 3) {
      map.jumpTo({ center: [metadata.center[0], metadata.center[1]], zoom: metadata.center[2] });
    } else if (metadata && metadata.bounds && metadata.bounds.length >= 4) {
      map.fitBounds([[metadata.bounds[0], metadata.bounds[1]], [metadata.bounds[2], metadata.bounds[3]]], { padding: 20 });
    }
  } catch (err) {
    console.warn("Unable to load PMTiles metadata", err);
  }
})();
</script>
</body>
</html>`))
