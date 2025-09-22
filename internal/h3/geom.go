package h3geom

import (
	"fmt"

	"github.com/paulmach/orb"
	h3 "github.com/uber/h3-go/v4"
)

// PolygonFromCell returns the GeoJSON polygon representing the boundary of an H3 cell.
func PolygonFromCell(cell h3.Cell) (orb.Polygon, error) {
	if !cell.IsValid() {
		return nil, fmt.Errorf("invalid H3 cell index")
	}

	boundary, err := cell.Boundary()
	if err != nil {
		return nil, fmt.Errorf("compute boundary: %w", err)
	}
	if len(boundary) == 0 {
		return nil, fmt.Errorf("empty boundary for cell %s", cell.String())
	}

	ring := make(orb.Ring, 0, len(boundary)+1)
	for _, vertex := range boundary {
		ring = append(ring, orb.Point{vertex.Lng, vertex.Lat})
	}

	if !ringClosed(ring) {
		ring = append(ring, ring[0])
	}

	return orb.Polygon{ring}, nil
}

func ringClosed(ring orb.Ring) bool {
	if len(ring) < 2 {
		return false
	}
	first := ring[0]
	last := ring[len(ring)-1]
	return first[0] == last[0] && first[1] == last[1]
}
