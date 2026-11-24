package domain

import (
	"strings"
	"testing"
)

func TestValidWorkerID(t *testing.T) {
	for _, id := range []string{"worker-1", "edge.eu_1", "A9", strings.Repeat("w", 255)} {
		if !ValidWorkerID(id) {
			t.Errorf("%q must be valid", id)
		}
	}
	for _, id := range []string{"", "edge 1", " edge", "edge\n", "edge:1", "edge/1", "édge", strings.Repeat("w", 256)} {
		if ValidWorkerID(id) {
			t.Errorf("%q must be rejected", id)
		}
	}
}
