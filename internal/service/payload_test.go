package service

import (
	"strings"
	"testing"
)

func TestPayloadExpansionAdmission(t *testing.T) {
	for _, test := range []struct {
		payload string
		ok      bool
	}{
		{`{"value":1e100000}`, true},
		{`{"value":-2.3e-5,"quoted":"1e9999999999"}`, true},
		{`[1e9999999999]`, false},
		{`[1e-100000]`, false},
		{"[" + strings.Repeat("1e100000,", 10) + "1e100000]", false},
	} {
		if got := payloadExpansionFits([]byte(test.payload)); got != test.ok {
			t.Errorf("payload expansion result=%t want=%t", got, test.ok)
		}
	}
}
