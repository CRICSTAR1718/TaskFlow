package domain

import (
	"encoding/json"
	"testing"
)

func TestPriorityJSON(t *testing.T) {
	tests := []struct {
		in   string
		want Priority
		ok   bool
	}{
		{`"high"`, PriorityHigh, true},
		{`"normal"`, PriorityNormal, true},
		{`"low"`, PriorityLow, true},
		{`"urgent"`, 0, false},
	}
	for _, tt := range tests {
		var p Priority
		err := json.Unmarshal([]byte(tt.in), &p)
		if (err == nil) != tt.ok || (tt.ok && p != tt.want) {
			t.Errorf("%s: got %v, err %v", tt.in, p, err)
		}
	}
}

func TestStatusTerminal(t *testing.T) {
	if !StatusDead.Terminal() || StatusRunning.Terminal() {
		t.Error("unexpected terminal result")
	}
}
