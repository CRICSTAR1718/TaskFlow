// Package domain defines the core TaskFlow types shared by every service.
package domain

import (
	"fmt"
	"strings"
)

// Priority orders jobs in the queue; a higher value runs first.
type Priority int

const (
	PriorityLow    Priority = 1
	PriorityNormal Priority = 2
	PriorityHigh   Priority = 3
)

// Valid reports whether p is one of the defined priorities.
func (p Priority) Valid() bool { return p >= PriorityLow && p <= PriorityHigh }

// String returns "high", "normal" or "low".
func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	}
	return fmt.Sprintf("priority(%d)", int(p))
}

// ParsePriority converts "high", "normal" or "low" (case-insensitive) to a Priority.
func ParsePriority(s string) (Priority, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "high":
		return PriorityHigh, nil
	case "normal":
		return PriorityNormal, nil
	case "low":
		return PriorityLow, nil
	}
	return 0, fmt.Errorf("unknown priority %q (want high, normal or low)", s)
}

// MarshalText encodes the priority as its name; it is used by encoding/json.
func (p Priority) MarshalText() ([]byte, error) {
	if !p.Valid() {
		return nil, fmt.Errorf("unknown priority %d", int(p))
	}
	return []byte(p.String()), nil
}

// UnmarshalText decodes a priority name; it is used by encoding/json.
func (p *Priority) UnmarshalText(b []byte) error {
	v, err := ParsePriority(string(b))
	if err != nil {
		return err
	}
	*p = v
	return nil
}
