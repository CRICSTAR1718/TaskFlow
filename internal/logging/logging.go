// Package logging builds the slog JSON logger shared by all services.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON slog.Logger writing to stdout at the given level
// (debug, info, warn, error). Unknown levels fall back to info.
func New(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(strings.ToLower(level))); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
