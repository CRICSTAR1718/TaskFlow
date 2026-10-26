package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

func init() {
	Register("echo", HandlerFunc(echo))
	Register("sleep", HandlerFunc(sleep))
	Register("fail", HandlerFunc(fail))
}

// echo logs its payload.
func echo(ctx context.Context, payload []byte) error {
	slog.InfoContext(ctx, "echo", slog.String("payload", string(payload)))
	return nil
}

// sleep waits for {"seconds": N} or until ctx ends (useful for timeout tests).
func sleep(ctx context.Context, payload []byte) error {
	var p struct {
		Seconds float64 `json:"seconds"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("decode sleep payload: %w", err)
	}
	select {
	case <-time.After(time.Duration(p.Seconds * float64(time.Second))):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// fail always errors (useful for retry/dead-letter tests).
func fail(_ context.Context, _ []byte) error {
	return errors.New("intentional failure")
}
