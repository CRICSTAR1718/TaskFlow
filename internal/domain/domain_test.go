package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestParsePriority(t *testing.T) {
	tests := []struct {
		in      string
		want    Priority
		wantErr bool
	}{
		{"high", PriorityHigh, false},
		{"normal", PriorityNormal, false},
		{"low", PriorityLow, false},
		{" HIGH ", PriorityHigh, false},
		{"Normal", PriorityNormal, false},
		{"", 0, true},
		{"urgent", 0, true},
		{"3", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParsePriority(tt.in)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("ParsePriority(%q) = %v, %v", tt.in, got, err)
			}
		})
	}
}

func TestPriorityJSON(t *testing.T) {
	type wrap struct {
		P Priority `json:"p"`
	}
	tests := []struct {
		name     string
		in       string
		want     Priority
		wantErr  bool
		wantJSON string
	}{
		{"high", `{"p":"high"}`, PriorityHigh, false, `{"p":"high"}`},
		{"low", `{"p":"low"}`, PriorityLow, false, `{"p":"low"}`},
		{"unknown", `{"p":"urgent"}`, 0, true, ""},
		{"number rejected", `{"p":3}`, 0, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w wrap
			err := json.Unmarshal([]byte(tt.in), &w)
			if (err != nil) != tt.wantErr {
				t.Fatalf("unmarshal err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if w.P != tt.want {
				t.Errorf("got %v want %v", w.P, tt.want)
			}
			out, err := json.Marshal(w)
			if err != nil || string(out) != tt.wantJSON {
				t.Errorf("marshal = %s, %v", out, err)
			}
		})
	}
	if _, err := json.Marshal(wrap{P: 0}); err == nil {
		t.Error("marshalling zero priority should fail")
	}
}

func TestCanTransition(t *testing.T) {
	tests := []struct {
		from, to Status
		want     bool
	}{
		{StatusPending, StatusQueued, true},
		{StatusPending, StatusCancelled, true},
		{StatusPending, StatusRunning, false},
		{StatusQueued, StatusRunning, true},
		{StatusRunning, StatusSucceeded, true},
		{StatusRunning, StatusRetrying, true},
		{StatusRunning, StatusDead, true},
		{StatusRunning, StatusQueued, true}, // dead-worker recovery
		{StatusRetrying, StatusQueued, true},
		{StatusRetrying, StatusDead, true},
		{StatusRetrying, StatusRunning, false},
		{StatusSucceeded, StatusPending, true}, // cron reschedule
		{StatusSucceeded, StatusRunning, false},
		{StatusDead, StatusQueued, false},
		{StatusCancelled, StatusPending, false},
		{StatusPending, StatusPending, false},
		{Status("BOGUS"), StatusQueued, false},
		{StatusPending, Status("BOGUS"), false},
	}
	for _, tt := range tests {
		t.Run(string(tt.from)+"->"+string(tt.to), func(t *testing.T) {
			if got := CanTransition(tt.from, tt.to); got != tt.want {
				t.Errorf("got %v want %v", got, tt.want)
			}
		})
	}
}

func TestStatusTerminal(t *testing.T) {
	tests := []struct {
		s    Status
		want bool
	}{
		{StatusDead, true},
		{StatusCancelled, true},
		{StatusSucceeded, false},
		{StatusRunning, false},
		{Status("BOGUS"), false},
	}
	for _, tt := range tests {
		if got := tt.s.Terminal(); got != tt.want {
			t.Errorf("%s.Terminal() = %v", tt.s, got)
		}
	}
}

func strp(s string) *string { return &s }

func TestJobValidate(t *testing.T) {
	job := func(mod func(*Job)) Job {
		j := Job{Type: "echo", Priority: PriorityNormal, TimeoutSeconds: 60, MaxRetries: 3}
		if mod != nil {
			mod(&j)
		}
		return j
	}
	bigPayload := func(n int) []byte { return []byte(`"` + strings.Repeat("a", n-2) + `"`) }

	tests := []struct {
		name    string
		job     Job
		wantErr string // substring of the error; "" means valid
	}{
		{"valid", job(nil), ""},
		{"valid cron", job(func(j *Job) { j.CronExpr = strp("*/5 * * * *") }), ""},
		{"valid payload", job(func(j *Job) { j.Payload = []byte(`{"a":1}`) }), ""},
		{"missing type", job(func(j *Job) { j.Type = "" }), "type is required"},
		{"zero priority", job(func(j *Job) { j.Priority = 0 }), "unknown priority 0"},
		{"priority too high", job(func(j *Job) { j.Priority = 4 }), "unknown priority 4"},
		{"timeout zero", job(func(j *Job) { j.TimeoutSeconds = 0 }), "timeout_seconds 0"},
		{"timeout min", job(func(j *Job) { j.TimeoutSeconds = 1 }), ""},
		{"timeout max", job(func(j *Job) { j.TimeoutSeconds = 3600 }), ""},
		{"timeout over", job(func(j *Job) { j.TimeoutSeconds = 3601 }), "timeout_seconds 3601"},
		{"retries negative", job(func(j *Job) { j.MaxRetries = -1 }), "max_retries -1"},
		{"retries zero", job(func(j *Job) { j.MaxRetries = 0 }), ""},
		{"retries max", job(func(j *Job) { j.MaxRetries = 10 }), ""},
		{"retries over", job(func(j *Job) { j.MaxRetries = 11 }), "max_retries 11"},
		{"bad cron", job(func(j *Job) { j.CronExpr = strp("not a cron") }), "cron_expr"},
		{"cron too many fields", job(func(j *Job) { j.CronExpr = strp("* * * * * * *") }), "cron_expr"},
		{"payload at limit", job(func(j *Job) { j.Payload = bigPayload(MaxPayloadBytes) }), ""},
		{"payload over limit", job(func(j *Job) { j.Payload = bigPayload(MaxPayloadBytes + 1) }), "payload is"},
		{"payload not json", job(func(j *Job) { j.Payload = []byte(`{oops`) }), "not valid JSON"},
		{"multiple errors", job(func(j *Job) { j.Type = ""; j.TimeoutSeconds = 0 }), "timeout_seconds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
			if !errors.Is(err, ErrInvalidJob) {
				t.Error("error does not wrap ErrInvalidJob")
			}
		})
	}
}
