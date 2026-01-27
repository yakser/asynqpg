package db

import (
	"testing"
	"time"
)

func TestNewDuration(t *testing.T) {
	d := NewDuration(5 * time.Second)
	if d.Duration() != 5*time.Second {
		t.Fatalf("expected 5s, got %v", d.Duration())
	}
}

func TestDuration_IsZero_True(t *testing.T) {
	d := NewDuration(0)
	if !d.IsZero() {
		t.Fatal("expected IsZero to return true for zero duration")
	}
}

func TestDuration_IsZero_False(t *testing.T) {
	d := NewDuration(time.Second)
	if d.IsZero() {
		t.Fatal("expected IsZero to return false for non-zero duration")
	}
}

func TestDuration_Value(t *testing.T) {
	d := NewDuration(1500 * time.Millisecond)
	v, err := d.Value()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("expected string value, got %T", v)
	}
	expected := (1500 * time.Millisecond).String()
	if s != expected {
		t.Fatalf("expected %q, got %q", expected, s)
	}
}

func TestDuration_Value_RoundsToMilliseconds(t *testing.T) {
	// 1500100 nanoseconds should be rounded to 1.5ms
	d := NewDuration(1500100 * time.Nanosecond)
	v, err := d.Value()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s := v.(string)
	expected := (1500100 * time.Nanosecond).Round(time.Millisecond).String()
	if s != expected {
		t.Fatalf("expected %q (rounded), got %q", expected, s)
	}
}

func TestDuration_Duration(t *testing.T) {
	original := 3*time.Hour + 15*time.Minute
	d := NewDuration(original)
	if d.Duration() != original {
		t.Fatalf("expected %v, got %v", original, d.Duration())
	}
}

func TestDuration_String(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		expected string
	}{
		{"zero", 0, "0s"},
		{"1 second", time.Second, "1s"},
		{"500ms", 500 * time.Millisecond, "500ms"},
		{"1 minute", time.Minute, "1m0s"},
		{"1 hour", time.Hour, "1h0m0s"},
		{"complex", 2*time.Hour + 30*time.Minute + 15*time.Second, "2h30m15s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDuration(tt.duration)
			if d.String() != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, d.String())
			}
		})
	}
}
