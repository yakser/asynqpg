package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewDuration(t *testing.T) {
	t.Parallel()

	d := NewDuration(5 * time.Second)
	require.Equal(t, 5*time.Second, d.Duration())
}

func TestDuration_IsZero_True(t *testing.T) {
	t.Parallel()

	d := NewDuration(0)
	require.True(t, d.IsZero())
}

func TestDuration_IsZero_False(t *testing.T) {
	t.Parallel()

	d := NewDuration(time.Second)
	require.False(t, d.IsZero())
}

func TestDuration_Value(t *testing.T) {
	t.Parallel()

	d := NewDuration(1500 * time.Millisecond)
	v, err := d.Value()
	require.NoError(t, err)
	s, ok := v.(string)
	require.True(t, ok, "expected string value, got %T", v)

	want := (1500 * time.Millisecond).String()
	require.Equal(t, want, s)
}

func TestDuration_Value_RoundsToMilliseconds(t *testing.T) {
	t.Parallel()

	// 1500100 nanoseconds should be rounded to 1.5ms
	d := NewDuration(1500100 * time.Nanosecond)
	v, err := d.Value()
	require.NoError(t, err)

	s := v.(string)
	want := (1500100 * time.Nanosecond).Round(time.Millisecond).String()
	require.Equal(t, want, s)
}

func TestDuration_Duration(t *testing.T) {
	t.Parallel()

	original := 3*time.Hour + 15*time.Minute
	d := NewDuration(original)
	require.Equal(t, original, d.Duration())
}

func TestDuration_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		duration time.Duration
		want     string
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
			t.Parallel()

			d := NewDuration(tt.duration)
			require.Equal(t, tt.want, d.String())
		})
	}
}
